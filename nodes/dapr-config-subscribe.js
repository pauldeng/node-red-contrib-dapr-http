'use strict';

const { context, propagation, trace, SpanKind } = require('@opentelemetry/api');

const {
  buildConfigurationCallbackPath,
  getConfiguration,
  subscribeConfiguration,
  unsubscribeConfiguration,
} = require('../lib/configuration-client');
const { ErrorCodes } = require('../lib/errors');
const { prepareConfigurationSubscribe } = require('../lib/configuration-messages');
const { decodeBody } = require('../lib/invoke-client');
const { getTracer, endSpan } = require('../lib/telemetry');

const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;
// Bounded so close() never hangs on a slow or unreachable sidecar.
const UNSUBSCRIBE_TIMEOUT_MS = 3000;
const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

async function tracedClientCall(operation, storeName, call) {
  const span = getTracer().startSpan(
    `configuration ${operation} ${storeName}`,
    {
      kind: SpanKind.CLIENT,
      attributes: {
        'dapr.configuration.operation': operation,
        'dapr.configuration.store': storeName,
      },
    },
    context.active()
  );
  try {
    const result = await call();
    endSpan(span);
    return result;
  } catch (err) {
    endSpan(span, err);
    throw err;
  }
}

// A no-op extract/span (tracing disabled) costs nothing and needs no branch of
// its own here. carrier is the callback POST's own headers -- daprd's own
// call is unlikely to carry a traceparent today, but extracting is harmless
// either way and matches dapr-subscribe's identical pattern for pub/sub.
function startConsumerSpan(storeName, key, carrier) {
  const parentContext = propagation.extract(context.active(), carrier);
  const span = getTracer().startSpan(
    `${storeName} configuration change`,
    {
      kind: SpanKind.CONSUMER,
      attributes: {
        'dapr.configuration.store': storeName,
        'dapr.configuration.key': key,
      },
    },
    parentContext
  );
  return { span, context: trace.setSpan(parentContext, span) };
}

module.exports = function registerDaprConfigSubscribe(RED) {
  function DaprConfigSubscribeNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (
      !connection?.options ||
      typeof connection.registerInternalRoute !== 'function' ||
      typeof connection.isSidecarHealthy !== 'function' ||
      typeof connection.onSidecarHealth !== 'function'
    ) {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      return;
    }

    let request;
    try {
      request = prepareConfigurationSubscribe(config);
    } catch (err) {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(err.message);
      return;
    }

    let subscriptionId = null;
    let closed = false;
    let backoff = INITIAL_BACKOFF_MS;
    let retryTimer = null;
    let attemptController = null;
    let acceptingCallbacks = false;

    const STATUS_BY_STATE = {
      connecting: { fill: 'blue', shape: 'dot', text: 'connecting' },
      subscribed: { fill: 'green', shape: 'dot', text: 'subscribed' },
      retrying: { fill: 'yellow', shape: 'ring', text: 'retrying' },
      failed: { fill: 'red', shape: 'ring', text: 'failed' },
    };
    const setState = (state, text) => {
      const style = STATUS_BY_STATE[state];
      node.status(text ? { ...style, text } : style);
    };
    setState('connecting');

    // Shared by every registered key route. A callback whose id does not
    // match the CURRENT subscription is a stale push from a superseded
    // subscription that raced with a resubscribe -- daprd already got a 2xx
    // for it, so it is dropped silently, not treated as an error.
    async function callbackHandler(key, ctx) {
      let payload;
      try {
        payload = decodeBody(ctx.body, 'application/json');
      } catch {
        return { status: 400 };
      }
      if (!isPlainObject(payload)) {
        return { status: 400 };
      }
      const { id, items } = payload;
      if (typeof id !== 'string' || !isPlainObject(items)) {
        return { status: 400 };
      }
      if (!acceptingCallbacks || id !== subscriptionId) {
        return { status: 200 };
      }
      const { span, context: spanContext } = startConsumerSpan(request.storeName, key, ctx.headers);
      context.with(spanContext, () => {
        node.send({
          payload: items,
          dapr: { storeName: request.storeName, keys: request.keys, subscriptionId: id },
        });
      });
      endSpan(span);
      return { status: 200 };
    }

    // One app-channel route per watched key (POST /configuration/<store>/<key>),
    // all sharing callbackHandler -- must be registered before any subscribe
    // call resolves, since daprd starts calling back immediately on success.
    const unregisterRoutes = [];
    let routeDefinitions;
    try {
      routeDefinitions = request.keys.map((key) => ({
        nodeId: `${node.id}:${key}`,
        verb: 'POST',
        path: buildConfigurationCallbackPath(request.storeName, key),
        key,
      }));
    } catch (err) {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(err.message);
      return;
    }
    try {
      for (const { key, ...definition } of routeDefinitions) {
        unregisterRoutes.push(
          connection.registerInternalRoute(definition, (ctx) => callbackHandler(key, ctx))
        );
      }
    } catch (err) {
      for (const unregister of unregisterRoutes) {
        unregister();
      }
      node.status({ fill: 'red', shape: 'ring', text: 'duplicate callback' });
      node.error(err.message);
      return;
    }

    function scheduleRetry() {
      if (closed || retryTimer || !connection.isSidecarHealthy()) {
        return;
      }
      retryTimer = setTimeout(attemptSubscribe, backoff);
      backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    }

    const clientOptions = (signal, timeoutMs = connection.options.limits.requestTimeoutMs) => ({
      baseUrl: connection.options.outbound.baseUrl,
      token: connection.options.daprApiToken,
      timeoutMs,
      signal,
      maxResponseBytes: connection.options.limits.bodyLimitBytes,
    });

    async function unsubscribe(id, signal, timeoutMs) {
      return tracedClientCall('unsubscribe', request.storeName, () =>
        unsubscribeConfiguration(clientOptions(signal, timeoutMs), {
          storeName: request.storeName,
          subscriptionId: id,
        })
      );
    }

    async function bestEffortUnsubscribe(id) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), UNSUBSCRIBE_TIMEOUT_MS);
      try {
        await unsubscribe(id, controller.signal, UNSUBSCRIBE_TIMEOUT_MS);
      } catch {
        // The sidecar may already be gone; lifecycle cleanup stays bounded.
      } finally {
        clearTimeout(timer);
      }
    }

    // A health failure may be a restarted sidecar (old id already gone) or a
    // transient probe failure (old id still live). Retire the old id before
    // replacing it so both cases converge to exactly one component subscription.
    async function retireCurrentSubscription(signal) {
      if (!subscriptionId) {
        return;
      }
      try {
        await unsubscribe(subscriptionId, signal);
      } catch (err) {
        if (
          err.code !== ErrorCodes.CONFIGURATION_OPERATION_FAILED ||
          !/does not exist/i.test(err.message)
        ) {
          throw err;
        }
      }
      subscriptionId = null;
    }

    // A sidecar restart drops daprd's own in-memory subscription state (the
    // same as pub/sub), so recovery always means a fresh subscribe call for a
    // new subscription id -- never an attempt to resume the old one.
    // Transient failures (sidecar unreachable) retry on a bounded
    // double/reset/cap backoff; a definitive "store not found/not
    // configured" response still retries on the same schedule rather than
    // giving up permanently, since an operator can fix the store name by
    // adding a component without a Node-RED redeploy -- only the displayed
    // status distinguishes "transient" from "misconfigured".
    async function attemptSubscribe() {
      retryTimer = null;
      if (closed || attemptController || !connection.isSidecarHealthy()) {
        return;
      }
      const controller = new AbortController();
      attemptController = controller;
      acceptingCallbacks = false;
      try {
        await retireCurrentSubscription(controller.signal);
        const result = await tracedClientCall('subscribe', request.storeName, () =>
          subscribeConfiguration(clientOptions(controller.signal), request)
        );
        if (closed) {
          await bestEffortUnsubscribe(result.id);
          return;
        }
        subscriptionId = result.id;
        acceptingCallbacks = true;
        const initial = await tracedClientCall('get', request.storeName, () =>
          getConfiguration(clientOptions(controller.signal), request)
        );
        if (closed || subscriptionId !== result.id) {
          return;
        }
        node.send({
          payload: initial.items,
          dapr: {
            storeName: request.storeName,
            keys: request.keys,
            subscriptionId: result.id,
          },
        });
        backoff = INITIAL_BACKOFF_MS;
        setState('subscribed');
      } catch (err) {
        acceptingCallbacks = false;
        if (closed) {
          return;
        }
        if (err.code === ErrorCodes.CONFIGURATION_OPERATION_FAILED) {
          setState('failed', 'configuration store not found');
        } else {
          setState('retrying');
        }
        scheduleRetry();
      } finally {
        if (attemptController === controller) {
          attemptController = null;
        }
      }
    }

    // onSidecarHealth immediately reports the connection's current state, then
    // again only on real transitions (never every poll) -- the natural trigger
    // for "sidecar became healthy, attempt a fresh subscribe now".
    const removeHealthListener = connection.onSidecarHealth((healthy) => {
      if (closed) {
        return;
      }
      if (retryTimer) {
        clearTimeout(retryTimer);
        retryTimer = null;
      }
      if (healthy) {
        backoff = INITIAL_BACKOFF_MS;
        attemptSubscribe();
      } else {
        acceptingCallbacks = false;
        if (attemptController) {
          attemptController.abort();
        }
        setState('retrying', 'sidecar unavailable');
      }
    });

    node.on('close', async (_removed, done) => {
      closed = true;
      acceptingCallbacks = false;
      removeHealthListener();
      if (retryTimer) {
        clearTimeout(retryTimer);
      }
      if (attemptController) {
        attemptController.abort();
      }
      for (const unregister of unregisterRoutes) {
        unregister();
      }
      if (subscriptionId) {
        await bestEffortUnsubscribe(subscriptionId);
      }
      done();
    });
  }

  RED.nodes.registerType('dapr-config-subscribe', DaprConfigSubscribeNode);
};
