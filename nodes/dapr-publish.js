'use strict';

const { context, propagation, trace, SpanKind } = require('@opentelemetry/api');

const { publish, publishBulk } = require('../lib/dapr-client');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { preparePublish, prepareBulkPublish } = require('../lib/messages');
const { getTracer, endSpan } = require('../lib/telemetry');

// msg.dapr.bulk, when present, overrides the node's configured Single/Bulk
// mode for that one message; a missing value preserves the configured mode.
// msg.dapr itself may be anything (garbage, a string, an array) — this only
// decides which prepare*/publish* pair to call, so it stays a loose,
// defensive check; the chosen prepare function is what actually validates
// msg.dapr's shape.
function resolveBulkMode(config, msg) {
  const dapr = msg.dapr;
  if (
    dapr &&
    typeof dapr === 'object' &&
    !Array.isArray(dapr) &&
    Object.prototype.hasOwnProperty.call(dapr, 'bulk')
  ) {
    if (typeof dapr.bulk !== 'boolean') {
      throw new DaprError(ErrorCodes.INVALID_MESSAGE, 'msg.dapr.bulk must be a boolean');
    }
    return dapr.bulk;
  }
  return config.bulkEnabled === true;
}

module.exports = function registerDaprPublish(RED) {
  function DaprPublishNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (
      !connection?.options ||
      typeof connection.isSidecarHealthy !== 'function' ||
      typeof connection.whenHealthKnown !== 'function' ||
      typeof connection.onSidecarHealth !== 'function'
    ) {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      node.on('input', (_msg, _send, done) => {
        done(new DaprError(ErrorCodes.INVALID_OPTIONS, 'Dapr connection is unavailable'));
      });
      return;
    }

    const updateStatus = (healthy) => {
      node.status(
        healthy
          ? { fill: 'green', shape: 'dot', text: 'connected' }
          : { fill: 'red', shape: 'ring', text: 'sidecar unavailable' }
      );
    };
    const removeHealthListener = connection.onSidecarHealth(updateStatus);

    const inflight = new Set(); // AbortControllers for publishes in flight

    node.on('input', async (msg, send, done) => {
      // Wait for the connection's first health probe to land before judging the
      // sidecar down, so a message sent immediately after deploy is not failed
      // against a sidecar that is actually up.
      await connection.whenHealthKnown();
      if (!connection.isSidecarHealthy()) {
        done(new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'Dapr sidecar is unavailable'));
        return;
      }
      let bulk;
      let request;
      try {
        bulk = resolveBulkMode(config, msg);
        request = bulk
          ? prepareBulkPublish(config, msg, {
              maxBodyBytes: connection.options.limits.bodyLimitBytes,
            })
          : preparePublish(config, msg);
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'invalid message' });
        done(err);
        return;
      }

      // A no-op span (tracing disabled) costs nothing and needs no branch of
      // its own here. Injected onto request.headers, which preparePublish/
      // prepareBulkPublish already validated — a manually forwarded
      // msg.dapr.headers.traceparent is preserved when tracing is disabled
      // (propagation.inject() is then a true no-op) and superseded by the
      // real one once enabled. One span covers the whole bulk request —
      // daprd creates its own per-entry producer spans server-side (see
      // pkg/api/http/http.go's onBulkPublish); span attributes carry only
      // the batch and failure counts, never entry payloads or metadata.
      const span = getTracer().startSpan(
        `${request.topic} ${bulk ? 'bulk ' : ''}publish`,
        {
          kind: SpanKind.PRODUCER,
          attributes: {
            'messaging.system': 'dapr',
            'messaging.destination.name': request.topic,
            'messaging.operation.name': 'publish',
            'dapr.pubsub.name': request.pubsubName,
            ...(bulk ? { 'dapr.bulk.entry_count': request.entries.length } : {}),
          },
        },
        context.active()
      );
      propagation.inject(trace.setSpan(context.active(), span), request.headers);

      const controller = new AbortController();
      inflight.add(controller);
      const transportOptions = {
        baseUrl: connection.options.outbound.baseUrl,
        token: connection.options.daprApiToken,
        timeoutMs: connection.options.limits.requestTimeoutMs,
        signal: controller.signal,
        maxResponseBytes: connection.options.limits.bodyLimitBytes,
      };
      try {
        if (bulk) {
          await publishBulk(transportOptions, request);
          span.setAttribute('dapr.bulk.failed_count', 0);
          msg.dapr = {
            ...msg.dapr,
            bulkResult: { failedEntries: [], entryCount: request.entries.length },
          };
        } else {
          await publish(transportOptions, request);
        }
        updateStatus(true);
        endSpan(span);
        send(msg);
        done();
      } catch (err) {
        if (err.code === ErrorCodes.BULK_PUBLISH_PARTIAL) {
          span.setAttribute('dapr.bulk.failed_count', err.bulkResult.failedEntries.length);
          msg.dapr = {
            ...msg.dapr,
            bulkResult: { ...err.bulkResult, entryCount: request.entries.length },
          };
        }
        endSpan(span, err);
        const text =
          err.code === ErrorCodes.INVALID_MESSAGE
            ? 'invalid message'
            : err.code === ErrorCodes.BULK_PUBLISH_PARTIAL
              ? 'bulk publish partial failure'
              : 'publish failed';
        node.status({ fill: 'red', shape: 'ring', text });
        done(err);
      } finally {
        inflight.delete(controller);
      }
    });

    node.on('close', (_removed, done) => {
      removeHealthListener();
      // Abort any publish still in flight so it cannot outlive the node. The
      // message fails (reaching a Catch node if one is wired) rather than being
      // silently completed after the node is gone — same contract as dapr-invoke.
      for (const controller of inflight) {
        controller.abort();
      }
      inflight.clear();
      done();
    });
  }

  RED.nodes.registerType('dapr-publish', DaprPublishNode);
};
