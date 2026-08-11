'use strict';

const { context, SpanKind } = require('@opentelemetry/api');

const {
  stateGet,
  stateSave,
  stateDelete,
  stateBulkGet,
  stateTransaction,
} = require('../lib/state-client');
const { DaprError, ErrorCodes } = require('../lib/errors');
const {
  resolveOperation,
  prepareStateGet,
  prepareStateSave,
  prepareStateDelete,
  prepareStateBulkGet,
  prepareStateTransaction,
} = require('../lib/state-messages');
const { getTracer, endSpan } = require('../lib/telemetry');

module.exports = function registerDaprState(RED) {
  function DaprStateNode(config) {
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

    const removeHealthListener = connection.onSidecarHealth((healthy) =>
      node.status(
        healthy
          ? { fill: 'green', shape: 'dot', text: 'ready' }
          : { fill: 'red', shape: 'ring', text: 'sidecar unavailable' }
      )
    );

    const inflight = new Set(); // AbortControllers for state calls in flight

    node.on('input', async (msg, send, done) => {
      // Wait for the connection's first health probe to land before judging the
      // sidecar down, so a message sent immediately after deploy is not failed
      // against a sidecar that is actually up.
      await connection.whenHealthKnown();
      if (!connection.isSidecarHealthy()) {
        done(new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'Dapr sidecar is unavailable'));
        return;
      }

      const dapr =
        msg.dapr && typeof msg.dapr === 'object' && !Array.isArray(msg.dapr) ? msg.dapr : {};
      const limits = { maxBodyBytes: connection.options.limits.bodyLimitBytes };
      let operation;
      let request;
      try {
        operation = resolveOperation(config, msg);
        switch (operation) {
          case 'get':
            request = prepareStateGet(config, msg);
            break;
          case 'save':
            request = prepareStateSave(config, msg, limits);
            break;
          case 'delete':
            request = prepareStateDelete(config, msg);
            break;
          case 'bulkGet':
            request = prepareStateBulkGet(config, msg, limits);
            break;
          case 'transaction':
            request = prepareStateTransaction(config, msg, limits);
            break;
        }
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'invalid message' });
        done(err);
        return;
      }

      // A no-op span (tracing disabled) costs nothing and needs no branch of its
      // own here. Unlike dapr-publish/dapr-invoke, no trace context is injected
      // onto the outbound call: the other end of a state call is a backing
      // store through the sidecar, not another traced app that could extract a
      // traceparent.
      const span = getTracer().startSpan(
        `state ${operation} ${request.storeName}`,
        {
          kind: SpanKind.CLIENT,
          attributes: {
            'dapr.state.store': request.storeName,
            'dapr.state.operation': operation,
          },
        },
        context.active()
      );

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
        let result;
        switch (operation) {
          case 'get':
            result = await stateGet(transportOptions, request);
            msg.payload = result.value;
            msg.dapr = {
              ...dapr,
              operation,
              storeName: request.storeName,
              key: request.key,
              statusCode: result.status,
              etag: result.etag,
            };
            break;
          case 'save':
            result = await stateSave(transportOptions, request);
            msg.dapr = {
              ...dapr,
              operation,
              storeName: request.storeName,
              key: request.item.key,
              statusCode: result.status,
            };
            break;
          case 'delete':
            result = await stateDelete(transportOptions, request);
            msg.dapr = {
              ...dapr,
              operation,
              storeName: request.storeName,
              key: request.key,
              statusCode: result.status,
            };
            break;
          case 'bulkGet':
            result = await stateBulkGet(transportOptions, request);
            msg.payload = result.items;
            msg.dapr = {
              ...dapr,
              operation,
              storeName: request.storeName,
              statusCode: result.status,
            };
            break;
          case 'transaction':
            result = await stateTransaction(transportOptions, request);
            msg.dapr = {
              ...dapr,
              operation,
              storeName: request.storeName,
              statusCode: result.status,
            };
            break;
        }
        endSpan(span);
        node.status({ fill: 'green', shape: 'dot', text: `${operation} ${result.status}` });
        send(msg);
        done();
      } catch (err) {
        endSpan(span, err);
        const text =
          {
            [ErrorCodes.INVALID_MESSAGE]: 'invalid message',
            [ErrorCodes.STATE_ETAG_MISMATCH]: 'etag mismatch',
            [ErrorCodes.STATE_OPERATION_FAILED]: 'state operation failed',
            [ErrorCodes.RESPONSE_TOO_LARGE]: 'response too large',
            [ErrorCodes.SIDECAR_UNAVAILABLE]: 'sidecar unavailable',
          }[err.code] || 'state operation failed';
        node.status({ fill: 'red', shape: 'ring', text });
        done(err);
      } finally {
        inflight.delete(controller);
      }
    });

    node.on('close', (_removed, done) => {
      removeHealthListener();
      // Abort any state call still in flight so it cannot outlive the node.
      for (const controller of inflight) {
        controller.abort();
      }
      inflight.clear();
      done();
    });
  }

  RED.nodes.registerType('dapr-state', DaprStateNode);
};
