'use strict';

const { context, SpanKind } = require('@opentelemetry/api');

const { getConfiguration } = require('../lib/configuration-client');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { prepareConfigurationGet } = require('../lib/configuration-messages');
const { getTracer, endSpan } = require('../lib/telemetry');

module.exports = function registerDaprConfigGet(RED) {
  function DaprConfigGetNode(config) {
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

    const inflight = new Set(); // AbortControllers for get calls in flight

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
      let request;
      try {
        request = prepareConfigurationGet(config, msg);
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'invalid message' });
        done(err);
        return;
      }

      // A no-op span (tracing disabled) costs nothing and needs no branch of
      // its own here. No trace context is injected onto the outbound call: the
      // other end is a configuration store through the sidecar, not another
      // traced app that could extract a traceparent.
      const span = getTracer().startSpan(
        `configuration get ${request.storeName}`,
        {
          kind: SpanKind.CLIENT,
          attributes: {
            'dapr.configuration.store': request.storeName,
            'dapr.configuration.key_count': request.keys.length,
          },
        },
        context.active()
      );

      const controller = new AbortController();
      inflight.add(controller);
      try {
        const result = await getConfiguration(
          {
            baseUrl: connection.options.outbound.baseUrl,
            token: connection.options.daprApiToken,
            timeoutMs: connection.options.limits.requestTimeoutMs,
            signal: controller.signal,
            maxResponseBytes: connection.options.limits.bodyLimitBytes,
          },
          request
        );
        msg.payload = result.items;
        msg.dapr = {
          ...dapr,
          storeName: request.storeName,
          keys: request.keys,
          statusCode: result.status,
        };
        endSpan(span);
        node.status({ fill: 'green', shape: 'dot', text: `get ${result.status}` });
        send(msg);
        done();
      } catch (err) {
        endSpan(span, err);
        const text =
          err.code === ErrorCodes.CONFIGURATION_OPERATION_FAILED
            ? 'config get failed'
            : 'sidecar unavailable';
        node.status({ fill: 'red', shape: 'ring', text });
        done(err);
      } finally {
        inflight.delete(controller);
      }
    });

    node.on('close', (_removed, done) => {
      removeHealthListener();
      // Abort any get call still in flight so it cannot outlive the node.
      for (const controller of inflight) {
        controller.abort();
      }
      inflight.clear();
      done();
    });
  }

  RED.nodes.registerType('dapr-config-get', DaprConfigGetNode);
};
