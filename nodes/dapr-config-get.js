'use strict';

const { context, SpanKind } = require('@opentelemetry/api');

const { getConfiguration } = require('../lib/configuration-client');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { prepareConfigurationGet } = require('../lib/configuration-messages');
const { requireConnection, openSidecarSession } = require('../lib/sidecar-session');
const { getTracer, endSpan } = require('../lib/telemetry');

module.exports = function registerDaprConfigGet(RED) {
  function DaprConfigGetNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);
    if (!requireConnection(node, connection)) {
      return;
    }
    const session = openSidecarSession(node, connection);

    node.on('input', async (msg, send, done) => {
      if (!(await session.isReady())) {
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

      try {
        const result = await session.call((transport) => getConfiguration(transport, request));
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
      }
    });
  }

  RED.nodes.registerType('dapr-config-get', DaprConfigGetNode);
};
