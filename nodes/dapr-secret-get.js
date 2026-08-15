'use strict';

const { context, SpanKind } = require('@opentelemetry/api');

const { getSecret } = require('../lib/secret-client');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { prepareSecretGet, resolveSecretProperty } = require('../lib/secret-messages');
const { getTracer, endSpan } = require('../lib/telemetry');
const { requireConnection, openSidecarSession } = require('../lib/sidecar-session');

const STATUS_TEXT_BY_CODE = {
  [ErrorCodes.INVALID_MESSAGE]: 'invalid message',
  [ErrorCodes.SECRET_ACCESS_DENIED]: 'access denied',
  [ErrorCodes.SECRET_OPERATION_FAILED]: 'secret get failed',
  [ErrorCodes.RESPONSE_TOO_LARGE]: 'response too large',
  [ErrorCodes.SIDECAR_UNAVAILABLE]: 'sidecar unavailable',
};

module.exports = function registerDaprSecretGet(RED) {
  function DaprSecretGetNode(config) {
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
      let property;
      try {
        request = prepareSecretGet(config, msg);
        property = resolveSecretProperty(config.property, RED.util.normalisePropertyExpression);
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'invalid message' });
        done(err);
        return;
      }

      // A no-op span (tracing disabled) costs nothing and needs no branch of
      // its own here. Deliberately omit the key attribute, matching
      // dapr-config-get's `key_count` (a count, never the key names).
      const span = getTracer().startSpan(
        `secret get ${request.storeName}`,
        {
          kind: SpanKind.CLIENT,
          attributes: { 'dapr.secret.store': request.storeName },
        },
        context.active()
      );

      try {
        const result = await session.call((transport) => getSecret(transport, request));
        if (!RED.util.setMessageProperty(msg, property, result.data, true)) {
          throw new DaprError(ErrorCodes.INVALID_MESSAGE, 'secret output property cannot be set');
        }
        msg.dapr = {
          ...dapr,
          storeName: request.storeName,
          key: request.key,
          statusCode: result.status,
        };
        endSpan(span);
        node.status({ fill: 'green', shape: 'dot', text: `get ${result.status}` });
        send(msg);
        done();
      } catch (err) {
        endSpan(span, err);
        node.status({
          fill: 'red',
          shape: 'ring',
          text: STATUS_TEXT_BY_CODE[err.code] || 'secret get failed',
        });
        done(err);
      }
    });
  }

  RED.nodes.registerType('dapr-secret-get', DaprSecretGetNode);
};
