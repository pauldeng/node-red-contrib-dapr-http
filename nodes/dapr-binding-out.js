'use strict';

const { context, SpanKind } = require('@opentelemetry/api');

const { invokeBinding } = require('../lib/binding-client');
const { decodeBody } = require('../lib/invoke-client');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { prepareBindingRequest } = require('../lib/binding-messages');
const { getTracer, endSpan } = require('../lib/telemetry');
const { requireConnection, openSidecarSession } = require('../lib/sidecar-session');

const METADATA_HEADER_PREFIX = 'metadata.';

// daprd returns a component's response metadata as "metadata.<key>" response
// headers, not in the body -- Node's http client lower-cases every incoming
// header name, so any casing the component set (e.g. "statusCode") is
// already lost by the time this runs. Stripped into a plain map so a flow
// does not have to know the wire convention.
function responseMetadata(headers) {
  const metadata = {};
  for (const [key, value] of Object.entries(headers || {})) {
    if (key.startsWith(METADATA_HEADER_PREFIX)) {
      metadata[key.slice(METADATA_HEADER_PREFIX.length)] = value;
    }
  }
  return metadata;
}

module.exports = function registerDaprBindingOut(RED) {
  function DaprBindingOutNode(config) {
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
        request = prepareBindingRequest(config, msg);
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'invalid message' });
        done(err);
        return;
      }

      // A no-op span (tracing disabled) costs nothing and needs no branch of
      // its own here. No trace context is injected onto the outbound call: the
      // other end is a component-defined binding through the sidecar, not
      // another traced app that could extract a traceparent.
      const span = getTracer().startSpan(
        `binding invoke ${request.bindingName}`,
        {
          kind: SpanKind.CLIENT,
          attributes: {
            'dapr.binding.name': request.bindingName,
            'dapr.binding.operation': request.operation,
          },
        },
        context.active()
      );

      try {
        const result = await session.call((transport) => invokeBinding(transport, request));
        msg.payload =
          result.status === 204 ? null : decodeBody(result.body, result.headers['content-type']);
        msg.dapr = {
          ...dapr,
          bindingName: request.bindingName,
          operation: request.operation,
          statusCode: result.status,
          metadata: responseMetadata(result.headers),
        };
        endSpan(span);
        node.status({ fill: 'green', shape: 'dot', text: `invoke ${result.status}` });
        send(msg);
        done();
      } catch (err) {
        endSpan(span, err);
        const text =
          {
            [ErrorCodes.INVALID_MESSAGE]: 'invalid message',
            [ErrorCodes.BINDING_INVOKE_FAILED]: 'binding failed',
            [ErrorCodes.RESPONSE_TOO_LARGE]: 'response too large',
            [ErrorCodes.SIDECAR_UNAVAILABLE]: 'sidecar unavailable',
          }[err.code] || 'binding failed';
        node.status({ fill: 'red', shape: 'ring', text });
        done(err);
      }
    });
  }

  RED.nodes.registerType('dapr-binding-out', DaprBindingOutNode);
};
