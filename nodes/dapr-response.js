'use strict';

const { encodeBody } = require('../lib/invoke-client');
const { sanitizeResponseHeaders, validateContentType } = require('../lib/http-headers');
const { DaprError, ErrorCodes } = require('../lib/errors');

module.exports = function registerDaprResponse(RED) {
  function DaprResponseNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (!connection || typeof connection.settleResponse !== 'function') {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      node.on('input', (_msg, _send, done) => {
        done(new DaprError(ErrorCodes.INVALID_OPTIONS, 'Dapr connection is unavailable'));
      });
      return;
    }

    node.on('input', (msg, _send, done) => {
      const dapr = msg.dapr && typeof msg.dapr === 'object' ? msg.dapr : {};
      if (typeof dapr.responseId !== 'string' || dapr.responseId === '') {
        done(new DaprError(ErrorCodes.INVALID_MESSAGE, 'msg.dapr.responseId is required'));
        return;
      }

      const configured = config.statusCode ? Number(config.statusCode) : 200;
      const status = dapr.statusCode !== undefined ? Number(dapr.statusCode) : configured;
      // 1xx codes are interim (RFC 9110 §15.2) and Node delivers them through the
      // 'information' event, not as a final response — writeHead(1xx) followed by
      // end() produces a bare socket hang up for the caller. Only a final status
      // (200-599) may be sent here.
      if (!Number.isInteger(status) || status < 200 || status > 599) {
        done(
          new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid response status: ${dapr.statusCode}`)
        );
        return;
      }

      let encoded;
      let contentType;
      try {
        // encodeBody can throw on a non-serializable payload (BigInt, circular).
        encoded = encodeBody(msg.payload);
        contentType = validateContentType(dapr.contentType);
      } catch (err) {
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.INVALID_MESSAGE, err.message, { cause: err })
        );
        return;
      }
      // Strip framing/hop-by-hop headers and reject illegal names/values (and
      // non-object responseHeaders) so a flow cannot corrupt the response framing.
      const headers = sanitizeResponseHeaders(dapr.responseHeaders);
      contentType = contentType || encoded.contentType;
      if (contentType && encoded.body !== undefined && !headers['content-type']) {
        headers['content-type'] = contentType;
      }

      // First response wins; a missing/expired/foreign id is rejected so it is
      // never mistaken for a delivered response.
      if (connection.settleResponse(dapr.responseId, { status, headers, body: encoded.body })) {
        node.status({ fill: 'green', shape: 'dot', text: String(status) });
        done();
      } else {
        node.status({ fill: 'yellow', shape: 'ring', text: 'no pending request' });
        done(
          new DaprError(
            ErrorCodes.INVALID_MESSAGE,
            `no pending request for responseId ${dapr.responseId} (expired, foreign, or already answered)`
          )
        );
      }
    });
  }

  RED.nodes.registerType('dapr-response', DaprResponseNode);
};
