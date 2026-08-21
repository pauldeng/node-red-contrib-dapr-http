'use strict';

const { DaprError, ErrorCodes } = require('../lib/errors');
const { resolveAckStatus } = require('../lib/ack');

module.exports = function registerDaprAck(RED) {
  function DaprAckNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (!connection || typeof connection.settleAck !== 'function') {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      node.on('input', (_msg, _send, done) => {
        done(new DaprError(ErrorCodes.INVALID_OPTIONS, 'Dapr connection is unavailable'));
      });
      return;
    }

    node.on('input', (msg, send, done) => {
      const dapr = msg.dapr || {};
      const { status, error } = resolveAckStatus(msg, config);
      if (error) {
        done(new DaprError(ErrorCodes.INVALID_MESSAGE, error));
        return;
      }
      if (typeof dapr.ackId !== 'string' || dapr.ackId === '') {
        done(new DaprError(ErrorCodes.INVALID_MESSAGE, 'msg.dapr.ackId is required'));
        return;
      }

      // First completion wins. A missing match — expired, foreign-generation, or
      // already-acknowledged — is rejected (done(error), no send) so it cannot be
      // mistaken for a successful acknowledgement, and never corrupts another.
      if (connection.settleAck(dapr.ackId, status)) {
        send(msg);
        done();
      } else {
        node.status({ fill: 'yellow', shape: 'ring', text: 'no pending delivery' });
        done(
          new DaprError(
            ErrorCodes.INVALID_MESSAGE,
            `no pending delivery for ackId ${dapr.ackId} (expired, foreign, or already acknowledged)`
          )
        );
      }
    });
  }

  RED.nodes.registerType('dapr-ack', DaprAckNode);
};
