'use strict';

const { sharedClients } = require('../lib/dapr-client');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { preparePublish } = require('../lib/messages');

module.exports = function registerDaprPublish(RED) {
  function DaprPublishNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (
      !connection?.options ||
      typeof connection.isSidecarHealthy !== 'function' ||
      typeof connection.onSidecarHealth !== 'function'
    ) {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      node.on('input', (_msg, _send, done) => {
        done(new DaprError(ErrorCodes.INVALID_OPTIONS, 'Dapr connection is unavailable'));
      });
      return;
    }

    const client = sharedClients.acquire(connection.options);
    const updateStatus = (healthy) => {
      node.status(
        healthy
          ? { fill: 'green', shape: 'dot', text: 'connected' }
          : { fill: 'red', shape: 'ring', text: 'sidecar unavailable' }
      );
    };
    const removeHealthListener = connection.onSidecarHealth(updateStatus);

    node.on('input', async (msg, send, done) => {
      if (!connection.isSidecarHealthy()) {
        done(new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'Dapr sidecar is unavailable'));
        return;
      }
      try {
        await client.publish(preparePublish(config, msg));
        updateStatus(true);
        send(msg);
        done();
      } catch (err) {
        const text = err.code === ErrorCodes.INVALID_MESSAGE ? 'invalid message' : 'publish failed';
        node.status({ fill: 'red', shape: 'ring', text });
        done(err);
      }
    });

    node.on('close', async (_removed, done) => {
      removeHealthListener();
      try {
        await client.release();
        done();
      } catch (err) {
        done(err);
      }
    });
  }

  RED.nodes.registerType('dapr-publish', DaprPublishNode);
};
