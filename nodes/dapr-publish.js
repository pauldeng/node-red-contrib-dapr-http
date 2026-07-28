'use strict';

const { publish } = require('../lib/dapr-client');
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
      const controller = new AbortController();
      inflight.add(controller);
      try {
        await publish(
          {
            baseUrl: connection.options.outbound.baseUrl,
            token: connection.options.daprApiToken,
            timeoutMs: connection.options.limits.requestTimeoutMs,
            signal: controller.signal,
            maxResponseBytes: connection.options.limits.bodyLimitBytes,
          },
          preparePublish(config, msg)
        );
        updateStatus(true);
        send(msg);
        done();
      } catch (err) {
        const text = err.code === ErrorCodes.INVALID_MESSAGE ? 'invalid message' : 'publish failed';
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
