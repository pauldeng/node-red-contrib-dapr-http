'use strict';

const crypto = require('node:crypto');

const { buildSubscription, parseDelivery } = require('../lib/subscriptions');

// Distinguishes an ack timeout (warn + RETRY) from an explicit ack-node RETRY.
const ACK_TIMEOUT = Symbol('ack-timeout');

// Transport and sensitive request headers excluded from the delivery metadata
// exposed to flows.
const EXCLUDED_HEADERS = new Set([
  'host',
  'connection',
  'content-length',
  'content-type',
  'transfer-encoding',
  'keep-alive',
  'accept',
  'accept-encoding',
  'user-agent',
  'dapr-api-token',
  'dapr-caller-app-id',
  'dapr-callee-app-id',
]);

module.exports = function registerDaprSubscribe(RED) {
  function DaprSubscribeNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (!connection?.options || typeof connection.registerSubscription !== 'function') {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      return;
    }

    let definition;
    try {
      definition = buildSubscription({
        nodeId: node.id,
        pubsubName: config.pubsubName,
        topic: config.topic,
        deadLetterTopic: config.deadLetterTopic,
        rawPayload: config.rawPayload,
        metadata: config.metadata,
        ackMode: config.ackMode,
      });
    } catch (err) {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(err.message);
      return;
    }

    // Resolve the ack just before the app channel's own request deadline (a
    // fixed 250 ms margin that holds even at the 1 s minimum) so a timeout
    // produces a clean 200 { RETRY } body rather than the 503 backstop.
    const ackTimeoutMs = Math.max(1, connection.options.limits.requestTimeoutMs - 250);

    let pending = 0;
    const showStatus = () => {
      node.status(
        pending > 0
          ? { fill: 'blue', shape: 'dot', text: `pending ${pending}` }
          : { fill: 'green', shape: 'dot', text: 'subscribed' }
      );
    };

    const deliver = async (ctx) => {
      let parsed;
      try {
        parsed = parseDelivery(ctx.body);
      } catch (err) {
        // A malformed delivery will not parse on retry either — drop it.
        node.error(`malformed delivery on ${definition.topic}: ${err.message}`);
        return { status: 200, body: { status: 'DROP' } };
      }

      // Delivery metadata: daprd forwards the component's message metadata and
      // tracing context as request headers. Pass them through, dropping
      // transport and sensitive headers.
      const metadata = {};
      for (const [header, value] of Object.entries(ctx.headers)) {
        if (typeof value === 'string' && !EXCLUDED_HEADERS.has(header)) {
          metadata[header] = value;
        }
      }
      const deliveryId =
        typeof parsed.cloudEvent.id === 'string' ? parsed.cloudEvent.id : crypto.randomUUID();
      const dapr = {
        pubsubName: definition.pubsubName,
        topic: definition.topic,
        route: definition.route,
        deliveryId,
        metadata,
        cloudEvent: parsed.cloudEvent,
      };

      if (definition.ackMode === 'auto') {
        node.send({ _msgid: RED.util.generateId(), payload: parsed.payload, dapr });
        return { status: 200, body: { status: 'SUCCESS' } };
      }

      // Explicit acknowledgement: emit the message carrying a correlation id and
      // wait for a dapr-ack node (or the timeout) to settle it.
      const ackId = crypto.randomUUID();
      let ackResult;
      try {
        ackResult = connection.addPendingAck(ackId, {
          timeoutMs: ackTimeoutMs,
          onTimeout: () => ACK_TIMEOUT,
        });
      } catch {
        // Capacity or duplicate id — ask Dapr to redeliver later.
        return { status: 200, body: { status: 'RETRY' } };
      }
      pending += 1;
      showStatus();
      node.send({
        _msgid: RED.util.generateId(),
        payload: parsed.payload,
        dapr: { ...dapr, ackId },
      });

      const outcome = await ackResult;
      pending -= 1;
      showStatus();
      if (outcome === ACK_TIMEOUT) {
        node.warn(`acknowledgement timed out for ${definition.pubsubName}/${definition.topic}`);
        return { status: 200, body: { status: 'RETRY' } };
      }
      return { status: 200, body: { status: outcome } };
    };

    let unregister;
    try {
      unregister = connection.registerSubscription(definition, deliver);
    } catch (err) {
      // Another subscribe node already owns this pubsub/topic on this connection.
      node.status({ fill: 'red', shape: 'ring', text: 'duplicate subscription' });
      node.error(err.message);
      return;
    }
    showStatus();

    node.on('close', (_removed, done) => {
      unregister();
      done();
    });
  }

  RED.nodes.registerType('dapr-subscribe', DaprSubscribeNode);
};
