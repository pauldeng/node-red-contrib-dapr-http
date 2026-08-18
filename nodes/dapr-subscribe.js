'use strict';

const crypto = require('node:crypto');

const { context, propagation, trace, SpanKind } = require('@opentelemetry/api');

const {
  buildSubscription,
  parseDelivery,
  parseBulkDelivery,
  traceCarrier,
} = require('../lib/subscriptions');
const { HOP_BY_HOP } = require('../lib/http-headers');
const { getTracer, endSpan } = require('../lib/telemetry');

// A non-SUCCESS outcome (RETRY/DROP) closes the consumer span as an error:
// the delivery was not successfully processed, which is exactly what a trace
// reviewing this topic wants to see flagged.
function endDeliverySpan(span, status) {
  endSpan(span, status === 'SUCCESS' ? undefined : new Error(`delivery ${status}`));
}

// Extract the W3C trace context from a carrier built by lib/subscriptions
// traceCarrier and start this delivery's consumer span as its child. A no-op
// extract/span (tracing disabled) costs nothing and needs no branch of its own
// here.
function startConsumerSpan(definition, carrier) {
  const parentContext = propagation.extract(context.active(), carrier);
  const span = getTracer().startSpan(
    `${definition.topic} process`,
    {
      kind: SpanKind.CONSUMER,
      attributes: {
        'messaging.system': 'dapr',
        'messaging.destination.name': definition.topic,
        'messaging.operation.name': 'process',
        'dapr.pubsub.name': definition.pubsubName,
      },
    },
    parentContext
  );
  return { span, context: trace.setSpan(parentContext, span) };
}

// Distinguishes an ack timeout (warn + RETRY) from an explicit ack-node RETRY.
const ACK_TIMEOUT = Symbol('ack-timeout');

// Transport and sensitive request headers excluded from the delivery metadata
// exposed to flows: the shared hop-by-hop/framing set (established in M6 —
// covers proxy-authorization, te, trailer, upgrade, etc.) plus headers with no
// value to a flow and Dapr's own internal headers.
const EXCLUDED_HEADERS = new Set([
  ...HOP_BY_HOP,
  'content-type',
  'accept',
  'accept-encoding',
  'user-agent',
  'dapr-api-token',
  'dapr-caller-app-id',
  'dapr-callee-app-id',
]);

// A bulk entry's own metadata (never HTTP headers — see deliverBulk) only ever
// needs the token excluded, as a defensive measure.
function filterEntryMetadata(metadata) {
  const out = {};
  for (const [key, value] of Object.entries(metadata)) {
    if (typeof value === 'string' && key.toLowerCase() !== 'dapr-api-token') {
      out[key] = value;
    }
  }
  return out;
}

function bulkSubscribeFromConfig(config) {
  if (!config.bulkEnabled) {
    return undefined;
  }
  return {
    enabled: true,
    maxMessagesCount: config.bulkMaxMessagesCount,
    maxAwaitDurationMs: config.bulkMaxAwaitDurationMs,
  };
}

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
        rules: config.rules,
        bulkSubscribe: bulkSubscribeFromConfig(config),
      });
    } catch (err) {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(err.message);
      return;
    }

    // A single static output, regardless of how many CEL rules are configured.
    // Node-RED has no documented public API for safely remapping existing
    // wires when a node's output count changes (only that `outputs` MAY be
    // configurable — see nodered.org/docs/creating-nodes/properties); the
    // wire-remapping JSON contract the core switch node relies on is an
    // undocumented internal detail, not something a contributed node should
    // depend on. Every matched delivery is emitted from this one output, with
    // msg.dapr.ruleId identifying which rule matched (or null for the
    // default/fallback) — a flow that needs separate branches wires a
    // standard Switch node on msg.dapr.ruleId downstream.
    const ruleIdForPath = (path) => {
      const route = definition.routes.find((r) => r.path === path);
      return route ? route.ruleId : null;
    };

    // Resolve the ack just before the app channel's own request deadline (a
    // fixed 250 ms margin that holds even at the 1 s minimum) so a timeout
    // produces a clean 200 { RETRY } body rather than the 503 backstop.
    const ackTimeoutMs = Math.max(1, connection.options.limits.requestTimeoutMs - 250);

    let pending = 0;
    const activeAckIds = new Set(); // ack ids this node still owns
    const showStatus = () => {
      node.status(
        pending > 0
          ? { fill: 'blue', shape: 'dot', text: `pending ${pending}` }
          : { fill: 'green', shape: 'dot', text: 'subscribed' }
      );
    };

    function metadataFromHeaders(headers) {
      const metadata = {};
      for (const [header, value] of Object.entries(headers)) {
        if (typeof value === 'string' && !EXCLUDED_HEADERS.has(header)) {
          metadata[header] = value;
        }
      }
      return metadata;
    }

    // Explicit acknowledgement of one payload: register a correlation id, send
    // the message, and await the dapr-ack node's status (or the shared timeout).
    // Used for both a single delivery and each entry of a bulk delivery.
    async function awaitAck(msg, signal) {
      const ackId = crypto.randomUUID();
      let ackResult;
      try {
        ackResult = connection.addPendingAck(ackId, {
          timeoutMs: ackTimeoutMs,
          onTimeout: () => ACK_TIMEOUT,
        });
      } catch {
        return 'RETRY'; // capacity or duplicate id — ask Dapr to redeliver later
      }
      pending += 1;
      activeAckIds.add(ackId);
      showStatus();

      // If the caller disconnects before an ack arrives, the app channel aborts
      // ctx.signal. Settle immediately so the correlation does not sit pending
      // until the ack timeout — repeated disconnects would otherwise exhaust
      // the shared pending-ack budget. The eventual response is never written
      // (the app channel already finished the request).
      const onAbort = () => connection.settleAck(ackId, 'RETRY');
      if (signal) {
        if (signal.aborted) {
          onAbort();
        } else {
          signal.addEventListener('abort', onAbort, { once: true });
        }
      }

      node.send({ ...msg, dapr: { ...msg.dapr, ackId } });
      const outcome = await ackResult;
      if (signal) {
        signal.removeEventListener('abort', onAbort);
      }
      activeAckIds.delete(ackId);
      pending -= 1;
      showStatus();
      if (outcome === ACK_TIMEOUT) {
        node.warn(`acknowledgement timed out for ${definition.pubsubName}/${definition.topic}`);
        return 'RETRY';
      }
      return outcome;
    }

    const deliverSingle = async (ctx) => {
      let parsed;
      try {
        parsed = parseDelivery(ctx.body);
      } catch (err) {
        // A malformed delivery will not parse on retry either — drop it.
        node.error(`malformed delivery on ${definition.topic}: ${err.message}`);
        return { status: 200, body: { status: 'DROP' } };
      }

      const deliveryId =
        typeof parsed.cloudEvent.id === 'string' ? parsed.cloudEvent.id : crypto.randomUUID();
      const dapr = {
        pubsubName: definition.pubsubName,
        topic: definition.topic,
        route: ctx.path,
        ruleId: ruleIdForPath(ctx.path),
        deliveryId,
        metadata: metadataFromHeaders(ctx.headers),
        cloudEvent: parsed.cloudEvent,
      };
      const msg = { _msgid: RED.util.generateId(), payload: parsed.payload, dapr };

      const consumer = startConsumerSpan(definition, traceCarrier(ctx.headers, parsed.cloudEvent));
      if (definition.ackMode === 'auto') {
        context.with(consumer.context, () => node.send(msg));
        endDeliverySpan(consumer.span, 'SUCCESS');
        return { status: 200, body: { status: 'SUCCESS' } };
      }
      const status = await context.with(consumer.context, () => awaitAck(msg, ctx.signal));
      endDeliverySpan(consumer.span, status);
      return { status: 200, body: { status } };
    };

    // Bulk delivery: every entry is answered independently — a malformed entry
    // is DROPped without touching the others, and (in manual ack mode) each
    // good entry gets its own correlation id so entries can resolve as a mix of
    // SUCCESS/RETRY/DROP. Bulk composes with CEL rules — Dapr groups bulk
    // entries by their matched route and delivers a separate batch per route
    // (ctx.path); every entry still emits from this node's one output, with
    // msg.dapr.ruleId identifying which route the batch matched. Each entry
    // carries its own metadata/contentType (bulk has no per-message HTTP
    // headers to fall back on), and for a rawPayload topic each entry's event
    // is a base64 string rather than a CloudEvent (see parseBulkDelivery).
    const deliverBulk = async (ctx) => {
      const ruleId = ruleIdForPath(ctx.path);
      let entries;
      try {
        entries = parseBulkDelivery(ctx.body, { rawPayload: definition.rawPayload });
      } catch (err) {
        node.error(`malformed bulk delivery on ${definition.topic}: ${err.message}`);
        return { status: 200, body: { statuses: [] } };
      }

      const statuses = await Promise.all(
        entries.map(async (entry) => {
          if (entry.error) {
            if (entry.entryId === null) {
              node.warn(
                `bulk delivery entry missing entryId on ${definition.topic}: ${entry.error}`
              );
              return null; // nothing to correlate a status to
            }
            node.error(`malformed bulk entry on ${definition.topic}: ${entry.error}`);
            return { entryId: entry.entryId, status: 'DROP' };
          }
          const deliveryId =
            entry.cloudEvent && typeof entry.cloudEvent.id === 'string'
              ? entry.cloudEvent.id
              : entry.entryId;
          const dapr = {
            pubsubName: definition.pubsubName,
            topic: definition.topic,
            route: ctx.path,
            ruleId,
            deliveryId,
            entryId: entry.entryId,
            batchId: entry.batchId,
            metadata: filterEntryMetadata(entry.metadata),
            contentType: entry.contentType,
            cloudEvent: entry.cloudEvent,
          };
          const msg = { _msgid: RED.util.generateId(), payload: entry.payload, dapr };

          // Entry metadata is the delivery carrier here — bulk has no
          // per-message HTTP headers — and is the only possible carrier for a
          // raw entry, which has no CloudEvent at all.
          const consumer = startConsumerSpan(
            definition,
            traceCarrier(entry.metadata, entry.cloudEvent)
          );
          if (definition.ackMode === 'auto') {
            context.with(consumer.context, () => node.send(msg));
            endDeliverySpan(consumer.span, 'SUCCESS');
            return { entryId: entry.entryId, status: 'SUCCESS' };
          }
          const status = await context.with(consumer.context, () => awaitAck(msg, ctx.signal));
          endDeliverySpan(consumer.span, status);
          return { entryId: entry.entryId, status };
        })
      );
      return { status: 200, body: { statuses: statuses.filter(Boolean) } };
    };

    const deliver = definition.bulkSubscribe ? deliverBulk : deliverSingle;

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

    node.on('close', async (_removed, done) => {
      // On a modified-node ("nodes") redeploy the connection stays up, so it
      // does not drain our correlations. Settle them as RETRY ourselves —
      // otherwise each in-flight ack would hang until the ack timeout.
      unregister();
      for (const ackId of activeAckIds) {
        connection.settleAck(ackId, 'RETRY');
      }
      activeAckIds.clear();
      if (pending > 0) {
        // Let the settled handlers write their response before the node tears down.
        await new Promise((resolve) => setImmediate(resolve));
      }
      done();
    });
  }

  RED.nodes.registerType('dapr-subscribe', DaprSubscribeNode);
};
