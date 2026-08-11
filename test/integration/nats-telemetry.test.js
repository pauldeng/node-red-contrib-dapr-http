'use strict';

// Milestone 3's primary real-Docker logging path: real daprd, NATS
// JetStream, and the pinned OTel Collector, all wired together. A
// dapr-subscribe delivery logs (via node.warn(), through the settings.js
// bridge) and publishes onward (through a second dapr-subscribe, to
// observe the round trip completed, broker-independently of the collector).
// Verifies: the log carries the active node span's trace/span IDs, the
// distributed trace includes both the inbound (consumer) and outbound
// (producer) Dapr boundaries on one trace, and telemetry export fails open
// across a real collector outage — the delivery/ack/onward-publish round
// trip keeps completing promptly while the collector is down, and export
// resumes (without needing replay of anything dropped during the outage)
// once a fresh collector is reachable again.
//
// Subsumes and extends the old Redis-backed
// test/integration/telemetry.test.js span-nesting test (now removed from
// that file) — this is the one place that coverage lives now, per
// Milestone 3's NATS-primary rebalance.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { startOtelCollector } = require('../helpers/otel-collector');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

const OTEL_LOGGING_HANDLER = `, otel: { level: 'debug', metrics: false, audit: false, handler: require('@pauldeng/node-red-contrib-dapr-http/logging') }`;

function numericSpanAttribute(span, key) {
  const attribute = span.attributes?.find((item) => item.key === key);
  return Number(attribute?.value?.intValue);
}

test(
  'a real daprd delivery through subscribe, logging, and onward publish exports a correlated log and a distributed trace to a real OTLP collector, and fails open across a collector outage',
  { timeout: 120000 },
  async (t) => {
    let collector = await startOtelCollector();
    t.after(() => collector.stop());
    const collectorPort = collector.port;

    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    // Startup ordering matches every other integration test (docs/testing.md):
    // deploy Node-RED first and wait for its own /healthz, then start daprd,
    // which fetches /dapr/subscribe exactly once at startup.
    const nr = new ContainerNodeRed();
    await nr.start({
      env: {
        OTEL_TRACES_SAMPLER: 'always_on',
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${collectorPort}`,
      },
      loggingExtra: OTEL_LOGGING_HANDLER,
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-telemetry' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
          tracingEnabled: true,
        },
        {
          id: 'sub1',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'orders',
          ackMode: 'auto',
          metadata: '{}',
          wires: [['warnFn']],
        },
        {
          // The active node span at the moment node.warn() fires is THIS
          // node's own flow span (a child of the consumer span, per
          // lib/telemetry.js's onReceive/onComplete hooks) — the log must
          // correlate with exactly that span, not the consumer span itself
          // or the producer span further downstream.
          id: 'warnFn',
          type: 'function',
          z: 'tab',
          name: 'warn',
          func: `const orderId = msg.payload && msg.payload.orderId;
node.warn('processing subscribed order ' + orderId);
msg.payload = [
  { entryId: 'forwarded-' + orderId + '-1', payload: { orderId } },
  { entryId: 'forwarded-' + orderId + '-2', payload: { orderId, copy: true } },
];
msg.dapr = { bulk: true }; // clear the delivery topic and select bulk publish
return msg;`,
          outputs: 1,
          wires: [['pub1']],
        },
        {
          id: 'pub1',
          type: 'dapr-publish',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'forwarded',
          metadata: '{}',
          wires: [[]],
        },
        // A second subscription observes the onward publish actually
        // completed — broker-level delivery, independent of whether the
        // collector is reachable, so this keeps working through the outage
        // below.
        {
          id: 'sub2',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'forwarded',
          ackMode: 'auto',
          metadata: '{}',
          wires: [['fwd']],
        },
        {
          id: 'fwd',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ payload: msg.payload });
return msg;`,
          outputs: 1,
          wires: [['req']],
        },
        {
          id: 'req',
          type: 'http request',
          z: 'tab',
          method: 'use',
          ret: 'txt',
          url: '',
          wires: [[]],
        },
      ],
    });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const nats = await startNats();
    t.after(() => nats.stop());
    await provisionStream(nats.port, {
      streamName: 'nrdapr-it',
      subjects: ['orders', 'forwarded'],
    });
    const component = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
    });
    const daprd = await startDaprd({
      appId: 'it-nats-telemetry-app',
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'pubsub-jetstream.yaml', yaml: component }],
    });
    t.after(() => daprd.stop());

    // startDaprd() resolving only proves daprd's OWN /healthz answers; the
    // dapr-connection node polls independently on its own bounded-backoff
    // schedule and may not have caught up yet.
    await waitFor(() => (/Dapr sidecar is available/.test(nr.logText()) ? true : null));

    const publish = (n) =>
      waitFor(async () => {
        const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ orderId: n }),
          timeoutMs: 4000,
        });
        return r.status === 204 ? r : null;
      });

    await publish(1);
    // Proves the full round trip completed while the collector is up:
    // subscribe -> log -> publish -> a second subscribe delivers it back.
    await waitFor(() => capture.received.some((r) => r?.payload?.orderId === 1) || null);

    let spans = [];
    let logRecords = [];
    for (let attempt = 0; attempt < 20 && (!spans.length || !logRecords.length); attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      spans = await collector.readSpans();
      logRecords = await collector.readLogRecords();
    }

    const names = spans.map((s) => s.name);
    const consumer = spans.find((s) => s.name === 'orders process');
    assert.ok(consumer, `expected a consumer span named "orders process", got: ${names}`);
    // Both function nodes in this flow (the warn node and sub2's own
    // forwarder) share the same span NAME — flow spans are named by
    // node.type, not node.id — so disambiguate by parent relationship
    // rather than name alone.
    const warnSpan = spans.find((s) => s.name === 'function' && s.parentSpanId === consumer.spanId);
    assert.ok(warnSpan, `expected the warn function's own flow span, got: ${names}`);
    // dapr-publish gets its own flow span too (started by onReceive before
    // its input handler runs), and its producer span's parent context is
    // THAT node span, not the warn function's — so the chain is
    // consumer -> warnFn(function) -> nodeSpan(dapr-publish) -> producer.
    const nodeSpan = spans.find(
      (s) => s.name === 'dapr-publish' && s.parentSpanId === warnSpan?.spanId
    );
    assert.ok(nodeSpan, `expected dapr-publish's own flow span, got: ${names}`);
    const producer = spans.find((s) => s.name === 'forwarded bulk publish');
    assert.ok(producer, `expected a producer span named "forwarded bulk publish", got: ${names}`);
    // The outbound publish's own W3C trace-context injection means the
    // second delivery (sub2, on "forwarded") extracts it and continues the
    // SAME trace — so this trace actually carries two full Dapr boundaries
    // end to end: the original inbound delivery and the re-delivery it
    // triggered, not just the one outbound publish in between.
    const consumer2 = spans.find(
      (s) => s.name === 'forwarded process' && s.traceId === consumer.traceId
    );
    assert.ok(
      consumer2,
      `expected the onward delivery's own consumer span to continue the same trace, got: ${names}`
    );

    assert.equal(warnSpan.traceId, consumer.traceId, 'the flow span shares the delivery trace');
    assert.equal(producer.traceId, consumer.traceId, 'the producer span shares the delivery trace');
    assert.equal(warnSpan.parentSpanId, consumer.spanId, 'the flow span nests under the delivery');
    assert.equal(
      nodeSpan.traceId,
      consumer.traceId,
      "dapr-publish's own node span shares the trace"
    );
    assert.equal(
      producer.parentSpanId,
      nodeSpan.spanId,
      "the outbound publish nests under dapr-publish's own node span"
    );
    assert.equal(
      consumer2.parentSpanId,
      producer.spanId,
      "the onward delivery's consumer span nests under the outbound publish that caused it"
    );
    assert.equal(consumer.kind, 5, 'CONSUMER');
    assert.equal(producer.kind, 4, 'PRODUCER');
    assert.equal(consumer2.kind, 5, 'CONSUMER');
    assert.equal(numericSpanAttribute(producer, 'dapr.bulk.entry_count'), 2);
    assert.equal(numericSpanAttribute(producer, 'dapr.bulk.failed_count'), 0);

    const record = logRecords.find((r) => r.body?.stringValue === 'processing subscribed order 1');
    assert.ok(record, `expected the warn log record, got: ${JSON.stringify(logRecords)}`);
    assert.equal(record.traceId, warnSpan.traceId, "the log shares the warn node's own span trace");
    assert.equal(
      record.spanId,
      warnSpan.spanId,
      "the log is attributed to the warn node's own span, not the consumer or producer span"
    );

    // Now take the collector down mid-run.
    await collector.stop();

    const start = Date.now();
    await publish(2);
    // The delivery/ack/onward-publish round trip must complete promptly —
    // fail-open, not blocked or delayed by an unreachable collector.
    await waitFor(() => capture.received.some((r) => r?.payload?.orderId === 2) || null, {
      timeoutMs: 10000,
    });
    assert.ok(
      Date.now() - start < 8000,
      'the round trip must not be delayed by an unreachable collector'
    );
    assert.doesNotMatch(nr.logText(), /UnhandledPromiseRejection/);

    // Restart a fresh collector on the SAME port the already-running
    // Node-RED process is configured to reach (its own env var was fixed at
    // its own startup) and confirm export resumes for later records. Bounded
    // loss of whatever was in flight during the outage is expected and not
    // asserted against — only that new telemetry gets through again.
    collector = await startOtelCollector({ port: collectorPort });
    t.after(() => collector.stop());

    await publish(3);
    await waitFor(() => capture.received.some((r) => r?.payload?.orderId === 3) || null);

    let recoveredLogRecords = [];
    for (
      let attempt = 0;
      attempt < 20 &&
      !recoveredLogRecords.some((r) => r.body?.stringValue === 'processing subscribed order 3');
      attempt += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      recoveredLogRecords = await collector.readLogRecords();
    }
    assert.ok(
      recoveredLogRecords.some((r) => r.body?.stringValue === 'processing subscribed order 3'),
      'telemetry export resumes once a fresh collector is reachable again'
    );
  }
);
