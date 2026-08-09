'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { startOtelCollector } = require('../helpers/otel-collector');
const { waitFor } = require('../helpers/wait-for');

test(
  'a real daprd delivery through subscribe into publish exports both spans, sharing one trace, to a real OTLP collector',
  { timeout: 90000 },
  async (t) => {
    const collector = await startOtelCollector();
    t.after(() => collector.stop());

    const appPort = await freePort();
    const daprHttpPort = await freePort();

    // Startup ordering matches every other integration test (docs/testing.md):
    // deploy Node-RED first and wait for its own /healthz, then start daprd,
    // which fetches /dapr/subscribe exactly once at startup.
    const nr = new ContainerNodeRed();
    await nr.start({
      env: {
        OTEL_TRACES_SAMPLER: 'always_on',
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${collector.port}`,
      },
      flows: [
        { id: 'tab', type: 'tab', label: 'it-telemetry' },
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
          wires: [['clear']],
        },
        {
          // dapr-publish's own documented override (msg.dapr.pubsubName/topic
          // beat the node's configured ones) would otherwise republish this
          // delivery straight back to "orders" -- the same topic sub1
          // subscribes to, looping forever through real daprd + Redis. A
          // plain function node, not a Dapr-aware one, proves the flow-span
          // hooks carry context through arbitrary nodes at the same time.
          id: 'clear',
          type: 'function',
          z: 'tab',
          func: 'msg.dapr = {}; return msg;',
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
      ],
    });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const redis = await startRedis();
    t.after(() => redis.stop());

    const daprd = await startDaprd({
      appId: 'it-telemetry-app',
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
    });
    t.after(() => daprd.stop());

    // startDaprd() resolving only proves daprd's OWN /healthz answers; the
    // dapr-connection node polls independently on its own bounded-backoff
    // schedule (up to 30s) and may not have caught up yet — dapr-publish's
    // fail-fast-while-unhealthy check would otherwise drop this delivery's
    // republish, and auto-ack still reports SUCCESS to daprd regardless, so
    // there is no retry to fall back on. Wait for the connection's own
    // confirmation before publishing.
    await waitFor(() => (/Dapr sidecar is available/.test(nr.logText()) ? true : null));

    const published = await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ orderId: 42 }),
        timeoutMs: 4000,
      });
      return r.status === 204 ? r : null;
    });
    assert.equal(published.status, 204);

    // Four spans, nested: the consumer span (dapr-subscribe's own delivery)
    // -> the plain function node's own flow span -> dapr-publish's own flow
    // span -> the producer span dapr-publish creates around its outbound
    // call. Bounded to a fixed number of polls rather than a longer waitFor
    // window: if the "clear" node's msg.dapr override were ever ineffective,
    // this would loop back to "orders" through real daprd + Redis, and a
    // long poll would spend its whole budget watching that loop instead of
    // failing promptly.
    let spans = [];
    for (let attempt = 0; attempt < 20 && spans.length < 4; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      spans = await collector.readSpans();
    }

    const consumer = spans.find((s) => s.name === 'orders process');
    const functionSpan = spans.find((s) => s.name === 'function');
    const nodeSpan = spans.find((s) => s.name === 'dapr-publish');
    const producer = spans.find((s) => s.name === 'forwarded publish');
    const names = spans.map((s) => s.name);
    assert.equal(
      spans.filter((s) => s.name === 'orders publish').length,
      0,
      `the "clear" node must stop pub1 from republishing to "orders" (the loop this guards against): ${names}`
    );
    assert.ok(consumer, `expected a consumer span named "orders process", got: ${names}`);
    assert.ok(functionSpan, `expected a flow span named "function", got: ${names}`);
    assert.ok(nodeSpan, `expected a flow span named "dapr-publish", got: ${names}`);
    assert.ok(producer, `expected a producer span named "forwarded publish", got: ${names}`);
    assert.equal(
      functionSpan.traceId,
      consumer.traceId,
      'the function span is part of the same trace'
    );
    assert.equal(nodeSpan.traceId, consumer.traceId, 'the node span is part of the same trace');
    assert.equal(producer.traceId, consumer.traceId, 'the producer span is part of the same trace');
    assert.equal(
      functionSpan.parentSpanId,
      consumer.spanId,
      'the function span nests under the delivery'
    );
    assert.equal(
      nodeSpan.parentSpanId,
      functionSpan.spanId,
      "dapr-publish's own node span nests under the function node's span"
    );
    assert.equal(
      producer.parentSpanId,
      nodeSpan.spanId,
      "the producer span nests under dapr-publish's own node span"
    );

    // OTLP's wire SpanKind enum reserves 0 for UNSPECIFIED, so it is the
    // @opentelemetry/api SpanKind enum (INTERNAL=0 ... CONSUMER=4) shifted up
    // by one: PRODUCER -> 4, CONSUMER -> 5. Confirmed against a real export,
    // not assumed from either enum's own source.
    assert.equal(consumer.kind, 5, 'CONSUMER');
    assert.equal(producer.kind, 4, 'PRODUCER');
  }
);
