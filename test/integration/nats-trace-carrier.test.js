'use strict';

// Pins the daprd behaviour that lib/subscriptions.js traceCarrier exists for.
//
// Every non-bulk delivery arrives carrying two W3C trace contexts, and they
// name DIFFERENT spans: the request headers carry the subscribing sidecar's
// own delivery span, while the CloudEvent carries the publishing side's. Only
// the second makes a consumer span a child of another service, so parenting on
// the headers collapses the hop into the subscribing app and any span-derived
// topology draws that app calling itself.
//
// Nothing in the unit or runtime tiers can prove that, because both fabricate
// the delivery: the two carriers only diverge when a real daprd with tracing
// enabled writes them. Hence a real sidecar, sampling every trace
// (fixtures/config-tracing.yaml), and a flow that reports both carriers back
// out through the capture hook.

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

const spanIdOf = (traceparent) => traceparent.split('-')[2];
const traceIdOf = (traceparent) => traceparent.split('-')[1];

test(
  'a real daprd delivery carries the publisher context in the CloudEvent and its own delivery span in the headers, and the consumer span parents on the publisher',
  { timeout: 120000 },
  async (t) => {
    const collector = await startOtelCollector();
    t.after(() => collector.stop());

    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    await nr.start({
      env: {
        OTEL_TRACES_SAMPLER: 'always_on',
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${collector.port}`,
      },
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-trace-carrier' },
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
          wires: [['report']],
        },
        {
          // Both carriers as the flow actually received them: the request
          // headers survive into msg.dapr.metadata (traceparent is not on
          // dapr-subscribe's EXCLUDED_HEADERS list) and the envelope is
          // msg.dapr.cloudEvent.
          id: 'report',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({
  orderId: msg.payload && msg.payload.orderId,
  headerTraceparent: msg.dapr.metadata.traceparent,
  envelopeTraceparent: msg.dapr.cloudEvent.traceparent,
});
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
    await provisionStream(nats.port, { streamName: 'nrdapr-it', subjects: ['orders'] });
    const daprd = await startDaprd({
      appId: 'it-nats-trace-carrier-app',
      appPort,
      httpPort: daprHttpPort,
      components: [
        {
          filename: 'pubsub-jetstream.yaml',
          yaml: jetstreamComponentYaml({
            name: 'pubsub',
            natsPort: nats.port,
            streamName: 'nrdapr-it',
          }),
        },
      ],
      configFixture: 'config-tracing.yaml',
    });
    t.after(() => daprd.stop());
    await waitFor(() => (/Dapr sidecar is available/.test(nr.logText()) ? true : null));

    // Publish WITHOUT a traceparent of our own: daprd starts the trace, so
    // every span in it is one daprd created, which is exactly the deployment
    // shape the bug was reported from.
    await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ orderId: 1 }),
        timeoutMs: 4000,
      });
      return r.status === 204 ? r : null;
    });

    const delivered = await capture.waitForMessage((m) => m?.orderId === 1, { timeoutMs: 30000 });
    assert.ok(
      delivered.headerTraceparent,
      `expected a traceparent request header on the delivery, got ${JSON.stringify(delivered)}`
    );
    assert.ok(
      delivered.envelopeTraceparent,
      `expected a traceparent in the CloudEvent, got ${JSON.stringify(delivered)}`
    );

    // The premise, stated as an assertion: same trace, different spans. If
    // daprd ever stops creating its own delivery span, this fails and
    // traceCarrier's whole preference can be reconsidered.
    assert.equal(
      traceIdOf(delivered.envelopeTraceparent),
      traceIdOf(delivered.headerTraceparent),
      'both carriers describe the same trace'
    );
    assert.notEqual(
      spanIdOf(delivered.headerTraceparent),
      spanIdOf(delivered.envelopeTraceparent),
      'the request header names the subscribing sidecar delivery span, not the publisher span'
    );

    // And the consumer span this package exports parents on the publisher's.
    const consumer = await waitFor(
      async () => (await collector.readSpans()).find((s) => s.name === 'orders process') ?? null,
      { timeoutMs: 30000 }
    );
    assert.equal(
      consumer.parentSpanId,
      spanIdOf(delivered.envelopeTraceparent),
      'the consumer span is a child of the publisher span carried by the CloudEvent'
    );
    assert.notEqual(
      consumer.parentSpanId,
      spanIdOf(delivered.headerTraceparent),
      'not a child of the subscribing sidecar own delivery span'
    );
    assert.equal(consumer.kind, 5, 'CONSUMER');
  }
);
