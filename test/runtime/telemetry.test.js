'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');
const { waitForFast: waitFor } = require('../helpers/wait-for');
const { setTimeout: delay } = require('node:timers/promises');

const healthPath = '/v1.0/healthz/outbound';
const publishPath = '/v1.0/publish/pubsub/orders';

// W3C traceparent: version-traceId-spanId-flags, e.g.
// 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
const TRACEPARENT_RE = /^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/;

// Retries through a rejection, not just a falsy result: an HTTP poll right
// after nr.deploy() can hit the app-channel listener before it has finished

function publishFlow({ appPort, daprPort, tracingEnabled }) {
  return [
    { id: 'tab', type: 'tab', label: 'telemetry' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      tracingEnabled,
    },
    { id: 'in', type: 'http in', z: 'tab', url: '/publish', method: 'post', wires: [['pub']] },
    {
      id: 'pub',
      type: 'dapr-publish',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic: 'orders',
      metadata: '{}',
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab', statusCode: '', headers: {} },
  ];
}

function post(nr) {
  return httpRequest(nr.nodeUrl('/publish'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ orderId: 1 }),
    timeoutMs: 5000,
  });
}

test(
  'dapr-publish injects a real W3C traceparent once a connection enables tracing',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', publishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    // always_on: deterministic sampling: every span is recorded regardless of
    // its random trace ID. No collector is reachable from this test, and it
    // must not need one — export happens off the message path (see
    // lib/telemetry.js's fail-open shutdown handling, unit-tested directly).
    await nr.start({ env: { OTEL_TRACES_SAMPLER: 'always_on' } });
    t.after(() => nr.stop());
    const appPort = await freePort();

    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port, tracingEnabled: true }));
    await dapr.waitForRequest(healthPath);
    await delay(50);

    const response = await post(nr);
    assert.equal(response.status, 200);

    const publish = dapr.requests.filter((request) => request.path === publishPath).at(-1);
    assert.match(publish.headers.traceparent, TRACEPARENT_RE);
    // Real, freshly generated -- not a placeholder or a fixed test value.
    assert.notEqual(
      publish.headers.traceparent,
      '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
    );
  }
);

test(
  'dapr-publish never injects a traceparent while tracing stays disabled',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', publishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const appPort = await freePort();

    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port, tracingEnabled: false }));
    await dapr.waitForRequest(healthPath);
    await delay(50);

    assert.equal((await post(nr)).status, 200);
    const publish = dapr.requests.filter((request) => request.path === publishPath).at(-1);
    assert.equal(publish.headers.traceparent, undefined);
  }
);

test(
  'dapr-invoke injects a real W3C traceparent once a connection enables tracing',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const invokePath = '/v1.0/invoke/target/method/echo';
    dapr.respond('POST', invokePath, (_req, res) => res.writeHead(200).end('{}'));

    const nr = new NodeRed();
    await nr.start({ env: { OTEL_TRACES_SAMPLER: 'always_on' } });
    t.after(() => nr.stop());
    const appPort = await freePort();

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'invoke-telemetry' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
        tracingEnabled: true,
      },
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'post', wires: [['inv']] },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'target',
        method: 'echo',
        verb: 'POST',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
    ]);
    await dapr.waitForRequest(healthPath);
    await delay(50);

    const response = await httpRequest(nr.nodeUrl('/call'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
      timeoutMs: 5000,
    });
    assert.equal(response.status, 200);

    const invoked = dapr.requests.filter((request) => request.path === invokePath).at(-1);
    assert.match(invoked.headers.traceparent, TRACEPARENT_RE);
  }
);

const deliveryPath = '/node-red-dapr/subscriptions/sub1';
// dapr-publish's own documented override (msg.dapr.pubsubName/topic beat the
// node's configured ones -- see lib/messages.js's preparePublish) means pub1
// republishes wherever sub1's delivery says, not to its own configured
// 'forwarded' topic: dapr-subscribe sets msg.dapr.topic to the topic it
// received on, and dapr-publish never sees a reason to override that.
const forwardedPublishPath = '/v1.0/publish/pubsub/orders';

// The inbound trace this delivery carries, mimicking a real daprd delivery
// that itself continued a trace from further upstream.
const inboundTraceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
const inboundTraceId = inboundTraceparent.split('-')[1];
const inboundSpanId = inboundTraceparent.split('-')[2];
// What a real daprd delivery carries in the CloudEvent: the PUBLISHER's span,
// on a different trace from the header the subscribing sidecar wrote.
const envelopeTraceparent = '00-9999999999999999999999999999aaaa-00f067aa0ba902b7-01';
const envelopeTraceId = envelopeTraceparent.split('-')[1];

test(
  'one trace ID survives a real daprd-style delivery through subscribe into an outbound publish',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', forwardedPublishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start({ env: { OTEL_TRACES_SAMPLER: 'always_on' } });
    t.after(() => nr.stop());
    const appPort = await freePort();

    // dapr-subscribe wired straight into dapr-publish. Proves that the
    // consumer context survives Node-RED's async dispatch into the next node's
    // flow span and producer span.
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'trace-continuity' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
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
        rawPayload: false,
        metadata: '{}',
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
    ]);
    await waitFor(async () => {
      const res = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return res.status === 200 && res.text.includes(deliveryPath) ? res : null;
    });

    const delivery = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
      method: 'POST',
      headers: { 'content-type': 'application/cloudevents+json', traceparent: inboundTraceparent },
      body: JSON.stringify({
        specversion: '1.0',
        id: 'e1',
        source: 'test',
        type: 'order',
        traceparent: envelopeTraceparent,
        data: { orderId: 7 },
      }),
      timeoutMs: 5000,
    });
    assert.equal(delivery.status, 200);
    assert.deepEqual(JSON.parse(delivery.text), { status: 'SUCCESS' });

    const forwarded = await waitFor(() =>
      dapr.requests.find((request) => request.path === forwardedPublishPath)
    );
    const [, traceId, spanId] = forwarded.headers.traceparent.split('-');
    assert.equal(traceId, envelopeTraceId, 'the publisher CloudEvent context wins over the header');
    assert.notEqual(
      traceId,
      inboundTraceId,
      'not parented on the subscribing sidecar delivery span'
    );
    assert.notEqual(spanId, inboundSpanId, 'a new span for the outbound publish, not a copy');
  }
);

test(
  'the trace survives a plain function node in between, via the onReceive/onComplete flow-span hooks',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', forwardedPublishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start({ env: { OTEL_TRACES_SAMPLER: 'always_on' } });
    t.after(() => nr.stop());
    const appPort = await freePort();

    // sub1 -> a plain function node (no OpenTelemetry awareness at all) ->
    // pub1. Node-RED's own dispatch is all that carries context across the
    // function node's own node.send() call; this node contributes nothing
    // beyond what any third-party node in the ecosystem already does.
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'trace-continuity-through-function' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
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
        rawPayload: false,
        metadata: '{}',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'return msg;',
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
    ]);
    await waitFor(async () => {
      const res = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return res.status === 200 && res.text.includes(deliveryPath) ? res : null;
    });

    await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
      method: 'POST',
      headers: { 'content-type': 'application/cloudevents+json', traceparent: inboundTraceparent },
      body: JSON.stringify({
        specversion: '1.0',
        id: 'e2',
        source: 'test',
        type: 'order',
        data: { orderId: 8 },
      }),
      timeoutMs: 5000,
    });

    const forwarded = await waitFor(() =>
      dapr.requests.find((request) => request.path === forwardedPublishPath)
    );
    const [, traceId] = forwarded.headers.traceparent.split('-');
    assert.equal(traceId, inboundTraceId, 'same trace across delivery -> function -> publish');
  }
);

test(
  'a bulk delivery continues the trace context carried by each entry CloudEvent',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', forwardedPublishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start({ env: { OTEL_TRACES_SAMPLER: 'always_on' } });
    t.after(() => nr.stop());
    const appPort = await freePort();

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bulk-trace-continuity' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
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
        rawPayload: false,
        metadata: '{}',
        bulkEnabled: true,
        bulkMaxMessagesCount: '10',
        bulkMaxAwaitDurationMs: '100',
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
    ]);
    await waitFor(async () => {
      const res = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return res.status === 200 && res.text.includes('bulkSubscribe') ? res : null;
    });

    const delivery = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        id: 'batch-1',
        metadata: {},
        entries: [
          {
            entryId: 'entry-1',
            contentType: 'application/cloudevents+json',
            metadata: {},
            event: {
              specversion: '1.0',
              id: 'e3',
              source: 'test',
              type: 'order',
              traceparent: inboundTraceparent,
              data: { orderId: 9 },
            },
          },
        ],
      }),
      timeoutMs: 5000,
    });
    assert.equal(delivery.status, 200);
    assert.deepEqual(JSON.parse(delivery.text), {
      statuses: [{ entryId: 'entry-1', status: 'SUCCESS' }],
    });

    const forwarded = await waitFor(() =>
      dapr.requests.find((request) => request.path === forwardedPublishPath)
    );
    assert.equal(
      forwarded.headers.traceparent.split('-')[1],
      inboundTraceId,
      'same trace across the bulk entry -> publish hop'
    );
  }
);

test(
  'dapr-actor-call injects a real W3C traceparent once a connection enables tracing',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const actorInvokePath = '/v1.0/actors/Order/o1/method/Ping';
    dapr.respond('POST', actorInvokePath, (_req, res) => res.writeHead(200).end());

    const nr = new NodeRed();
    await nr.start({ env: { OTEL_TRACES_SAMPLER: 'always_on' } });
    t.after(() => nr.stop());
    const appPort = await freePort();

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'actor-call-telemetry' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
        tracingEnabled: true,
      },
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'post', wires: [['call']] },
      {
        id: 'call',
        type: 'dapr-actor-call',
        z: 'tab',
        connection: 'c1',
        actorType: 'Order',
        actorId: 'o1',
        method: 'Ping',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
    ]);
    await dapr.waitForRequest(healthPath);
    // The health probe landing is not the same event as the connection
    // having processed and marked itself healthy -- wait on the real
    // "sidecar is available" log line rather than a fixed settle.
    await nr.waitForLog(/Dapr sidecar is available/i);

    const response = await httpRequest(nr.nodeUrl('/call'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
      timeoutMs: 5000,
    });
    assert.equal(response.status, 200);

    const invoked = dapr.requests.filter((request) => request.path === actorInvokePath).at(-1);
    assert.match(invoked.headers.traceparent, TRACEPARENT_RE);
  }
);

test(
  'dapr-actor-schedule injects a real W3C traceparent once a connection enables tracing',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const reminderPath = '/v1.0/actors/Order/o1/reminders/wake';
    dapr.respond('POST', reminderPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start({ env: { OTEL_TRACES_SAMPLER: 'always_on' } });
    t.after(() => nr.stop());
    const appPort = await freePort();

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'actor-schedule-telemetry' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
        tracingEnabled: true,
      },
      { id: 'in', type: 'http in', z: 'tab', url: '/set', method: 'post', wires: [['sched']] },
      {
        id: 'sched',
        type: 'dapr-actor-schedule',
        z: 'tab',
        connection: 'c1',
        operation: 'set',
        actorType: 'Order',
        actorId: 'o1',
        scheduleName: 'wake',
        dueTime: '5s',
        period: '',
        ttl: '',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
    ]);
    await dapr.waitForRequest(healthPath);
    // The health probe landing is not the same event as the connection
    // having processed and marked itself healthy -- wait on the real
    // "sidecar is available" log line rather than a fixed settle.
    await nr.waitForLog(/Dapr sidecar is available/i);

    const response = await httpRequest(nr.nodeUrl('/set'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
      timeoutMs: 5000,
    });
    assert.equal(response.status, 200);

    const set = dapr.requests.filter((request) => request.path === reminderPath).at(-1);
    assert.match(set.headers.traceparent, TRACEPARENT_RE);
  }
);
