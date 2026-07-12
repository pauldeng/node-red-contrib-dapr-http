'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');

const healthPath = '/v1.0/healthz/outbound';
const publishPath = '/v1.0/publish/pubsub/orders';

async function waitFor(fn, { timeoutMs = 10000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error('waitFor timed out');
}

function publishFlow({ appPort, daprPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'publish' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    { id: 'in', type: 'http in', z: 'tab', url: '/publish', method: 'post', wires: [['before']] },
    {
      id: 'in-binary',
      type: 'http in',
      z: 'tab',
      url: '/publish-binary',
      method: 'post',
      wires: [['binary']],
    },
    {
      id: 'binary',
      type: 'function',
      z: 'tab',
      func: "msg.payload = Buffer.from([0, 1, 255]); msg.marker = 'preserved'; msg.dapr = { metadata: { ttlInSeconds: '10' } }; return msg;",
      outputs: 1,
      wires: [['pub']],
    },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: "const input = msg.payload; msg.payload = input.value; msg.marker = 'preserved'; msg.dapr = input.publishOptions || { metadata: { ttlInSeconds: '10' } }; return msg;",
      outputs: 1,
      wires: [['pub']],
    },
    {
      id: 'pub',
      type: 'dapr-publish',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic: 'orders',
      contentType: '',
      metadata: '{"rawPayload":"false","ttlInSeconds":"60"}',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: 'msg.payload = { published: msg.payload, marker: msg.marker }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    {
      id: 'errors',
      type: 'catch',
      z: 'tab',
      scope: ['pub'],
      uncaught: false,
      wires: [['failure']],
    },
    {
      id: 'failure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab', statusCode: '', headers: {} },
  ];
}

function post(nr, payload, publishOptions) {
  return httpRequest(nr.nodeUrl('/publish'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: payload, publishOptions }),
    timeoutMs: 5000,
  });
}

test(
  'real Node-RED publishes through the SDK, preserves msg, and survives full redeploy',
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

    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await waitFor(() => dapr.requests.find((request) => request.path === healthPath));
    await new Promise((resolve) => setTimeout(resolve, 50));

    const payloads = [{ orderId: 42 }, 0, false, null, ''];
    for (const payload of payloads) {
      const response = await post(nr, payload);
      assert.equal(response.status, 200, `payload ${JSON.stringify(payload)}: ${response.text}`);
      assert.deepEqual(JSON.parse(response.text), { published: payload, marker: 'preserved' });
    }

    const publishes = dapr.requests.filter((request) => request.path === publishPath);
    assert.equal(publishes.length, payloads.length);
    assert.deepEqual(
      publishes.map((request) => request.body.toString()),
      payloads.map((payload) => (payload === '' ? '' : JSON.stringify(payload)))
    );
    assert.equal(publishes[0].headers['content-type'], 'application/json');
    assert.equal(publishes.at(-1).headers['content-type'], 'text/plain');
    assert.deepEqual(publishes[0].query, {
      'metadata.rawPayload': 'false',
      'metadata.ttlInSeconds': '10',
    });

    assert.equal(
      (
        await httpRequest(nr.nodeUrl('/publish-binary'), {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: '{}',
          timeoutMs: 5000,
        })
      ).status,
      200
    );
    const binaryPublish = dapr.requests.filter((request) => request.path === publishPath).at(-1);
    assert.equal(binaryPublish.headers['content-type'], 'application/octet-stream');
    assert.deepEqual(binaryPublish.body, Buffer.from([0, 1, 255]));

    const cloudEvent = {
      specversion: '1.0',
      id: 'event-1',
      source: 'node-red',
      type: 'example.order',
      data: { orderId: 43 },
    };
    assert.equal(
      (
        await post(nr, cloudEvent, {
          contentType: 'application/cloudevents+json',
          metadata: { rawPayload: 'true' },
        })
      ).status,
      200
    );
    const cloudEventPublish = dapr.requests
      .filter((request) => request.path === publishPath)
      .at(-1);
    assert.equal(cloudEventPublish.headers['content-type'], 'application/cloudevents+json');
    assert.deepEqual(JSON.parse(cloudEventPublish.body.toString()), cloudEvent);
    assert.deepEqual(cloudEventPublish.query, {
      'metadata.rawPayload': 'true',
      'metadata.ttlInSeconds': '60',
    });

    const previousHealthCount = dapr.requests.filter(
      (request) => request.path === healthPath
    ).length;
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await waitFor(
      () =>
        dapr.requests.filter((request) => request.path === healthPath).length > previousHealthCount
    );
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal((await post(nr, { after: 'redeploy' })).status, 200);
  }
);

test(
  'publish fails fast while sidecar is down and recovers with health',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(503).end());
    dapr.respond('POST', publishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const appPort = await freePort();
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await waitFor(() => dapr.requests.find((request) => request.path === healthPath));

    const started = Date.now();
    const unavailable = await post(nr, { orderId: 1 });
    assert.equal(unavailable.status, 503);
    assert.ok(
      Date.now() - started < 1500,
      'unhealthy sidecar must fail without the SDK readiness wait'
    );
    assert.equal(
      dapr.requests.some((request) => request.path === publishPath),
      false
    );

    const previousHealthCount = dapr.requests.filter(
      (request) => request.path === healthPath
    ).length;
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    await waitFor(
      () =>
        dapr.requests.filter((request) => request.path === healthPath).length > previousHealthCount,
      { timeoutMs: 5000 }
    );
    await new Promise((resolve) => setTimeout(resolve, 50));

    dapr.respond('POST', publishPath, (_req, res) => res.writeHead(500).end('publish rejected'));
    const rejected = await post(nr, { orderId: 2 });
    assert.equal(rejected.status, 503, 'SDK error responses must reach a Catch node');

    dapr.respond('POST', publishPath, (_req, res) => res.writeHead(204).end());
    const recovered = await post(nr, { orderId: 2 });
    assert.equal(recovered.status, 200);
    assert.ok(dapr.requests.some((request) => request.path === publishPath));
  }
);

test('many publish nodes can share one connection without listener warnings', async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  const flow = [
    { id: 'tab', type: 'tab', label: 'many publishers' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(dapr.port),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
  ];
  for (let i = 0; i < 11; i += 1) {
    flow.push({
      id: `pub-${i}`,
      type: 'dapr-publish',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic: `topic-${i}`,
      metadata: '{}',
      wires: [],
    });
  }

  await nr.deploy(flow);
  await waitFor(() => dapr.requests.find((request) => request.path === healthPath));
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.doesNotMatch(nr.logText(), /MaxListenersExceededWarning/);
});
