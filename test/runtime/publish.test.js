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
const bulkPublishPath = '/v1.0/publish/bulk/pubsub/orders';

function publishFlow({ appPort, daprPort, bulkEnabled = true }) {
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
    {
      id: 'in-bulk',
      type: 'http in',
      z: 'tab',
      url: '/publish-bulk',
      method: 'post',
      wires: [['before-bulk']],
    },
    {
      id: 'before-bulk',
      type: 'function',
      z: 'tab',
      func: `if (!Array.isArray(msg.payload)) {
  msg.dapr = { bulk: msg.payload.bulk };
  msg.payload = msg.payload.entries;
}
return msg;`,
      outputs: 1,
      wires: [['pub-bulk']],
    },
    {
      id: 'pub-bulk',
      type: 'dapr-publish',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic: 'orders',
      contentType: '',
      metadata: '{}',
      bulkEnabled,
      wires: [['success-bulk']],
    },
    {
      id: 'success-bulk',
      type: 'function',
      z: 'tab',
      func: 'msg.payload = { published: true, bulkResult: msg.dapr.bulkResult }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    {
      id: 'errors-bulk',
      type: 'catch',
      z: 'tab',
      scope: ['pub-bulk'],
      uncaught: false,
      wires: [['failure-bulk']],
    },
    {
      id: 'failure-bulk',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code, bulkResult: msg.dapr && msg.dapr.bulkResult }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab', statusCode: '', headers: {} },
  ];
}

function postBulk(nr, entries, bulk) {
  return httpRequest(nr.nodeUrl('/publish-bulk'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(bulk === undefined ? entries : { entries, bulk }),
    timeoutMs: 5000,
  });
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
  'real Node-RED publishes over HTTP, preserves msg, and survives full redeploy',
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
    await dapr.waitForRequest(healthPath);
    await delay(50);

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

    // A flow can carry trace context (or any other request header) through the
    // publish, so a subscribe → publish hop keeps one W3C trace.
    const traceparent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
    assert.equal(
      (await post(nr, { traced: true }, { headers: { traceparent }, metadata: {} })).status,
      200
    );
    const tracedPublish = dapr.requests.filter((request) => request.path === publishPath).at(-1);
    assert.equal(tracedPublish.headers.traceparent, traceparent);

    // A malformed header is rejected as an invalid message, before any socket
    // to the sidecar is opened.
    const publishCountBefore = dapr.requests.filter(
      (request) => request.path === publishPath
    ).length;
    assert.equal(
      (await post(nr, { bad: true }, { headers: { 'x bad name': 'v' }, metadata: {} })).status,
      503
    );
    assert.equal(
      dapr.requests.filter((request) => request.path === publishPath).length,
      publishCountBefore,
      'an invalid header must never reach the sidecar'
    );

    const previousHealthCount = dapr.requests.filter(
      (request) => request.path === healthPath
    ).length;
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await waitFor(
      () =>
        dapr.requests.filter((request) => request.path === healthPath).length > previousHealthCount
    );
    await delay(50);
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
    await dapr.waitForRequest(healthPath);

    const started = Date.now();
    const unavailable = await post(nr, { orderId: 1 });
    assert.equal(unavailable.status, 503);
    assert.ok(
      Date.now() - started < 1500,
      'an unhealthy sidecar must fail fast, with no readiness wait'
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
    await delay(50);

    dapr.respond('POST', publishPath, (_req, res) => res.writeHead(500).end('publish rejected'));
    const rejected = await post(nr, { orderId: 2 });
    assert.equal(rejected.status, 503, 'a sidecar error response must reach a Catch node');

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
  await dapr.waitForRequest(healthPath);
  await delay(100);
  assert.doesNotMatch(nr.logText(), /MaxListenersExceededWarning/);
});

test(
  'bulk publish (all success): entries reach the bulk endpoint and msg.dapr.bulkResult reports no failures',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', bulkPublishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const appPort = await freePort();
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await dapr.waitForRequest(healthPath);
    await delay(50);

    const response = await postBulk(nr, [
      { entryId: 'e1', payload: { orderId: 1 } },
      { entryId: 'e2', payload: { orderId: 2 }, metadata: { partitionKey: 'p1' } },
    ]);
    assert.equal(response.status, 200);
    assert.deepEqual(JSON.parse(response.text), {
      published: true,
      bulkResult: { failedEntries: [], entryCount: 2 },
    });

    const [bulkRequest] = dapr.requests.filter((request) => request.path === bulkPublishPath);
    assert.equal(bulkRequest.headers['content-type'], 'application/json');
    assert.deepEqual(JSON.parse(bulkRequest.body.toString()), [
      { entryId: 'e1', event: { orderId: 1 }, contentType: 'application/json', metadata: {} },
      {
        entryId: 'e2',
        event: { orderId: 2 },
        contentType: 'application/json',
        metadata: { partitionKey: 'p1' },
      },
    ]);
  }
);

test(
  'bulk publish partial failure calls done(BULK_PUBLISH_PARTIAL) with the failed entries, and never sends the message on',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', bulkPublishPath, (_req, res) =>
      res.writeHead(500).end(
        JSON.stringify({
          failedEntries: [{ entryId: 'e2', error: 'broker unavailable' }],
          errorCode: 'ERR_PUBSUB_PUBLISH_MESSAGE',
        })
      )
    );

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const appPort = await freePort();
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await dapr.waitForRequest(healthPath);
    await delay(50);

    const response = await postBulk(nr, [
      { entryId: 'e1', payload: 1 },
      { entryId: 'e2', payload: 2 },
    ]);
    assert.equal(response.status, 503, 'a partial bulk failure reaches a Catch node, not success');
    assert.deepEqual(JSON.parse(response.text), {
      message: 'DaprError: Dapr bulk publish reported 1 of 2 entries failed',
      code: 'BULK_PUBLISH_PARTIAL',
      bulkResult: {
        failedEntries: [{ entryId: 'e2', error: 'broker unavailable' }],
        errorCode: 'ERR_PUBSUB_PUBLISH_MESSAGE',
        entryCount: 2,
      },
    });
  }
);

test(
  'a malformed bulk batch (duplicate entryId) is rejected as INVALID_MESSAGE before contacting daprd',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', bulkPublishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const appPort = await freePort();
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await dapr.waitForRequest(healthPath);
    await delay(50);

    const response = await postBulk(nr, [
      { entryId: 'dup', payload: 1 },
      { entryId: 'dup', payload: 2 },
    ]);
    assert.equal(response.status, 503);
    assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
    assert.equal(
      dapr.requests.some((request) => request.path === bulkPublishPath),
      false,
      'a client-side validation failure must never reach the sidecar'
    );
  }
);

test(
  'a non-boolean msg.dapr.bulk override is rejected instead of being truthiness-coerced',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', bulkPublishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const appPort = await freePort();
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port }));
    await dapr.waitForRequest(healthPath);

    const response = await postBulk(nr, [{ entryId: 'e1', payload: 1 }], 'false');
    assert.equal(response.status, 503);
    assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
    assert.equal(
      dapr.requests.some((request) => request.path === bulkPublishPath),
      false,
      'an invalid mode override must never publish'
    );
  }
);

test(
  'msg.dapr.bulk enables or disables bulk mode for one message across redeploys',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', publishPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', bulkPublishPath, (_req, res) => res.writeHead(204).end());

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const appPort = await freePort();

    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port, bulkEnabled: true }));
    await dapr.waitForRequest(healthPath);
    assert.equal((await postBulk(nr, [{ entryId: 'single', payload: 1 }], false)).status, 200);
    assert.equal(dapr.requests.filter((request) => request.path === publishPath).length, 1);
    assert.equal(dapr.requests.filter((request) => request.path === bulkPublishPath).length, 0);

    const healthRequests = dapr.requests.filter((request) => request.path === healthPath).length;
    await nr.deploy(publishFlow({ appPort, daprPort: dapr.port, bulkEnabled: false }));
    await waitFor(
      () => dapr.requests.filter((request) => request.path === healthPath).length > healthRequests
    );
    assert.equal((await postBulk(nr, [{ entryId: 'bulk', payload: 1 }], true)).status, 200);
    assert.equal(dapr.requests.filter((request) => request.path === bulkPublishPath).length, 1);
  }
);
