'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');
const { startCapture } = require('../helpers/capture');

const healthPath = '/v1.0/healthz/outbound';
const STORE = 'configstore';
const KEY = 'featureFlag';
const getPath = `/v1.0/configuration/${STORE}`;
const subscribePath = `/v1.0/configuration/${STORE}/subscribe`;
const unsubscribePath = (id) => `/v1.0/configuration/${STORE}/${id}/unsubscribe`;

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

function configSubscribeFlow({ appPort, daprPort, captureUrl, key = KEY }) {
  return [
    { id: 'tab', type: 'tab', label: 'config-subscribe' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    {
      id: 'sub',
      type: 'dapr-config-subscribe',
      z: 'tab',
      connection: 'c1',
      storeName: STORE,
      keys: key,
      metadata: '{}',
      wires: [['fwd']],
    },
    {
      id: 'fwd',
      type: 'function',
      z: 'tab',
      func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ payload: msg.payload, dapr: msg.dapr });
return msg;`,
      outputs: 1,
      wires: [['req']],
    },
    { id: 'req', type: 'http request', z: 'tab', method: 'use', ret: 'txt', url: '', wires: [[]] },
  ];
}

// Simulates daprd's own async push into the app's registered callback route
// -- a real HTTP call from the (fake-sidecar) test process back into the real
// Node-RED child process under test, exactly the way real daprd would.
function pushConfigChange(appPort, body, { delayMs = 30, headers = {}, key = KEY } = {}) {
  return new Promise((resolve) => {
    setTimeout(() => {
      httpRequest(
        new URL(`/configuration/${STORE}/${key}`, `http://127.0.0.1:${appPort}`).toString(),
        {
          method: 'POST',
          headers: { 'content-type': 'application/json', ...headers },
          body: JSON.stringify(body),
          timeoutMs: 4000,
        }
      ).then(resolve, resolve);
    }, delayMs);
  });
}

function respondEmptyGet(dapr) {
  dapr.respond('GET', getPath, (_req, res) =>
    res.writeHead(200, { 'content-type': 'application/json' }).end('{}')
  );
}

test(
  'emits the initial snapshot and rejects malformed or mesh-originated callbacks',
  { timeout: 60000 },
  async (t) => {
    const routeKey = 'customer:feature flag';
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('GET', subscribePath, (_req, res) =>
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ id: 'sub-1' }))
    );
    dapr.respond('GET', getPath, (_req, res) =>
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ [routeKey]: { value: 'initial', version: '1' } }))
    );

    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy(
      configSubscribeFlow({ appPort, daprPort: dapr.port, captureUrl: capture.url, key: routeKey })
    );

    const initial = await waitFor(() => capture.received[0] || null);
    assert.deepEqual(initial.payload, { [routeKey]: { value: 'initial', version: '1' } });

    const malformed = await pushConfigChange(
      appPort,
      { id: 'sub-1', items: ['not', 'an', 'object'] },
      { delayMs: 0, key: routeKey }
    );
    assert.equal(malformed.status, 400);

    const meshCaller = await pushConfigChange(
      appPort,
      { id: 'sub-1', items: { featureFlag: { value: 'forged' } } },
      { delayMs: 0, headers: { 'dapr-caller-app-id': 'evil-app' }, key: routeKey }
    );
    assert.equal(meshCaller.status, 403);
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(capture.received.length, 1);
  }
);

test(
  'subscribe succeeds, and a real push from daprd delivers a message',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    respondEmptyGet(dapr);
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const capture = await startCapture();
    t.after(() => capture.stop());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    let appPortSeen;
    dapr.respond('GET', subscribePath, (_req, res) => {
      appPortSeen = appPort;
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ id: 'sub-1' }));
      pushConfigChange(appPort, {
        id: 'sub-1',
        items: { featureFlag: { value: 'true', version: '2' } },
      });
    });

    await nr.deploy(configSubscribeFlow({ appPort, daprPort: dapr.port, captureUrl: capture.url }));
    await waitFor(() => dapr.requests.find((request) => request.path === healthPath));

    const received = await waitFor(
      () =>
        capture.received.find((message) => message?.payload?.featureFlag?.value === 'true') || null
    );
    assert.equal(appPortSeen, appPort);
    assert.deepEqual(received.payload, { featureFlag: { value: 'true', version: '2' } });
    assert.equal(received.dapr.storeName, STORE);
    assert.deepEqual(received.dapr.keys, [KEY]);
    assert.equal(received.dapr.subscriptionId, 'sub-1');

    const [subscribeRequest] = dapr.requests.filter((r) => r.path === subscribePath);
    assert.deepEqual(subscribeRequest.query, { key: KEY });
  }
);

test(
  'a callback carrying a stale (superseded) subscription id is dropped silently',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    respondEmptyGet(dapr);
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('GET', subscribePath, (_req, res) =>
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ id: 'sub-real' }))
    );

    const capture = await startCapture();
    t.after(() => capture.stop());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy(configSubscribeFlow({ appPort, daprPort: dapr.port, captureUrl: capture.url }));
    await waitFor(() => dapr.requests.find((request) => request.path === healthPath));
    await waitFor(() => dapr.requests.find((request) => request.path === subscribePath));
    await new Promise((resolve) => setTimeout(resolve, 100)); // let subscriptionId land

    // A push carrying an id from a different (superseded) subscription --
    // daprd already got a 2xx for it, so this must never reach the flow.
    const stale = await pushConfigChange(
      appPort,
      { id: 'sub-old', items: { featureFlag: { value: 'stale', version: '1' } } },
      { delayMs: 0 }
    );
    assert.equal(stale.status, 200);
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.equal(capture.received.length, 1, 'a stale-id callback must never reach the flow');

    // The real subscription id delivers normally.
    await pushConfigChange(
      appPort,
      { id: 'sub-real', items: { featureFlag: { value: 'true', version: '2' } } },
      { delayMs: 0 }
    );
    const received = await waitFor(
      () =>
        capture.received.find((message) => message?.payload?.featureFlag?.value === 'true') || null
    );
    assert.deepEqual(received.payload, { featureFlag: { value: 'true', version: '2' } });
  }
);

test(
  'a subscribe attempt is gated on sidecar health, and resubscribes once health recovers',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    respondEmptyGet(dapr);
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(503).end());
    dapr.respond('GET', subscribePath, (_req, res) =>
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ id: 'sub-1' }))
    );

    const capture = await startCapture();
    t.after(() => capture.stop());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy(configSubscribeFlow({ appPort, daprPort: dapr.port, captureUrl: capture.url }));
    await waitFor(() => dapr.requests.find((request) => request.path === healthPath));
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.equal(
      dapr.requests.some((r) => r.path === subscribePath),
      false,
      'no subscribe attempt while the sidecar is unhealthy'
    );

    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    await waitFor(() => dapr.requests.find((request) => request.path === subscribePath));

    await pushConfigChange(appPort, {
      id: 'sub-1',
      items: { featureFlag: { value: 'true', version: '2' } },
    });
    const received = await waitFor(
      () =>
        capture.received.find((message) => message?.payload?.featureFlag?.value === 'true') || null
    );
    assert.deepEqual(received.payload, { featureFlag: { value: 'true', version: '2' } });
  }
);

test(
  'a health recovery retires the previous subscription before replacing it',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    respondEmptyGet(dapr);
    let healthy = true;
    let subscribeCount = 0;
    dapr.respond('GET', healthPath, (_req, res) =>
      healthy ? res.writeHead(204).end() : res.writeHead(503).end()
    );
    dapr.respond('GET', subscribePath, (_req, res) => {
      subscribeCount += 1;
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ id: `sub-${subscribeCount}` }));
    });
    dapr.respond('GET', getPath, (_req, res) =>
      res.writeHead(200, { 'content-type': 'application/json' }).end('{}')
    );
    dapr.respond('GET', unsubscribePath('sub-1'), (_req, res) =>
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ ok: true }))
    );

    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy(configSubscribeFlow({ appPort, daprPort: dapr.port, captureUrl: capture.url }));
    await waitFor(() => subscribeCount === 1);

    healthy = false;
    await waitFor(() => (/Dapr sidecar is unavailable/.test(nr.logText()) ? true : null), {
      timeoutMs: 15000,
    });
    healthy = true;
    await waitFor(() => subscribeCount === 2);

    const unsubscribeIndex = dapr.requests.findIndex((r) => r.path === unsubscribePath('sub-1'));
    const secondSubscribeIndex = dapr.requests.findIndex(
      (r, index) => r.path === subscribePath && index > unsubscribeIndex
    );
    assert.ok(unsubscribeIndex >= 0, 'the prior subscription must be retired');
    assert.ok(secondSubscribeIndex > unsubscribeIndex, 'retirement must finish before replacement');
  }
);

test(
  'on redeploy, callback routes are unregistered and unsubscribe is called, bounded even if the sidecar never answers',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    respondEmptyGet(dapr);
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('GET', subscribePath, (_req, res) =>
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ id: 'sub-1' }))
    );
    // The unsubscribe call is never answered -- close() must not hang on it.
    dapr.respond('GET', unsubscribePath('sub-1'), () => {});

    const capture = await startCapture();
    t.after(() => capture.stop());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy(configSubscribeFlow({ appPort, daprPort: dapr.port, captureUrl: capture.url }));
    await waitFor(() => dapr.requests.find((request) => request.path === subscribePath));
    await new Promise((resolve) => setTimeout(resolve, 100)); // let subscriptionId land

    const started = Date.now();
    // A "nodes"-only redeploy that drops the subscribe node but keeps the
    // connection (and its app-channel listener) alive, so a 404 afterward is
    // attributable to the route being gone, not the whole listener closing.
    await nr.deploy(
      [
        {
          id: 'c1',
          type: 'dapr-connection',
          name: 'sidecar',
          daprHost: '127.0.0.1',
          daprPort: String(dapr.port),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
      ],
      { deploymentType: 'nodes' }
    );
    await waitFor(() => dapr.requests.find((request) => request.path === unsubscribePath('sub-1')));
    assert.ok(
      Date.now() - started < 10000,
      'the redeploy must complete promptly even though the sidecar never answers unsubscribe'
    );

    // The callback route is no longer dispatched: like a removed pub/sub
    // delivery route, a stale sidecar gets a retryable 503 rather than reaching
    // a node that no longer exists.
    const afterTeardown = await httpRequest(
      `http://127.0.0.1:${appPort}/configuration/${STORE}/${KEY}`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ id: 'sub-1', items: {} }),
        timeoutMs: 2000,
      }
    );
    assert.equal(afterTeardown.status, 503);
  }
);
