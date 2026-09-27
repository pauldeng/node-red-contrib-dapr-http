'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createRuntimeFixture } = require('../helpers/runtime-fixture');
const useRuntime = createRuntimeFixture();
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');
const { createSignal } = require('../helpers/signal');

const healthPath = '/v1.0/healthz/outbound';
const STORE = 'orders-store';
const getPath = (key) => `/v1.0/state/${STORE}/${key}`;
const savePath = `/v1.0/state/${STORE}`;
const transactionPath = `/v1.0/state/${STORE}/transaction`;

function stateFlow({ appPort, daprPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'state' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      requestTimeoutSec: '2',
    },
    {
      id: 'in',
      type: 'http in',
      z: 'tab',
      url: '/state',
      method: 'post',
      wires: [['before']],
    },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload.dapr; msg.payload = msg.payload.payload; return msg;',
      outputs: 1,
      wires: [['state']],
    },
    {
      id: 'state',
      type: 'dapr-state',
      z: 'tab',
      connection: 'c1',
      operation: 'get',
      storeName: STORE,
      key: '',
      consistency: '',
      concurrency: '',
      ttlSeconds: '',
      metadata: '{}',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: 'msg.payload = { dapr: msg.dapr, payload: msg.payload }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    {
      id: 'errors',
      type: 'catch',
      z: 'tab',
      scope: ['state'],
      uncaught: false,
      wires: [['failure']],
    },
    {
      id: 'failure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab', statusCode: '', headers: {} },
  ];
}

function post(nr, dapr, payload, signal) {
  return httpRequest(nr.nodeUrl('/state'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ dapr, payload }),
    timeoutMs: 5000,
    signal,
  });
}

async function startFlow(t, respondents = [], { fresh = false } = {}) {
  const nr = await useRuntime(t, { fresh });
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
  for (const [method, path, responder] of respondents) {
    dapr.respond(method, path, responder);
  }

  const appPort = await freePort();
  await nr.deploy(stateFlow({ appPort, daprPort: dapr.port }));
  await dapr.waitForRequest(healthPath);
  return { dapr, nr, appPort };
}

test('save: POSTs a single-item array and passes msg through', async (t) => {
  const { dapr, nr } = await startFlow(t, [
    ['POST', savePath, (_req, res) => res.writeHead(204).end()],
  ]);

  const response = await post(nr, { operation: 'save', key: 'order-2', etag: 'v1' }, { total: 99 });
  assert.equal(response.status, 200);
  const body = JSON.parse(response.text);
  assert.deepEqual(body.payload, { total: 99 });
  assert.equal(body.dapr.statusCode, 204);

  const [received] = dapr.requests.filter((r) => r.path === savePath);
  assert.deepEqual(JSON.parse(received.body.toString()), [
    { key: 'order-2', value: { total: 99 }, etag: 'v1' },
  ]);
});

test('delete: sends the etag as an If-Match header, not a query param', async (t) => {
  const { dapr, nr } = await startFlow(t, [
    ['DELETE', getPath('order-3'), (_req, res) => res.writeHead(204).end()],
  ]);

  const response = await post(nr, { operation: 'delete', key: 'order-3', etag: 'v2' });
  assert.equal(response.status, 200);

  const [received] = dapr.requests.filter((r) => r.path === getPath('order-3'));
  assert.equal(received.method, 'DELETE');
  assert.equal(received.headers['if-match'], 'v2');
});

test('transaction: an operation of "set" (not "upsert"/"delete") is rejected before contacting daprd', async (t) => {
  const { dapr, nr } = await startFlow(t, [
    ['POST', transactionPath, (_req, res) => res.writeHead(204).end()],
  ]);

  const response = await post(nr, { operation: 'transaction' }, [
    { operation: 'set', key: 'a', value: 1 },
  ]);
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
  assert.equal(
    dapr.requests.some((r) => r.path === transactionPath),
    false
  );
});

test('a missing key for get is rejected as INVALID_MESSAGE before contacting daprd', async (t) => {
  const { dapr, nr } = await startFlow(t);

  const response = await post(nr, { operation: 'get', key: '' });
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
  assert.equal(
    dapr.requests.some((r) => r.path.startsWith('/v1.0/state/')),
    false
  );
});

test('state calls fail fast while the sidecar is unhealthy, with no readiness wait', async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(503).end());

  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  await nr.deploy(stateFlow({ appPort, daprPort: dapr.port }));
  await dapr.waitForRequest(healthPath);

  const started = Date.now();
  const response = await post(nr, { operation: 'get', key: 'order-1' });
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'SIDECAR_UNAVAILABLE');
  assert.ok(Date.now() - started < 1500, 'an unhealthy sidecar must fail fast');
  assert.equal(
    dapr.requests.some((r) => r.path === getPath('order-1')),
    false
  );
});

test(
  'an in-flight state call is aborted when the node is redeployed',
  { timeout: 60000 },
  async (t) => {
    const aborted = createSignal();
    const { dapr, nr, appPort } = await startFlow(
      t,
      [
        [
          'GET',
          getPath('hang'),
          (_req, res) => {
            res.on('close', () => {
              if (!res.writableEnded) {
                aborted.fire();
              }
            });
          },
        ],
      ],
      { fresh: true }
    );

    const caller = new AbortController();
    t.after(() => caller.abort());
    const pending = (async () => {
      try {
        await post(nr, { operation: 'get', key: 'hang' }, undefined, caller.signal);
      } catch {
        // Redeploy intentionally interrupts this request.
      }
    })();
    await dapr.waitForRequest(getPath('hang'));
    assert.equal(aborted.hasFired, false);

    await nr.deploy(stateFlow({ appPort, daprPort: dapr.port }));
    await aborted.fired;
    // The node has proved cancellation; the removed HTTP response node cannot answer.
    caller.abort();
    await pending;
    assert.equal(aborted.hasFired, true);
  }
);
