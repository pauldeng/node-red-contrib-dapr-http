'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createRuntimeFixture } = require('../helpers/runtime-fixture');
const useRuntime = createRuntimeFixture();
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');

const healthPath = '/v1.0/healthz/outbound';
const STORE = 'configstore';
const getPath = () => `/v1.0/configuration/${STORE}`;

function configGetFlow({ appPort, daprPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'config-get' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    { id: 'in', type: 'http in', z: 'tab', url: '/config', method: 'post', wires: [['before']] },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload; return msg;',
      outputs: 1,
      wires: [['get']],
    },
    {
      id: 'get',
      type: 'dapr-config-get',
      z: 'tab',
      connection: 'c1',
      storeName: STORE,
      keys: '',
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
      scope: ['get'],
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

function post(nr, dapr) {
  return httpRequest(nr.nodeUrl('/config'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(dapr),
    timeoutMs: 5000,
  });
}

async function startFlow(t, respondents = []) {
  const nr = await useRuntime(t);
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
  for (const [method, path, responder] of respondents) {
    dapr.respond(method, path, responder);
  }

  const appPort = await freePort();
  await nr.deploy(configGetFlow({ appPort, daprPort: dapr.port }));
  await dapr.waitForRequest(healthPath);
  return { dapr, nr };
}

test('a 204 (no items) resolves an empty object, not a Catch failure', async (t) => {
  const { nr, dapr } = await startFlow(t, [
    ['GET', getPath(), (_req, res) => res.writeHead(204).end()],
  ]);

  const response = await post(nr, {});
  assert.equal(response.status, 200);
  assert.deepEqual(JSON.parse(response.text).payload, {});
  assert.deepEqual(dapr.requests.find((r) => r.path === getPath()).query, {});
});

test('a non-2xx response reaches a Catch node as CONFIGURATION_OPERATION_FAILED', async (t) => {
  const { nr } = await startFlow(t, [
    [
      'GET',
      getPath(),
      (_req, res) =>
        res.writeHead(400).end(JSON.stringify({ errorCode: 'ERR_CONFIGURATION_STORE_NOT_FOUND' })),
    ],
  ]);

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'CONFIGURATION_OPERATION_FAILED');
});

test('configuration get calls fail fast while the sidecar is unhealthy, with no readiness wait', async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(503).end());

  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  await nr.deploy(configGetFlow({ appPort, daprPort: dapr.port }));
  await dapr.waitForRequest(healthPath);

  const started = Date.now();
  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'SIDECAR_UNAVAILABLE');
  assert.ok(Date.now() - started < 1500, 'an unhealthy sidecar must fail fast');
  assert.equal(
    dapr.requests.some((r) => r.path === getPath()),
    false
  );
});
