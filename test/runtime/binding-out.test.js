'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createRuntimeFixture } = require('../helpers/runtime-fixture');
const useRuntime = createRuntimeFixture();
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');
const { startCapture } = require('../helpers/capture');
const { createSignal } = require('../helpers/signal');

const healthPath = '/v1.0/healthz/outbound';
const BINDING = 'orders-binding';
const invokePath = () => `/v1.0/bindings/${BINDING}`;

function bindingFlow({ appPort, daprPort, bodyLimitMb, statusCaptureUrl }) {
  const flow = [
    { id: 'tab', type: 'tab', label: 'binding-out' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      bodyLimitMb,
    },
    { id: 'in', type: 'http in', z: 'tab', url: '/binding', method: 'post', wires: [['before']] },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: `msg.dapr = msg.payload.dapr;
if (msg.payload.payload?.__circular) {
  msg.payload = {};
  msg.payload.self = msg.payload;
} else {
  msg.payload = msg.payload.payload;
}
return msg;`,
      outputs: 1,
      wires: [['out']],
    },
    {
      id: 'out',
      type: 'dapr-binding-out',
      z: 'tab',
      connection: 'c1',
      bindingName: BINDING,
      operation: 'create',
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
      scope: ['out'],
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
  if (statusCaptureUrl) {
    flow.push(
      {
        id: 'status-monitor',
        type: 'status',
        z: 'tab',
        scope: ['out'],
        wires: [['status-body']],
      },
      {
        id: 'status-body',
        type: 'function',
        z: 'tab',
        func: `msg.url = ${JSON.stringify(statusCaptureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify(msg.status);
return msg;`,
        outputs: 1,
        wires: [['status-request']],
      },
      {
        id: 'status-request',
        type: 'http request',
        z: 'tab',
        method: 'use',
        ret: 'txt',
        url: '',
        wires: [[]],
      }
    );
  }
  return flow;
}

function post(nr, { dapr = {}, payload = null } = {}, signal) {
  return httpRequest(nr.nodeUrl('/binding'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ dapr, payload }),
    timeoutMs: 5000,
    signal,
  });
}

async function startFlow(t, respondents = [], flowOptions = {}, { fresh = false } = {}) {
  const nr = await useRuntime(t, { fresh });
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
  for (const [method, path, responder] of respondents) {
    dapr.respond(method, path, responder);
  }

  const appPort = await freePort();
  await nr.deploy(bindingFlow({ appPort, daprPort: dapr.port, ...flowOptions }));
  await dapr.waitForRequest(healthPath);
  return { dapr, nr, appPort };
}

test('a 200 response decodes the component body and carries the data/operation through', async (t) => {
  const { dapr, nr } = await startFlow(t, [
    [
      'POST',
      invokePath(),
      (_req, res) =>
        res
          .writeHead(200, { 'content-type': 'application/json', 'metadata.statusCode': '201' })
          .end(JSON.stringify({ id: 'created-1' })),
    ],
  ]);

  const response = await post(nr, { payload: { id: 1 } });
  assert.equal(response.status, 200);
  const body = JSON.parse(response.text);
  assert.deepEqual(body.payload, { id: 'created-1' });
  assert.equal(body.dapr.statusCode, 200);
  assert.equal(body.dapr.bindingName, BINDING);
  assert.equal(body.dapr.operation, 'create');
  // Node's http client lower-cases every incoming header name.
  assert.equal(body.dapr.metadata.statuscode, '201');

  const [received] = dapr.requests.filter((r) => r.path === invokePath());
  assert.equal(received.method, 'POST');
  assert.deepEqual(JSON.parse(received.body), {
    data: { id: 1 },
    metadata: {},
    operation: 'create',
  });
});

test('a 204 response resolves a null payload, not a Catch failure', async (t) => {
  const { nr } = await startFlow(t, [
    ['POST', invokePath(), (_req, res) => res.writeHead(204).end()],
  ]);

  const response = await post(nr, {});
  assert.equal(response.status, 200);
  assert.equal(JSON.parse(response.text).payload, null);
});

test('a non-2xx, non-204 response reaches a Catch node as BINDING_INVOKE_FAILED', async (t) => {
  const { nr } = await startFlow(t, [
    [
      'POST',
      invokePath(),
      (_req, res) =>
        res.writeHead(500).end(JSON.stringify({ errorCode: 'ERR_INVOKE_OUTPUT_BINDING' })),
    ],
  ]);

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'BINDING_INVOKE_FAILED');
});

test('late validation and oversized responses retain their error identity in Catch and status', async (t) => {
  const capture = await startCapture();
  t.after(() => capture.stop());
  const { nr } = await startFlow(
    t,
    [['POST', invokePath(), (_req, res) => res.writeHead(200).end('x'.repeat(2 ** 20 + 1))]],
    { bodyLimitMb: '1', statusCaptureUrl: capture.url }
  );

  const invalid = await post(nr, { payload: { __circular: true } });
  assert.equal(invalid.status, 503);
  assert.equal(JSON.parse(invalid.text).code, 'INVALID_MESSAGE');
  await capture.waitForMessage((status) => status?.text === 'invalid message');

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'RESPONSE_TOO_LARGE');
  await capture.waitForMessage((status) => status?.text === 'response too large');
});

test('missing bindingName/operation is rejected as INVALID_MESSAGE before contacting daprd', async (t) => {
  const { dapr, nr } = await startFlow(t, [
    ['POST', invokePath(), (_req, res) => res.writeHead(200).end()],
  ]);

  const response = await post(nr, { dapr: { operation: '' } });
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
  assert.equal(
    dapr.requests.some((r) => r.path === invokePath()),
    false
  );
});

test('binding invoke calls fail fast while the sidecar is unhealthy, with no readiness wait', async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(503).end());

  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  await nr.deploy(bindingFlow({ appPort, daprPort: dapr.port }));
  await dapr.waitForRequest(healthPath);

  const started = Date.now();
  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'SIDECAR_UNAVAILABLE');
  assert.ok(Date.now() - started < 1500, 'an unhealthy sidecar must fail fast');
  assert.equal(
    dapr.requests.some((r) => r.path === invokePath()),
    false
  );
});

test(
  'an in-flight binding invoke is aborted when the node is redeployed',
  { timeout: 60000 },
  async (t) => {
    const aborted = createSignal();
    const { dapr, nr, appPort } = await startFlow(
      t,
      [
        [
          'POST',
          invokePath(),
          (_req, res) => {
            res.on('close', () => {
              if (!res.writableEnded) {
                aborted.fire();
              }
            });
          },
        ],
      ],
      {},
      { fresh: true }
    );

    const caller = new AbortController();
    t.after(() => caller.abort());
    const pending = (async () => {
      try {
        await post(nr, {}, caller.signal);
      } catch {
        // Redeploy intentionally interrupts this request.
      }
    })();
    await dapr.waitForRequest(invokePath());
    assert.equal(aborted.hasFired, false);

    await nr.deploy(bindingFlow({ appPort, daprPort: dapr.port }));
    await aborted.fired;
    // The node has proved cancellation; the removed HTTP response node cannot answer.
    caller.abort();
    await pending;
    assert.equal(aborted.hasFired, true);
  }
);
