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
const STORE = 'vault';
const KEY = 'apiKey';
const getPath = () => `/v1.0/secrets/${STORE}/${KEY}`;

// A distinctive string standing in for a real secret's key/name -- a leak
// would be this exact substring surviving into the flow's own HTTP response.
const MARKER = 'sk-live-marker-should-never-leak';

function secretGetFlow({ appPort, daprPort, property, bodyLimitMb, statusCaptureUrl }) {
  const flow = [
    { id: 'tab', type: 'tab', label: 'secret-get' },
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
    { id: 'in', type: 'http in', z: 'tab', url: '/secret', method: 'post', wires: [['before']] },
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
      type: 'dapr-secret-get',
      z: 'tab',
      connection: 'c1',
      storeName: STORE,
      key: KEY,
      property: property || 'payload',
      metadata: '{}',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: 'msg.payload = { dapr: msg.dapr, payload: msg.payload, secret: msg.secret }; return msg;',
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
  if (statusCaptureUrl) {
    flow.push(
      {
        id: 'status-monitor',
        type: 'status',
        z: 'tab',
        scope: ['get'],
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

function post(nr, dapr, signal) {
  return httpRequest(nr.nodeUrl('/secret'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(dapr),
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
  await nr.deploy(secretGetFlow({ appPort, daprPort: dapr.port, ...flowOptions }));
  await dapr.waitForRequest(healthPath);
  return { dapr, nr, appPort };
}

test('a 200 response sets the default "payload" property to the store\'s own response', async (t) => {
  const { dapr, nr } = await startFlow(t, [
    [
      'GET',
      getPath(),
      (_req, res) =>
        res
          .writeHead(200, { 'content-type': 'application/json' })
          .end(JSON.stringify({ [KEY]: 'real-value' })),
    ],
  ]);

  const response = await post(nr, {});
  assert.equal(response.status, 200);
  const body = JSON.parse(response.text);
  assert.deepEqual(body.payload, { [KEY]: 'real-value' });
  assert.equal(body.dapr.statusCode, 200);
  assert.equal(body.dapr.storeName, STORE);
  assert.equal(body.dapr.key, KEY);

  const [received] = dapr.requests.filter((r) => r.path === getPath());
  assert.equal(received.method, 'GET');
});

test('a configured non-default property receives the result instead of msg.payload', async (t) => {
  const { nr } = await startFlow(
    t,
    [
      [
        'GET',
        getPath(),
        (_req, res) =>
          res
            .writeHead(200, { 'content-type': 'application/json' })
            .end(JSON.stringify({ [KEY]: 'v' })),
      ],
    ],
    { property: 'secret' }
  );

  const response = await post(nr, {});
  assert.equal(response.status, 200);
  const body = JSON.parse(response.text);
  // msg.payload was never touched by the node -- it still carries the
  // request body posted to this flow's own "in" node, proving the
  // configured "secret" property (not "payload") received the result.
  assert.deepEqual(body.payload, {});
  assert.deepEqual(body.secret, { [KEY]: 'v' });
});

test('a message-dependent output property is rejected before the secret is fetched', async (t) => {
  const { dapr, nr } = await startFlow(
    t,
    [
      [
        'GET',
        getPath(),
        (_req, res) =>
          res
            .writeHead(200, { 'content-type': 'application/json' })
            .end(JSON.stringify({ [KEY]: 'v' })),
      ],
    ],
    { property: 'payload[msg._msgid]' }
  );

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
  assert.equal(
    dapr.requests.some((request) => request.path === getPath()),
    false
  );
});

test('a prototype output property is rejected before the secret is fetched', async (t) => {
  const { dapr, nr } = await startFlow(
    t,
    [
      [
        'GET',
        getPath(),
        (_req, res) =>
          res
            .writeHead(200, { 'content-type': 'application/json' })
            .end(JSON.stringify({ [KEY]: 'v' })),
      ],
    ],
    { property: '__proto__.secret' }
  );

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
  assert.equal(
    dapr.requests.some((request) => request.path === getPath()),
    false
  );
});

test('an output property blocked by an existing primitive reaches Catch instead of reporting success', async (t) => {
  const { nr } = await startFlow(
    t,
    [
      [
        'GET',
        getPath(),
        (_req, res) =>
          res
            .writeHead(200, { 'content-type': 'application/json' })
            .end(JSON.stringify({ [KEY]: 'v' })),
      ],
    ],
    { property: '_msgid.value' }
  );

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
});

test('a 204 response resolves a null property value, not a Catch failure', async (t) => {
  const { nr } = await startFlow(t, [['GET', getPath(), (_req, res) => res.writeHead(204).end()]]);

  const response = await post(nr, {});
  assert.equal(response.status, 200);
  assert.equal(JSON.parse(response.text).payload, null);
});

test('an oversized response retains its error identity in Catch and node status', async (t) => {
  const capture = await startCapture();
  t.after(() => capture.stop());
  const { nr } = await startFlow(
    t,
    [
      [
        'GET',
        getPath(),
        (_req, res) =>
          res
            .writeHead(200, { 'content-type': 'application/json' })
            .end(JSON.stringify({ [KEY]: 'x'.repeat(2 ** 20 + 1) })),
      ],
    ],
    { bodyLimitMb: '1', statusCaptureUrl: capture.url }
  );

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'RESPONSE_TOO_LARGE');
  await capture.waitForMessage((status) => status?.text === 'response too large');
});

test("a 403 response reaches a Catch node as SECRET_ACCESS_DENIED, never carrying daprd's own body", async (t) => {
  const { nr } = await startFlow(t, [
    [
      'GET',
      getPath(),
      (_req, res) =>
        res.writeHead(403).end(
          JSON.stringify({
            errorCode: 'ERR_PERMISSION_DENIED',
            message: `denied for "${MARKER}"`,
          })
        ),
    ],
  ]);

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  const body = JSON.parse(response.text);
  assert.equal(body.code, 'SECRET_ACCESS_DENIED');
  assert.ok(!response.text.includes(MARKER), 'the flow response must never include the marker');
});

test("a 500 response reaches a Catch node as SECRET_OPERATION_FAILED, never carrying daprd's own body", async (t) => {
  const { nr } = await startFlow(t, [
    [
      'GET',
      getPath(),
      (_req, res) =>
        res.writeHead(500).end(
          JSON.stringify({
            errorCode: 'ERR_SECRET_GET',
            message: `failed getting secret with key ${MARKER} from secret store ${STORE}: secret ${MARKER} not found`,
          })
        ),
    ],
  ]);

  const response = await post(nr, {});
  assert.equal(response.status, 503);
  const body = JSON.parse(response.text);
  assert.equal(body.code, 'SECRET_OPERATION_FAILED');
  assert.ok(!response.text.includes(MARKER), 'the flow response must never include the marker');
});

test('missing storeName/key is rejected as INVALID_MESSAGE before contacting daprd', async (t) => {
  const { dapr, nr } = await startFlow(t, [
    ['GET', getPath(), (_req, res) => res.writeHead(200).end('{}')],
  ]);

  const response = await post(nr, { key: '' });
  assert.equal(response.status, 503);
  assert.equal(JSON.parse(response.text).code, 'INVALID_MESSAGE');
  assert.equal(
    dapr.requests.some((r) => r.path === getPath()),
    false
  );
});

test('secret get calls fail fast while the sidecar is unhealthy, with no readiness wait', async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(503).end());

  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  await nr.deploy(secretGetFlow({ appPort, daprPort: dapr.port }));
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

test(
  'an in-flight secret get is aborted when the node is redeployed',
  { timeout: 60000 },
  async (t) => {
    const aborted = createSignal();
    const { dapr, nr, appPort } = await startFlow(
      t,
      [
        [
          'GET',
          getPath(),
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
    await dapr.waitForRequest(getPath());
    assert.equal(aborted.hasFired, false);

    await nr.deploy(secretGetFlow({ appPort, daprPort: dapr.port }));
    await aborted.fired;
    // The node has proved cancellation; the removed HTTP response node cannot answer.
    caller.abort();
    await pending;
    assert.equal(aborted.hasFired, true);
  }
);
