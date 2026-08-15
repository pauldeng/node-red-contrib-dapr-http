'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');
const { waitFor } = require('../helpers/wait-for');
const { createSignal } = require('../helpers/signal');

// RED.auth.needsPermission('dapr-connection.read') guards the admin route
// (nodes/dapr-connection.js) but every request below succeeds with no auth
// header at all -- confirmed here, not assumed: this test harness's default
// settings configure no adminAuth, and Node-RED's own single-user default
// grants every permission string in that mode. A real deployment that
// configures adminAuth would enforce this permission normally.
const METADATA_PATH = '/v1.0/metadata';
const HEALTH_PATH = '/v1.0/healthz/outbound';

function connectionFlow({ appPort, daprPort, overrides = {} }) {
  return [
    { id: 'tab', type: 'tab', label: 'connection-metadata' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      ...overrides,
    },
  ];
}

function adminMetadata(nr, id = 'c1') {
  return httpRequest(nr.adminUrl(`/dapr-connection/${id}/metadata`), { timeoutMs: 5000 });
}

// A config node's constructor runs asynchronously relative to the admin
// POST /flows that deploy() awaits (test/helpers/node-red.js's own
// waitForHttp comment: "a route becomes live a short, non-deterministic
// moment after a deploy"). The health poll landing is this codebase's own
// established readiness signal for "the connection node has fully
// constructed" -- reused here instead of inventing a second one.
async function startFlow(t, respondents = []) {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', HEALTH_PATH, (_req, res) => res.writeHead(204).end());
  for (const [method, path, responder] of respondents) {
    dapr.respond(method, path, responder);
  }

  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  await nr.deploy(connectionFlow({ appPort, daprPort: dapr.port }));
  await dapr.waitForRequest(HEALTH_PATH);
  return { appPort, dapr, nr };
}

test('a deployed connection returns the curated metadata shape', async (t) => {
  const { nr } = await startFlow(t, [
    [
      'GET',
      METADATA_PATH,
      (_req, res) =>
        res.writeHead(200, { 'content-type': 'application/json' }).end(
          JSON.stringify({
            id: 'my-app',
            runtimeVersion: '1.18.2',
            components: [
              { name: 'pubsub', type: 'pubsub.redis', version: 'v1', capabilities: [] },
              { name: 'statestore', type: 'state.redis', version: 'v1' },
            ],
            subscriptions: [{ pubsubname: 'pubsub', topic: 'orders' }],
            extended: { daprRuntimeVersion: '1.18.2' },
            enabledFeatures: ['SomeFeature'],
          })
        ),
    ],
  ]);

  const response = await adminMetadata(nr);
  assert.equal(response.status, 200);
  assert.equal(response.headers['cache-control'], 'no-store');
  const body = JSON.parse(response.text);
  assert.deepEqual(body, {
    ok: true,
    id: 'my-app',
    runtimeVersion: '1.18.2',
    components: [
      { name: 'pubsub', type: 'pubsub.redis' },
      { name: 'statestore', type: 'state.redis' },
    ],
    componentCount: 2,
    subscriptionCount: 1,
  });
  // Only the curated fields ever reach the response -- confirm nothing else
  // (capabilities, extended, enabledFeatures) leaked through.
  assert.equal('capabilities' in body.components[0], false);
  assert.equal('extended' in body, false);
  assert.equal('enabledFeatures' in body, false);
});

test('an invalid deployed connection is distinct from an unknown id', async (t) => {
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  await nr.deploy(connectionFlow({ appPort, daprPort: 3500, overrides: { daprPort: '0' } }));
  await nr.waitForLog('daprPort must be an integer');

  const response = await adminMetadata(nr);
  assert.equal(response.status, 409);
  assert.deepEqual(JSON.parse(response.text), {
    ok: false,
    message: 'connection configuration is invalid',
  });
});

test('an undeployed connection id is reported as not found', async (t) => {
  const { nr } = await startFlow(t, []);

  const response = await adminMetadata(nr, 'no-such-connection');
  assert.equal(response.status, 404);
  const body = JSON.parse(response.text);
  assert.equal(body.ok, false);
  assert.match(body.message, /deploy it first/);
});

test('a non-2xx sidecar response is reported as a bounded 502, never the raw daprd body', async (t) => {
  const MARKER = 'raw-daprd-error-body-should-never-leak';
  const { nr } = await startFlow(t, [
    [
      'GET',
      METADATA_PATH,
      (_req, res) => res.writeHead(500).end(JSON.stringify({ message: MARKER })),
    ],
  ]);

  const response = await adminMetadata(nr);
  assert.equal(response.status, 502);
  const body = JSON.parse(response.text);
  assert.equal(body.ok, false);
  assert.ok(!response.text.includes(MARKER), 'the raw daprd error body must never leak');
});

test('an unreachable sidecar is reported as a bounded 502', async (t) => {
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  const closedPort = await freePort();
  await nr.deploy(connectionFlow({ appPort, daprPort: closedPort }));

  // No fake sidecar exists to observe a health-poll request against, so poll
  // the admin route itself until the connection node has actually
  // constructed (stops 404-ing) instead.
  const response = await waitFor(async () => {
    const r = await adminMetadata(nr);
    return r.status !== 404 ? r : null;
  });
  assert.equal(response.status, 502);
  const body = JSON.parse(response.text);
  assert.equal(body.ok, false);
  assert.match(body.message, /could not reach/);
});

test(
  'redeploy aborts an in-flight metadata request owned by the old connection',
  { timeout: 10000 },
  async (t) => {
    const metadataClosed = createSignal();
    const { appPort, dapr, nr } = await startFlow(t, [
      [
        'GET',
        METADATA_PATH,
        (_req, res) => {
          res.on('close', () => {
            metadataClosed.fire();
          });
        },
      ],
    ]);

    const pending = adminMetadata(nr).catch(() => null);
    await dapr.waitForRequest(METADATA_PATH);
    await nr.deploy(connectionFlow({ appPort, daprPort: dapr.port }));

    await metadataClosed.fired;
    await pending;
  }
);
