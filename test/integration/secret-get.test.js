'use strict';

// Real daprd 1.18.2 + a real secretstores.local.file component -- secrets
// need no broker or store at all (mirrors acl/invoke/shutdown/binding-out's
// own broker-agnostic setup), so this belongs in the test:integration:dapr
// bucket, not test:integration:redis.
//
// Confirmed against dapr/dapr v1.18.1 source (pkg/api/http/secrets.go,
// pkg/api/universal/secrets.go, pkg/messages/predefined.go) and
// components-contrib's secretstores/local/file/filestore.go: a store-scoping
// denial (403/ERR_PERMISSION_DENIED) happens BEFORE the component is ever
// called, so it never overlaps with "key not found in the store"
// (500/ERR_SECRET_GET) -- both real daprd outcomes are exercised here
// against ONE Configuration resource (test/integration/fixtures/secret-scopes.yaml:
// allow by default, deny "forbiddenKey" specifically). Both of daprd's own
// real error messages for these failures embed the requested key by name
// ("secret %s not found", "access denied by policy to get %q from %q") --
// this suite's central assertion is that neither the key nor daprd's raw
// message ever reaches this flow's own HTTP response, proving the
// leak-prevention design holds against real daprd, not just a unit fake.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd, secretComponentYaml } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

const STORE = 'secretstore';
const PRESENT_KEY = 'apiKey';
const PRESENT_VALUE = 'it-test-secret-value';
const ABSENT_KEY = 'missingKey';
const DENIED_KEY = 'forbiddenKey';

function flow({ appPort, daprHttpPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-secret-get' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
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
      key: PRESENT_KEY,
      property: 'payload',
      metadata: '{}',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200; msg.payload = { dapr: msg.dapr, payload: msg.payload }; return msg;',
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
    { id: 'res', type: 'http response', z: 'tab' },
  ];
}

function get(nr, { key } = {}) {
  return httpRequest(nr.nodeUrl('/secret'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(key ? { key } : {}),
    timeoutMs: 5000,
  });
}

test(
  'a real secretstores.local.file component distinguishes present, absent, and denied secrets -- and never leaks a key into the flow response',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    await nr.start({ flows: flow({ appPort, daprHttpPort }) });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const daprd = await startDaprd({
      appId: 'it-secret-get-app',
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'secretstore.yaml', yaml: secretComponentYaml() }],
      extraFixtures: ['secrets.json'],
      configFixture: 'secret-scopes.yaml',
    });
    t.after(() => daprd?.stop());

    // startDaprd() resolving only proves daprd's OWN /healthz answers; the
    // dapr-connection node polls independently on its own bounded-backoff
    // schedule and may not have caught up yet.
    await waitFor(() => (nr.logText().includes('Dapr sidecar is available') ? true : null));

    // A real, present, allowed secret.
    const present = await get(nr);
    assert.equal(present.status, 200);
    const presentBody = JSON.parse(present.text);
    assert.deepEqual(presentBody.payload, { [PRESENT_KEY]: PRESENT_VALUE });
    assert.equal(presentBody.dapr.statusCode, 200);
    assert.equal(presentBody.dapr.storeName, STORE);
    assert.equal(presentBody.dapr.key, PRESENT_KEY);

    // Allowed by scope, but genuinely absent from the file -- real daprd's
    // own ERR_SECRET_GET message embeds this exact key; the assertion below
    // proves it never reaches this flow's own HTTP response.
    const absent = await get(nr, { key: ABSENT_KEY });
    assert.equal(absent.status, 503);
    const absentBody = JSON.parse(absent.text);
    assert.equal(absentBody.code, 'SECRET_OPERATION_FAILED');
    assert.ok(
      !absent.text.includes(ABSENT_KEY),
      'the flow response must never include the requested key'
    );

    // Denied by real Dapr secret-scoping, before the component is ever
    // called -- a reliably distinct outcome from "absent."
    const denied = await get(nr, { key: DENIED_KEY });
    assert.equal(denied.status, 503);
    const deniedBody = JSON.parse(denied.text);
    assert.equal(deniedBody.code, 'SECRET_ACCESS_DENIED');
    assert.ok(
      !denied.text.includes(DENIED_KEY),
      'the flow response must never include the requested key'
    );
  }
);
