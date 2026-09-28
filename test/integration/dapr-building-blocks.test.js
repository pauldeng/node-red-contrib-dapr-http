'use strict';

// Real daprd 1.18.x, no broker or store at all: service invocation, output
// bindings, secrets, and Test Connection metadata all share this one
// ContainerNodeRed + one daprd session (mirrors acl.test.js/shutdown.test.js's
// own broker-agnostic setup), run as sequential subtests. Combined from four
// previously separate files (invoke-matrix.test.js, binding-out.test.js,
// secret-get.test.js, metadata.test.js) — every assertion below is unchanged
// from those files except the metadata subtest, which now expects BOTH
// components this shared daprd registers (see that subtest for detail).
//
// bindings.http's Invoke (components-contrib v1.18.0, bindings/http/http.go):
// operation is upper-cased into an HTTP method ("post"/"create" -> POST,
// sending req.Data as the request body); on a 2xx target response, the
// component's real response body and {statusCode, status, <headers>}
// metadata pass straight through. On a non-2xx target response (with the
// component's own default errorIfNot2XX=true), the component still builds
// that same response+metadata but ALSO returns an error -- daprd's handler
// then discards the real response entirely and answers with the generic
// 500 ERR_INVOKE_OUTPUT_BINDING envelope instead (confirmed against source,
// documented in nodes/dapr-binding-out.html as a real gap, not hidden here).
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
const http = require('node:http');
const { readFile } = require('node:fs/promises');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd, bindingComponentYaml, secretComponentYaml } = require('../helpers/integration');
const { startCapture } = require('../helpers/capture');
const { waitFor } = require('../helpers/wait-for');

// ../helpers/http's httpRequest decodes the response body via .toString()
// (utf8), which is lossy for arbitrary bytes >= 0x80 — fine for every other
// assertion here, but not for the byte-exact binary-response check below.
// This keeps the response as a raw Buffer instead.
function rawHttpRequest(urlStr, { method = 'GET', headers = {}, body, timeoutMs = 10000 } = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const req = http.request(
      { hostname: url.hostname, port: url.port, path: url.pathname, method, headers, agent: false },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () =>
          resolve({ status: res.statusCode, headers: res.headers, buffer: Buffer.concat(chunks) })
        );
      }
    );
    req.on('error', reject);
    const timer = setTimeout(
      () => req.destroy(new Error(`request exceeded ${timeoutMs}ms`)),
      timeoutMs
    );
    req.on('close', () => clearTimeout(timer));
    if (body !== undefined) {
      req.write(body);
    }
    req.end();
  });
}

function echoService(id, verb) {
  return {
    id,
    type: 'dapr-service',
    z: 'tab',
    connection: 'c1',
    verb,
    methodPath: '/echo',
    wires: [['reflect']],
  };
}

const BINDING = 'orders-binding';
const MISSING_BINDING = 'no-such-binding';
const SECRET_STORE = 'secretstore';
const PRESENT_KEY = 'apiKey';
const PRESENT_VALUE = 'it-test-secret-value';
const ABSENT_KEY = 'missingKey';
const DENIED_KEY = 'forbiddenKey';

function buildFlow() {
  return [
    { id: 'tab', type: 'tab', label: 'it-dapr-building-blocks' },
    // c1 (dapr-connection) is spliced in by the test once ports are known.

    // -- invoke: verbs, query, headers, binary, non-2xx, unknown method --
    echoService('svc-get', 'GET'),
    echoService('svc-post', 'POST'),
    echoService('svc-put', 'PUT'),
    echoService('svc-patch', 'PATCH'),
    echoService('svc-delete', 'DELETE'),
    {
      id: 'reflect',
      type: 'function',
      z: 'tab',
      func: `msg.dapr.statusCode = 200;
msg.dapr.responseHeaders = { 'x-echo-verb': msg.dapr.verb };
msg.payload = {
  verb: msg.dapr.verb,
  query: msg.dapr.query,
  custom: msg.dapr.headers['x-custom'] || null,
  body: msg.payload,
};
return msg;`,
      outputs: 1,
      wires: [['resp']],
    },
    { id: 'resp', type: 'dapr-response', z: 'tab', connection: 'c1', statusCode: '200', wires: [] },

    {
      id: 'svc-binary',
      type: 'dapr-service',
      z: 'tab',
      connection: 'c1',
      verb: 'PUT',
      methodPath: '/binary-echo',
      wires: [['binaryEcho']],
    },
    {
      id: 'binaryEcho',
      type: 'function',
      z: 'tab',
      func: `msg.dapr.statusCode = 200;
msg.dapr.contentType = 'application/octet-stream';
return msg;`,
      outputs: 1,
      wires: [['resp']],
    },

    {
      id: 'svc-fail',
      type: 'dapr-service',
      z: 'tab',
      connection: 'c1',
      verb: 'POST',
      methodPath: '/fail',
      wires: [['failer']],
    },
    {
      id: 'failer',
      type: 'function',
      z: 'tab',
      func: `msg.dapr.statusCode = 409;
msg.payload = { error: 'conflict' };
return msg;`,
      outputs: 1,
      wires: [['resp']],
    },

    {
      id: 'svc-slow',
      type: 'dapr-service',
      z: 'tab',
      connection: 'c1',
      verb: 'GET',
      methodPath: '/slow',
      wires: [['delayNode']],
    },
    {
      // Node-RED's config-node dependency scanner treats any string PROPERTY
      // VALUE that matches another node's id as a reference to it. Naming this
      // node 'delay' (its own type name) while also setting pauseType: 'delay'
      // makes it look like a self-reference — "Circular config node dependency
      // detected: delay" — which silently aborts the rest of that tab's node
      // startup. Any id other than the literal string 'delay' avoids it.
      id: 'delayNode',
      type: 'delay',
      z: 'tab',
      pauseType: 'delay',
      timeout: 4,
      timeoutUnits: 'seconds',
      rate: 1,
      nbRateUnits: '1',
      rateUnits: 'second',
      randomFirst: '1',
      randomLast: '5',
      randomUnits: 'seconds',
      drop: false,
      wires: [['slowResp']],
    },
    {
      id: 'slowResp',
      type: 'function',
      z: 'tab',
      func: `msg.dapr.statusCode = 200;
msg.payload = { slow: true };
return msg;`,
      outputs: 1,
      wires: [['resp']],
    },

    // Reuse the same outbound invoke node for the successful round trip.
    {
      id: 'echo-in',
      type: 'http in',
      z: 'tab',
      url: '/call-echo',
      method: 'post',
      wires: [['echo-override']],
    },
    {
      id: 'echo-override',
      type: 'function',
      z: 'tab',
      func: "msg.dapr = { method: 'echo', verb: 'POST' }; return msg;",
      outputs: 1,
      wires: [['inv']],
    },

    // Outbound side: dapr-invoke, triggered over HTTP, targeting /slow with a
    // 1s per-message timeout override — proves the OUTBOUND direction also
    // aborts against a real, genuinely slow real-daprd-mediated call, not just
    // a fake sidecar that never replies.
    {
      id: 'trigger',
      type: 'http in',
      z: 'tab',
      url: '/call-slow',
      method: 'post',
      wires: [['setOverride']],
    },
    {
      id: 'setOverride',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = { timeoutSec: 1 }; return msg;',
      outputs: 1,
      wires: [['inv']],
    },
    {
      id: 'inv',
      type: 'dapr-invoke',
      z: 'tab',
      connection: 'c1',
      // appId is spliced in by the test once it's known.
      method: 'slow',
      verb: 'GET',
      wires: [['invOut']],
    },
    {
      id: 'invOut',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200; return msg;',
      outputs: 1,
      wires: [['invHttpRes']],
    },
    { id: 'invHttpRes', type: 'http response', z: 'tab' },
    { id: 'catchInv', type: 'catch', z: 'tab', scope: ['inv'], wires: [['onInvErr']] },
    {
      id: 'onInvErr',
      type: 'function',
      z: 'tab',
      func: "msg.statusCode = 504; msg.payload = 'timeout'; return msg;",
      outputs: 1,
      wires: [['invHttpRes']],
    },

    // -- binding-out: real bindings.http component round trip --
    {
      id: 'bindIn',
      type: 'http in',
      z: 'tab',
      url: '/invoke',
      method: 'post',
      wires: [['bindBefore']],
    },
    {
      id: 'bindBefore',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload.dapr; msg.payload = msg.payload.data; return msg;',
      outputs: 1,
      wires: [['bindOut']],
    },
    {
      id: 'bindOut',
      type: 'dapr-binding-out',
      z: 'tab',
      connection: 'c1',
      bindingName: BINDING,
      operation: 'post',
      metadata: '{}',
      wires: [['bindSuccess']],
    },
    {
      id: 'bindSuccess',
      type: 'function',
      z: 'tab',
      func: `msg.statusCode = 200;
if (Buffer.isBuffer(msg.payload)) { msg.payload = msg.payload.toString(); }
msg.payload = { dapr: msg.dapr, payload: msg.payload };
return msg;`,
      outputs: 1,
      wires: [['bindRes']],
    },
    {
      id: 'bindErrors',
      type: 'catch',
      z: 'tab',
      scope: ['bindOut'],
      uncaught: false,
      wires: [['bindFailure']],
    },
    {
      id: 'bindFailure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code }; return msg;',
      outputs: 1,
      wires: [['bindRes']],
    },
    { id: 'bindRes', type: 'http response', z: 'tab' },

    // -- secret-get: present, absent, and denied secrets --
    {
      id: 'secretIn',
      type: 'http in',
      z: 'tab',
      url: '/secret',
      method: 'post',
      wires: [['secretBefore']],
    },
    {
      id: 'secretBefore',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload; return msg;',
      outputs: 1,
      wires: [['secretGet']],
    },
    {
      id: 'secretGet',
      type: 'dapr-secret-get',
      z: 'tab',
      connection: 'c1',
      storeName: SECRET_STORE,
      key: PRESENT_KEY,
      property: 'payload',
      metadata: '{}',
      wires: [['secretSuccess']],
    },
    {
      id: 'secretSuccess',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200; msg.payload = { dapr: msg.dapr, payload: msg.payload }; return msg;',
      outputs: 1,
      wires: [['secretRes']],
    },
    {
      id: 'secretErrors',
      type: 'catch',
      z: 'tab',
      scope: ['secretGet'],
      uncaught: false,
      wires: [['secretFailure']],
    },
    {
      id: 'secretFailure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code }; return msg;',
      outputs: 1,
      wires: [['secretRes']],
    },
    { id: 'secretRes', type: 'http response', z: 'tab' },
  ];
}

function invokeBinding(nr, { bindingName, data } = {}) {
  return httpRequest(nr.nodeUrl('/invoke'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ dapr: bindingName ? { bindingName } : {}, data }),
    timeoutMs: 5000,
  });
}

function getSecret(nr, { key } = {}) {
  return httpRequest(nr.nodeUrl('/secret'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(key ? { key } : {}),
    timeoutMs: 5000,
  });
}

test(
  'real daprd, no broker: invoke, output bindings, secrets, and Test Connection metadata',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-dapr-building-blocks-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const flows = buildFlow()
      .map((n) => (n.id === 'inv' ? { ...n, appId } : n))
      .concat([
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
      ]);

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({ flows });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [
        { filename: 'secretstore.yaml', yaml: secretComponentYaml() },
        { filename: 'orders-binding.yaml', yaml: bindingComponentYaml(capture.url) },
      ],
      extraFixtures: ['secrets.json'],
      configFixture: 'secret-scopes.yaml',
    });
    t.after(() => daprd.stop());

    // This tier already requires Linux host networking. Inspect the actual
    // listening socket, so a firewall cannot conceal a wildcard bind.
    const sockets = (
      await Promise.all(['/proc/net/tcp', '/proc/net/tcp6'].map((p) => readFile(p, 'utf8')))
    )
      .flatMap((table) => table.trim().split('\n').slice(1))
      .map((line) => line.trim().split(/\s+/))
      .filter(
        (fields) => fields[3] === '0A' && parseInt(fields[1].split(':')[1], 16) === daprHttpPort
      );
    assert.deepEqual(
      sockets.map((fields) => fields[1].split(':')[0]),
      ['0100007F'],
      'the unauthenticated fixture API must listen only on IPv4 loopback'
    );

    // startDaprd() resolving only proves daprd's OWN /healthz answers; the
    // dapr-connection node polls independently on its own bounded-backoff
    // schedule and may not have caught up yet.
    await waitFor(() => (nr.logText().includes('Dapr sidecar is available') ? true : null));

    await t.test(
      'the full invocation matrix — verbs, query, headers, binary, non-2xx, timeout, unknown method — round-trips through real daprd',
      async () => {
        // Every supported verb, round-tripped through real daprd's own invoke API.
        for (const verb of ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']) {
          const res = await waitFor(async () => {
            const r = await httpRequest(
              `${daprd.baseUrl}/v1.0/invoke/${appId}/method/echo?foo=bar&num=1`,
              {
                method: verb,
                headers: { 'content-type': 'application/json', 'x-custom': 'hello-value' },
                body:
                  verb === 'GET' || verb === 'DELETE' ? undefined : JSON.stringify({ ping: verb }),
                timeoutMs: 4000,
              }
            );
            return r.status === 200 ? r : null;
          });
          const body = JSON.parse(res.text);
          if (verb !== 'GET' && verb !== 'DELETE') assert.deepEqual(body.body, { ping: verb });
          assert.equal(body.verb, verb, `verb ${verb} echoed correctly`);
          assert.deepEqual(
            body.query,
            { foo: 'bar', num: '1' },
            `query string preserved for ${verb}`
          );
          assert.equal(body.custom, 'hello-value', `custom header preserved for ${verb}`);
          assert.equal(
            res.headers['x-echo-verb'],
            verb,
            `custom response header preserved for ${verb}`
          );
        }

        // Success through the visible dapr-invoke node, not only direct HTTP calls.
        const viaNode = await nr.waitForHttp('/call-echo', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ n: 2 }),
          until: (r) => r.status === 200,
        });
        assert.deepEqual(JSON.parse(viaNode.text).body, { n: 2 });

        // Binary request AND response body, byte-for-byte — including bytes that
        // are not valid standalone UTF-8, to prove real Buffer handling rather
        // than string handling that happens to work for text-like data.
        const binaryReq = Buffer.from([0, 1, 2, 250, 251, 252, 253, 254, 255]);
        const binaryRes = await waitFor(async () => {
          const r = await rawHttpRequest(
            `${daprd.baseUrl}/v1.0/invoke/${appId}/method/binary-echo`,
            {
              method: 'PUT',
              headers: { 'content-type': 'application/octet-stream' },
              body: binaryReq,
              timeoutMs: 4000,
            }
          );
          return r.status === 200 ? r : null;
        });
        assert.equal(binaryRes.headers['content-type'], 'application/octet-stream');
        assert.deepEqual(binaryRes.buffer, binaryReq);

        // Non-2xx response: real daprd must forward the app's status and body
        // verbatim, not treat it as a transport failure.
        const failRes = await httpRequest(`${daprd.baseUrl}/v1.0/invoke/${appId}/method/fail`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: '{}',
          timeoutMs: 4000,
        });
        assert.equal(failRes.status, 409);
        assert.deepEqual(JSON.parse(failRes.text), { error: 'conflict' });

        // Unknown method: no dapr-service is registered at /nope, so the
        // app-channel's own 404 comes back through daprd untouched.
        const unknownRes = await httpRequest(`${daprd.baseUrl}/v1.0/invoke/${appId}/method/nope`, {
          method: 'GET',
          timeoutMs: 4000,
        });
        assert.equal(unknownRes.status, 404);

        // Outbound timeout: dapr-invoke's 1s override must abort well before the
        // real /slow service's 4s artificial delay resolves.
        const slowCall = await waitFor(async () => {
          const t0 = Date.now();
          const r = await httpRequest(nr.nodeUrl('/call-slow'), {
            method: 'POST',
            body: '',
            timeoutMs: 6000,
          });
          return r.status === 504 ? { r, elapsed: Date.now() - t0 } : null;
        });
        assert.equal(slowCall.r.text, 'timeout');
        assert.ok(
          slowCall.elapsed < 3000,
          `outbound timeout override should fire well before the service's 4s delay (was ${slowCall.elapsed}ms)`
        );
      }
    );

    await t.test(
      'a real bindings.http component round-trips a POST and reports a missing binding',
      async () => {
        const response = await invokeBinding(nr, { data: { id: 1, note: 'real round trip' } });
        assert.equal(response.status, 200);
        const body = JSON.parse(response.text);
        assert.equal(body.dapr.statusCode, 200);
        assert.equal(body.dapr.bindingName, BINDING);
        assert.equal(body.dapr.operation, 'post');
        // Node's http client lower-cases every incoming header name.
        assert.equal(body.dapr.metadata.statuscode, '200');
        assert.equal(body.payload, 'ok');

        // The POST really reached the capture server -- proof this is a genuine
        // round trip through a real daprd + a real component, not an artifact of
        // this package's own code.
        assert.deepEqual(capture.received.at(-1), { id: 1, note: 'real round trip' });

        const missing = await invokeBinding(nr, { bindingName: MISSING_BINDING, data: {} });
        assert.equal(missing.status, 503);
        const missingBody = JSON.parse(missing.text);
        assert.equal(missingBody.code, 'BINDING_INVOKE_FAILED');
        assert.match(missingBody.message, /ERR_INVOKE_OUTPUT_BINDING/);
      }
    );

    await t.test(
      'a real secretstores.local.file component distinguishes present, absent, and denied secrets -- and never leaks a key into the flow response',
      async () => {
        // A real, present, allowed secret.
        const present = await getSecret(nr);
        assert.equal(present.status, 200);
        const presentBody = JSON.parse(present.text);
        assert.deepEqual(presentBody.payload, { [PRESENT_KEY]: PRESENT_VALUE });
        assert.equal(presentBody.dapr.statusCode, 200);
        assert.equal(presentBody.dapr.storeName, SECRET_STORE);
        assert.equal(presentBody.dapr.key, PRESENT_KEY);

        // Allowed by scope, but genuinely absent from the file -- real daprd's
        // own ERR_SECRET_GET message embeds this exact key; the assertion below
        // proves it never reaches this flow's own HTTP response.
        const absent = await getSecret(nr, { key: ABSENT_KEY });
        assert.equal(absent.status, 503);
        const absentBody = JSON.parse(absent.text);
        assert.equal(absentBody.code, 'SECRET_OPERATION_FAILED');
        assert.ok(
          !absent.text.includes(ABSENT_KEY),
          'the flow response must never include the requested key'
        );

        // Denied by real Dapr secret-scoping, before the component is ever
        // called -- a reliably distinct outcome from "absent."
        const denied = await getSecret(nr, { key: DENIED_KEY });
        assert.equal(denied.status, 503);
        const deniedBody = JSON.parse(denied.text);
        assert.equal(deniedBody.code, 'SECRET_ACCESS_DENIED');
        assert.ok(
          !denied.text.includes(DENIED_KEY),
          'the flow response must never include the requested key'
        );
      }
    );

    await t.test(
      'Test Connection returns only curated metadata from real daprd 1.18.4',
      async () => {
        const response = await httpRequest(nr.adminUrl('/dapr-connection/c1/metadata'));
        assert.equal(response.status, 200);
        assert.equal(response.headers['cache-control'], 'no-store');
        const body = JSON.parse(response.text);
        // Changed from the original single-component expectation: this shared
        // daprd registers BOTH the secret store and the output-binding
        // component (the two other subtests in this file need), so the
        // combined component set replaces the old, binding-out-less list.
        // Component order is sorted by name here rather than asserted
        // verbatim -- real daprd's own /v1.0/metadata component order is not a
        // documented contract this package should pin a test to.
        assert.deepEqual(
          {
            ...body,
            components: [...body.components].sort((a, b) => a.name.localeCompare(b.name)),
          },
          {
            ok: true,
            id: appId,
            runtimeVersion: '1.18.4',
            components: [
              { name: 'orders-binding', type: 'bindings.http' },
              { name: 'secretstore', type: 'secretstores.local.file' },
            ],
            componentCount: 2,
            subscriptionCount: 0,
          }
        );
      }
    );
  }
);
