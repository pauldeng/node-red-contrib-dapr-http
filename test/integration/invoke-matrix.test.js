'use strict';

// Expands test/integration/invoke.test.js's basic round trip into the full
// invocation matrix IMPLEMENTATION_PLAN.md calls for: all supported verbs,
// query strings, headers, binary data, non-2xx responses, timeout, and
// unknown-method 404 — all through real daprd, not the fake sidecar.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
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

const FLOW = [
  { id: 'tab', type: 'tab', label: 'it-invoke-matrix' },
  // c1 (dapr-connection) is spliced in by the test once ports are known.
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
    wires: [['out']],
  },
  {
    id: 'out',
    type: 'function',
    z: 'tab',
    func: 'msg.statusCode = 200; return msg;',
    outputs: 1,
    wires: [['httpres']],
  },
  { id: 'httpres', type: 'http response', z: 'tab' },
  { id: 'catchInv', type: 'catch', z: 'tab', scope: ['inv'], wires: [['onErr']] },
  {
    id: 'onErr',
    type: 'function',
    z: 'tab',
    func: "msg.statusCode = 504; msg.payload = 'timeout'; return msg;",
    outputs: 1,
    wires: [['httpres']],
  },
];

test(
  'the full invocation matrix — verbs, query, headers, binary, non-2xx, timeout, unknown method — round-trips through real daprd',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-invoke-matrix';
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const flows = FLOW.map((n) => (n.id === 'inv' ? { ...n, appId } : n)).concat([
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

    const redis = await startRedis();
    t.after(() => redis.stop());
    const daprd = await startDaprd({
      appId,
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
    });
    t.after(() => daprd.stop());

    // Every supported verb, round-tripped through real daprd's own invoke API.
    for (const verb of ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']) {
      const res = await waitFor(async () => {
        const r = await httpRequest(
          `${daprd.baseUrl}/v1.0/invoke/${appId}/method/echo?foo=bar&num=1`,
          {
            method: verb,
            headers: { 'content-type': 'application/json', 'x-custom': 'hello-value' },
            body: verb === 'GET' || verb === 'DELETE' ? undefined : JSON.stringify({ ping: verb }),
            timeoutMs: 4000,
          }
        );
        return r.status === 200 ? r : null;
      });
      const body = JSON.parse(res.text);
      assert.equal(body.verb, verb, `verb ${verb} echoed correctly`);
      assert.deepEqual(body.query, { foo: 'bar', num: '1' }, `query string preserved for ${verb}`);
      assert.equal(body.custom, 'hello-value', `custom header preserved for ${verb}`);
      assert.equal(
        res.headers['x-echo-verb'],
        verb,
        `custom response header preserved for ${verb}`
      );
    }

    // Binary request AND response body, byte-for-byte — including bytes that
    // are not valid standalone UTF-8, to prove real Buffer handling rather
    // than string handling that happens to work for text-like data.
    const binaryReq = Buffer.from([0, 1, 2, 250, 251, 252, 253, 254, 255]);
    const binaryRes = await waitFor(async () => {
      const r = await rawHttpRequest(`${daprd.baseUrl}/v1.0/invoke/${appId}/method/binary-echo`, {
        method: 'PUT',
        headers: { 'content-type': 'application/octet-stream' },
        body: binaryReq,
        timeoutMs: 4000,
      });
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
