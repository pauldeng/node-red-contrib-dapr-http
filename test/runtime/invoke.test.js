'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');

const healthPath = '/v1.0/healthz/outbound';

async function waitFor(fn, { timeoutMs = 12000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) {
        return value;
      }
      last = value;
    } catch (err) {
      last = err;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`waitFor timed out (${last instanceof Error ? last.message : last})`);
}

const connectionNode = (appPort, daprPort, extra = {}) => ({
  id: 'c1',
  type: 'dapr-connection',
  daprHost: '127.0.0.1',
  daprPort: String(daprPort),
  bindAddress: '127.0.0.1',
  appPort: String(appPort),
  ...extra,
});

test(
  'invoke: forwards the request and API token, returns the parsed response',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('POST', '/v1.0/invoke/target/method/echo', (req, res, ctx) => {
      // fake-dapr has already read the body into ctx.body.
      dapr.lastInvokeToken = req.headers['dapr-api-token'];
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ echoed: JSON.parse(ctx.body.toString()), verb: req.method }));
    });

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start({ env: { DAPR_API_TOKEN: 'tok' } });
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'invoke' },
      connectionNode(appPort, dapr.port),
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'post', wires: [['inv']] },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'target',
        method: 'echo',
        verb: 'POST',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
    ]);

    const response = await waitFor(async () => {
      const r = await httpRequest(nr.nodeUrl('/call'), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ n: 1 }),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    assert.deepEqual(JSON.parse(response.text), { echoed: { n: 1 }, verb: 'POST' });
    assert.equal(dapr.lastInvokeToken, 'tok', 'the Dapr API token is forwarded on invoke');
  }
);

test('invoke: a per-message timeout override bounds a slow call', { timeout: 60000 }, async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
  // Sidecar never replies within the override window.
  dapr.respond('POST', '/v1.0/invoke/slow/method/wait', (_req, res) => {
    setTimeout(() => res.writeHead(204).end(), 5000).unref();
  });

  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  // Connection default is 30s; the message override must win at ~1s.
  await nr.deploy([
    { id: 'tab', type: 'tab', label: 'to' },
    connectionNode(appPort, dapr.port, { requestTimeoutSec: '30' }),
    { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'post', wires: [['set']] },
    {
      id: 'set',
      type: 'function',
      z: 'tab',
      func: "msg.dapr = { appId: 'slow', method: 'wait', verb: 'POST', timeoutSec: 1 }; return msg;",
      outputs: 1,
      wires: [['inv']],
    },
    {
      id: 'inv',
      type: 'dapr-invoke',
      z: 'tab',
      connection: 'c1',
      appId: 'slow',
      method: 'wait',
      verb: 'POST',
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab' },
    { id: 'cat', type: 'catch', z: 'tab', scope: ['inv'], wires: [['err']] },
    {
      id: 'err',
      type: 'function',
      z: 'tab',
      func: "msg.statusCode = 504; msg.payload = 'timeout'; return msg;",
      outputs: 1,
      wires: [['res']],
    },
  ]);

  const started = await waitFor(async () => {
    const t0 = Date.now();
    const r = await httpRequest(nr.nodeUrl('/call'), { method: 'POST', body: '', timeoutMs: 4000 });
    return r.status === 504 ? { r, elapsed: Date.now() - t0 } : null;
  });
  assert.equal(started.r.text, 'timeout');
  assert.ok(
    started.elapsed < 3000,
    `override deadline should fire well before 5s (was ${started.elapsed}ms)`
  );
});

test(
  'invoke: preserves a nested method path, query string, and a non-2xx response',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('GET', '/v1.0/invoke/echo/method/a/b/c', (req, res) => {
      dapr.lastUrl = req.url;
      req.resume();
      res.writeHead(404, { 'content-type': 'application/json' }).end('{"e":1}');
    });

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'echo' },
      connectionNode(appPort, dapr.port),
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'get', wires: [['set']] },
      {
        id: 'set',
        type: 'function',
        z: 'tab',
        func: "msg.dapr = { appId: 'echo', method: 'a/b/c', verb: 'GET', query: { q: '1' } }; return msg;",
        outputs: 1,
        wires: [['inv']],
      },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'echo',
        method: 'x',
        verb: 'GET',
        wires: [['out']],
      },
      {
        id: 'out',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { status: msg.dapr.statusCode, body: msg.payload }; msg.statusCode = 200; return msg;',
        outputs: 1,
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
    ]);

    const r = await waitFor(async () => {
      const x = await httpRequest(nr.nodeUrl('/call'), { timeoutMs: 4000 });
      return x.status === 200 ? x : null;
    });
    assert.deepEqual(JSON.parse(r.text), { status: 404, body: { e: 1 } });
    assert.equal(dapr.lastUrl, '/v1.0/invoke/echo/method/a/b/c?q=1');
  }
);

test(
  'invoke: round-trips a PUT with a binary body and binary response',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    dapr.respond('PUT', '/v1.0/invoke/bin/method/echo', (req, res, ctx) => {
      res.writeHead(200, { 'content-type': 'application/octet-stream' }).end(ctx.body);
    });

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bin' },
      connectionNode(appPort, dapr.port),
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'get', wires: [['set']] },
      {
        id: 'set',
        type: 'function',
        z: 'tab',
        func: "msg.payload = Buffer.from([1, 2, 3, 255]); msg.dapr = { appId: 'bin', method: 'echo', verb: 'PUT' }; return msg;",
        outputs: 1,
        wires: [['inv']],
      },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'bin',
        method: 'echo',
        verb: 'PUT',
        wires: [['out']],
      },
      {
        id: 'out',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { isBuffer: Buffer.isBuffer(msg.payload), bytes: Array.from(msg.payload) }; msg.statusCode = 200; return msg;',
        outputs: 1,
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
    ]);

    const r = await waitFor(async () => {
      const x = await httpRequest(nr.nodeUrl('/call'), { timeoutMs: 4000 });
      return x.status === 200 ? x : null;
    });
    assert.deepEqual(JSON.parse(r.text), { isBuffer: true, bytes: [1, 2, 3, 255] });
  }
);

test(
  'invoke: a non-serializable payload or bad timeout fails via done(error)',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const invokeWithCatch = (setFunc) => [
      { id: 'tab', type: 'tab', label: 'bad' },
      connectionNode(appPort, dapr.port),
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'get', wires: [['set']] },
      { id: 'set', type: 'function', z: 'tab', func: setFunc, outputs: 1, wires: [['inv']] },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'x',
        method: 'm',
        verb: 'POST',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
      { id: 'cat', type: 'catch', z: 'tab', scope: ['inv'], wires: [['err']] },
      {
        id: 'err',
        type: 'function',
        z: 'tab',
        func: "msg.statusCode = 400; msg.payload = 'rejected'; return msg;",
        outputs: 1,
        wires: [['res']],
      },
    ];

    // BigInt payload → encodeBody throws → clean done(error), never an uncaught throw.
    await nr.deploy(invokeWithCatch('msg.payload = 10n; return msg;'));
    const bad1 = await waitFor(async () => {
      const x = await httpRequest(nr.nodeUrl('/call'), { timeoutMs: 4000 });
      return x.status === 400 ? x : null;
    });
    assert.equal(bad1.text, 'rejected');

    // An out-of-range per-message timeout is rejected, not silently ignored.
    await nr.deploy(invokeWithCatch('msg.dapr = { timeoutSec: 9999 }; return msg;'));
    const bad2 = await waitFor(async () => {
      const x = await httpRequest(nr.nodeUrl('/call'), { timeoutMs: 4000 });
      return x.status === 400 ? x : null;
    });
    assert.equal(bad2.text, 'rejected');
  }
);

test(
  'invoke: an in-flight call is aborted when the node is redeployed',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    let aborted = false;
    dapr.respond('POST', '/v1.0/invoke/hang/method/wait', (_req, res) => {
      // Never respond; res 'close' with an unfinished body means the caller (the
      // invoke node) dropped the connection. (req 'close' fires as soon as the
      // request stream is read, so it is not a disconnect signal.)
      res.on('close', () => {
        if (!res.writableEnded) {
          aborted = true;
        }
      });
    });

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const flow = [
      { id: 'tab', type: 'tab', label: 'hang' },
      connectionNode(appPort, dapr.port),
      { id: 'in', type: 'http in', z: 'tab', url: '/go', method: 'get', wires: [['inv']] },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'hang',
        method: 'wait',
        verb: 'POST',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
    ];
    await nr.deploy(flow);
    // Poll-fire /go until one triggers an invoke that reaches the sidecar (the
    // http-in route goes live a moment after deploy). Each fired request hangs
    // because the sidecar never replies.
    await waitFor(async () => {
      httpRequest(nr.nodeUrl('/go'), { timeoutMs: 6000 }).catch(() => {});
      await new Promise((resolve) => setTimeout(resolve, 100));
      return dapr.requests.some((r) => r.path === '/v1.0/invoke/hang/method/wait') ? true : null;
    });
    assert.equal(aborted, false); // still in flight

    await nr.deploy(flow); // redeploy closes the invoke node → aborts the call
    await waitFor(async () => (aborted ? true : null));
    assert.equal(aborted, true);
  }
);

function serviceFlow(appPort, daprPort, extra = {}) {
  return [
    { id: 'tab', type: 'tab', label: 'service' },
    connectionNode(appPort, daprPort, extra.connection || {}),
    {
      id: 'svc',
      type: 'dapr-service',
      z: 'tab',
      connection: 'c1',
      verb: 'POST',
      methodPath: '/orders',
      wires: [['fn']],
    },
    {
      id: 'fn',
      type: 'function',
      z: 'tab',
      func: "msg.payload = { ok: true, caller: msg.dapr.callerAppId, hadToken: 'dapr-api-token' in msg.dapr.headers, gotBody: msg.payload }; return msg;",
      outputs: 1,
      wires: [['resp']],
    },
    { id: 'resp', type: 'dapr-response', z: 'tab', connection: 'c1', statusCode: '200', wires: [] },
  ];
}

const serviceUrl = (appPort, path) => `http://127.0.0.1:${appPort}${path}`;

test(
  'service: round-trip, caller id, header filtering, 404, 405, and Admin API isolation',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy(serviceFlow(appPort, dapr.port));

    const ok = await waitFor(async () => {
      const r = await httpRequest(serviceUrl(appPort, '/orders'), {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'dapr-caller-app-id': 'checkout',
          'dapr-api-token': 'x',
        },
        body: JSON.stringify({ q: 1 }),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    assert.deepEqual(JSON.parse(ok.text), {
      ok: true,
      caller: 'checkout',
      hadToken: false, // dapr-api-token filtered out of the exposed headers
      gotBody: { q: 1 },
    });

    // Unknown method → 404; wrong verb → 405.
    assert.equal(
      (await httpRequest(serviceUrl(appPort, '/nope'), { timeoutMs: 2000 })).status,
      404
    );
    const wrong = await httpRequest(serviceUrl(appPort, '/orders'), {
      method: 'GET',
      timeoutMs: 2000,
    });
    assert.equal(wrong.status, 405);

    // The Node-RED Admin API must not be reachable through the app-channel port.
    assert.equal(
      (await httpRequest(serviceUrl(appPort, '/flows'), { timeoutMs: 2000 })).status,
      404
    );
    assert.equal(
      (await httpRequest(serviceUrl(appPort, '/settings'), { timeoutMs: 2000 })).status,
      404
    );
  }
);

test('service: no response before the timeout returns 504', { timeout: 60000 }, async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  await nr.deploy([
    { id: 'tab', type: 'tab', label: 'slow' },
    connectionNode(appPort, dapr.port, { requestTimeoutSec: '2' }),
    {
      id: 'svc',
      type: 'dapr-service',
      z: 'tab',
      connection: 'c1',
      verb: 'POST',
      methodPath: '/slow',
      wires: [[]],
    },
  ]);

  const res = await waitFor(
    async () => {
      const r = await httpRequest(serviceUrl(appPort, '/slow'), {
        method: 'POST',
        body: '',
        timeoutMs: 5000,
      });
      return r.status !== 503 ? r : null; // skip the brief not-activated window
    },
    { timeoutMs: 15000 }
  );
  assert.equal(res.status, 504);
});

test(
  'service: unsafe response headers cannot corrupt the reply framing',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'hdr' },
      connectionNode(appPort, dapr.port),
      {
        id: 'svc',
        type: 'dapr-service',
        z: 'tab',
        connection: 'c1',
        verb: 'POST',
        methodPath: '/hdr',
        wires: [['fn']],
      },
      {
        id: 'fn',
        type: 'function',
        z: 'tab',
        // A wrong Content-Length + a CRLF-injecting header must be neutralized.
        func: "msg.payload = 'ok'; msg.dapr.responseHeaders = { 'content-length': '999', 'x-inject': 'a\\r\\nEvil: 1', 'x-good': 'yes' }; return msg;",
        outputs: 1,
        wires: [['resp']],
      },
      {
        id: 'resp',
        type: 'dapr-response',
        z: 'tab',
        connection: 'c1',
        statusCode: '200',
        wires: [],
      },
    ]);

    const res = await waitFor(async () => {
      const r = await httpRequest(serviceUrl(appPort, '/hdr'), {
        method: 'POST',
        body: '',
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    assert.equal(res.text, 'ok'); // full body delivered despite the bogus content-length
    assert.equal(res.headers['x-good'], 'yes');
    assert.notEqual(res.headers['content-length'], '999'); // bogus length stripped, framing intact
    assert.equal('evil' in res.headers, false); // CRLF injection dropped
  }
);

test('service: a request pending at redeploy completes as 503', { timeout: 60000 }, async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const flow = [
    { id: 'tab', type: 'tab', label: 'hold' },
    connectionNode(appPort, dapr.port),
    {
      id: 'svc',
      type: 'dapr-service',
      z: 'tab',
      connection: 'c1',
      verb: 'POST',
      methodPath: '/hold',
      wires: [[]],
    },
  ];
  await nr.deploy(flow);
  // Wait until the route is active: the listener is bound (no ECONNREFUSED) and a
  // request is held open (our short client deadline fires) rather than answered 503.
  await waitFor(async () => {
    try {
      const r = await httpRequest(serviceUrl(appPort, '/hold'), {
        method: 'POST',
        body: '',
        timeoutMs: 400,
      });
      return r.status === 503 ? null : true; // 503 = not activated yet
    } catch (err) {
      if (err.code === 'ECONNREFUSED') {
        return null; // listener not bound yet
      }
      return true; // client deadline on a held request → route is active
    }
  });

  const pending = httpRequest(serviceUrl(appPort, '/hold'), {
    method: 'POST',
    body: '',
    timeoutMs: 8000,
  });
  await new Promise((resolve) => setTimeout(resolve, 300));
  await nr.deploy(flow);
  const res = await pending;
  assert.equal(res.status, 503);
});

test(
  'service: a modified-node redeploy settles the service’s pending requests as 503',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const svc = (name) => [
      { id: 'tab', type: 'tab', label: 'mod' },
      connectionNode(appPort, dapr.port),
      {
        id: 'svc',
        type: 'dapr-service',
        z: 'tab',
        name,
        connection: 'c1',
        verb: 'POST',
        methodPath: '/hold',
        wires: [[]],
      },
    ];
    await nr.deploy(svc('v1'));
    await waitFor(async () => {
      try {
        const r = await httpRequest(serviceUrl(appPort, '/hold'), {
          method: 'POST',
          body: '',
          timeoutMs: 400,
        });
        return r.status === 503 ? null : true;
      } catch (err) {
        return err.code === 'ECONNREFUSED' ? null : true; // deadline on a held request → active
      }
    });

    const pending = httpRequest(serviceUrl(appPort, '/hold'), {
      method: 'POST',
      body: '',
      timeoutMs: 8000,
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
    // "nodes" redeploy: only the modified service node restarts; the connection
    // (unchanged) stays up and does NOT drain, so the service node itself must 503.
    await nr.deploy(svc('v2'), { deploymentType: 'nodes' });
    const res = await pending;
    assert.equal(res.status, 503);
  }
);

test(
  'service: hop-by-hop request headers are not exposed to the flow',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'hdr2' },
      connectionNode(appPort, dapr.port),
      {
        id: 'svc',
        type: 'dapr-service',
        z: 'tab',
        connection: 'c1',
        verb: 'POST',
        methodPath: '/in',
        wires: [['fn']],
      },
      {
        id: 'fn',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { keys: Object.keys(msg.dapr.headers).sort(), caller: msg.dapr.callerAppId }; return msg;',
        outputs: 1,
        wires: [['resp']],
      },
      {
        id: 'resp',
        type: 'dapr-response',
        z: 'tab',
        connection: 'c1',
        statusCode: '200',
        wires: [],
      },
    ]);

    const r = await waitFor(async () => {
      const x = await httpRequest(serviceUrl(appPort, '/in'), {
        method: 'POST',
        body: '',
        headers: {
          'proxy-authorization': 'Basic SECRET',
          te: 'trailers',
          trailer: 'x',
          upgrade: 'websocket',
          connection: 'upgrade',
          'x-custom': 'keep',
          'dapr-caller-app-id': 'c',
        },
        timeoutMs: 4000,
      });
      return x.status === 200 ? x : null;
    });
    const body = JSON.parse(r.text);
    assert.equal(body.caller, 'c'); // caller id still surfaced separately
    assert.ok(body.keys.includes('x-custom'));
    for (const banned of ['proxy-authorization', 'te', 'trailer', 'upgrade', 'connection']) {
      assert.equal(body.keys.includes(banned), false, `${banned} must not be exposed`);
    }
  }
);

test(
  'service: a caller that disconnects frees its pending correlation at once',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    // The reply is delayed 600ms; the connection timeout is the default 30s, so
    // without the abort-free the correlation would still be pending when the
    // response node fires. With the fix it is freed on disconnect, so the delayed
    // response finds nothing and logs "no pending request".
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'disc' },
      connectionNode(appPort, dapr.port),
      {
        id: 'svc',
        type: 'dapr-service',
        z: 'tab',
        connection: 'c1',
        verb: 'POST',
        methodPath: '/slow',
        wires: [['dly']],
      },
      {
        id: 'dly',
        type: 'delay',
        z: 'tab',
        pauseType: 'delay',
        timeout: '600',
        timeoutUnits: 'milliseconds',
        wires: [['resp']],
      },
      {
        id: 'resp',
        type: 'dapr-response',
        z: 'tab',
        connection: 'c1',
        statusCode: '200',
        wires: [],
      },
    ]);

    // Wait until active (a request is held), then disconnect early.
    await waitFor(async () => {
      try {
        const r = await httpRequest(serviceUrl(appPort, '/slow'), {
          method: 'POST',
          body: '',
          timeoutMs: 150,
        });
        return r.status === 503 ? null : true;
      } catch (err) {
        return err.code === 'ECONNREFUSED' ? null : true;
      }
    });
    // This request is answered by the held probe above having disconnected; issue
    // one more and drop it well before the 600ms reply.
    await httpRequest(serviceUrl(appPort, '/slow'), {
      method: 'POST',
      body: '',
      timeoutMs: 120,
    }).catch(() => {});

    await waitFor(async () => (nr.logText().includes('no pending request') ? true : null), {
      timeoutMs: 5000,
    });
  }
);

test(
  'service: dapr.statusCode 100 is rejected, not sent as a 1xx interim response',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: '1xx' },
      // Short response timeout so the rejected reply resolves as a 504 quickly
      // instead of the default 30s: rejecting the response node's input does not
      // itself fail the caller's pending request, only the app-channel deadline does.
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '2' }),
      {
        id: 'svc',
        type: 'dapr-service',
        z: 'tab',
        connection: 'c1',
        verb: 'POST',
        methodPath: '/one',
        wires: [['fn']],
      },
      {
        id: 'fn',
        type: 'function',
        z: 'tab',
        func: 'msg.dapr.statusCode = 100; return msg;',
        outputs: 1,
        wires: [['resp']],
      },
      {
        id: 'resp',
        type: 'dapr-response',
        z: 'tab',
        connection: 'c1',
        statusCode: '200',
        wires: [],
      },
    ]);

    // A rejected statusCode must never hang or corrupt the caller's socket
    // (writeHead(100)+end() otherwise produces a bare socket hang up); the caller
    // must instead see a clean final response once the deadline fires.
    const r = await waitFor(
      async () => {
        const x = await httpRequest(serviceUrl(appPort, '/one'), {
          method: 'POST',
          body: '',
          timeoutMs: 5000,
        });
        return x.status !== 503 ? x : null;
      },
      { timeoutMs: 15000 }
    );
    assert.equal(r.status, 504); // the response node's input was rejected, so the deadline fired
    assert.ok(
      nr.logText().includes('invalid response status'),
      'the response node should log rejection of the 1xx status'
    );
  }
);

test(
  'invoke: an unsafe query or content type override is rejected before the sidecar call',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    let sidecarHit = false;
    dapr.respond('POST', '/v1.0/invoke/x/method/m', (_req, res) => {
      sidecarHit = true;
      res.writeHead(204).end();
    });

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    const flowWith = (setFunc) => [
      { id: 'tab', type: 'tab', label: 'badoverride' },
      connectionNode(appPort, dapr.port),
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'get', wires: [['set']] },
      { id: 'set', type: 'function', z: 'tab', func: setFunc, outputs: 1, wires: [['inv']] },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'x',
        method: 'm',
        verb: 'POST',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
      { id: 'cat', type: 'catch', z: 'tab', scope: ['inv'], wires: [['err']] },
      {
        id: 'err',
        type: 'function',
        z: 'tab',
        func: "msg.statusCode = 400; msg.payload = 'rejected'; return msg;",
        outputs: 1,
        wires: [['res']],
      },
    ];

    // A string query would otherwise be enumerated into '0'/'1' query params.
    await nr.deploy(flowWith("msg.dapr = { query: 'ab' }; return msg;"));
    const badQuery = await waitFor(async () => {
      const x = await httpRequest(nr.nodeUrl('/call'), { timeoutMs: 4000 });
      return x.status === 400 ? x : null;
    });
    assert.equal(badQuery.text, 'rejected');
    assert.equal(sidecarHit, false, 'the sidecar must never be called with the bad query');

    // A non-string content type would otherwise be sent verbatim (e.g. "42").
    await nr.deploy(flowWith('msg.dapr = { contentType: 42 }; return msg;'));
    const badType = await waitFor(async () => {
      const x = await httpRequest(nr.nodeUrl('/call'), { timeoutMs: 4000 });
      return x.status === 400 ? x : null;
    });
    assert.equal(badType.text, 'rejected');
    assert.equal(sidecarHit, false, 'the sidecar must never be called with the bad content type');
  }
);

test(
  'invoke: a malformed header is rejected as INVALID_MESSAGE, not misclassified as a sidecar outage',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    let sidecarHit = false;
    dapr.respond('POST', '/v1.0/invoke/x/method/m', (_req, res) => {
      sidecarHit = true;
      res.writeHead(204).end();
    });

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'badheader' },
      connectionNode(appPort, dapr.port),
      { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'get', wires: [['set']] },
      {
        id: 'set',
        type: 'function',
        z: 'tab',
        // An illegal header name/token: http.request() would otherwise throw
        // synchronously and be misclassified as SIDECAR_UNAVAILABLE.
        func: "msg.dapr = { headers: { 'bad header': 'x' } }; return msg;",
        outputs: 1,
        wires: [['inv']],
      },
      {
        id: 'inv',
        type: 'dapr-invoke',
        z: 'tab',
        connection: 'c1',
        appId: 'x',
        method: 'm',
        verb: 'POST',
        wires: [['res']],
      },
      { id: 'res', type: 'http response', z: 'tab' },
      { id: 'cat', type: 'catch', z: 'tab', scope: ['inv'], wires: [['err']] },
      {
        id: 'err',
        type: 'function',
        z: 'tab',
        func: 'msg.statusCode = 400; msg.payload = JSON.stringify({ code: msg.error && msg.error.code }); return msg;',
        outputs: 1,
        wires: [['res']],
      },
    ]);

    const r = await waitFor(async () => {
      const x = await httpRequest(nr.nodeUrl('/call'), { timeoutMs: 4000 });
      return x.status === 400 ? x : null;
    });
    assert.deepEqual(JSON.parse(r.text), { code: 'INVALID_MESSAGE' });
    assert.equal(sidecarHit, false, 'the sidecar must never be contacted with a malformed header');
  }
);
