'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const http = require('node:http');

const { NodeRed } = require('../helpers/node-red');
const { httpRequest } = require('../helpers/http');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');

// A minimal fixture built from Node-RED core nodes only: an HTTP endpoint whose
// handler stamps a marker and responds. Used to prove the harness can deploy,
// observe, and redeploy without any product code.
function helloFlow(marker) {
  return [
    { id: 'tab', type: 'tab', label: 'harness' },
    { id: 'in', type: 'http in', z: 'tab', url: '/hello', method: 'get', wires: [['fn']] },
    {
      id: 'fn',
      type: 'function',
      z: 'tab',
      func: `msg.payload = { marker: '${marker}' }; return msg;`,
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab', statusCode: '200' },
  ];
}

// A fixture that reaches OUT to the fake Dapr sidecar, proving both helpers
// integrate end to end: HTTP in -> HTTP request(fake daprd) -> HTTP response.
function daprProbeFlow(daprUrl) {
  return [
    { id: 'tab', type: 'tab', label: 'probe' },
    { id: 'in', type: 'http in', z: 'tab', url: '/probe', method: 'get', wires: [['req']] },
    {
      id: 'req',
      type: 'http request',
      z: 'tab',
      method: 'GET',
      ret: 'obj',
      url: `${daprUrl}/v1.0/healthz/outbound`,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab', statusCode: '200' },
  ];
}

test(
  'harness: start, deploy a core-node flow, observe, redeploy, stop',
  { timeout: 60000 },
  async (t) => {
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy(helloFlow('M2-a'));
    let r = await nr.waitForHttp('/hello', {
      until: (x) => x.status === 200 && x.json().marker === 'M2-a',
    });
    assert.equal(r.json().marker, 'M2-a');

    // Redeploy a modified flow while the same process keeps running.
    await nr.deploy(helloFlow('M2-b'));
    r = await nr.waitForHttp('/hello', {
      until: (x) => x.status === 200 && x.json().marker === 'M2-b',
    });
    assert.equal(r.json().marker, 'M2-b');

    // Characterize Express's qs override through real Node-RED HTTP nodes.
    const beforeParserDeploy = nr.logText().length;
    await nr.deploy([
      { id: 'parse-tab', type: 'tab', label: 'parser' },
      {
        id: 'parse-in',
        type: 'http in',
        z: 'parse-tab',
        url: '/parse',
        method: 'post',
        wires: [['parse-fn']],
      },
      {
        id: 'parse-fn',
        type: 'function',
        z: 'parse-tab',
        outputs: 1,
        func: 'msg.payload = { query: msg.req.query, body: msg.payload }; return msg;',
        wires: [['parse-res']],
      },
      { id: 'parse-res', type: 'http response', z: 'parse-tab' },
    ]);
    await nr.waitForLog('Started flows', { after: beforeParserDeploy });
    const parsed = await httpRequest(nr.nodeUrl('/parse?filter[name]=demo&tags[]=a&tags[]=b'), {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: 'enabled=false&count=0',
    });
    assert.equal(parsed.status, 200);
    assert.deepEqual(JSON.parse(parsed.text), {
      query: { filter: { name: 'demo' }, tags: ['a', 'b'] },
      body: { enabled: 'false', count: '0' },
    });
  }
);

test(
  'start waits for the startup flow load before an immediate deploy',
  { timeout: 30000 },
  async (t) => {
    const nr = new NodeRed();
    await nr.start({ startupFlowLoadDelayMs: 1500 });
    t.after(async () => {
      await nr.stop();
    });

    await nr.deploy(helloFlow('after-startup-load'));
    const r = await nr.waitForHttp('/hello', {
      until: (x) => x.status === 200 && x.json().marker === 'after-startup-load',
    });
    assert.equal(r.json().marker, 'after-startup-load');
  }
);

test(
  'harness + fake daprd: a flow can call the sidecar and it records the call',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', '/v1.0/healthz/outbound', (req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok' }));
    });

    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy(daprProbeFlow(dapr.url));
    const r = await nr.waitForHttp('/probe', { until: (x) => x.status === 200 });
    assert.deepEqual(r.json(), { status: 'ok' });

    const probe = dapr.requests.find(
      (rec) => rec.method === 'GET' && rec.path === '/v1.0/healthz/outbound'
    );
    assert.ok(probe, 'fake daprd should have recorded the outbound health probe');
  }
);

test(
  'start() tears down the process and temp dir when readiness fails',
  { timeout: 30000 },
  async () => {
    const nr = new NodeRed();
    // A 1 ms readiness budget cannot be met, forcing the failure path.
    await assert.rejects(nr.start({ readyTimeoutMs: 1 }), /did not become ready/);
    assert.equal(nr.userDir, null, 'temp directory should have been removed');
    assert.ok(
      nr.proc && (nr.proc.killed || nr.proc.exitCode !== null),
      'spawned node-red process should have been terminated'
    );
  }
);

test('waitForHttp aborts a hung request within its timeout', { timeout: 15000 }, async (t) => {
  // A server that accepts connections but never responds; a plain fetch would
  // wait indefinitely, so this proves the per-request abort is enforced.
  const sockets = new Set();
  const hung = net.createServer((s) => {
    sockets.add(s);
    s.on('close', () => sockets.delete(s));
  });
  await new Promise((resolve) => hung.listen(0, '127.0.0.1', resolve));
  t.after(
    () =>
      new Promise((resolve) => {
        for (const s of sockets) s.destroy();
        hung.close(resolve);
      })
  );

  const nr = new NodeRed();
  nr.port = hung.address().port; // point the poller at the hung server, no node-red

  const started = Date.now();
  await assert.rejects(nr.waitForHttp('/x', { timeoutMs: 300, intervalMs: 50 }), /not satisfied/);
  const elapsed = Date.now() - started;
  assert.ok(elapsed < 1500, `should abort promptly for a 300ms deadline, took ${elapsed}ms`);
});

test(
  'waitForHttp enforces a wall-clock deadline against a slow trickle',
  { timeout: 15000 },
  async (t) => {
    // A server that keeps the socket active by dribbling one byte every 100 ms.
    // A socket-inactivity timeout would reset on each byte and never fire; the
    // absolute deadline must still abort near 300 ms.
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      const iv = setInterval(() => {
        res.write('x');
      }, 100);
      res.on('close', () => clearInterval(iv));
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    t.after(
      () =>
        new Promise((resolve) => {
          server.closeAllConnections();
          server.close(resolve);
        })
    );

    const nr = new NodeRed();
    nr.port = server.address().port;

    const started = Date.now();
    await assert.rejects(nr.waitForHttp('/x', { timeoutMs: 300, intervalMs: 50 }), /not satisfied/);
    const elapsed = Date.now() - started;
    assert.ok(elapsed < 1500, `slow trickle must not defeat the deadline, took ${elapsed}ms`);
  }
);
