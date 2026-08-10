'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { NodeRed, freePort } = require('../helpers/node-red');
const { httpRequest } = require('../helpers/http');

// A minimal real node:http server standing in for an OTLP/HTTP receiver: this
// tier only needs to prove a real settings.js-installed bridge actually
// attempts a real network export (and fails open when it can't), not decode
// the wire body — that belongs to the pinned collector fixture at the
// integration tier (test/integration/telemetry.test.js's own sibling for
// logs), mirroring how the trace pipeline split the same way across tiers.
function fakeOtlpReceiver() {
  const requests = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      requests.push({ path: req.url, headers: req.headers });
      res.writeHead(200, { 'content-type': 'application/x-protobuf' }).end();
    });
  });
  return new Promise((resolve, reject) => {
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        requests,
        port,
        stop: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

const OTEL_LOGGING = `, otel: { level: 'debug', metrics: false, audit: false, handler: require('@pauldeng/node-red-contrib-dapr-http/logging') }`;

function triggerFlow() {
  return [
    { id: 'tab', type: 'tab', label: 'logging' },
    { id: 'in', type: 'http in', z: 'tab', url: '/trigger', method: 'post', wires: [['fn']] },
    {
      id: 'fn',
      type: 'function',
      z: 'tab',
      name: 'warn',
      func: "node.warn('hello from the runtime tier'); return msg;",
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab' },
  ];
}

async function waitFor(fn, { timeoutMs = 10000, intervalMs = 100 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = fn();
    if (value) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error('waitFor timed out');
}

test(
  'a node.warn() call reaches a real OTLP/HTTP endpoint once settings.js installs the bridge',
  { timeout: 60000 },
  async (t) => {
    const otlp = await fakeOtlpReceiver();
    t.after(() => otlp.stop());

    const nr = new NodeRed();
    await nr.start({
      loggingExtra: OTEL_LOGGING,
      env: { OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `http://127.0.0.1:${otlp.port}/v1/logs` },
    });
    t.after(() => nr.stop());

    await nr.deploy(triggerFlow());
    const res = await nr.waitForHttp('/trigger', { method: 'POST' });
    assert.equal(res.status, 200);

    await waitFor(() => otlp.requests.length > 0);
    assert.equal(otlp.requests[0].path, '/v1/logs');
  }
);

test(
  'an unreachable OTLP endpoint fails open: Node-RED keeps responding and never crashes',
  { timeout: 60000 },
  async (t) => {
    const closedPort = await freePort(); // freed immediately: nothing listens here

    const nr = new NodeRed();
    await nr.start({
      loggingExtra: OTEL_LOGGING,
      env: { OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `http://127.0.0.1:${closedPort}/v1/logs` },
    });
    t.after(() => nr.stop());

    await nr.deploy(triggerFlow());
    const res = await nr.waitForHttp('/trigger', { method: 'POST' });
    assert.equal(res.status, 200);
    // Node-RED's own request/response cycle above already proves nothing
    // blocked or crashed; give the batch processor a moment to actually try
    // (and fail) its export before confirming the process is still healthy.
    await new Promise((resolve) => setTimeout(resolve, 1500));
    const stillUp = await httpRequest(nr.adminUrl('/settings'), { timeoutMs: 5000 });
    assert.equal(stillUp.status, 200);
  }
);
