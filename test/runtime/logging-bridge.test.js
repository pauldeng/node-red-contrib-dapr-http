'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { NodeRed, freePort } = require('../helpers/node-red');
const { httpRequest } = require('../helpers/http');
const { waitForFast: waitFor } = require('../helpers/wait-for');
const { setTimeout: delay } = require('node:timers/promises');

// A minimal real node:http server standing in for an OTLP/HTTP receiver: this
// Decode the export body too: Node-RED emits startup logs, so counting HTTP
// requests alone can make an in-flow assertion pass before the flow runs.
function fakeOtlpReceiver() {
  const requests = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const document = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      const records = (document.resourceLogs || []).flatMap((resource) =>
        (resource.scopeLogs || []).flatMap((scope) => scope.logRecords || [])
      );
      requests.push({ path: req.url, headers: req.headers, records });
      res.writeHead(200, { 'content-type': 'application/json' }).end('{}');
    });
  });
  return new Promise((resolve, reject) => {
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        requests,
        logRecords: () => requests.flatMap((request) => request.records),
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

    const record = await waitFor(() =>
      otlp.logRecords().find((item) => item.body?.stringValue === 'hello from the runtime tier')
    );
    assert.ok(record);
    assert.equal(
      otlp.requests.find((request) => request.records.includes(record)).path,
      '/v1/logs'
    );
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
    await delay(1500);
    const stillUp = await httpRequest(nr.adminUrl('/settings'), { timeoutMs: 5000 });
    assert.equal(stillUp.status, 200);
  }
);

test(
  'startup logs (before any flow deploys) export the same as an in-flow log, uncorrelated',
  { timeout: 60000 },
  async (t) => {
    const otlp = await fakeOtlpReceiver();
    t.after(() => otlp.stop());

    const nr = new NodeRed();
    // The bridge is installed by settings.js and RED.log.init() runs at
    // process startup, before this test ever calls deploy() — any request
    // already in otlp.requests at this point came from a genuinely
    // out-of-flow record (e.g. Node-RED's own "Server now running" info log).
    await nr.start({
      loggingExtra: OTEL_LOGGING,
      env: { OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `http://127.0.0.1:${otlp.port}/v1/logs` },
    });
    t.after(() => nr.stop());

    const records = await waitFor(() => {
      const exported = otlp.logRecords();
      return exported.length ? exported : null;
    });
    assert.ok(records.every((record) => !record.traceId && !record.spanId));
  }
);

test(
  'logging-only mode: no connection enables tracing, and logging still exports normally',
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

    // No dapr-connection node at all in this flow -- tracing is never
    // acquired anywhere in the process, only the logging bridge is active.
    await nr.deploy(triggerFlow());
    const res = await nr.waitForHttp('/trigger', { method: 'POST' });
    assert.equal(res.status, 200);

    const record = await waitFor(() =>
      otlp.logRecords().find((item) => item.body?.stringValue === 'hello from the runtime tier')
    );
    assert.equal(record.traceId, undefined);
    assert.equal(record.spanId, undefined);
    assert.doesNotMatch(nr.logText(), /UnhandledPromiseRejection|TypeError|ProviderMismatch/);
  }
);

test(
  'multiple traced connections share the one log lease without duplicate-provider warnings',
  { timeout: 60000 },
  async (t) => {
    const otlp = await fakeOtlpReceiver();
    t.after(() => otlp.stop());

    const nr = new NodeRed();
    await nr.start({
      loggingExtra: OTEL_LOGGING,
      env: {
        OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `http://127.0.0.1:${otlp.port}/v1/logs`,
        OTEL_TRACES_SAMPLER: 'always_on',
      },
    });
    t.after(() => nr.stop());

    const appPort1 = await freePort();
    const appPort2 = await freePort();
    await nr.deploy([
      ...triggerFlow(),
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: '1',
        bindAddress: '127.0.0.1',
        appPort: String(appPort1),
        tracingEnabled: true,
      },
      {
        id: 'c2',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: '1',
        bindAddress: '127.0.0.1',
        appPort: String(appPort2),
        tracingEnabled: true,
      },
    ]);
    const res = await nr.waitForHttp('/trigger', { method: 'POST' });
    assert.equal(res.status, 200);

    await waitFor(() =>
      otlp.logRecords().find((item) => item.body?.stringValue === 'hello from the runtime tier')
    );
    assert.equal(
      otlp.logRecords().filter((item) => item.body?.stringValue === 'hello from the runtime tier')
        .length,
      1
    );
    assert.doesNotMatch(nr.logText(), /MaxListenersExceededWarning/);
  }
);

test(
  'logging keeps exporting across a full flow redeploy, with no duplicate-provider errors',
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
    assert.equal((await nr.waitForHttp('/trigger', { method: 'POST' })).status, 200);
    const matchingLogs = () =>
      otlp.logRecords().filter((item) => item.body?.stringValue === 'hello from the runtime tier');
    await waitFor(() => matchingLogs().length === 1);

    // A full redeploy tears down and recreates every node, but the logging
    // bridge itself is a settings.js-level, process-lifetime singleton --
    // nothing in the redeploy path should touch it.
    await nr.deploy(triggerFlow());
    assert.equal((await nr.waitForHttp('/trigger', { method: 'POST' })).status, 200);

    await waitFor(() => matchingLogs().length === 2);
    assert.equal(matchingLogs().length, 2);
    assert.doesNotMatch(nr.logText(), /UnhandledPromiseRejection|already registered|duplicate/i);
  }
);
