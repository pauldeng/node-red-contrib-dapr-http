'use strict';

// The daprd+broker-backed distributed-trace test (subscribe -> bulk publish,
// nested consumer/flow/producer spans) moved to nats-telemetry.test.js per
// Milestone 3's NATS-primary rebalance, and grew into that file's own
// combined logging+tracing primary path. This file keeps only the one test
// that never needed a broker (or daprd) at all.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { ContainerNodeRed } = require('../helpers/node-red-container');
const { startOtelCollector } = require('../helpers/otel-collector');

const OTEL_LOGGING_HANDLER = `, otel: { level: 'debug', metrics: false, audit: false, handler: require('@pauldeng/node-red-contrib-dapr-http/logging') }`;

test(
  'a real node.warn() call inside a traced node is exported carrying that same span, correlated, to a real OTLP collector',
  { timeout: 90000 },
  async (t) => {
    const collector = await startOtelCollector();
    t.after(() => collector.stop());

    // No real daprd needed: registerFlowSpanHooks() activates process-wide
    // the moment any dapr-connection acquires tracing, independent of the
    // sidecar's own reachability (see lib/telemetry.js's acquire()) — this
    // test only needs the generic flow-span + logging-bridge correlation,
    // not a Dapr boundary span, so daprHost/appPort are never dialed.
    const nr = new ContainerNodeRed();
    await nr.start({
      env: {
        OTEL_TRACES_SAMPLER: 'always_on',
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${collector.port}`,
      },
      loggingExtra: OTEL_LOGGING_HANDLER,
      flows: [
        { id: 'tab', type: 'tab', label: 'it-logging' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: '1',
          bindAddress: '127.0.0.1',
          appPort: '1',
          tracingEnabled: true,
        },
        { id: 'in', type: 'http in', z: 'tab', url: '/trigger', method: 'post', wires: [['fn']] },
        {
          id: 'fn',
          type: 'function',
          z: 'tab',
          name: 'warn',
          func: "node.warn('integration log correlation check'); return msg;",
          outputs: 1,
          wires: [['res']],
        },
        { id: 'res', type: 'http response', z: 'tab' },
      ],
    });
    t.after(() => nr.stop());

    const res = await nr.waitForHttp('/trigger', { method: 'POST' });
    assert.equal(res.status, 200);

    let spans = [];
    let logRecords = [];
    for (let attempt = 0; attempt < 20 && (!spans.length || !logRecords.length); attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      spans = await collector.readSpans();
      logRecords = await collector.readLogRecords();
    }

    const functionSpan = spans.find((s) => s.name === 'function');
    assert.ok(
      functionSpan,
      `expected a flow span named "function", got: ${spans.map((s) => s.name)}`
    );
    const record = logRecords.find(
      (r) => r.body?.stringValue === 'integration log correlation check'
    );
    assert.ok(record, `expected the warn log record, got: ${JSON.stringify(logRecords)}`);

    assert.equal(record.traceId, functionSpan.traceId, "the log shares the function node's trace");
    assert.equal(
      record.spanId,
      functionSpan.spanId,
      "the log is attributed to the function node's own span, not some other span"
    );
    assert.equal(record.severityNumber, 13, 'WARN');
    assert.equal(record.severityText, 'WARN');
  }
);
