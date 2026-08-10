'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { SeverityNumber } = require('@opentelemetry/api-logs');
const { ROOT_CONTEXT } = require('@opentelemetry/api');

const { logs } = require('@opentelemetry/api-logs');
const {
  LoggerProvider,
  InMemoryLogRecordExporter,
  SimpleLogRecordProcessor,
} = require('@opentelemetry/sdk-logs');

const createHandler = require('../../lib/logging-bridge');

// Node-RED's own numeric log levels, see @node-red/util/lib/log.js.
const FATAL = 10;
const ERROR = 20;
const WARN = 30;
const INFO = 40;
const DEBUG = 50;
const TRACE = 60;
const AUDIT = 98;
const METRIC = 99;

// A fake lease/logger pair standing in for the real OTel SDK, so these tests
// exercise the bridge's own mapping/redaction/lifecycle logic without
// touching global tracer/logger registration at all.
function fakeDeps({ acquireFails = false } = {}) {
  const emitted = [];
  let released = 0;
  const acquire = async () => {
    if (acquireFails) {
      throw new Error('acquire failed');
    }
    return { release: async () => released++ };
  };
  const getLogger = () => ({ emit: (record) => emitted.push(record) });
  return { acquire, getLogger, emitted, releaseCount: () => released };
}

// Lease acquisition is asynchronous; flush one event-loop turn before reading
// the fake logger's captured records.
const tick = () => new Promise((resolve) => setImmediate(resolve));

// Every test below releases the process-lifetime handler state it creates.

test('acquires the log lease once at creation, before any entry arrives', async (t) => {
  let acquireCalls = 0;
  const deps = fakeDeps();
  const acquire = () => {
    acquireCalls++;
    return deps.acquire();
  };
  createHandler(undefined, { acquire, getLogger: deps.getLogger });
  t.after(() => createHandler.shutdown());
  await tick();
  assert.equal(acquireCalls, 1);
});

test('maps every Node-RED level to its OTel severity number and text', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  for (const level of [FATAL, ERROR, WARN, INFO, DEBUG, TRACE]) {
    handle({ level, msg: 'x' });
  }
  await tick();
  assert.deepEqual(
    deps.emitted.map((r) => [r.severityNumber, r.severityText]),
    [
      [SeverityNumber.FATAL, 'FATAL'],
      [SeverityNumber.ERROR, 'ERROR'],
      [SeverityNumber.WARN, 'WARN'],
      [SeverityNumber.INFO, 'INFO'],
      [SeverityNumber.DEBUG, 'DEBUG'],
      [SeverityNumber.TRACE, 'TRACE'],
    ]
  );
});

test('audit and metric levels map to INFO severity but keep their own node_red.level', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: AUDIT, msg: 'audited' });
  handle({ level: METRIC, msg: 'metered' });
  await tick();
  assert.deepEqual(
    deps.emitted.map((r) => [r.severityNumber, r.attributes['node_red.level']]),
    [
      [SeverityNumber.INFO, 'audit'],
      [SeverityNumber.INFO, 'metric'],
    ]
  );
});

test('maps Node-RED audit and metric event shapes without exporting sensitive correlation fields', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({
    level: AUDIT,
    event: 'auth.login',
    user: { username: 'alice' },
    path: '/admin',
    ip: '1.2.3.4',
  });
  handle({
    level: METRIC,
    event: 'node.function.done',
    nodeid: 'fn-1',
    msgid: 'message-secret',
    value: 42,
  });
  await tick();

  assert.equal(deps.emitted[0].eventName, 'auth.login');
  assert.equal(deps.emitted[0].body, 'auth.login');
  assert.deepEqual(deps.emitted[0].attributes, { 'node_red.level': 'audit' });
  assert.equal(deps.emitted[1].eventName, 'node.function.done');
  assert.equal(deps.emitted[1].body, '42');
  assert.equal(deps.emitted[1].attributes['node_red.id'], 'fn-1');
  assert.equal('msgid' in deps.emitted[1].attributes, false);
});

test('an unknown level is exported as unspecified and malformed entries fail open', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  assert.doesNotThrow(() => {
    handle(null);
    handle(undefined);
    handle({ level: 123, msg: 'unknown' });
  });
  await tick();
  assert.equal(deps.emitted.length, 1);
  assert.equal(deps.emitted[0].severityNumber, SeverityNumber.UNSPECIFIED);
  assert.equal(deps.emitted[0].severityText, 'UNSPECIFIED');
  assert.equal(deps.emitted[0].attributes['node_red.level'], '123');
});

test('carries node id, type, name, and flow id as bounded attributes', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: INFO, msg: 'hello', id: 'n1', type: 'function', name: 'my fn', z: 'flow1' });
  await tick();
  const [record] = deps.emitted;
  assert.equal(record.attributes['node_red.id'], 'n1');
  assert.equal(record.attributes['node_red.type'], 'function');
  assert.equal(record.attributes['node_red.name'], 'my fn');
  assert.equal(record.attributes['node_red.flow_id'], 'flow1');
});

test('bounds every user-controlled identity attribute', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({
    level: INFO,
    msg: 'hello',
    id: 'i'.repeat(10000),
    type: 't'.repeat(10000),
    name: 'n'.repeat(10000),
    z: 'z'.repeat(10000),
  });
  await tick();
  for (const value of Object.values(deps.emitted[0].attributes)) {
    assert.ok(String(value).length < 10000);
  }
});

test('an out-of-flow log (no node identity) carries only node_red.level', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: INFO, msg: 'Server now running' });
  await tick();
  assert.deepEqual(deps.emitted[0].attributes, { 'node_red.level': 'info' });
});

test('a plain string message is used as the body verbatim', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: INFO, msg: 'plain text' });
  await tick();
  assert.equal(deps.emitted[0].body, 'plain text');
});

test('an Error message records bounded exception attributes and uses its message as the body', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  const err = new Error('boom');
  handle({ level: ERROR, msg: err });
  await tick();
  const [record] = deps.emitted;
  assert.equal(record.body, 'boom');
  assert.equal(record.attributes['exception.type'], 'Error');
  assert.equal(record.attributes['exception.message'], 'boom');
  assert.match(record.attributes['exception.stacktrace'], /^Error: boom/);
});

test('a long message body is bounded, not sent unbounded', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: INFO, msg: 'x'.repeat(10000) });
  await tick();
  assert.ok(deps.emitted[0].body.length < 10000);
  assert.match(deps.emitted[0].body, /truncated/);
});

test('an arbitrary logged object never gets recursively serialized', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: WARN, msg: { some: 'object', nested: { a: 1 } } });
  await tick();
  // Plain-object default toString(), never JSON.stringify of the structure.
  assert.equal(deps.emitted[0].body, '[object Object]');
});

test('an arbitrary object custom toString is never invoked', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  let calls = 0;
  handle({
    level: WARN,
    msg: {
      toString() {
        calls++;
        throw new Error('must not run');
      },
    },
  });
  await tick();
  assert.equal(calls, 0);
  assert.equal(deps.emitted[0].body, '[object Object]');
});

test('a throwing message getter fails open without losing the log record', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  const msg = {};
  Object.defineProperty(msg, 'message', {
    get() {
      throw new Error('must not escape');
    },
  });
  assert.doesNotThrow(() => handle({ level: WARN, msg }));
  await tick();
  assert.equal(deps.emitted[0].body, '[object Object]');
});

test('an object with its own .message is reported through that message, not its raw shape', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: WARN, msg: { message: 'custom failure', secret: 'do-not-leak' } });
  await tick();
  assert.equal(deps.emitted[0].body, 'custom failure');
  assert.doesNotMatch(deps.emitted[0].body, /do-not-leak/);
});

test('a non-object, non-string message (e.g. a number or nothing at all) still produces a bounded body', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({ level: INFO, msg: 42 });
  handle({ level: INFO, msg: undefined });
  await tick();
  assert.equal(deps.emitted[0].body, '42');
  assert.equal(deps.emitted[1].body, 'undefined');
});

test('an audit entry never carries its req-derived user/path/ip fields', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  handle({
    level: AUDIT,
    event: 'audit-event',
    user: { username: 'alice' },
    path: '/admin',
    ip: '1.2.3.4',
  });
  await tick();
  assert.deepEqual(Object.keys(deps.emitted[0].attributes), ['node_red.level']);
});

test('a log entry that arrives before the lease resolves is emitted once it does, not dropped', async (t) => {
  let resolveAcquire;
  const slowAcquire = () => new Promise((resolve) => (resolveAcquire = resolve));
  const emitted = [];
  const getLogger = () => ({ emit: (record) => emitted.push(record) });
  const handle = createHandler(undefined, { acquire: slowAcquire, getLogger });
  t.after(() => createHandler.shutdown());

  handle({ level: INFO, msg: 'queued before ready' });
  await tick();
  assert.equal(emitted.length, 0); // lease not resolved yet, nothing thrown

  resolveAcquire({ release: async () => {} });
  await tick();
  await tick();
  assert.equal(emitted.length, 1);
  assert.equal(emitted[0].body, 'queued before ready');
});

test('the pre-initialization queue is bounded and retains the newest startup logs', async (t) => {
  let resolveAcquire;
  const slowAcquire = () => new Promise((resolve) => (resolveAcquire = resolve));
  const emitted = [];
  const handle = createHandler(undefined, {
    acquire: slowAcquire,
    getLogger: () => ({ emit: (record) => emitted.push(record) }),
  });
  t.after(() => createHandler.shutdown());

  for (let i = 0; i < 300; i += 1) {
    handle({ level: INFO, msg: `startup-${i}` });
  }
  resolveAcquire({ release: async () => {} });
  await tick();
  await tick();

  assert.equal(emitted.length, 256);
  assert.equal(emitted[0].body, 'startup-44');
  assert.equal(emitted.at(-1).body, 'startup-299');
});

test('records the original Node-RED timestamp and active context before asynchronous initialization', async (t) => {
  let resolveAcquire;
  const slowAcquire = () => new Promise((resolve) => (resolveAcquire = resolve));
  const emitted = [];
  const activeContext = ROOT_CONTEXT.setValue(Symbol.for('logging-test-context'), 'expected');
  const handle = createHandler(undefined, {
    acquire: slowAcquire,
    getLogger: () => ({ emit: (record) => emitted.push(record) }),
    activeContext: () => activeContext,
  });
  t.after(() => createHandler.shutdown());

  handle({ level: INFO, msg: 'timestamped', timestamp: 123456789 });
  resolveAcquire({ release: async () => {} });
  await tick();
  await tick();

  assert.equal(emitted[0].timestamp, 123456789);
  assert.equal(emitted[0].context, activeContext);
  assert.equal(emitted[0].eventName, 'node_red.log');
});

test('fails open: a lease that never acquires drops entries instead of throwing', async (t) => {
  const deps = fakeDeps({ acquireFails: true });
  const handle = createHandler(undefined, deps);
  t.after(() => createHandler.shutdown());
  assert.doesNotThrow(() => handle({ level: ERROR, msg: 'should not crash' }));
  await tick();
  assert.equal(deps.emitted.length, 0);
});

test('fails open when a host-owned logger throws during emit', async (t) => {
  const deps = fakeDeps();
  const handle = createHandler(undefined, {
    acquire: deps.acquire,
    getLogger: () => ({
      emit() {
        throw new Error('host logger failed');
      },
    }),
  });
  t.after(() => createHandler.shutdown());
  await tick();
  assert.doesNotThrow(() => handle({ level: ERROR, msg: 'must not escape' }));
});

test('resolves the logger once after acquisition rather than on the hot path', async (t) => {
  const deps = fakeDeps();
  let getLoggerCalls = 0;
  const getLogger = () => {
    getLoggerCalls++;
    return deps.getLogger();
  };
  const handle = createHandler(undefined, { acquire: deps.acquire, getLogger });
  t.after(() => createHandler.shutdown());
  handle({ level: INFO, msg: 'one' });
  handle({ level: INFO, msg: 'two' });
  await tick();
  assert.equal(getLoggerCalls, 1);
});

test('shutdown() releases every lease acquired by an active handler', async () => {
  const deps = fakeDeps();
  createHandler(undefined, deps);
  await tick();
  await createHandler.shutdown();
  assert.equal(deps.releaseCount(), 1);
});

test('shutdown waits for an in-flight acquisition and releases its lease', async () => {
  let resolveAcquire;
  let released = 0;
  createHandler(undefined, {
    acquire: () => new Promise((resolve) => (resolveAcquire = resolve)),
    getLogger: () => ({ emit() {} }),
  });

  const stopping = createHandler.shutdown();
  resolveAcquire({ release: async () => released++ });
  await stopping;
  assert.equal(released, 1);
});

test('shutdown() with no active lease left is a safe no-op', async () => {
  await assert.doesNotReject(createHandler.shutdown());
});

// --- Real telemetry.js wiring, not fakes: proves createHandler()'s default
// deps (telemetry.acquireLogs/getLogger) actually reach a real OTel
// LoggerProvider end to end. A registered in-memory provider stands in for
// the host-owned case (see telemetry.test.js's own such test), which keeps
// this off the network entirely.

test('with no injected deps, a real emitted record reaches a real OTel LoggerProvider', async () => {
  const exporter = new InMemoryLogRecordExporter();
  const provider = new LoggerProvider({ processors: [new SimpleLogRecordProcessor({ exporter })] });
  logs.setGlobalLoggerProvider(provider);
  try {
    const handle = createHandler();
    handle({ level: 40, msg: 'real wiring check', id: 'n9', type: 'function' });
    await tick();
    await tick();
    const [record] = exporter.getFinishedLogRecords();
    assert.equal(record.body, 'real wiring check');
    assert.equal(record.attributes['node_red.id'], 'n9');
  } finally {
    await createHandler.shutdown();
    logs.disable();
    await provider.shutdown();
  }
});
