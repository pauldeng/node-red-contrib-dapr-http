'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { SpanStatusCode, ROOT_CONTEXT, context, propagation, trace } = require('@opentelemetry/api');
const { logs } = require('@opentelemetry/api-logs');
const { setTimeout: delay } = require('node:timers/promises');
const {
  NodeTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  AlwaysOnSampler,
  AlwaysOffSampler,
} = require('@opentelemetry/sdk-trace-node');
const {
  LoggerProvider,
  InMemoryLogRecordExporter,
  SimpleLogRecordProcessor,
} = require('@opentelemetry/sdk-logs');

const {
  acquire,
  getTracer,
  endSpan,
  resolveSampler,
  startNodeSpan,
  completeNodeSpan,
  registerFlowSpanHooks,
  acquireLogs,
  getLogger,
  resolveLogProcessorOptions,
  shouldExportLogs,
} = require('../../lib/telemetry');

// A standalone provider/exporter, entirely separate from the module's own
// acquire()-managed singleton, so endSpan()'s status/exception recording can
// be inspected in memory without touching global tracer registration.
function inMemoryTracer({ sampler = new AlwaysOnSampler() } = {}) {
  const exporter = new InMemorySpanExporter();
  const provider = new NodeTracerProvider({
    sampler,
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  return { provider, tracer: provider.getTracer('test'), exporter };
}

// --- resolveSampler: pure, no globals touched ---

test('resolveSampler defaults to parentbased_traceidratio at 0.1 when unset', () => {
  const sampler = resolveSampler({});
  assert.equal(
    sampler.toString(),
    'ParentBased{root=TraceIdRatioBased{0.1}, remoteParentSampled=AlwaysOnSampler, remoteParentNotSampled=AlwaysOffSampler, localParentSampled=AlwaysOnSampler, localParentNotSampled=AlwaysOffSampler}'
  );
});

test('resolveSampler honors OTEL_TRACES_SAMPLER_ARG for the default sampler', () => {
  const sampler = resolveSampler({ OTEL_TRACES_SAMPLER_ARG: '0.4' });
  assert.match(sampler.toString(), /TraceIdRatioBased\{0\.4\}/);
});

test('resolveSampler falls back to 0.1 for an out-of-range OTEL_TRACES_SAMPLER_ARG', () => {
  assert.match(
    resolveSampler({ OTEL_TRACES_SAMPLER_ARG: '5' }).toString(),
    /TraceIdRatioBased\{0\.1\}/
  );
  assert.match(
    resolveSampler({ OTEL_TRACES_SAMPLER_ARG: '-1' }).toString(),
    /TraceIdRatioBased\{0\.1\}/
  );
  assert.match(
    resolveSampler({ OTEL_TRACES_SAMPLER_ARG: 'abc' }).toString(),
    /TraceIdRatioBased\{0\.1\}/
  );
});

test('resolveSampler honors OTEL_TRACES_SAMPLER=always_on', () => {
  assert.equal(resolveSampler({ OTEL_TRACES_SAMPLER: 'always_on' }).toString(), 'AlwaysOnSampler');
});

test('resolveSampler honors OTEL_TRACES_SAMPLER=always_off', () => {
  assert.equal(
    resolveSampler({ OTEL_TRACES_SAMPLER: 'always_off' }).toString(),
    'AlwaysOffSampler'
  );
});

test('resolveSampler honors OTEL_TRACES_SAMPLER=parentbased_always_on', () => {
  assert.match(
    resolveSampler({ OTEL_TRACES_SAMPLER: 'parentbased_always_on' }).toString(),
    /root=AlwaysOnSampler/
  );
});

test('resolveSampler honors OTEL_TRACES_SAMPLER=parentbased_always_off', () => {
  assert.match(
    resolveSampler({ OTEL_TRACES_SAMPLER: 'parentbased_always_off' }).toString(),
    /root=AlwaysOffSampler/
  );
});

test('resolveSampler honors OTEL_TRACES_SAMPLER=traceidratio (not parent-based)', () => {
  const sampler = resolveSampler({
    OTEL_TRACES_SAMPLER: 'traceidratio',
    OTEL_TRACES_SAMPLER_ARG: '0.7',
  });
  assert.equal(sampler.toString(), 'TraceIdRatioBased{0.7}');
});

test('resolveSampler falls back to the default ratio sampler for an unrecognized value', () => {
  assert.match(
    resolveSampler({ OTEL_TRACES_SAMPLER: 'not-a-real-sampler' }).toString(),
    /TraceIdRatioBased\{0\.1\}/
  );
});

// Deterministic isRecording() for lifecycle checks. No collector is running
// here, so every acquire() below really does try (and fail) to reach
// 127.0.0.1:4318 on release. acquire()'s own env parameter only reaches
// resolveSampler()/buildResource() — OTLPTraceExporter() reads real
// process.env directly (as it must: production always calls acquire() with
// no override, so it and resolveSampler() see the same environment) — so an
// OTEL_EXPORTER_OTLP_TIMEOUT set here would be silently inert; the bound that
// keeps this failure fast is lib/telemetry.js's own SHUTDOWN_TIMEOUT_MS.
const ALWAYS_ON = { OTEL_TRACES_SAMPLER: 'always_on' };

// --- acquire/release: process-wide lifecycle ---
// Every test below fully releases what it acquires, mirroring
// test/unit/app-channel.test.js's convention for module-scoped shared state.

test('the global tracer is a no-op before any connection acquires tracing', () => {
  const span = getTracer().startSpan('probe');
  assert.equal(span.isRecording(), false);
  span.end();
});

test('a failed provider initialization does not poison a later acquire', async () => {
  const badEnv = {
    get OTEL_TRACES_SAMPLER() {
      throw new Error('bad sampler environment');
    },
  };
  await assert.rejects(acquire({ env: badEnv }), /bad sampler environment/);

  const handle = await acquire({ env: ALWAYS_ON });
  try {
    assert.equal(getTracer().startSpan('recovered').isRecording(), true);
  } finally {
    await handle.release();
  }
});

test('acquire() activates the global tracer; release() deactivates it', async () => {
  const handle = await acquire({ env: ALWAYS_ON });
  try {
    const span = getTracer().startSpan('probe');
    assert.equal(span.isRecording(), true);
    span.end();
  } finally {
    await handle.release();
  }
  const span = getTracer().startSpan('probe-after-release');
  assert.equal(span.isRecording(), false);
  span.end();
});

test('acquire() ref-counts: the tracer stays active until every handle releases', async () => {
  const a = await acquire({ env: ALWAYS_ON });
  const b = await acquire({ env: ALWAYS_ON });
  try {
    await a.release();
    assert.equal(getTracer().startSpan('still-active').isRecording(), true);
  } finally {
    await b.release();
  }
  assert.equal(getTracer().startSpan('now-inactive').isRecording(), false);
});

test('release() is idempotent', async () => {
  const handle = await acquire({ env: ALWAYS_ON });
  await handle.release();
  await handle.release(); // must not throw, must not double-decrement
  assert.equal(getTracer().startSpan('after-double-release').isRecording(), false);
});

test('concurrent acquire() calls share one provider and register it exactly once', async () => {
  const [a, b] = await Promise.all([acquire({ env: ALWAYS_ON }), acquire({ env: ALWAYS_ON })]);
  try {
    assert.equal(getTracer().startSpan('concurrent').isRecording(), true);
    await a.release();
    assert.equal(getTracer().startSpan('still-up').isRecording(), true);
  } finally {
    await b.release();
  }
  assert.equal(getTracer().startSpan('down').isRecording(), false);
});

test('acquire/release preserves an OpenTelemetry provider installed by the host', async () => {
  const external = inMemoryTracer();
  external.provider.register();
  try {
    const handle = await acquire({ env: ALWAYS_ON });
    await handle.release();

    endSpan(getTracer().startSpan('host-owned'));
    assert.equal(external.exporter.getFinishedSpans().at(-1)?.name, 'host-owned');
  } finally {
    trace.disable();
    context.disable();
    propagation.disable();
    await external.provider.shutdown();
  }
});

// --- acquireLogs/getLogger: an independent lease sharing only resource
// detection with the trace lease above. ---

function inMemoryLogger() {
  const exporter = new InMemoryLogRecordExporter();
  const provider = new LoggerProvider({
    processors: [new SimpleLogRecordProcessor({ exporter })],
  });
  return { provider, exporter };
}

test('log batch processor options honor the standard OTEL_BLRP environment variables', () => {
  assert.deepEqual(resolveLogProcessorOptions({}), {
    scheduledDelayMillis: 1000,
    exportTimeoutMillis: 30000,
    maxQueueSize: 2048,
    maxExportBatchSize: 512,
  });
  assert.deepEqual(
    resolveLogProcessorOptions({
      OTEL_BLRP_SCHEDULE_DELAY: '25',
      OTEL_BLRP_EXPORT_TIMEOUT: '50',
      OTEL_BLRP_MAX_QUEUE_SIZE: '8',
      OTEL_BLRP_MAX_EXPORT_BATCH_SIZE: '4',
    }),
    {
      scheduledDelayMillis: 25,
      exportTimeoutMillis: 50,
      maxQueueSize: 8,
      maxExportBatchSize: 4,
    }
  );
});

test('invalid log batch options fall back safely and batch size never exceeds queue size', () => {
  assert.deepEqual(
    resolveLogProcessorOptions({
      OTEL_BLRP_SCHEDULE_DELAY: '-1',
      OTEL_BLRP_EXPORT_TIMEOUT: 'not-a-number',
      OTEL_BLRP_MAX_QUEUE_SIZE: '10',
      OTEL_BLRP_MAX_EXPORT_BATCH_SIZE: '100',
    }),
    {
      scheduledDelayMillis: 1000,
      exportTimeoutMillis: 30000,
      maxQueueSize: 10,
      maxExportBatchSize: 10,
    }
  );
});

test('OTEL_LOGS_EXPORTER and OTEL_SDK_DISABLED can disable log export', () => {
  assert.equal(shouldExportLogs({}), true);
  assert.equal(shouldExportLogs({ OTEL_LOGS_EXPORTER: 'otlp' }), true);
  assert.equal(shouldExportLogs({ OTEL_LOGS_EXPORTER: 'none' }), false);
  assert.equal(shouldExportLogs({ OTEL_SDK_DISABLED: 'TRUE' }), false);
});

test('the global logger is a no-op before anything acquires logging', () => {
  // A no-op Logger accepts emit() and discards it; nothing to assert other
  // than that it never throws.
  assert.doesNotThrow(() => getLogger().emit({ body: 'probe' }));
});

test('acquireLogs() activates the global logger; release() deactivates it', async () => {
  const handle = await acquireLogs();
  try {
    assert.ok(logs.getLoggerProvider() instanceof LoggerProvider);
  } finally {
    await handle.release();
  }
  assert.equal(logs.getLoggerProvider() instanceof LoggerProvider, false);
});

test('acquireLogs() ref-counts: the logger stays active until every handle releases', async () => {
  const a = await acquireLogs();
  const b = await acquireLogs();
  const registeredWhileBothHeld = logs.getLoggerProvider();
  try {
    await a.release();
    assert.equal(logs.getLoggerProvider(), registeredWhileBothHeld);
  } finally {
    await b.release();
  }
  assert.notEqual(logs.getLoggerProvider(), registeredWhileBothHeld);
});

test('acquireLogs() release() is idempotent', async () => {
  const handle = await acquireLogs();
  await handle.release();
  await assert.doesNotReject(handle.release());
});

test('enabling logs does not register a tracer, and disabling tracing does not stop logging', async () => {
  const logsHandle = await acquireLogs();
  try {
    assert.equal(getTracer().startSpan('should-be-noop').isRecording(), false);

    const traceHandle = await acquire({ env: ALWAYS_ON });
    await traceHandle.release();
    // The trace lease's own disable() calls must never have touched logs.*.
    assert.doesNotThrow(() => getLogger().emit({ body: 'still up' }));
  } finally {
    await logsHandle.release();
  }
});

test('acquireLogs preserves a LoggerProvider installed by the host', async () => {
  const external = inMemoryLogger();
  const installed = logs.setGlobalLoggerProvider(external.provider);
  assert.equal(installed, external.provider);
  try {
    const handle = await acquireLogs();
    await handle.release();

    getLogger().emit({ body: 'host-owned' });
    assert.equal(external.exporter.getFinishedLogRecords().at(-1)?.body, 'host-owned');
  } finally {
    logs.disable();
    await external.provider.shutdown();
  }
});

// --- endSpan: shared status/exception recording for every boundary span ---

test('endSpan() with no error sets status OK', () => {
  const { tracer, exporter } = inMemoryTracer();
  endSpan(tracer.startSpan('ok'));
  const [span] = exporter.getFinishedSpans();
  assert.equal(span.status.code, SpanStatusCode.OK);
  assert.deepEqual(span.events, []);
});

test('endSpan() with an error records the exception and sets status ERROR', () => {
  const { tracer, exporter } = inMemoryTracer();
  endSpan(tracer.startSpan('failed'), new Error('publish failed with status 500'));
  const [span] = exporter.getFinishedSpans();
  assert.equal(span.status.code, SpanStatusCode.ERROR);
  assert.equal(span.status.message, 'publish failed with status 500');
  assert.equal(span.events.length, 1);
  assert.equal(span.events[0].name, 'exception');
});

// --- startNodeSpan / completeNodeSpan: one span per node per message ---

const FAKE_NODE = { id: 'n1', type: 'function', name: 'my function' };

// completeNodeSpan() -> registry.settle() resolves its promise synchronously,
// but finishNodeSpan() resumes on the next microtask — await one tick before
// reading the exporter.
const tick = () => new Promise((resolve) => setImmediate(resolve));

test('startNodeSpan() starts a child span with node identity attributes', async () => {
  const { tracer, exporter } = inMemoryTracer();
  const parentSpan = tracer.startSpan('parent');
  const parentContext = trace.setSpan(ROOT_CONTEXT, parentSpan);

  const ctx = startNodeSpan(tracer, parentContext, FAKE_NODE);
  completeNodeSpan(ctx, undefined);
  await tick();

  const [span] = exporter.getFinishedSpans();
  assert.equal(span.name, 'function');
  assert.equal(span.attributes['node_red.id'], 'n1');
  assert.equal(span.attributes['node_red.type'], 'function');
  assert.equal(span.attributes['node_red.name'], 'my function');
  assert.equal(span.parentSpanContext.spanId, parentSpan.spanContext().spanId);
  assert.equal(span.status.code, SpanStatusCode.OK);
});

test('startNodeSpan() omits node_red.name when the node has none', async () => {
  const { tracer, exporter } = inMemoryTracer();
  const ctx = startNodeSpan(tracer, ROOT_CONTEXT, { id: 'n2', type: 'switch' });
  completeNodeSpan(ctx, undefined);
  await tick();
  const [span] = exporter.getFinishedSpans();
  assert.equal('node_red.name' in span.attributes, false);
});

test('completeNodeSpan() with an error records the exception and sets status ERROR', async () => {
  const { tracer, exporter } = inMemoryTracer();
  const ctx = startNodeSpan(tracer, ROOT_CONTEXT, FAKE_NODE);
  completeNodeSpan(ctx, new Error('node threw'));
  await tick();
  const [span] = exporter.getFinishedSpans();
  assert.equal(span.status.code, SpanStatusCode.ERROR);
  assert.equal(span.events[0].name, 'exception');
});

test('completeNodeSpan() on a context with no token is a safe no-op', () => {
  assert.doesNotThrow(() => completeNodeSpan(ROOT_CONTEXT, undefined));
});

test('a node span with a no-op tracer never registers a pending entry', () => {
  const { tracer, exporter } = inMemoryTracer({ sampler: new AlwaysOffSampler() });
  const ctx = startNodeSpan(tracer, ROOT_CONTEXT, FAKE_NODE);
  // No token was set, so completing it is a no-op; nothing was ever pending
  // to leak, and ending it here would double-end an already-ended span.
  completeNodeSpan(ctx, undefined);
  assert.equal(exporter.getFinishedSpans().length, 0);
});

test('an unsampled child completion cannot settle its recorded parent delivery', async () => {
  const { tracer, exporter } = inMemoryTracer();
  const { tracer: unsampled } = inMemoryTracer({ sampler: new AlwaysOffSampler() });
  const parent = startNodeSpan(tracer, ROOT_CONTEXT, FAKE_NODE);
  try {
    const child = startNodeSpan(unsampled, parent, { id: 'child', type: 'function' });
    completeNodeSpan(child, new Error('child failure'));
    await tick();
    assert.equal(exporter.getFinishedSpans().length, 0, 'parent is still processing');
  } finally {
    completeNodeSpan(parent, undefined);
    await tick();
  }
  assert.equal(exporter.getFinishedSpans()[0].status.code, SpanStatusCode.OK);
});

test('a node span that never completes closes itself as incomplete after its timeout', async () => {
  const { tracer, exporter } = inMemoryTracer();
  startNodeSpan(tracer, ROOT_CONTEXT, FAKE_NODE, { timeoutMs: 20 });
  await delay(80);
  const [span] = exporter.getFinishedSpans();
  assert.equal(span.attributes['node_red.span.incomplete'], true);
  assert.equal(span.status.code, SpanStatusCode.OK); // incomplete, not an error
});

// --- registerFlowSpanHooks: the RED.hooks wiring ---

function fakeHooks() {
  const registered = new Map();
  return {
    registered,
    add(name, fn) {
      registered.set(name, fn);
    },
  };
}

test('registerFlowSpanHooks() registers exactly the four documented hooks', () => {
  const hooks = fakeHooks();
  registerFlowSpanHooks(hooks);
  assert.deepEqual([...hooks.registered.keys()].sort(), [
    'onComplete',
    'onReceive',
    'onSend',
    'preDeliver',
  ]);
});

// These two tests need a real AsyncLocalStorageContextManager registered:
// with nothing registered (the state every other test in this file leaves
// things in), @opentelemetry/api's own default context implementation does
// not thread context.with()'s argument back out through context.active() at
// all -- proven directly, not assumed -- so they would pass or fail on an
// artifact of that default rather than on this module's own logic. Every
// real deployment has a context manager registered the moment any connection
// enables tracing, which is exactly the state these tests recreate.
test('onSend stashes the active context on every event; preDeliver restores it', async () => {
  const handle = await acquire({ env: ALWAYS_ON });
  try {
    const hooks = fakeHooks();
    registerFlowSpanHooks(hooks);
    const span = getTracer().startSpan('sender');
    const senderContext = trace.setSpan(context.active(), span);

    const sendEvents = [{}, {}];
    context.with(senderContext, () => hooks.registered.get('onSend')(sendEvents));
    for (const sendEvent of sendEvents) {
      assert.equal(sendEvent._otelContext, senderContext);
    }

    let observedInsideDone = null;
    hooks.registered.get('preDeliver')(sendEvents[0], () => {
      observedInsideDone = context.active();
    });
    assert.equal(observedInsideDone, senderContext);
  } finally {
    await handle.release();
  }
});

test('preDeliver falls back to the active context when a SendEvent was never seen by onSend', async () => {
  const handle = await acquire({ env: ALWAYS_ON });
  try {
    const hooks = fakeHooks();
    registerFlowSpanHooks(hooks);
    let observed;
    hooks.registered.get('preDeliver')({}, () => {
      observed = context.active();
    });
    assert.equal(observed, context.active());
  } finally {
    await handle.release();
  }
});

test('onReceive starts a node span as a child of the active context; onComplete settles it', async () => {
  const handle = await acquire({ env: ALWAYS_ON });
  try {
    const hooks = fakeHooks();
    const { tracer, exporter } = inMemoryTracer();
    registerFlowSpanHooks(hooks, { tracer });

    const parentSpan = tracer.startSpan('parent');
    const parentContext = trace.setSpan(ROOT_CONTEXT, parentSpan);

    let receivedContext = null;
    context.with(parentContext, () => {
      hooks.registered.get('onReceive')({ destination: { node: FAKE_NODE } }, () => {
        receivedContext = context.active();
      });
    });

    const nodeSpan = trace.getSpan(receivedContext);
    assert.notEqual(nodeSpan, parentSpan);

    let completeDoneCalled = false;
    context.with(receivedContext, () => {
      hooks.registered.get('onComplete')({ error: undefined }, () => {
        completeDoneCalled = true;
      });
    });
    assert.equal(completeDoneCalled, true);
    await tick();

    const [finished] = exporter.getFinishedSpans();
    assert.equal(finished.name, 'function');
    assert.equal(finished.status.code, SpanStatusCode.OK);
    assert.equal(finished.parentSpanContext.spanId, parentSpan.spanContext().spanId);
  } finally {
    await handle.release();
  }
});

test('flow hooks use the current tracer after tracing is disabled and enabled again', async () => {
  const hooks = fakeHooks();
  registerFlowSpanHooks(hooks);

  async function runGeneration(name) {
    const generation = inMemoryTracer();
    generation.provider.register();
    try {
      let receivedContext;
      hooks.registered.get('onReceive')({ destination: { node: FAKE_NODE } }, () => {
        receivedContext = context.active();
      });
      context.with(receivedContext, () =>
        hooks.registered.get('onComplete')({ error: undefined }, () => {})
      );
      await tick();
      assert.equal(generation.exporter.getFinishedSpans().at(-1)?.name, 'function', name);
    } finally {
      trace.disable();
      context.disable();
      propagation.disable();
      await generation.provider.shutdown();
    }
  }

  await runGeneration('first provider');
  await runGeneration('replacement provider');
});
