'use strict';

const crypto = require('node:crypto');

const {
  trace,
  context,
  propagation,
  SpanStatusCode,
  createContextKey,
  ProxyTracerProvider,
} = require('@opentelemetry/api');
const {
  NodeTracerProvider,
  BatchSpanProcessor,
  ParentBasedSampler,
  TraceIdRatioBasedSampler,
  AlwaysOnSampler,
  AlwaysOffSampler,
} = require('@opentelemetry/sdk-trace-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
const { detectResources, envDetector, defaultResource } = require('@opentelemetry/resources');
const { logs } = require('@opentelemetry/api-logs');
const { LoggerProvider, BatchLogRecordProcessor } = require('@opentelemetry/sdk-logs');
const { OTLPLogExporter } = require('@opentelemetry/exporter-logs-otlp-http');

const { PendingRegistry } = require('./pending');

const { version: PACKAGE_VERSION } = require('../package.json');
const TRACER_NAME = '@pauldeng/node-red-contrib-dapr-http';

// Our own proposed default. sdk-trace-base's own buildSamplerFromEnv()
// defaults to parentbased_always_on when OTEL_TRACES_SAMPLER is unset; this
// package proposes parentbased_traceidratio@0.1 instead, so it is built here
// rather than delegated to the SDK's default.
const DEFAULT_SAMPLE_RATIO = 0.1;
const NOOP_TRACER_PROVIDER = new ProxyTracerProvider().getDelegate();

function boundedRatio(raw) {
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 && n <= 1 ? n : DEFAULT_SAMPLE_RATIO;
}

// Mirrors the standard OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG contract
// (see sdk-trace-base's buildSamplerFromEnv), with this package's own default
// substituted for the unset case.
function resolveSampler(env) {
  const ratio = () => boundedRatio(env.OTEL_TRACES_SAMPLER_ARG);
  switch (env.OTEL_TRACES_SAMPLER) {
    case 'always_on':
      return new AlwaysOnSampler();
    case 'always_off':
      return new AlwaysOffSampler();
    case 'parentbased_always_on':
      return new ParentBasedSampler({ root: new AlwaysOnSampler() });
    case 'parentbased_always_off':
      return new ParentBasedSampler({ root: new AlwaysOffSampler() });
    case 'traceidratio':
      return new TraceIdRatioBasedSampler(ratio());
    default:
      // Covers both the unset case and an unrecognized value — same fallback
      // sdk-trace-base's own buildSamplerFromEnv uses for the latter.
      return new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(ratio()) });
  }
}

// service.name and other resource attributes come from the standard
// OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME env vars (envDetector), merged
// over the OTel-spec fallback (defaultResource's "unknown_service:node" etc.)
// so unset env vars still produce a valid resource.
async function buildResource() {
  const detected = detectResources({ detectors: [envDetector] });
  await detected.waitForAsyncAttributes();
  return defaultResource().merge(detected);
}

async function initProvider(env) {
  const current = trace.getTracerProvider();
  if (!(current instanceof ProxyTracerProvider) || current.getDelegate() !== NOOP_TRACER_PROVIDER) {
    // Node-RED may already be started through an application-owned OTel SDK.
    // Use that provider and leave its lifecycle alone; replacing or disabling
    // it would break every other instrumented module in the process.
    return { provider: null, ownsGlobals: false };
  }

  const resource = await buildResource(env);
  const provider = new NodeTracerProvider({
    resource,
    sampler: resolveSampler(env),
    // OTLPTraceExporter reads OTEL_EXPORTER_OTLP_(TRACES_)ENDPOINT/HEADERS
    // itself when constructed with no explicit url/headers. BatchSpanProcessor's
    // own defaults (maxQueueSize 2048, maxExportBatchSize 512) already bound
    // memory; nothing here needs overriding for that.
    spanProcessors: [new BatchSpanProcessor(new OTLPTraceExporter())],
  });
  // Registers the AsyncLocalStorage-backed context manager and the W3C
  // TraceContext + Baggage propagator as the process globals — this is the
  // one call that makes every dapr-* module's direct use of
  // @opentelemetry/api (context.with, propagation.inject/extract,
  // trace.getTracer(...).startSpan) do something, instead of the no-op
  // default those calls harmlessly resolve to when nothing has registered.
  provider.register();
  const registered = trace.getTracerProvider();
  return {
    provider,
    // Another initializer can win while buildResource() is awaiting. Check
    // the actual delegate after registration so release never tears down a
    // provider this package did not install.
    ownsGlobals: registered instanceof ProxyTracerProvider && registered.getDelegate() === provider,
  };
}

// Process-wide singleton, ref-counted across every dapr-connection with
// tracing enabled. OTel config here is entirely env-var driven (the standard
// OTEL_* variables), so there is no per-caller config to merge. refCount
// is incremented synchronously, before the first await, so a release() racing
// a concurrent in-flight acquire() can never tear down a generation the
// second caller is joining.
let sharedPromise = null;
let refCount = 0;

// BasicTracerProvider.shutdown() flushes via the span processor's own
// _shutdown(), which — unlike forceFlush() — has no timeout and no catch of
// its own: an unreachable collector's export attempt (with its underlying
// HTTP client's own retry/backoff) can otherwise make a connection's close
// handler hang for several seconds and then reject. Bounded and swallowed
// here so a stopped or unreachable collector never delays or fails a
// connection teardown — the same fail-open guarantee this package gives the
// message path, extended to shutdown.
const SHUTDOWN_TIMEOUT_MS = 3000;

async function shutdownIgnoringErrors(provider) {
  try {
    await provider.shutdown();
  } catch {
    // Telemetry teardown is fail-open.
  }
}

async function shutdownFailOpen(provider) {
  const { promise: timeout, resolve } = Promise.withResolvers();
  const timer = setTimeout(resolve, SHUTDOWN_TIMEOUT_MS);
  timer.unref?.(); // never hold the process open on the losing branch
  await Promise.race([shutdownIgnoringErrors(provider), timeout]);
  clearTimeout(timer);
}

async function acquire({ env = process.env } = {}) {
  refCount += 1;
  if (!sharedPromise) {
    sharedPromise = initProvider(env);
  }
  let state;
  try {
    state = await sharedPromise;
  } catch (err) {
    refCount -= 1;
    if (refCount === 0) {
      sharedPromise = null;
    }
    throw err;
  }

  let released = false;
  return {
    async release() {
      if (released) {
        return;
      }
      released = true;
      refCount -= 1;
      if (refCount > 0) {
        return;
      }
      sharedPromise = null;
      if (!state.ownsGlobals) {
        if (state.provider) {
          await shutdownFailOpen(state.provider);
        }
        return;
      }
      nodeSpanRegistry.drain(INCOMPLETE);
      // Reverse the global registration first, so no new span can start
      // against a provider that is about to shut down; the already-buffered
      // spans still flush via this function's own `provider` reference.
      trace.disable();
      context.disable();
      propagation.disable();
      // PendingRegistry settles through promise reactions. Queue shutdown
      // behind those reactions so drained incomplete spans reach the batch
      // processor before it closes.
      await Promise.resolve(); // allow-promise: flush settled span reactions before exporter shutdown
      await shutdownFailOpen(state.provider);
    },
  };
}

// Safe to call whether or not any connection has ever acquired tracing: with
// nothing registered this returns a no-op tracer (isRecording() === false on
// every span it creates), so callers never need an "is tracing enabled"
// branch of their own.
function getTracer() {
  return trace.getTracer(TRACER_NAME, PACKAGE_VERSION);
}

// --- Application-log export: a second, independent lease sharing only the
// resource-detection logic above. Enabling it never touches trace.* globals
// or registers a tracer, and it is never released by a connection's own
// acquire()/release() (see lib/logging-bridge.js: this lease is process-
// lifetime, held by the settings.js logging bridge, not by a connection).
async function initLoggerProvider(env) {
  const resource = await buildResource(env);
  const processors = [];
  if (shouldExportLogs(env)) {
    processors.push(
      new BatchLogRecordProcessor({
        exporter: new OTLPLogExporter(),
        ...resolveLogProcessorOptions(env),
      })
    );
  }
  const provider = new LoggerProvider({
    resource,
    processors,
  });
  // logs.setGlobalLoggerProvider is itself non-clobbering: if a provider is
  // already registered (host-owned, or a concurrent initializer that won the
  // race), it returns that one instead of installing ours. Unlike trace's
  // ProxyTracerProvider, api-logs does not expose a public way to check "is
  // anything registered yet" before constructing a candidate — but
  // constructing one is free (neither the processor nor the exporter opens
  // anything until a record is actually emitted through it), so the loser is
  // simply shut down unused rather than needing a separate probe.
  const installed = logs.setGlobalLoggerProvider(provider);
  const ownsGlobals = installed === provider;
  if (!ownsGlobals) {
    await shutdownFailOpen(provider);
  }
  return { provider, ownsGlobals };
}

let logsSharedPromise = null;
let logsRefCount = 0;

const LOG_PROCESSOR_DEFAULTS = {
  scheduledDelayMillis: 1000,
  exportTimeoutMillis: 30000,
  maxQueueSize: 2048,
  maxExportBatchSize: 512,
};

function envInteger(value, fallback, minimum) {
  if (value === undefined || value === null || String(value).trim() === '') {
    return fallback;
  }
  const text = String(value).trim();
  if (!/^\d+$/.test(text)) {
    return fallback;
  }
  const parsed = Number(text);
  return Number.isSafeInteger(parsed) && parsed >= minimum ? parsed : fallback;
}

function resolveLogProcessorOptions(env) {
  const maxQueueSize = envInteger(
    env.OTEL_BLRP_MAX_QUEUE_SIZE,
    LOG_PROCESSOR_DEFAULTS.maxQueueSize,
    1
  );
  const requestedBatchSize = envInteger(
    env.OTEL_BLRP_MAX_EXPORT_BATCH_SIZE,
    LOG_PROCESSOR_DEFAULTS.maxExportBatchSize,
    1
  );
  return {
    scheduledDelayMillis: envInteger(
      env.OTEL_BLRP_SCHEDULE_DELAY,
      LOG_PROCESSOR_DEFAULTS.scheduledDelayMillis,
      0
    ),
    exportTimeoutMillis: envInteger(
      env.OTEL_BLRP_EXPORT_TIMEOUT,
      LOG_PROCESSOR_DEFAULTS.exportTimeoutMillis,
      0
    ),
    maxQueueSize,
    maxExportBatchSize: Math.min(requestedBatchSize, maxQueueSize),
  };
}

function shouldExportLogs(env) {
  if (String(env.OTEL_SDK_DISABLED).trim().toLowerCase() === 'true') {
    return false;
  }
  const configured = String(env.OTEL_LOGS_EXPORTER ?? '').trim();
  if (!configured) {
    return true;
  }
  const exporters = configured.split(',').map((value) => value.trim().toLowerCase());
  return !exporters.includes('none') && exporters.includes('otlp');
}

async function acquireLogs({ env = process.env } = {}) {
  logsRefCount += 1;
  if (!logsSharedPromise) {
    logsSharedPromise = initLoggerProvider(env);
  }
  let state;
  try {
    state = await logsSharedPromise;
  } catch (err) {
    logsRefCount -= 1;
    if (logsRefCount === 0) {
      logsSharedPromise = null;
    }
    throw err;
  }

  let released = false;
  return {
    async release() {
      if (released) {
        return;
      }
      released = true;
      logsRefCount -= 1;
      if (logsRefCount > 0) {
        return;
      }
      logsSharedPromise = null;
      if (!state.ownsGlobals) {
        return; // host-owned, or already shut down above as the race's loser
      }
      logs.disable();
      await shutdownFailOpen(state.provider);
    },
  };
}

// The logging bridge resolves this once after its process-lifetime lease is
// acquired; it never emits through that Logger after releasing the lease.
function getLogger() {
  return logs.getLogger(TRACER_NAME, PACKAGE_VERSION);
}

// Shared status/exception recording for every Dapr boundary span (producer,
// consumer, client, server): no error -> OK; an error -> the exception
// recorded as a span event and status ERROR. A no-op span (tracing disabled,
// or sampled out) accepts and discards all of this the same as a real one.
function endSpan(span, err) {
  if (err) {
    span.recordException(err);
    span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
  } else {
    span.setStatus({ code: SpanStatusCode.OK });
  }
  span.end();
}

// --- Node-RED flow spans: one span per node per message, via RED.hooks ---
//
// Global once registered (registerFlowSpanHooks is called once, at node-type
// registration, not per connection): once any connection enables tracing,
// every node in every flow gets a span, matching how OTel auto-instrumentation
// works elsewhere in the ecosystem (on means on for the process). A no-op span
// (nothing has called acquire()) costs nothing and needs no branch of its own.
//
// The mechanism:
// - onSend captures the active context on each SendEvent (a property on the
//   event object, never on msg).
// - preDeliver re-enters that captured context before letting Node-RED
//   schedule the (by default asynchronous, via setImmediate) delivery, so the
//   destination node's onReceive sees the sender's context regardless of
//   what ran in between — explicit capture/restore at the framework
//   boundary, not a reliance on every node's own code correctly propagating
//   AsyncLocalStorage across its own async gaps.
// - onReceive starts this node's span as a child of the active context, mints
//   a delivery token, and enters a new context carrying both before letting
//   the node's own input handler run.
// - onComplete reads the token back from the active context and settles it.
//   _msgid is deliberately never used as this key: Node-RED preserves one
//   _msgid across cloned branches and successive nodes, so it cannot identify
//   one node's one delivery the way a freshly minted token does.
const NODE_SPAN_TOKEN_KEY = createContextKey(
  '@pauldeng/node-red-contrib-dapr-http node span delivery token'
);
const SEND_CONTEXT_KEY = '_otelContext'; // property on a SendEvent, never on msg

// Reused Node-RED-wide for exactly the reason lib/pending.js already exists:
// a bounded, first-wins correlation store. Distinct from a connection's own
// pendingAcks/pendingResponses (which stay per-connection, keyed by ackId/
// responseId, and are reused as-is for consumer/server span lifecycle) —
// this one is process-wide because a node span applies to every node in every
// flow, not only Dapr ones.
const nodeSpanRegistry = new PendingRegistry({ max: 10000 });
// How long a node span may stay open with no done() to close it before it is
// force-closed as node_red.span.incomplete. Many existing nodes' input
// handlers take only (msg) or (msg, send) — never the 3-arg (msg, send, done)
// form Node-RED's own onComplete hook depends on — so this timeout, not a
// done() call, is how most such spans actually end; kept at the same 30s
// default as a connection's own request timeout for consistency, not tuned
// against it.
const NODE_SPAN_TIMEOUT_MS = 30000;
const INCOMPLETE = Symbol('node-span-incomplete');

function nodeSpanAttributes(node) {
  const attrs = { 'node_red.id': node.id, 'node_red.type': node.type };
  if (node.name) {
    attrs['node_red.name'] = node.name;
  }
  return attrs;
}

async function finishNodeSpan(span, settlement) {
  try {
    const outcome = await settlement;
    if (outcome === INCOMPLETE) {
      span.setAttribute('node_red.span.incomplete', true);
      endSpan(span);
    } else {
      endSpan(span, outcome);
    }
  } catch {
    // Instrumentation must never create an unhandled rejection.
  }
}

// Starts one node's span as a child of parentContext, registers its bounded
// timeout-or-settle lifecycle, and returns the context the node's own input
// handler should run in (span + delivery token both set). `tracer` is passed
// in rather than read from getTracer() here so this stays a pure function a
// test can drive with an in-memory tracer.
function startNodeSpan(tracer, parentContext, node, { timeoutMs = NODE_SPAN_TIMEOUT_MS } = {}) {
  const span = tracer.startSpan(node.type, { attributes: nodeSpanAttributes(node) }, parentContext);
  const spanContext = trace.setSpan(parentContext, span);
  if (!span.isRecording()) {
    // A child without its own tracked delivery must not inherit the token
    // that would let its completion settle its parent's pending span.
    return spanContext.deleteValue(NODE_SPAN_TOKEN_KEY);
  }
  const token = crypto.randomUUID();
  let settlement;
  try {
    settlement = nodeSpanRegistry.add(token, { timeoutMs, onTimeout: () => INCOMPLETE });
  } catch {
    // Capacity or (astronomically unlikely) a duplicate UUID: close the span
    // now rather than never, and skip the token — nothing will look it up.
    endSpan(span);
    return spanContext.deleteValue(NODE_SPAN_TOKEN_KEY);
  }
  void finishNodeSpan(span, settlement);
  return spanContext.setValue(NODE_SPAN_TOKEN_KEY, token);
}

// Reads the delivery token back from the active context (set by
// startNodeSpan) and settles that node's span. A context carrying no token —
// tracing disabled, or completeNodeSpan reached from somewhere startNodeSpan
// never ran — is a safe no-op.
function completeNodeSpan(activeContext, error) {
  const token = activeContext.getValue(NODE_SPAN_TOKEN_KEY);
  if (token) {
    nodeSpanRegistry.settle(token, error);
  }
}

// Registers the four hooks that give every Node-RED node its own span. Takes
// RED.hooks itself (not RED), so this stays testable with a fake {add}; the
// tracer can likewise be injected so a test can use an in-memory one instead
// of resolving the current process-wide tracer for each delivery.
function registerFlowSpanHooks(hooks, { tracer } = {}) {
  // onSend must stay synchronous (Node-RED's own contract for this hook) —
  // stashing a property is exactly that.
  hooks.add('onSend', (sendEvents) => {
    const active = context.active();
    for (const sendEvent of sendEvents) {
      sendEvent[SEND_CONTEXT_KEY] = active;
    }
  });
  hooks.add('preDeliver', (sendEvent, done) => {
    context.with(sendEvent[SEND_CONTEXT_KEY] ?? context.active(), () => done());
  });
  hooks.add('onReceive', (receiveEvent, done) => {
    // Resolve the process tracer for each delivery. trace.disable() replaces
    // OTel's proxy provider, so retaining a tracer from hook registration
    // would keep using the shut-down provider after tracing is re-enabled.
    const ctx = startNodeSpan(
      tracer ?? getTracer(),
      context.active(),
      receiveEvent.destination.node
    );
    context.with(ctx, () => done());
  });
  hooks.add('onComplete', (completeEvent, done) => {
    completeNodeSpan(context.active(), completeEvent.error);
    done();
  });
}

module.exports = {
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
};
