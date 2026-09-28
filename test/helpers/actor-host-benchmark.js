'use strict';

// Manual observation, not a timing gate. No HTTP, Node-RED, storage or OTLP
// exporter: isolates handler/context/span overhead with bounded concurrency.
const assert = require('node:assert/strict');
const { performance } = require('node:perf_hooks');
const { setImmediate: tick } = require('node:timers/promises');
const { trace, context, propagation } = require('@opentelemetry/api');
const {
  NodeTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  AlwaysOnSampler,
} = require('@opentelemetry/sdk-trace-node');
const { createActorHost } = require('../../lib/actor-host');
const { FIXED_LIMITS } = require('../../lib/options');

async function measure(tracing) {
  const exporter = tracing ? new InMemorySpanExporter() : null;
  const provider = tracing
    ? new NodeTracerProvider({
        sampler: new AlwaysOnSampler(),
        spanProcessors: [new SimpleSpanProcessor(exporter)],
      })
    : null;
  provider?.register();
  const concurrency = 32;
  const calls = 4096;
  const host = createActorHost({
    limits: { ...FIXED_LIMITS, maxPending: concurrency, requestTimeoutMs: 30000 },
    client: {
      readRecord: async () => {
        await tick();
        return { exists: false };
      },
      saveRecord: tick,
    },
  });
  async function run(count) {
    const started = performance.now();
    for (let batch = 0; batch < count / concurrency; batch++) {
      const results = await Promise.all(
        Array.from({ length: concurrency }, (_, id) =>
          host.invoke({
            actorType: 'Counter',
            actorId: String(id),
            method: 'Add',
            ctx: { deadlineAt: Date.now() + 30000, signal: new AbortController().signal },
            emit: (msg) =>
              host.settleActorReply(msg.dapr.actorRequestId, {
                outcome: 'complete',
                responseJson: '1',
                nextStateJson: '{"count":1}',
              }),
          })
        )
      );
      assert.ok(results.every((result) => result.status === 200));
      if (exporter) assert.equal(exporter.getFinishedSpans().length, concurrency * 3);
      exporter?.reset(); // keep benchmark memory bounded independently of calls
    }
    return performance.now() - started;
  }
  try {
    await run(1024);
    global.gc?.(); // optional --expose-gc: compare retained heap after warmup/work
    const before = process.memoryUsage();
    const cpuStart = process.cpuUsage();
    const elapsedMs = await run(calls);
    const cpu = process.cpuUsage(cpuStart);
    global.gc?.();
    const after = process.memoryUsage();
    return {
      tracing,
      calls,
      concurrency,
      elapsedMs: +elapsedMs.toFixed(1),
      callsPerSecond: Math.round((calls * 1000) / elapsedMs),
      cpuMs: +((cpu.user + cpu.system) / 1000).toFixed(1),
      gcAvailable: typeof global.gc === 'function',
      heapUsedBeforeBytes: before.heapUsed,
      heapUsedAfterBytes: after.heapUsed,
      rssBeforeBytes: before.rss,
      rssAfterBytes: after.rss,
    };
  } finally {
    trace.disable();
    context.disable();
    propagation.disable();
    await provider?.shutdown();
  }
}

async function main() {
  try {
    for (const tracing of [false, true, false, true]) {
      console.log(JSON.stringify(await measure(tracing)));
    }
  } catch (err) {
    console.error(err);
    process.exitCode = 1;
  }
}

void main();
