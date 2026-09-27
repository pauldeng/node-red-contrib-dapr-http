'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { setImmediate: tick, setTimeout: delay } = require('node:timers/promises');
const { trace, context, propagation, SpanStatusCode } = require('@opentelemetry/api');
const {
  NodeTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  AlwaysOnSampler,
} = require('@opentelemetry/sdk-trace-node');

const { createActorHost } = require('../../lib/actor-host');
const { DaprError, ErrorCodes } = require('../../lib/errors');
const { FIXED_LIMITS } = require('../../lib/options');

// Registers a real, in-memory-exported tracer as the process-wide provider
// (lib/telemetry.js's getTracer() resolves whatever is globally registered),
// runs `fn(exporter)`, then tears the registration down -- the same
// register()/disable() pattern test/unit/telemetry.test.js's own
// "flow hooks use the current tracer..." test uses, applied here to actor
// spans instead of node spans.
async function withTracing(fn) {
  const exporter = new InMemorySpanExporter();
  const provider = new NodeTracerProvider({
    sampler: new AlwaysOnSampler(),
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  provider.register();
  try {
    await fn(exporter);
  } finally {
    trace.disable();
    context.disable();
    propagation.disable();
    await provider.shutdown();
  }
}

function limits(over = {}) {
  return {
    ...FIXED_LIMITS,
    maxPending: 10,
    requestTimeoutMs: 100000,
    drainTimeoutMs: 100000,
    bodyLimitBytes: 4 * 1024 * 1024,
    ...over,
  };
}

function fakeClient(over = {}) {
  return {
    baseUrl: 'http://fake',
    token: undefined,
    readRecord: async () => ({ exists: false }),
    saveRecord: async () => ({ status: 204 }),
    deleteRecord: async () => ({ status: 204 }),
    ...over,
  };
}

// The actor host commits a delete through the exact same gate/deadline/close
// path as an upsert, dispatched only by which field the proposal carries
// (lib/actor-host.js). The commit-ownership tests below are parameterized
// over both proposal shapes instead of writing a parallel suite; `commitKey`
// is the client method each kind commits through.
const COMMIT_KINDS = {
  save: { commitKey: 'saveRecord', proposalExtra: { nextStateJson: '{}' } },
  delete: { commitKey: 'deleteRecord', proposalExtra: { deleteState: true } },
};

function commitProposal(kind, responseJson = '"ok"') {
  return { outcome: 'complete', responseJson, ...COMMIT_KINDS[kind].proposalExtra };
}

// A fakeClient whose one commit method (saveRecord or deleteRecord,
// depending on `kind`) is `impl`; the other kind's commit method stays the
// harmless default, so a host bug that commits through the wrong method is
// caught by the counter never incrementing.
function commitCountingClient(kind, impl) {
  return fakeClient({ [COMMIT_KINDS[kind].commitKey]: impl });
}

function makeCtx({ body = Buffer.alloc(0), deadlineAt, aborted = false, headers } = {}) {
  const controller = new AbortController();
  if (aborted) {
    controller.abort();
  }
  return { body, signal: controller.signal, deadlineAt, controller, headers };
}

test('a read finishing after the flow deadline never emits or commits', async () => {
  let clock = 0;
  let emitted = false;
  const host = createActorHost({
    limits: limits({ requestTimeoutMs: 1000, drainTimeoutMs: 300 }),
    now: () => clock,
    client: fakeClient({
      readRecord: async () => {
        clock = 800;
        return { exists: false };
      },
    }),
  });
  const result = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    ctx: makeCtx({ deadlineAt: 1000 }),
    emit: (msg) => {
      emitted = true;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  assert.equal(emitted, false);
  assert.equal(result.status, 503);
});

test('emission failure removes the reply identity and pending entry', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  let requestId;
  try {
    await assert.rejects(
      host.invoke({
        actorType: 'T',
        actorId: 'a',
        method: 'M',
        ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
        emit: (msg) => {
          requestId = msg.dapr.actorRequestId;
          throw new Error('emit failed');
        },
      }),
      /emit failed/
    );
    assert.equal(host.actorIdentity(requestId), null);
    assert.equal(
      host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' }),
      false
    );
  } finally {
    host.drain();
  }
});

test('drain during a state read prevents subsequent flow work', async () => {
  const read = Promise.withResolvers();
  const host = createActorHost({
    limits: limits(),
    client: fakeClient({ readRecord: () => read.promise }),
  });
  let emitted = false;
  const result = host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
    emit: (msg) => {
      emitted = true;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  host.drain();
  read.resolve({ exists: false });
  assert.equal((await result).status, 503);
  assert.equal(emitted, false);
});

test('an early reply still caps its commit timeout to the drain reserve', async () => {
  let commitTimeout;
  const host = createActorHost({
    limits: limits({ requestTimeoutMs: 30000, drainTimeoutMs: 5000 }),
    now: () => 0,
    client: fakeClient({
      saveRecord: async (_opts, request) => {
        commitTimeout = request.timeoutMs;
      },
    }),
  });
  const { promise, requestId } = await invokeAndCaptureRequest(host, {
    ctx: makeCtx({ deadlineAt: 30000 }),
  });
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1', nextStateJson: '1' });
  assert.equal((await promise).status, 200);
  assert.ok(commitTimeout > 0 && commitTimeout < 5000, `commit timeout was ${commitTimeout}`);
});

// Runs `host.invoke` and resolves once its emitted message has been captured,
// without waiting for the invocation itself to settle (it will not, until the
// test calls settleActorReply or lets it expire).
async function invokeAndCaptureRequest(host, over = {}) {
  let requestId;
  let message;
  const emit = (msg) => {
    requestId = msg.dapr.actorRequestId;
    message = msg;
  };
  const ctx = over.ctx || makeCtx({ deadlineAt: Date.now() + 100000 });
  const promise = host.invoke({ actorType: 'T', actorId: 'a', method: 'M', emit, ...over, ctx });
  await tick();
  return { promise, requestId, message, ctx };
}

test('a second call for the same actor is rejected busy while the first is active', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const { promise: p1, requestId } = await invokeAndCaptureRequest(host);
  const r2 = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    emit: () => assert.fail('must not evaluate a busy call'),
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(r2.status, 503);
  const body = JSON.parse(r2.body);
  assert.equal(body.error.code, ErrorCodes.ACTOR_BUSY);
  // Clean up: settle and let p1 finish so no timer or handle outlives the test.
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' });
  await p1;
});

test('a different actor proceeds independently while the first is active', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const { promise: p1, requestId: r1 } = await invokeAndCaptureRequest(host);
  const { promise: p2, requestId: r2 } = await invokeAndCaptureRequest(host, { actorId: 'b' });
  assert.ok(r1);
  assert.ok(r2);
  assert.notEqual(r1, r2);
  host.settleActorReply(r1, { outcome: 'complete', responseJson: '1' });
  host.settleActorReply(r2, { outcome: 'complete', responseJson: '2' });
  assert.equal((await p1).status, 200);
  assert.equal((await p2).status, 200);
});

test('admission is rejected once the active-handler budget is exhausted, even for a different actor', async () => {
  const host = createActorHost({ limits: limits({ maxPending: 1 }), client: fakeClient() });
  const { promise: p1, requestId } = await invokeAndCaptureRequest(host);
  const r2 = await host.invoke({
    actorType: 'T',
    actorId: 'other',
    method: 'M',
    emit: () => assert.fail('must not evaluate over capacity'),
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(r2.status, 503);
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' });
  await p1;
});

for (const kind of Object.keys(COMMIT_KINDS)) {
  test(`settleActorReply is first-wins and commits exactly once (${kind})`, async () => {
    let commits = 0;
    const client = commitCountingClient(kind, async () => {
      commits += 1;
    });
    const host = createActorHost({ limits: limits(), client });
    const { promise, requestId } = await invokeAndCaptureRequest(host);
    assert.equal(host.settleActorReply(requestId, commitProposal(kind, '"first"')), true);
    assert.equal(host.settleActorReply(requestId, commitProposal(kind, '"second"')), false);
    const result = await promise;
    assert.equal(result.status, 200);
    assert.equal(result.body, '"first"');
    assert.equal(commits, 1);
  });
}

test('a proposal that never arrives expires (reserve timeout) with no write', async () => {
  let saveCalls = 0;
  const client = fakeClient({
    saveRecord: async () => {
      saveCalls += 1;
      return { status: 204 };
    },
  });
  const fixedLimits = limits({ requestTimeoutMs: 100000, drainTimeoutMs: 50 });
  // reserveMs = min(drainTimeoutMs, requestTimeoutMs/2) = 50. Give it just 10ms
  // of actual budget so the reserve timeout fires quickly.
  const host = createActorHost({ limits: fixedLimits, client });
  const deadlineAt = Date.now() + 60;
  const { promise } = await invokeAndCaptureRequest(host, { ctx: makeCtx({ deadlineAt }) });
  const result = await promise;
  assert.equal(result.status, 503);
  assert.equal(JSON.parse(result.body).error.code, ErrorCodes.ACTOR_REPLY_EXPIRED);
  assert.equal(saveCalls, 0);
});

for (const kind of Object.keys(COMMIT_KINDS)) {
  test(`a reply arriving after expiry is rejected (first-wins already spent) and never commits (${kind})`, async () => {
    let commitCalls = 0;
    const client = commitCountingClient(kind, async () => {
      commitCalls += 1;
      return { status: 204 };
    });
    const fixedLimits = limits({ requestTimeoutMs: 100000, drainTimeoutMs: 50 });
    const host = createActorHost({ limits: fixedLimits, client });
    const deadlineAt = Date.now() + 60;
    const { promise, requestId } = await invokeAndCaptureRequest(host, {
      ctx: makeCtx({ deadlineAt }),
    });
    const result = await promise;
    assert.equal(result.status, 503);
    const late = host.settleActorReply(requestId, commitProposal(kind));
    assert.equal(late, false);
    assert.equal(commitCalls, 0);
  });
}

for (const kind of Object.keys(COMMIT_KINDS)) {
  test(`remaining time checked immediately before the commit: insufficient time means no write (${kind})`, async () => {
    let commitCalls = 0;
    const client = commitCountingClient(kind, async () => {
      commitCalls += 1;
      return { status: 204 };
    });
    const fixedLimits = limits({ requestTimeoutMs: 1000, drainTimeoutMs: 300 });
    let nowValue = Date.now();
    const now = () => nowValue;
    const host = createActorHost({ limits: fixedLimits, client, now });
    const deadlineAt = nowValue + 1000;
    const { promise, requestId } = await invokeAndCaptureRequest(host, {
      ctx: makeCtx({ deadlineAt }),
    });
    assert.equal(typeof requestId, 'string', 'the flow must run before its deadline expires');
    // Advance the clock past (deadlineAt - COMMIT_MARGIN_MS) before the
    // proposal settles, so the pre-commit recheck finds no time left.
    nowValue = deadlineAt - 100;
    assert.equal(host.settleActorReply(requestId, commitProposal(kind)), true);
    const result = await promise;
    assert.equal(result.status, 503);
    assert.equal(JSON.parse(result.body).error.code, ErrorCodes.ACTOR_REPLY_EXPIRED);
    assert.equal(commitCalls, 0);
  });
}

test('a caller disconnect before the commit starts means no commit runs', async () => {
  let saveCalls = 0;
  const client = fakeClient({
    saveRecord: async () => {
      saveCalls += 1;
      return { status: 204 };
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const { promise, requestId, ctx } = await invokeAndCaptureRequest(host);
  // Settle first (first-wins already claims the reply), then disconnect in the
  // same synchronous tick, before the awaited continuation resumes and checks
  // ctx.signal.aborted immediately before starting the commit.
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1', nextStateJson: '{}' });
  ctx.controller.abort();
  const result = await promise;
  assert.equal(result.status, 503);
  assert.equal(saveCalls, 0);
});

test('a caller disconnect during the commit does not abort it; the gate is held until it settles', async () => {
  const { promise: saveGate, resolve: releaseSave } = Promise.withResolvers();
  let saveOpts;
  const client = fakeClient({
    saveRecord: async (opts) => {
      saveOpts = opts;
      await saveGate;
      return { status: 204 };
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const { promise, requestId, ctx } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '"ok"',
    nextStateJson: '{}',
  });
  await tick(); // let it reach and call client.saveRecord, which now blocks on saveGate
  ctx.controller.abort();
  await tick();
  // The commit call never received the caller's own AbortSignal.
  assert.equal(saveOpts.signal, undefined);
  // The gate is still held: a same-actor call is still rejected busy.
  const busy = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    emit: () => assert.fail('must not evaluate while the gate is held'),
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(busy.status, 503);
  releaseSave();
  const result = await promise;
  assert.equal(result.status, 200);
  assert.equal(result.body, '"ok"');
});

for (const kind of Object.keys(COMMIT_KINDS)) {
  test(`no success is returned before the commit resolves (${kind})`, async () => {
    const { promise: commitGate, resolve: releaseCommit } = Promise.withResolvers();
    const client = commitCountingClient(kind, async () => {
      await commitGate;
      return { status: 204 };
    });
    const host = createActorHost({ limits: limits(), client });
    const { promise, requestId } = await invokeAndCaptureRequest(host);
    host.settleActorReply(requestId, commitProposal(kind));
    await tick();
    const busy = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      emit: () => assert.fail('must not evaluate before the first commit settles'),
      ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
    });
    assert.equal(busy.status, 503); // still gated: the commit has not resolved yet
    releaseCommit();
    assert.equal((await promise).status, 200);
  });
}

for (const kind of Object.keys(COMMIT_KINDS)) {
  test(`a transport failure mid-commit answers ACTOR_COMMIT_UNKNOWN, not a definite failure (${kind})`, async () => {
    const client = commitCountingClient(kind, async () => {
      throw new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'boom');
    });
    const host = createActorHost({ limits: limits(), client });
    const { promise, requestId } = await invokeAndCaptureRequest(host);
    host.settleActorReply(requestId, commitProposal(kind));
    const result = await promise;
    assert.equal(result.status, 500);
    assert.equal(JSON.parse(result.body).error.code, ErrorCodes.ACTOR_COMMIT_UNKNOWN);
  });

  test(`a confirmed non-2xx commit failure is a definite failure, not ACTOR_COMMIT_UNKNOWN (${kind})`, async () => {
    const client = commitCountingClient(kind, async () => {
      throw new DaprError(ErrorCodes.STATE_OPERATION_FAILED, 'daprd said no');
    });
    const host = createActorHost({ limits: limits(), client });
    const { promise, requestId } = await invokeAndCaptureRequest(host);
    host.settleActorReply(requestId, commitProposal(kind));
    const result = await promise;
    assert.equal(result.status, 500);
    const body = JSON.parse(result.body);
    assert.equal(body.error.code, ErrorCodes.STATE_OPERATION_FAILED);
    assert.ok(!result.body.includes('daprd said no'));
  });
}

test('a fail reply answers 500 with the proposal error body verbatim and never commits', async () => {
  let saveCalls = 0;
  const client = fakeClient({
    saveRecord: async () => {
      saveCalls += 1;
      return { status: 204 };
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  const errorBody = JSON.stringify({ error: { code: 'BAD_KEY', message: 'nope' } });
  host.settleActorReply(requestId, { outcome: 'fail', errorBody });
  const result = await promise;
  assert.equal(result.status, 500);
  assert.equal(result.body, errorBody);
  assert.equal(saveCalls, 0);
});

test('a complete reply with a null response answers 200 with a literal "null" body', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: 'null' });
  const result = await promise;
  assert.equal(result.status, 200);
  assert.equal(result.body, 'null');
  assert.equal(result.headers['content-type'], 'application/json');
});

test('a readRecord failure answers a sanitized 500 with no flow emission', async () => {
  const client = fakeClient({
    readRecord: async () => {
      throw new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'unreachable: 10.0.0.1:3500 refused');
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const result = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    emit: () => assert.fail('must not emit when the state read fails'),
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(result.status, 500);
  assert.ok(!result.body.includes('10.0.0.1'));
});

test('an invalid JSON body answers 400 before any emission', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const result = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    emit: () => assert.fail('must not emit on invalid JSON'),
    ctx: makeCtx({ body: Buffer.from('{not json'), deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(result.status, 400);
});

test('an empty body is delivered as an absent (undefined) payload', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const { promise, message, requestId } = await invokeAndCaptureRequest(host);
  assert.equal('payload' in message, true);
  assert.equal(message.payload, undefined);
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' });
  await promise;
});

test('the actor identity is available while awaiting a reply and cleared once it settles', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  assert.deepEqual(host.actorIdentity(requestId), { actorType: 'T', actorId: 'a' });
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' });
  await promise;
  assert.equal(host.actorIdentity(requestId), null);
});

test('drain() unblocks a handler still awaiting a reply with a 503, and no write', async () => {
  let saveCalls = 0;
  const client = fakeClient({
    saveRecord: async () => {
      saveCalls += 1;
      return { status: 204 };
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const { promise } = await invokeAndCaptureRequest(host);
  host.drain();
  const result = await promise;
  assert.equal(result.status, 503);
  assert.equal(saveCalls, 0);
});

// ---- milestone 3: commit-aware close (whenCommitsSettled) -----------------

for (const kind of Object.keys(COMMIT_KINDS)) {
  test(`whenCommitsSettled resolves only after a started commit actually settles (${kind}, close waits for an in-flight ${kind})`, async () => {
    const commitGate = Promise.withResolvers();
    let commitCalls = 0;
    const host = createActorHost({
      limits: limits({ drainTimeoutMs: 5000 }),
      client: commitCountingClient(kind, async () => {
        commitCalls += 1;
        await commitGate.promise;
        return { status: 204 };
      }),
    });
    const { promise, requestId } = await invokeAndCaptureRequest(host);
    host.settleActorReply(requestId, commitProposal(kind));
    await tick(); // reach and call the commit method, which now blocks on commitGate
    host.drain(); // a concurrent close/redeploy starts draining
    const settled = host.whenCommitsSettled();
    const race = await Promise.race([settled, delay(50, 'not-yet')]); // allow-timer: bounded negative -- the commit must still be in flight
    assert.equal(race, 'not-yet', 'whenCommitsSettled must not resolve before the commit does');
    assert.equal(commitCalls, 1);
    commitGate.resolve();
    assert.equal(await settled, false, 'the commit settled on its own; the backstop never fired');
    const result = await promise;
    assert.equal(result.status, 200);
    assert.equal(result.body, '"ok"');
  });
}

test('a handler still waiting for a proposal at drain gets 503 and never commits; whenCommitsSettled needs no wait for it', async () => {
  let saveCalls = 0;
  const host = createActorHost({
    limits: limits(),
    client: fakeClient({
      saveRecord: async () => {
        saveCalls += 1;
        return { status: 204 };
      },
    }),
  });
  const { promise } = await invokeAndCaptureRequest(host);
  host.drain();
  const result = await promise;
  assert.equal(result.status, 503);
  assert.equal(saveCalls, 0);
  // Nothing committing -- must resolve immediately, not wait out
  // drainTimeoutMs, and report no backstop.
  assert.equal(await host.whenCommitsSettled(), false);
});

test('an invocation admitted after drain() is rejected without evaluating, and never commits', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  host.drain();
  const result = await host.invoke({
    actorType: 'T',
    actorId: 'late',
    method: 'M',
    emit: () => assert.fail('must not evaluate a call admitted after drain'),
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(result.status, 503);
  assert.equal(await host.whenCommitsSettled(), false);
});

test('a replayed request for an already-active actor is rejected busy with no second read and no second commit', async () => {
  let reads = 0;
  let saves = 0;
  const host = createActorHost({
    limits: limits(),
    client: fakeClient({
      readRecord: async () => {
        reads += 1;
        return { exists: false };
      },
      saveRecord: async () => {
        saves += 1;
        return { status: 204 };
      },
    }),
  });
  const { promise: p1, requestId } = await invokeAndCaptureRequest(host);
  const replay = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    emit: () => assert.fail('a replay overlapping an active handler must not re-read state'),
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(replay.status, 503);
  assert.equal(JSON.parse(replay.body).error.code, ErrorCodes.ACTOR_BUSY);
  assert.equal(reads, 1, 'the replay must not trigger a second state read');
  host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '1',
    nextStateJson: '{}',
  });
  assert.equal((await p1).status, 200);
  assert.equal(saves, 1, 'exactly one commit for the original handler, none for the replay');
});

test('a new call for the same actor after the first handler finished is a fresh turn with its own read', async () => {
  let reads = 0;
  const host = createActorHost({
    limits: limits(),
    client: fakeClient({
      readRecord: async () => {
        reads += 1;
        return { exists: false };
      },
    }),
  });
  const { promise: p1, requestId: r1 } = await invokeAndCaptureRequest(host);
  host.settleActorReply(r1, { outcome: 'complete', responseJson: '1' });
  assert.equal((await p1).status, 200);

  const { promise: p2, requestId: r2 } = await invokeAndCaptureRequest(host);
  assert.notEqual(r1, r2);
  host.settleActorReply(r2, { outcome: 'complete', responseJson: '2' });
  assert.equal((await p2).status, 200);
  assert.equal(reads, 2);
});

test('whenCommitsSettled is bounded by drainTimeoutMs when a commit never settles', async () => {
  const stuck = Promise.withResolvers();
  let saveStarted = false;
  const host = createActorHost({
    // Must exceed the 250ms commit margin or no commit can start at all.
    limits: limits({ drainTimeoutMs: 500 }),
    client: fakeClient({
      saveRecord: () => {
        saveStarted = true;
        return stuck.promise;
      },
    }),
  });
  const { requestId, promise } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '"ok"',
    nextStateJson: '{}',
  });
  await tick();
  assert.equal(saveStarted, true, 'the test must actually enter a commit');
  host.drain();
  const start = Date.now();
  const deadline = new AbortController();
  try {
    // Keep the isolated test alive while the host's unref'ed backstop runs,
    // and fail if that backstop is removed. Cancel the losing deadline.
    const result = await Promise.race([
      host.whenCommitsSettled(),
      delay(2000, 'timed-out', { signal: deadline.signal }),
    ]);
    assert.notEqual(result, 'timed-out', 'must not wait indefinitely for a stalled commit');
    assert.ok(Date.now() - start >= 450, 'must wait for the drain backstop');
    assert.equal(result, true, 'whenCommitsSettled reports that the backstop fired');
  } finally {
    deadline.abort();
    stuck.resolve();
    await promise;
  }
});

test('an existing record cannot be aliased across requests: client.readRecord (lib/actor-client.js) parses fresh JSON per request, so mutating one flow copy never affects the next', async () => {
  const storedJson = JSON.stringify({ revision: 1 });
  const client = fakeClient({
    readRecord: async () => ({ exists: true, value: JSON.parse(storedJson) }),
  });
  const host = createActorHost({ limits: limits(), client });

  const { promise: p1, message: m1, requestId: r1 } = await invokeAndCaptureRequest(host);
  assert.equal(m1.dapr.actor.stateExists, true);
  assert.deepEqual(m1.dapr.actor.state, { revision: 1 });
  m1.dapr.actor.state.revision = 999; // mutate the first request's own copy
  host.settleActorReply(r1, { outcome: 'complete', responseJson: '1' });
  await p1;

  const { promise: p2, message: m2, requestId: r2 } = await invokeAndCaptureRequest(host);
  assert.deepEqual(
    m2.dapr.actor.state,
    { revision: 1 },
    "a fresh read is unaffected by the previous request's mutation"
  );
  host.settleActorReply(r2, { outcome: 'complete', responseJson: '2' });
  await p2;
});

// ---- milestone 4: reminder-triggered invocations ---------------------------

test('a reminder trigger emits msg.dapr.actor with trigger identity from the route, method as the reminder name', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  let received;
  const ctx = makeCtx({
    deadlineAt: Date.now() + 100000,
    body: Buffer.from('{"data":{"greeting":"hi"},"dueTime":"","period":""}'),
  });
  const promise = host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'reminder',
    trigger: { kind: 'reminder', name: 'demo_reminder' },
    ctx,
    emit: (msg) => {
      received = msg;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  const result = await promise;
  assert.equal(result.status, 200);
  assert.deepEqual(received.payload, { greeting: 'hi' });
  assert.equal(received.dapr.actor.method, 'demo_reminder');
  assert.deepEqual(received.dapr.actor.trigger, { kind: 'reminder', name: 'demo_reminder' });
  assert.equal(received.dapr.actor.type, 'T');
  assert.equal(received.dapr.actor.id, 'a');
});

test('a reminder envelope with no "data" key emits msg.payload undefined', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  let received;
  const ctx = makeCtx({
    deadlineAt: Date.now() + 100000,
    body: Buffer.from('{"dueTime":"","period":""}'),
  });
  const promise = host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'reminder',
    trigger: { kind: 'reminder', name: 'r1' },
    ctx,
    emit: (msg) => {
      received = msg;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  await promise;
  assert.equal('payload' in received, true);
  assert.equal(received.payload, undefined);
});

test('a reminder envelope with data: null preserves null, distinct from absent', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  let received;
  const ctx = makeCtx({
    deadlineAt: Date.now() + 100000,
    body: Buffer.from('{"data":null,"dueTime":"","period":""}'),
  });
  const promise = host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'reminder',
    trigger: { kind: 'reminder', name: 'r1' },
    ctx,
    emit: (msg) => {
      received = msg;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  await promise;
  assert.equal(received.payload, null);
});

test('a reminder envelope with falsy data (0, false, "") is preserved, not treated as absent', async () => {
  for (const [wire, expected] of [
    ['{"data":0,"dueTime":"","period":""}', 0],
    ['{"data":false,"dueTime":"","period":""}', false],
    ['{"data":"","dueTime":"","period":""}', ''],
  ]) {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    let received;
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000, body: Buffer.from(wire) });
    const promise = host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'reminder',
      trigger: { kind: 'reminder', name: 'r1' },
      ctx,
      emit: (msg) => {
        received = msg;
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
      },
    });
    await promise;
    assert.equal(received.payload, expected, `wire=${wire}`);
  }
});

test('a malformed reminder envelope (bad JSON) is a 400 before emission', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const ctx = makeCtx({ deadlineAt: Date.now() + 100000, body: Buffer.from('not json') });
  const result = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'reminder',
    trigger: { kind: 'reminder', name: 'r1' },
    ctx,
    emit: () => assert.fail('must not emit for a malformed envelope'),
  });
  assert.equal(result.status, 400);
  assert.equal(JSON.parse(result.body).error.code, ErrorCodes.INVALID_MESSAGE);
});

test('a reminder envelope that is an array or a JSON scalar (not an object) is a 400 before emission', async () => {
  for (const wire of ['[1,2,3]', '"a string"', '42', 'null']) {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000, body: Buffer.from(wire) });
    const result = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'reminder',
      trigger: { kind: 'reminder', name: 'r1' },
      ctx,
      emit: () => assert.fail('must not emit for a non-object envelope'),
    });
    assert.equal(result.status, 400, `wire=${wire}`);
  }
});

test('an absent reminder body is treated as an empty envelope, not a parse failure', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  let received;
  const ctx = makeCtx({ deadlineAt: Date.now() + 100000, body: Buffer.alloc(0) });
  const promise = host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'reminder',
    trigger: { kind: 'reminder', name: 'r1' },
    ctx,
    emit: (msg) => {
      received = msg;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  const result = await promise;
  assert.equal(result.status, 200);
  assert.equal(received.payload, undefined);
});

test('trigger identity is never taken from the body, even if the body forges one', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  let received;
  const ctx = makeCtx({
    deadlineAt: Date.now() + 100000,
    body: Buffer.from(
      '{"data":1,"dueTime":"","period":"","trigger":{"kind":"reminder","name":"forged"}}'
    ),
  });
  const promise = host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'reminder',
    trigger: { kind: 'reminder', name: 'real-name' },
    ctx,
    emit: (msg) => {
      received = msg;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  await promise;
  assert.equal(received.dapr.actor.trigger.name, 'real-name');
  assert.equal(received.dapr.actor.method, 'real-name');
});

test('an ordinary (non-reminder) method invocation parses its body as the raw argument, unaffected by trigger handling', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  let received;
  const ctx = makeCtx({ deadlineAt: Date.now() + 100000, body: Buffer.from('[1,2,3]') });
  const promise = host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'GetMyData',
    ctx,
    emit: (msg) => {
      received = msg;
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
    },
  });
  await promise;
  assert.deepEqual(received.payload, [1, 2, 3]);
  assert.equal(received.dapr.actor.method, 'GetMyData');
  assert.equal('trigger' in received.dapr.actor, false);
});

// ---- milestone 6: observability -- spans -----------------------------------

const VALID_TRACEPARENT = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

function findSpan(exporter, name) {
  return exporter.getFinishedSpans().find((s) => s.name === name);
}

test('a valid traceparent header parents the server span', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({
      deadlineAt: Date.now() + 100000,
      headers: { traceparent: VALID_TRACEPARENT },
    });
    const result = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' }),
    });
    assert.equal(result.status, 200);
    const span = findSpan(exporter, 'actor T.M');
    assert.ok(span, 'expected a server span named "actor T.M"');
    assert.equal(span.spanContext().traceId, VALID_TRACEPARENT.split('-')[1]);
    assert.equal(span.parentSpanContext.spanId, VALID_TRACEPARENT.split('-')[2]);
    assert.equal(span.kind, 1 /* SpanKind.SERVER */);
    assert.equal(span.attributes['rpc.system'], 'dapr');
    assert.equal(span.attributes['dapr.actor.type'], 'T');
    assert.equal(span.attributes['dapr.actor.method'], 'M');
    assert.equal(span.attributes['dapr.actor.trigger'], 'method');
    assert.equal(span.attributes['dapr.actor.id'], 'a');
    assert.equal(span.attributes['dapr.actor.outcome'], 'OK');
    assert.equal(span.status.code, SpanStatusCode.OK);
    assert.equal('id' in span.attributes, false);
  });
});

test('a missing traceparent starts a fresh root span', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000 });
    await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' }),
    });
    const span = findSpan(exporter, 'actor T.M');
    assert.equal(span.parentSpanContext, undefined);
  });
});

test('a malformed traceparent starts a fresh root span, never crashes', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({
      deadlineAt: Date.now() + 100000,
      headers: { traceparent: 'not-a-real-traceparent' },
    });
    const result = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' }),
    });
    assert.equal(result.status, 200);
    const span = findSpan(exporter, 'actor T.M');
    assert.equal(span.parentSpanContext, undefined);
  });
});

test('a reminder trigger names the span without the reminder name, and reports it as an attribute', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000 });
    await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'remind/',
      trigger: { kind: 'reminder', name: 'nightly' },
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' }),
    });
    const span = findSpan(exporter, 'actor T reminder');
    assert.ok(span, 'expected a span named "actor T reminder" with no reminder name in it');
    assert.equal(span.attributes['dapr.actor.method'], 'nightly');
    assert.equal(span.attributes['dapr.actor.trigger'], 'reminder');
    assert.equal(
      exporter.getFinishedSpans().some((s) => s.name.includes('nightly')),
      false
    );
  });
});

test('the entire asynchronous actor turn runs in its server context, isolated from ambient context', async () => {
  await withTracing(async (exporter) => {
    const ambient = trace.getTracer('test').startSpan('unrelated');
    const seen = [];
    const observe = async () => {
      await tick();
      seen.push(trace.getSpan(context.active())?.spanContext().spanId);
    };
    const client = fakeClient({
      readRecord: async () => {
        await observe();
        return { exists: false };
      },
      saveRecord: observe,
    });
    const host = createActorHost({ limits: limits(), client });
    await context.with(trace.setSpan(context.active(), ambient), () =>
      host.invoke({
        actorType: 'T',
        actorId: 'a',
        method: 'M',
        ctx: makeCtx({
          deadlineAt: Date.now() + 100000,
          headers: { traceparent: VALID_TRACEPARENT },
        }),
        emit: (msg) => host.settleActorReply(msg.dapr.actorRequestId, commitProposal('save')),
      })
    );
    ambient.end();
    const server = findSpan(exporter, 'actor T.M');
    assert.deepEqual(seen, [server.spanContext().spanId, server.spanContext().spanId]);
  });
});

test('the state read and commit are CLIENT child spans of the server span', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000 });
    await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, {
          outcome: 'complete',
          responseJson: '1',
          nextStateJson: '{}',
        }),
    });
    const server = findSpan(exporter, 'actor T.M');
    const read = findSpan(exporter, 'actor state read');
    const save = findSpan(exporter, 'actor state save');
    assert.ok(read && save, 'expected both a read and a save child span');
    assert.equal(read.kind, 2 /* SpanKind.CLIENT */);
    assert.equal(save.kind, 2);
    assert.equal(read.parentSpanContext.spanId, server.spanContext().spanId);
    assert.equal(save.parentSpanContext.spanId, server.spanContext().spanId);
    assert.equal(read.spanContext().traceId, server.spanContext().traceId);
  });
});

test('a delete commit opens an "actor state delete" child span, not "save"', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000 });
    await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, {
          outcome: 'complete',
          responseJson: '1',
          deleteState: true,
        }),
    });
    assert.ok(findSpan(exporter, 'actor state delete'));
    assert.equal(findSpan(exporter, 'actor state save'), undefined);
  });
});

test('the state read injects a traceparent onto the sidecar request', async () => {
  await withTracing(async () => {
    let capturedHeaders;
    const client = fakeClient({
      readRecord: async (opts) => {
        capturedHeaders = opts.headers;
        return { exists: false };
      },
    });
    const host = createActorHost({ limits: limits(), client });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000 });
    await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' }),
    });
    assert.match(capturedHeaders.traceparent, /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  });
});

test('a commit still injects a traceparent onto its own sidecar request', async () => {
  await withTracing(async () => {
    let capturedHeaders;
    const client = fakeClient({
      saveRecord: async (opts) => {
        capturedHeaders = opts.headers;
        return { status: 204 };
      },
    });
    const host = createActorHost({ limits: limits(), client });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000 });
    await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, {
          outcome: 'complete',
          responseJson: '1',
          nextStateJson: '{}',
        }),
    });
    assert.match(capturedHeaders.traceparent, /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  });
});

test('flow emission runs inside the server span context, so a downstream flow span would nest under it', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const ctx = makeCtx({ deadlineAt: Date.now() + 100000 });
    let capturedSpanId;
    await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx,
      emit: (msg) => {
        capturedSpanId = trace.getSpan(context.active())?.spanContext().spanId;
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' });
      },
    });
    const server = findSpan(exporter, 'actor T.M');
    assert.equal(capturedSpanId, server.spanContext().spanId);
  });
});

test('a busy rejection still gets a span, ended once, with the ACTOR_BUSY outcome', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    // Occupy the actor first so the second call is rejected busy before any
    // span-bearing work runs inside invokeBody.
    const first = host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' }),
    });
    const busy = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
      emit: () => assert.fail('must not evaluate a busy call'),
    });
    assert.equal(busy.status, 503);
    await first;
    const spans = exporter.getFinishedSpans().filter((s) => s.name === 'actor T.M');
    assert.equal(spans.length, 2);
    const busySpan = spans.find(
      (s) => s.attributes['dapr.actor.outcome'] === ErrorCodes.ACTOR_BUSY
    );
    assert.ok(busySpan, 'expected one span with outcome ACTOR_BUSY');
    assert.equal(busySpan.status.code, SpanStatusCode.ERROR);
  });
});

test('an invalid body is a single span with the INVALID_MESSAGE outcome', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const result = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx: makeCtx({ deadlineAt: Date.now() + 100000, body: Buffer.from('{not json') }),
      emit: () => assert.fail('must not emit on invalid JSON'),
    });
    assert.equal(result.status, 400);
    const spans = exporter.getFinishedSpans().filter((s) => s.name === 'actor T.M');
    assert.equal(spans.length, 1);
    assert.equal(spans[0].attributes['dapr.actor.outcome'], ErrorCodes.INVALID_MESSAGE);
  });
});

test('a drain-expired invocation is a single span with the ACTOR_REPLY_EXPIRED outcome', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const promise = host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
      emit: () => {},
    });
    await tick();
    host.drain();
    const result = await promise;
    assert.equal(result.status, 503);
    const spans = exporter.getFinishedSpans().filter((s) => s.name === 'actor T.M');
    assert.equal(spans.length, 1);
    assert.equal(spans[0].attributes['dapr.actor.outcome'], ErrorCodes.ACTOR_REPLY_EXPIRED);
  });
});

test('a transport failure mid-commit is a single span with the ACTOR_COMMIT_UNKNOWN outcome', async () => {
  await withTracing(async (exporter) => {
    const client = fakeClient({
      saveRecord: async () => {
        throw new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'boom');
      },
    });
    const host = createActorHost({ limits: limits(), client });
    const result = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, {
          outcome: 'complete',
          responseJson: '1',
          nextStateJson: '{}',
        }),
    });
    assert.equal(result.status, 500);
    const spans = exporter.getFinishedSpans().filter((s) => s.name === 'actor T.M');
    assert.equal(spans.length, 1);
    assert.equal(spans[0].attributes['dapr.actor.outcome'], ErrorCodes.ACTOR_COMMIT_UNKNOWN);
    // The commit's own CLIENT span must also have settled exactly once, as an error.
    const commitSpan = exporter.getFinishedSpans().find((s) => s.name === 'actor state save');
    assert.equal(commitSpan.status.code, SpanStatusCode.ERROR);
  });
});

test('an app-declared fail reply is a single span with the ACTOR_REPLY_FAILED outcome', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const result = await host.invoke({
      actorType: 'T',
      actorId: 'a',
      method: 'M',
      ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
      emit: (msg) =>
        host.settleActorReply(msg.dapr.actorRequestId, {
          outcome: 'fail',
          errorBody: JSON.stringify({ error: { code: 'BAD', message: 'no' } }),
        }),
    });
    assert.equal(result.status, 500);
    const spans = exporter.getFinishedSpans().filter((s) => s.name === 'actor T.M');
    assert.equal(spans.length, 1);
    assert.equal(spans[0].attributes['dapr.actor.outcome'], 'ACTOR_REPLY_FAILED');
  });
});

test('a disconnect while still awaiting a reply is a single span with the ABORTED outcome', async () => {
  await withTracing(async (exporter) => {
    const host = createActorHost({ limits: limits(), client: fakeClient() });
    const { promise, ctx } = await invokeAndCaptureRequest(host); // emit never replies
    ctx.controller.abort(); // triggers the onAbort listener, settling ABORTED
    const result = await promise;
    assert.equal(result.status, 503);
    const spans = exporter.getFinishedSpans().filter((s) => s.name === 'actor T.M');
    assert.equal(spans.length, 1);
    assert.equal(spans[0].attributes['dapr.actor.outcome'], 'ABORTED');
  });
});

test('an in-flight commit keeps its span open until it settles, then ends it once', async () => {
  await withTracing(async (exporter) => {
    const gate = Promise.withResolvers();
    const client = fakeClient({
      saveRecord: async () => {
        await gate.promise;
        return { status: 204 };
      },
    });
    const host = createActorHost({ limits: limits(), client });
    const { promise, requestId } = await invokeAndCaptureRequest(host);
    host.settleActorReply(requestId, {
      outcome: 'complete',
      responseJson: '"ok"',
      nextStateJson: '{}',
    });
    await tick();
    assert.equal(
      findSpan(exporter, 'actor state save'),
      undefined,
      'still in flight, not yet ended'
    );
    gate.resolve();
    await promise;
    const commitSpan = findSpan(exporter, 'actor state save');
    assert.ok(commitSpan);
    assert.equal(commitSpan.status.code, SpanStatusCode.OK);
  });
});

test('tracing disabled (no provider registered) costs nothing: invocation still succeeds with no exporter involved', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const result = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    ctx: makeCtx({ deadlineAt: Date.now() + 100000, headers: { traceparent: VALID_TRACEPARENT } }),
    emit: (msg) =>
      host.settleActorReply(msg.dapr.actorRequestId, { outcome: 'complete', responseJson: '1' }),
  });
  assert.equal(result.status, 200);
});

// ---- milestone 6: observability -- active-handler count for status --------

test('watchActive is seeded with the current count and pushed on admit/settle', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const seen = [];
  const unwatch = host.watchActive('T', 'M', (n) => seen.push(n));
  assert.deepEqual(seen, [0]);
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  assert.deepEqual(seen, [0, 1]);
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' });
  await promise;
  assert.deepEqual(seen, [0, 1, 0]);
  unwatch();
});

test('watchActive counts a committing invocation as still active until the commit settles', async () => {
  const gate = Promise.withResolvers();
  const client = fakeClient({
    saveRecord: async () => {
      await gate.promise;
      return { status: 204 };
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const seen = [];
  host.watchActive('T', 'M', (n) => seen.push(n));
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '"ok"',
    nextStateJson: '{}',
  });
  await tick();
  assert.deepEqual(seen, [0, 1], 'still counted active while the commit is in flight');
  gate.resolve();
  await promise;
  assert.deepEqual(seen, [0, 1, 0]);
});

test("watchActive for a different registration never sees another one's changes", async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const seen = [];
  host.watchActive('Other', 'M', (n) => seen.push(n));
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' });
  await promise;
  assert.deepEqual(seen, [0]);
});

// ---- milestone 6: observability -- diagnostics -----------------------------

test('a commit-unknown outcome reports ACTOR_COMMIT_UNKNOWN through onDiagnostic', async () => {
  const client = fakeClient({
    saveRecord: async () => {
      throw new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'boom');
    },
  });
  const events = [];
  const host = createActorHost({
    limits: limits(),
    client,
    onDiagnostic: (code, detail) => events.push({ code, detail }),
  });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '1',
    nextStateJson: '{}',
  });
  await promise;
  assert.equal(events.length, 1);
  assert.equal(events[0].code, 'ACTOR_COMMIT_UNKNOWN');
  assert.equal(events[0].detail.actorType, 'T');
  assert.equal(events[0].detail.method, 'M');
});

test('a throwing onDiagnostic never changes the request outcome', async () => {
  const client = fakeClient({
    saveRecord: async () => {
      throw new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'boom');
    },
  });
  const host = createActorHost({
    limits: limits(),
    client,
    onDiagnostic: () => {
      throw new Error('logger exploded');
    },
  });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '1',
    nextStateJson: '{}',
  });
  const result = await promise;
  assert.equal(result.status, 500);
  assert.equal(JSON.parse(result.body).error.code, ErrorCodes.ACTOR_COMMIT_UNKNOWN);
});
