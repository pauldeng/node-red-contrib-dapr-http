'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { setImmediate: tick } = require('node:timers/promises');

const { createActorHost } = require('../../lib/actor-host');
const { DaprError, ErrorCodes } = require('../../lib/errors');

function limits(over = {}) {
  return {
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
    ...over,
  };
}

function makeCtx({ body = Buffer.alloc(0), deadlineAt, aborted = false } = {}) {
  const controller = new AbortController();
  if (aborted) {
    controller.abort();
  }
  return { body, signal: controller.signal, deadlineAt, controller };
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

test('settleActorReply is first-wins; a second settlement for the same id is ignored', async () => {
  const host = createActorHost({ limits: limits(), client: fakeClient() });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  const first = host.settleActorReply(requestId, { outcome: 'complete', responseJson: '"first"' });
  const second = host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '"second"',
  });
  assert.equal(first, true);
  assert.equal(second, false);
  const result = await promise;
  assert.equal(result.status, 200);
  assert.equal(result.body, '"first"');
});

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

test('a reply arriving after expiry is rejected (first-wins already spent) and never commits', async () => {
  let saveCalls = 0;
  const client = fakeClient({
    saveRecord: async () => {
      saveCalls += 1;
      return { status: 204 };
    },
  });
  const fixedLimits = limits({ requestTimeoutMs: 100000, drainTimeoutMs: 50 });
  const host = createActorHost({ limits: fixedLimits, client });
  const deadlineAt = Date.now() + 60;
  const { promise, requestId } = await invokeAndCaptureRequest(host, {
    ctx: makeCtx({ deadlineAt }),
  });
  const result = await promise;
  assert.equal(result.status, 503);
  const late = host.settleActorReply(requestId, {
    outcome: 'complete',
    responseJson: '1',
    nextStateJson: '{}',
  });
  assert.equal(late, false);
  assert.equal(saveCalls, 0);
});

test('remaining time checked immediately before the commit: insufficient time means no write', async () => {
  let saveCalls = 0;
  const client = fakeClient({
    saveRecord: async () => {
      saveCalls += 1;
      return { status: 204 };
    },
  });
  const fixedLimits = limits();
  let nowValue = Date.now();
  const now = () => nowValue;
  const host = createActorHost({ limits: fixedLimits, client, now });
  const deadlineAt = nowValue + 1000;
  const { promise, requestId } = await invokeAndCaptureRequest(host, {
    ctx: makeCtx({ deadlineAt }),
  });
  // Advance the clock past (deadlineAt - COMMIT_MARGIN_MS) before the proposal
  // settles, so the pre-commit recheck finds no time left.
  nowValue = deadlineAt - 100;
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1', nextStateJson: '{}' });
  const result = await promise;
  assert.equal(result.status, 503);
  assert.equal(JSON.parse(result.body).error.code, ErrorCodes.ACTOR_REPLY_EXPIRED);
  assert.equal(saveCalls, 0);
});

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

test('no success is returned before the save resolves', async () => {
  const { promise: saveGate, resolve: releaseSave } = Promise.withResolvers();
  const client = fakeClient({
    saveRecord: async () => {
      await saveGate;
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
  const busy = await host.invoke({
    actorType: 'T',
    actorId: 'a',
    method: 'M',
    emit: () => assert.fail('must not evaluate before the first commit settles'),
    ctx: makeCtx({ deadlineAt: Date.now() + 100000 }),
  });
  assert.equal(busy.status, 503); // still gated: the commit has not resolved yet
  releaseSave();
  assert.equal((await promise).status, 200);
});

test('a transport failure mid-commit answers ACTOR_COMMIT_UNKNOWN, not a definite failure', async () => {
  const client = fakeClient({
    saveRecord: async () => {
      throw new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'boom');
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1', nextStateJson: '{}' });
  const result = await promise;
  assert.equal(result.status, 500);
  assert.equal(JSON.parse(result.body).error.code, ErrorCodes.ACTOR_COMMIT_UNKNOWN);
});

test('a confirmed non-2xx commit failure is a definite failure, not ACTOR_COMMIT_UNKNOWN', async () => {
  const client = fakeClient({
    saveRecord: async () => {
      throw new DaprError(ErrorCodes.STATE_OPERATION_FAILED, 'daprd said no');
    },
  });
  const host = createActorHost({ limits: limits(), client });
  const { promise, requestId } = await invokeAndCaptureRequest(host);
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1', nextStateJson: '{}' });
  const result = await promise;
  assert.equal(result.status, 500);
  const body = JSON.parse(result.body);
  assert.equal(body.error.code, ErrorCodes.STATE_OPERATION_FAILED);
  assert.ok(!result.body.includes('daprd said no'));
});

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

test('an existing record is passed as a private deep copy, distinguishing stored null from absent', async () => {
  const stored = { value: { revision: 1 } };
  const client = fakeClient({ readRecord: async () => ({ exists: true, value: stored.value }) });
  const host = createActorHost({ limits: limits(), client });
  const { promise, message, requestId } = await invokeAndCaptureRequest(host);
  assert.equal(message.dapr.actor.stateExists, true);
  assert.deepEqual(message.dapr.actor.state, { revision: 1 });
  message.dapr.actor.state.revision = 999; // mutate the flow's copy
  assert.equal(stored.value.revision, 1); // the original is untouched
  host.settleActorReply(requestId, { outcome: 'complete', responseJson: '1' });
  await promise;
});
