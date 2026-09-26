'use strict';

// Exercises the exact exported Function-node code from
// examples/actor-demo.json -- there is no second copy of the
// DemoActor/DemoActorCounter logic to keep in sync. Each Function's `func`
// source is compiled with `new Function('msg', func)` and driven with
// synthetic messages shaped like the actor-method node's documented output
// (see nodes/dapr-actor-method.html and docs/architecture.md's "Actor
// request ownership"): `msg.payload` is the method argument and
// `msg.dapr.actor` carries { stateExists, state }.
//
// The actor node types (dapr-actor-method/-reply/-call) are exercised
// through a real Node-RED runtime elsewhere (test/runtime/actors.test.js,
// test/integration/actors.test.js); this file only proves the flow's plain-JS
// logic and its wiring shape (parsed JSON, no deploy).

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  validateActorSegment,
  validateScheduleTime,
  validateOverwrite,
  serializeProposal,
} = require('../../lib/actor-messages');

const FLOW_PATH = path.resolve(__dirname, '../../examples/actor-demo.json');
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

function loadFlow() {
  return JSON.parse(fs.readFileSync(FLOW_PATH, 'utf8'));
}

function functionNode(flow, name) {
  const node = flow.find((n) => n.type === 'function' && n.name === name);
  assert.ok(node, `function node "${name}" not found`);
  return node;
}

function compile(node) {
  return new Function('msg', node.func); // eslint-disable-line no-new-func
}

function makeMsg(
  payload,
  { stateExists = false, state = null, actorType = 'DemoActor', trigger } = {}
) {
  const msg = {
    payload,
    dapr: {
      actor: {
        type: actorType,
        id: 'test-actor',
        method: trigger ? trigger.name : 'test',
        stateExists,
        state,
      },
      actorRequestId: 'req-1',
    },
  };
  if (trigger) {
    msg.dapr.actor.trigger = trigger;
  }
  return msg;
}

// Every Function in this flow returns [completeMsg|null, failMsg|null].
function assertComplete(result) {
  assert.ok(result[0], 'expected the complete output to carry a message');
  assert.equal(result[1], null, 'the fail output must be null on success');
  return result[0];
}

function assertFail(result) {
  assert.equal(result[0], null, 'the complete output must be null on failure');
  assert.ok(result[1], 'expected the fail output to carry a message');
  return result[1];
}

function assertErrorCode(failMsg, code) {
  assert.match(failMsg.dapr.actor.error.code, /^[A-Z][A-Z0-9_]{0,63}$/);
  assert.equal(failMsg.dapr.actor.error.code, code);
  assert.equal(typeof failMsg.dapr.actor.error.message, 'string');
}

function getMyDataFn() {
  return compile(functionNode(loadFlow(), 'GetMyData: return stored data'));
}
function setMyDataFn() {
  return compile(functionNode(loadFlow(), 'SetMyData: store with timestamp'));
}
function clearMyDataFn() {
  return compile(functionNode(loadFlow(), 'ClearMyData: clear stored data'));
}
function countFn() {
  return compile(functionNode(loadFlow(), 'count: increment by one'));
}
function countByFn() {
  return compile(functionNode(loadFlow(), 'countBy: increment by amount*multiplier'));
}
function getCounterFn() {
  return compile(functionNode(loadFlow(), 'getCounter: return stored counter'));
}
function catchFn() {
  return compile(functionNode(loadFlow(), 'Catch: safe error'));
}
function reminderFn() {
  return compile(functionNode(loadFlow(), 'reminder: record last reminder'));
}

// ---------------------------------------------------------------------------
// GetMyData
// ---------------------------------------------------------------------------

test('GetMyData: unprovisioned actor returns null, no write', () => {
  const result = assertComplete(getMyDataFn()(makeMsg(undefined, { stateExists: false })));
  assert.equal(result.payload, null);
  assert.equal(result.dapr.actor.nextState, undefined);
});

test('GetMyData: returns the stored record verbatim, no write', () => {
  const state = { greeting: 'hello world', ts: '2026-01-01T00:00:00.000Z' };
  const result = assertComplete(getMyDataFn()(makeMsg(undefined, { stateExists: true, state })));
  assert.deepEqual(result.payload, state);
  assert.equal(result.dapr.actor.nextState, undefined);
});

// ---------------------------------------------------------------------------
// SetMyData
// ---------------------------------------------------------------------------

test('SetMyData: copies the given object and adds a ts timestamp as nextState', () => {
  const result = assertComplete(setMyDataFn()(makeMsg({ greeting: 'hello world' })));
  assert.equal(result.payload, null);
  assert.equal(result.dapr.actor.nextState.greeting, 'hello world');
  assert.match(result.dapr.actor.nextState.ts, ISO_TIMESTAMP);
});

test('SetMyData: overwrites an existing record with the new object plus a fresh ts', () => {
  const existing = { greeting: 'old', ts: '2020-01-01T00:00:00.000Z' };
  const result = assertComplete(
    setMyDataFn()(makeMsg({ greeting: 'new' }, { stateExists: true, state: existing }))
  );
  assert.equal(result.dapr.actor.nextState.greeting, 'new');
  assert.notEqual(result.dapr.actor.nextState.ts, existing.ts);
});

test('SetMyData: a non-object argument fails INVALID_ARGUMENT', () => {
  for (const payload of [undefined, null, 'a string', 42, true, ['array']]) {
    const fail = assertFail(setMyDataFn()(makeMsg(payload)));
    assertErrorCode(fail, 'INVALID_ARGUMENT');
  }
});

test('SetMyData preserves an own __proto__ JSON field without changing the record prototype', () => {
  const data = JSON.parse('{"__proto__":{"label":"stored data"},"greeting":"hello"}');
  const result = assertComplete(setMyDataFn()(makeMsg(data)));
  const state = result.dapr.actor.nextState;
  assert.equal(Object.getPrototypeOf(state), Object.prototype);
  assert.ok(Object.hasOwn(state, '__proto__'));
  assert.deepEqual(state.__proto__, data.__proto__);
});

// ---------------------------------------------------------------------------
// ClearMyData
// ---------------------------------------------------------------------------

test('ClearMyData: proposes a delete, with payload null and no nextState', () => {
  const existing = { greeting: 'hello world', ts: '2020-01-01T00:00:00.000Z' };
  const result = assertComplete(
    clearMyDataFn()(makeMsg(undefined, { stateExists: true, state: existing }))
  );
  assert.equal(result.payload, null);
  assert.equal(result.dapr.actor.deleteState, true);
  assert.equal(Object.hasOwn(result.dapr.actor, 'nextState'), false);
  assert.doesNotThrow(() => serializeProposal(result, 'complete'));
});

test('ClearMyData: works the same on an unprovisioned actor (no record yet)', () => {
  const result = assertComplete(clearMyDataFn()(makeMsg(undefined, { stateExists: false })));
  assert.equal(result.payload, null);
  assert.equal(result.dapr.actor.deleteState, true);
});

// ---------------------------------------------------------------------------
// count
// ---------------------------------------------------------------------------

test('count: an unprovisioned actor starts at 1', () => {
  const result = assertComplete(countFn()(makeMsg(undefined, { stateExists: false })));
  assert.equal(result.payload, 1);
  assert.deepEqual(result.dapr.actor.nextState, { counter: 1 });
});

test('count: increments an existing counter by one', () => {
  const result = assertComplete(
    countFn()(makeMsg(undefined, { stateExists: true, state: { counter: 5 } }))
  );
  assert.equal(result.payload, 6);
  assert.deepEqual(result.dapr.actor.nextState, { counter: 6 });
});

// ---------------------------------------------------------------------------
// countBy
// ---------------------------------------------------------------------------

test('countBy: multiplier defaults to 1', () => {
  const result = assertComplete(
    countByFn()(makeMsg({ amount: 5 }, { stateExists: true, state: { counter: 10 } }))
  );
  assert.equal(result.payload, 15);
  assert.deepEqual(result.dapr.actor.nextState, { counter: 15 });
});

test('countBy: amount * multiplier, on an unprovisioned actor', () => {
  const result = assertComplete(
    countByFn()(makeMsg({ amount: 5, multiplier: 2 }, { stateExists: false }))
  );
  assert.equal(result.payload, 10);
  assert.deepEqual(result.dapr.actor.nextState, { counter: 10 });
});

test('countBy: a non-finite or missing amount fails INVALID_ARGUMENT', () => {
  for (const payload of [
    {},
    { amount: 'five' },
    { amount: NaN },
    { amount: Infinity },
    undefined,
  ]) {
    const fail = assertFail(countByFn()(makeMsg(payload)));
    assertErrorCode(fail, 'INVALID_ARGUMENT');
  }
});

test('countBy: a non-finite multiplier fails INVALID_ARGUMENT', () => {
  for (const multiplier of ['two', NaN, Infinity]) {
    const fail = assertFail(countByFn()(makeMsg({ amount: 1, multiplier })));
    assertErrorCode(fail, 'INVALID_ARGUMENT');
  }
});

// ---------------------------------------------------------------------------
// getCounter
// ---------------------------------------------------------------------------

test('getCounter: an unprovisioned actor reads 0, no write', () => {
  const result = assertComplete(getCounterFn()(makeMsg(undefined, { stateExists: false })));
  assert.equal(result.payload, 0);
  assert.equal(result.dapr.actor.nextState, undefined);
});

test('getCounter: returns the stored counter, no write', () => {
  const result = assertComplete(
    getCounterFn()(makeMsg(undefined, { stateExists: true, state: { counter: 42 } }))
  );
  assert.equal(result.payload, 42);
  assert.equal(result.dapr.actor.nextState, undefined);
});

// ---------------------------------------------------------------------------
// Catch function
// ---------------------------------------------------------------------------

test('Catch: safe error builds a sanitized INTERNAL error and drops any partial nextState', () => {
  const fn = catchFn();
  const msg = makeMsg({}, { stateExists: false });
  msg.dapr.actor.nextState = { counter: 'leaked-partial-write' };
  const result = fn(msg);
  assert.equal(result, msg, 'Catch must mutate and return the same message');
  assertErrorCode(result, 'INTERNAL');
  assert.equal(result.dapr.actor.error.message, 'unexpected error');
  assert.equal(result.dapr.actor.nextState, undefined);
});

test('Catch: safe error also drops deleteState, which would otherwise make the fail reply itself invalid', () => {
  const fn = catchFn();
  const msg = makeMsg(null, { stateExists: true });
  msg.dapr.actor.deleteState = true;
  const result = fn(msg);
  assertErrorCode(result, 'INTERNAL');
  assert.equal(Object.hasOwn(result.dapr.actor, 'deleteState'), false);
  assert.doesNotThrow(() => serializeProposal(result, 'fail'));
});

// ---------------------------------------------------------------------------
// reminder trigger
// ---------------------------------------------------------------------------

test('reminder trigger: first occurrence of a name starts its count at 1 and records the data', () => {
  const msg = makeMsg(
    { message: 'reminder_state' },
    { stateExists: false, trigger: { kind: 'reminder', name: 'demo_reminder' } }
  );
  const result = assertComplete(reminderFn()(msg));
  assert.equal(result.payload, null);
  assert.deepEqual(result.dapr.actor.nextState, {
    lastReminder: { name: 'demo_reminder', data: { message: 'reminder_state' }, count: 1 },
  });
});

test('reminder trigger: a repeat occurrence of the same name increments its count without disturbing the rest of the record', () => {
  const existing = {
    greeting: 'hello',
    ts: '2020-01-01T00:00:00.000Z',
    lastReminder: { name: 'demo_reminder', data: { message: 'reminder_state' }, count: 2 },
  };
  const msg = makeMsg(
    { message: 'reminder_state' },
    {
      stateExists: true,
      state: existing,
      trigger: { kind: 'reminder', name: 'demo_reminder' },
    }
  );
  const result = assertComplete(reminderFn()(msg));
  assert.equal(result.dapr.actor.nextState.greeting, 'hello');
  assert.equal(result.dapr.actor.nextState.ts, '2020-01-01T00:00:00.000Z');
  assert.deepEqual(result.dapr.actor.nextState.lastReminder, {
    name: 'demo_reminder',
    data: { message: 'reminder_state' },
    count: 3,
  });
});

test('reminder trigger: a different reminder name starts its own count at 1', () => {
  const existing = { lastReminder: { name: 'demo_reminder', data: null, count: 5 } };
  const msg = makeMsg(undefined, {
    stateExists: true,
    state: existing,
    trigger: { kind: 'reminder', name: 'demo_reminder_once' },
  });
  const result = assertComplete(reminderFn()(msg));
  assert.deepEqual(result.dapr.actor.nextState.lastReminder, {
    name: 'demo_reminder_once',
    data: null,
    count: 1,
  });
});

test('reminder trigger: arbitrary SetMyData fields cannot poison the reminder count', () => {
  for (const lastReminder of [{ name: 'demo_reminder' }, { name: 'demo_reminder', count: '2' }]) {
    const stored = assertComplete(setMyDataFn()(makeMsg({ lastReminder })));
    const result = assertComplete(
      reminderFn()(
        makeMsg(null, {
          stateExists: true,
          state: stored.dapr.actor.nextState,
          trigger: { kind: 'reminder', name: 'demo_reminder' },
        })
      )
    );
    assert.equal(result.dapr.actor.nextState.lastReminder.count, 1);
    assert.doesNotThrow(() => serializeProposal(result, 'complete'));
  }
});

test('reminder trigger: a reminder created with no data records null, not undefined', () => {
  const msg = makeMsg(undefined, {
    stateExists: false,
    trigger: { kind: 'reminder', name: 'demo_reminder' },
  });
  const result = assertComplete(reminderFn()(msg));
  assert.equal(result.dapr.actor.nextState.lastReminder.data, null);
});

test('reminder trigger: an unprovisioned actor (no record yet) still commits cleanly', () => {
  const msg = makeMsg('plain-string', {
    stateExists: false,
    state: null,
    trigger: { kind: 'reminder', name: 'demo_reminder' },
  });
  const result = assertComplete(reminderFn()(msg));
  assert.deepEqual(result.dapr.actor.nextState, {
    lastReminder: { name: 'demo_reminder', data: 'plain-string', count: 1 },
  });
});

test('reminder trigger: its output is accepted by the real reply proposal validator', () => {
  const msg = makeMsg(
    { message: 'reminder_state' },
    { stateExists: false, trigger: { kind: 'reminder', name: 'demo_reminder' } }
  );
  const result = assertComplete(reminderFn()(msg));
  assert.doesNotThrow(() => serializeProposal(result, 'complete', { maxBodyBytes: 1_000_000 }));
});

// ---------------------------------------------------------------------------
// Flow wiring
// ---------------------------------------------------------------------------

test('flow wiring: each method node feeds its function, which feeds both reply nodes', () => {
  const flow = loadFlow();
  const byId = (id) => flow.find((n) => n.id === id);

  const methodToFn = {
    GetMyData: 'ex-actor-getmydata-fn',
    SetMyData: 'ex-actor-setmydata-fn',
    ClearMyData: 'ex-actor-clearmydata-fn',
    count: 'ex-actor-count-fn',
    countBy: 'ex-actor-countby-fn',
    getCounter: 'ex-actor-getcounter-fn',
  };

  const replyComplete = flow.find((n) => n.type === 'dapr-actor-reply' && n.outcome === 'complete');
  const replyFail = flow.find((n) => n.type === 'dapr-actor-reply' && n.outcome === 'fail');
  assert.ok(replyComplete, 'a dapr-actor-reply node with outcome "complete" must exist');
  assert.ok(replyFail, 'a dapr-actor-reply node with outcome "fail" must exist');

  const methodNodes = flow.filter((n) => n.type === 'dapr-actor-method');
  assert.equal(methodNodes.length, 7);
  assert.deepEqual(
    new Set(methodNodes.map((n) => n.actorType)),
    new Set(['DemoActor', 'DemoActorCounter'])
  );

  const ordinaryMethods = methodNodes.filter((n) => n.trigger !== 'reminder');
  assert.equal(ordinaryMethods.length, 6);
  for (const method of ordinaryMethods) {
    assert.equal(method.wires.length, 1);
    const fnId = method.wires[0][0];
    assert.equal(
      fnId,
      methodToFn[method.method],
      `${method.method} must wire to its own function node`
    );

    const fn = byId(fnId);
    assert.equal(fn.type, 'function');
    assert.equal(fn.outputs, 2);
    assert.deepEqual(
      fn.wires[0],
      [replyComplete.id],
      `${fn.name} output 1 must wire to the complete reply`
    );
    assert.deepEqual(
      fn.wires[1],
      [replyFail.id],
      `${fn.name} output 2 must wire to the fail reply`
    );
  }

  const reminderMethods = methodNodes.filter((n) => n.trigger === 'reminder');
  assert.equal(reminderMethods.length, 1, 'exactly one reminder-trigger method node');
  const reminderMethod = reminderMethods[0];
  assert.equal(reminderMethod.actorType, 'DemoActor');
  assert.equal(reminderMethod.wires.length, 1);
  const reminderFnNode = byId(reminderMethod.wires[0][0]);
  assert.equal(reminderFnNode.type, 'function');
  assert.equal(reminderFnNode.name, 'reminder: record last reminder');
  assert.equal(reminderFnNode.outputs, 2);
  assert.deepEqual(reminderFnNode.wires[0], [replyComplete.id]);
  assert.deepEqual(reminderFnNode.wires[1], [replyFail.id]);
});

test('flow wiring: Catch covers method functions and complete reply, excluding fail reply', () => {
  const flow = loadFlow();
  const byId = (id) => flow.find((n) => n.id === id);

  const catchNode = flow.find((n) => n.type === 'catch');
  assert.ok(catchNode, 'a catch node must exist');
  assert.equal(
    catchNode.uncaught,
    false,
    'the catch node must not also catch uncaught errors flow-wide'
  );

  const scopedNames = catchNode.scope.map((id) => byId(id).name).sort();
  assert.deepEqual(scopedNames, [
    'ClearMyData: clear stored data',
    'GetMyData: return stored data',
    'SetMyData: store with timestamp',
    'count: increment by one',
    'countBy: increment by amount*multiplier',
    'getCounter: return stored counter',
    'reminder: record last reminder',
    'reply: complete',
  ]);

  assert.equal(catchNode.wires.length, 1);
  const catchFnId = catchNode.wires[0][0];
  const catchFunctionNode = byId(catchFnId);
  assert.equal(catchFunctionNode.type, 'function');
  assert.equal(catchFunctionNode.name, 'Catch: safe error');

  const replyFail = flow.find((n) => n.type === 'dapr-actor-reply' && n.outcome === 'fail');
  assert.equal(catchNode.scope.includes(replyFail.id), false, 'fail reply must not catch itself');
  assert.deepEqual(
    catchFunctionNode.wires,
    [[replyFail.id]],
    'Catch -> fail path must reach the fail reply'
  );
});

test('flow shape: one connection, actor-call and actor-schedule each fed by their own Injects, a comment with the generic story', () => {
  const flow = loadFlow();

  const connections = flow.filter((n) => n.type === 'dapr-connection');
  assert.equal(connections.length, 1);

  const call = flow.find((n) => n.type === 'dapr-actor-call');
  assert.ok(call);
  assert.equal(call.connection, connections[0].id);

  const schedule = flow.find((n) => n.type === 'dapr-actor-schedule');
  assert.ok(schedule);
  assert.equal(schedule.connection, connections[0].id);

  const injects = flow.filter((n) => n.type === 'inject');
  assert.ok(injects.length >= 9, 'the example should cover several caller and schedule fixtures');

  const callInjects = injects.filter((i) => i.wires?.[0]?.[0] === call.id);
  const scheduleInjects = injects.filter((i) => i.wires?.[0]?.[0] === schedule.id);
  assert.equal(
    callInjects.length + scheduleInjects.length,
    injects.length,
    'every Inject must feed either the shared actor-call or the shared actor-schedule node'
  );
  assert.ok(callInjects.length >= 6);
  assert.ok(scheduleInjects.length >= 3);

  for (const inject of callInjects) {
    const override = inject.props.find((p) => p.p === 'dapr.actorCall');
    assert.ok(override, `${inject.name} must override msg.dapr.actorCall`);
    const parsed = JSON.parse(override.v);
    assert.ok(
      parsed.type === 'DemoActor' || parsed.type === 'DemoActorCounter',
      `${inject.name} must target one of this example's actor types`
    );
    assert.ok(parsed.id && parsed.method);
  }

  for (const inject of scheduleInjects) {
    const override = inject.props.find((p) => p.p === 'dapr.actorSchedule');
    assert.ok(override, `${inject.name} must override msg.dapr.actorSchedule`);
    const parsed = JSON.parse(override.v);
    assert.ok(
      ['set', 'get', 'delete'].includes(parsed.operation),
      `${inject.name} must set a valid operation`
    );
    assert.doesNotThrow(() => validateActorSegment(parsed.actorType, 'actorType'));
    assert.doesNotThrow(() => validateActorSegment(parsed.actorId, 'actorId'));
    assert.doesNotThrow(() => validateActorSegment(parsed.scheduleName, 'scheduleName'));
    if (Object.hasOwn(parsed, 'dueTime')) {
      assert.doesNotThrow(() => validateScheduleTime(parsed.dueTime, 'dueTime'));
    }
    if (Object.hasOwn(parsed, 'period')) {
      assert.doesNotThrow(() => validateScheduleTime(parsed.period, 'period'));
    }
    if (Object.hasOwn(parsed, 'overwrite')) {
      assert.doesNotThrow(() => validateOverwrite(parsed.overwrite));
    }
  }

  assert.equal(call.wires.length, 1);
  const debugNode = flow.find((n) => n.id === call.wires[0][0]);
  assert.equal(debugNode.type, 'debug');

  assert.equal(schedule.wires.length, 1);
  const scheduleDebugNode = flow.find((n) => n.id === schedule.wires[0][0]);
  assert.equal(scheduleDebugNode.type, 'debug');

  const comment = flow.find((n) => n.type === 'comment');
  assert.ok(comment, 'a top-level comment node with the generic actor story must exist');
  assert.match(comment.name, /own saved state/);
  assert.match(comment.info, /one at a time/);
  assert.match(comment.info, /dapr\/python-sdk/);
  assert.match(comment.info, /dapr\/js-sdk/);
});

test("flow shape: the schedule node's own default config passes the real validators", () => {
  const flow = loadFlow();
  const schedule = flow.find((n) => n.type === 'dapr-actor-schedule');
  assert.doesNotThrow(() => validateActorSegment(schedule.actorType, 'actorType'));
  assert.doesNotThrow(() => validateActorSegment(schedule.actorId, 'actorId'));
  assert.doesNotThrow(() => validateActorSegment(schedule.scheduleName, 'scheduleName'));
  assert.doesNotThrow(() => validateScheduleTime(schedule.dueTime, 'dueTime'));
  assert.doesNotThrow(() => validateScheduleTime(schedule.period, 'period'));
  assert.doesNotThrow(() => validateScheduleTime(schedule.ttl, 'ttl'));
  assert.doesNotThrow(() => validateOverwrite(schedule.overwrite));
});

test('flow shape: tab and connection follow the package examples convention', () => {
  const flow = loadFlow();
  const tab = flow.find((n) => n.type === 'tab');
  assert.ok(tab);
  assert.match(tab.info, /Dapr host\/port/);

  const connection = flow.find((n) => n.type === 'dapr-connection');
  assert.equal(connection.bindAddress, '127.0.0.1');
  assert.equal(connection.appPort, '3000');
});
