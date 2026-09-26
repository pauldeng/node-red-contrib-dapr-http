'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  validateActorSegment,
  serializeProposal,
  validateScheduleTime,
  validateOverwrite,
  prepareReminderData,
  REMINDER_METHOD,
} = require('../../lib/actor-messages');
const { DaprError, ErrorCodes } = require('../../lib/errors');

test('validateActorSegment accepts an ordinary identifier', () => {
  assert.equal(validateActorSegment('DemoActor', 'actorType'), 'DemoActor');
});

test('validateActorSegment rejects absent, empty, oversized, "/", control chars, "." and ".."', () => {
  const cases = [undefined, '', 'a'.repeat(257), 'a/b', 'a\u0000b', '.', '..'];
  for (const value of cases) {
    assert.throws(() => validateActorSegment(value, 'actorType'), DaprError);
  }
});

test('validateActorSegment is case-sensitive: no normalization', () => {
  assert.equal(validateActorSegment('AbC', 'actorType'), 'AbC');
});

test('validateActorSegment defaults to INVALID_MESSAGE, but a caller can ask for INVALID_OPTIONS', () => {
  try {
    validateActorSegment('', 'actorType');
    assert.fail('expected a throw');
  } catch (err) {
    assert.equal(err.code, ErrorCodes.INVALID_MESSAGE);
  }
  try {
    validateActorSegment('', 'actorType', ErrorCodes.INVALID_OPTIONS);
    assert.fail('expected a throw');
  } catch (err) {
    assert.equal(err.code, ErrorCodes.INVALID_OPTIONS);
  }
});

// ---- serializeProposal: complete ----

test('a complete reply with no nextState serializes only the response', () => {
  const proposal = serializeProposal({ payload: { ok: true } }, 'complete');
  assert.deepEqual(proposal, { outcome: 'complete', responseJson: '{"ok":true}' });
});

test('a complete reply payload of null is valid', () => {
  const proposal = serializeProposal({ payload: null }, 'complete');
  assert.equal(proposal.responseJson, 'null');
});

test('a complete reply with an explicit undefined payload is invalid', () => {
  assert.throws(() => serializeProposal({ payload: undefined }, 'complete'), DaprError);
  assert.throws(() => serializeProposal({}, 'complete'), DaprError);
});

test('nextState absent (property not present) means no write', () => {
  const proposal = serializeProposal({ payload: 1, dapr: { actor: {} } }, 'complete');
  assert.equal('nextStateJson' in proposal, false);
});

for (const falsy of [null, false, 0, '', [], {}]) {
  test(`nextState ${JSON.stringify(falsy)} is a valid write`, () => {
    const proposal = serializeProposal(
      { payload: 1, dapr: { actor: { nextState: falsy } } },
      'complete'
    );
    assert.equal(proposal.nextStateJson, JSON.stringify(falsy));
  });
}

test('nextState explicit undefined is invalid', () => {
  assert.throws(
    () => serializeProposal({ payload: 1, dapr: { actor: { nextState: undefined } } }, 'complete'),
    DaprError
  );
});

test('nextState a Buffer is invalid', () => {
  assert.throws(
    () =>
      serializeProposal(
        { payload: 1, dapr: { actor: { nextState: Buffer.from('x') } } },
        'complete'
      ),
    DaprError
  );
});

test('nextState a non-finite number is invalid', () => {
  for (const bad of [NaN, Infinity, -Infinity]) {
    assert.throws(
      () => serializeProposal({ payload: 1, dapr: { actor: { nextState: bad } } }, 'complete'),
      DaprError
    );
  }
});

test('nextState a BigInt is invalid', () => {
  assert.throws(
    () => serializeProposal({ payload: 1, dapr: { actor: { nextState: 1n } } }, 'complete'),
    DaprError
  );
});

test('nextState a function is invalid', () => {
  assert.throws(
    () => serializeProposal({ payload: 1, dapr: { actor: { nextState: () => {} } } }, 'complete'),
    DaprError
  );
});

test('nextState a circular reference is invalid', () => {
  const cyclic = {};
  cyclic.self = cyclic;
  assert.throws(
    () => serializeProposal({ payload: 1, dapr: { actor: { nextState: cyclic } } }, 'complete'),
    DaprError
  );
});

test('nextState a shared-but-acyclic reference is valid (not mistaken for a cycle)', () => {
  const shared = { a: 1 };
  const value = { left: shared, right: shared };
  const proposal = serializeProposal(
    { payload: 1, dapr: { actor: { nextState: value } } },
    'complete'
  );
  assert.equal(proposal.nextStateJson, JSON.stringify(value));
});

function nested(depth) {
  let value = 0;
  for (let i = 0; i < depth; i += 1) {
    value = [value];
  }
  return value;
}

test('nextState nesting exactly 32 levels deep is valid', () => {
  const value = nested(32);
  const proposal = serializeProposal(
    { payload: 1, dapr: { actor: { nextState: value } } },
    'complete'
  );
  assert.equal(proposal.nextStateJson, JSON.stringify(value));
});

test('nextState nesting 33 levels deep is invalid', () => {
  const value = nested(33);
  assert.throws(
    () => serializeProposal({ payload: 1, dapr: { actor: { nextState: value } } }, 'complete'),
    DaprError
  );
});

test('nextState exceeding the connection body limit is invalid', () => {
  const big = { blob: 'x'.repeat(100) };
  assert.throws(
    () =>
      serializeProposal({ payload: 1, dapr: { actor: { nextState: big } } }, 'complete', {
        maxBodyBytes: 10,
      }),
    DaprError
  );
  // Under the limit still succeeds.
  const proposal = serializeProposal(
    { payload: 1, dapr: { actor: { nextState: { a: 1 } } } },
    'complete',
    { maxBodyBytes: 1024 }
  );
  assert.equal(proposal.nextStateJson, '{"a":1}');
});

test('an oversized response is rejected before accepting a state proposal', () => {
  const big = { blob: 'x'.repeat(100) };
  assert.throws(
    () =>
      serializeProposal({ payload: big, dapr: { actor: { nextState: 1 } } }, 'complete', {
        maxBodyBytes: 10,
      }),
    { code: ErrorCodes.INVALID_MESSAGE }
  );
});

// ---- serializeProposal: deleteState ----

test('deleteState inherited from a prototype cannot propose a deletion or invalidate a fail reply', () => {
  const actor = Object.create({ deleteState: true });
  actor.error = { code: 'EXPECTED', message: 'failed' };
  const msg = { payload: null, dapr: { actor } };
  assert.deepEqual(serializeProposal(msg, 'complete'), {
    outcome: 'complete',
    responseJson: 'null',
  });
  assert.equal(serializeProposal(msg, 'fail').outcome, 'fail');
});

test('deleteState absent is unaffected: nextState still behaves normally', () => {
  const proposal = serializeProposal(
    { payload: 1, dapr: { actor: { nextState: { a: 1 } } } },
    'complete'
  );
  assert.deepEqual(proposal, { outcome: 'complete', responseJson: '1', nextStateJson: '{"a":1}' });
  assert.equal(Object.hasOwn(proposal, 'deleteState'), false);
});

test('deleteState: false permits an ordinary nextState and adds no deleteState key', () => {
  const proposal = serializeProposal(
    { payload: 1, dapr: { actor: { deleteState: false, nextState: { a: 1 } } } },
    'complete'
  );
  assert.deepEqual(proposal, { outcome: 'complete', responseJson: '1', nextStateJson: '{"a":1}' });
});

test('deleteState: true with no nextState proposes a delete, with no nextStateJson', () => {
  const proposal = serializeProposal(
    { payload: 1, dapr: { actor: { deleteState: true } } },
    'complete'
  );
  assert.deepEqual(proposal, { outcome: 'complete', responseJson: '1', deleteState: true });
  assert.equal('nextStateJson' in proposal, false);
});

for (const nextState of [{ a: 1 }, null, undefined, 0, false, '']) {
  test(`deleteState: true with a present nextState (${JSON.stringify(nextState)}) is invalid`, () => {
    assert.throws(
      () =>
        serializeProposal(
          { payload: 1, dapr: { actor: { deleteState: true, nextState } } },
          'complete'
        ),
      { code: ErrorCodes.INVALID_MESSAGE }
    );
  });
}

test('deleteState: true on a fail reply is invalid', () => {
  assert.throws(
    () =>
      serializeProposal(
        {
          dapr: {
            actor: { deleteState: true, error: { code: 'X', message: 'm' } },
          },
        },
        'fail'
      ),
    { code: ErrorCodes.INVALID_MESSAGE }
  );
});

test('deleteState: false on a fail reply is allowed', () => {
  const proposal = serializeProposal(
    { dapr: { actor: { deleteState: false, error: { code: 'X', message: 'm' } } } },
    'fail'
  );
  assert.equal(proposal.outcome, 'fail');
});

for (const bad of [undefined, null, 0, 1, 'true', 'false', {}, []]) {
  test(`deleteState: any non-boolean value (${JSON.stringify(bad)}) is invalid`, () => {
    assert.throws(
      () => serializeProposal({ payload: 1, dapr: { actor: { deleteState: bad } } }, 'complete'),
      { code: ErrorCodes.INVALID_MESSAGE }
    );
  });
}

test('deleteState: true still requires msg.payload for the complete reply', () => {
  assert.throws(() => serializeProposal({ dapr: { actor: { deleteState: true } } }, 'complete'), {
    code: ErrorCodes.INVALID_MESSAGE,
  });
});

// ---- serializeProposal: fail ----

test('a fail reply serializes a sanitized error envelope', () => {
  const proposal = serializeProposal(
    { dapr: { actor: { error: { code: 'UNKNOWN_KEY', message: 'no such key' } } } },
    'fail'
  );
  assert.deepEqual(proposal, {
    outcome: 'fail',
    errorBody: JSON.stringify({ error: { code: 'UNKNOWN_KEY', message: 'no such key' } }),
  });
});

test('a fail reply requires msg.dapr.actor.error', () => {
  assert.throws(() => serializeProposal({}, 'fail'), DaprError);
  assert.throws(() => serializeProposal({ dapr: { actor: {} } }, 'fail'), DaprError);
});

test('a fail reply rejects a code that does not match the pattern', () => {
  for (const code of ['lowercase', '1STARTSDIGIT', 'HAS SPACE', 'a'.repeat(65).toUpperCase()]) {
    assert.throws(
      () => serializeProposal({ dapr: { actor: { error: { code, message: 'm' } } } }, 'fail'),
      DaprError
    );
  }
});

test('a fail reply requires a string message', () => {
  assert.throws(
    () => serializeProposal({ dapr: { actor: { error: { code: 'X', message: 42 } } } }, 'fail'),
    DaprError
  );
});

test('a fail reply truncates the message to 512 characters', () => {
  const long = 'x'.repeat(600);
  const proposal = serializeProposal(
    { dapr: { actor: { error: { code: 'X', message: long } } } },
    'fail'
  );
  const parsed = JSON.parse(proposal.errorBody);
  assert.equal(parsed.error.message.length, 512);
});

test('fail replies respect the byte limit even with multibyte error text', () => {
  assert.throws(
    () =>
      serializeProposal(
        { dapr: { actor: { error: { code: 'FAILED', message: '界'.repeat(512) } } } },
        'fail',
        { maxBodyBytes: 1024 }
      ),
    { code: ErrorCodes.INVALID_MESSAGE }
  );
});

test('an unknown outcome is invalid', () => {
  assert.throws(() => serializeProposal({ payload: 1 }, 'other'), DaprError);
});

// ---- REMINDER_METHOD --------------------------------------------------------
//
// Deliberately the OPPOSITE of a normal registry key: it must be a value no
// real, user-configured method name can ever equal, or an ordinary method
// registration could collide with (or be silently routed into) the reminder
// registration -- see lib/connection-registry.js's addActorMethod.

test('REMINDER_METHOD is never a valid actor segment -- no real method name can equal it', () => {
  assert.throws(() => validateActorSegment(REMINDER_METHOD, 'method'), DaprError);
});

// ---- validateScheduleTime ---------------------------------------------------

test('validateScheduleTime: absent or empty means omitted', () => {
  assert.equal(validateScheduleTime(undefined, 'dueTime'), undefined);
  assert.equal(validateScheduleTime('', 'dueTime'), undefined);
});

test('validateScheduleTime: an ordinary string is returned unchanged', () => {
  assert.equal(validateScheduleTime('10s', 'dueTime'), '10s');
  assert.equal(validateScheduleTime('x'.repeat(128), 'dueTime'), 'x'.repeat(128));
});

test('validateScheduleTime: rejects null, a non-string, and over-length', () => {
  for (const bad of [null, 42, true, {}, [], 'x'.repeat(129)]) {
    assert.throws(() => validateScheduleTime(bad, 'dueTime'), DaprError);
  }
});

// ---- validateOverwrite -------------------------------------------------------

test('validateOverwrite: absent defaults to true', () => {
  assert.equal(validateOverwrite(undefined), true);
});

test('validateOverwrite: an explicit boolean is returned as-is', () => {
  assert.equal(validateOverwrite(true), true);
  assert.equal(validateOverwrite(false), false);
});

test('validateOverwrite: rejects a non-boolean', () => {
  for (const bad of [null, '', 'true', 1, 0, {}]) {
    assert.throws(() => validateOverwrite(bad), DaprError);
  }
});

// ---- prepareReminderData ----------------------------------------------------

test('prepareReminderData: hasData false means no data key at all', () => {
  assert.deepEqual(prepareReminderData(undefined, false), { hasData: false, data: undefined });
});

test('prepareReminderData: an explicit null is preserved as valid data', () => {
  assert.deepEqual(prepareReminderData(null, true), { hasData: true, data: null });
});

test('prepareReminderData: an ordinary object is accepted', () => {
  const value = { greeting: 'hi' };
  assert.deepEqual(prepareReminderData(value, true), { hasData: true, data: value });
});

test('prepareReminderData: a non-serializable value (BigInt, circular, Buffer) is invalid', () => {
  assert.throws(() => prepareReminderData(1n, true), DaprError);
  const cyclic = {};
  cyclic.self = cyclic;
  assert.throws(() => prepareReminderData(cyclic, true), DaprError);
  assert.throws(() => prepareReminderData(Buffer.from('x'), true), DaprError);
});

test('prepareReminderData: bounds the escaped envelope size against the connection body limit', () => {
  // '{"blob":"xx"}' is 13 bytes with no escapable characters; envelope adds 34.
  const small = { blob: 'xx' };
  assert.deepEqual(prepareReminderData(small, true, 13 + 34), { hasData: true, data: small });
  assert.throws(() => prepareReminderData(small, true, 13 + 34 - 1), DaprError);
});

test('prepareReminderData: HTML-escaped worst case -- each <, >, & counts as 6 bytes, not 1', () => {
  // JSON.stringify({a:'<&>'}) === '{"a":"<&>"}' -- 11 bytes raw, but escaped it
  // is '{"a":"\\u003c\\u0026\\u003e"}': each of the 3 special characters becomes
  // a 6-byte escape (net +5 each), so the escaped length is 11 + 3*5 = 26.
  const value = { a: '<&>' };
  const rawBytes = Buffer.byteLength(JSON.stringify(value), 'utf8');
  assert.equal(rawBytes, 11);
  const escapedBytes = rawBytes + 3 * 5;
  assert.deepEqual(prepareReminderData(value, true, escapedBytes + 34), {
    hasData: true,
    data: value,
  });
  assert.throws(() => prepareReminderData(value, true, escapedBytes + 34 - 1), DaprError);
  // Without accounting for escaping, the raw byte count alone would have
  // wrongly fit a limit that the real escaped wire body cannot.
  assert.throws(() => prepareReminderData(value, true, rawBytes + 34), DaprError);
});

test('prepareReminderData: no maxBodyBytes means no bound is enforced', () => {
  const value = { blob: 'x'.repeat(1000) };
  assert.deepEqual(prepareReminderData(value, true), { hasData: true, data: value });
});

test('prepareReminderData: bounds Go-escaped Unicode separators in keys and values', () => {
  const value = { '\u2028': '\u2029' };
  const envelope = '{"data":{"\\u2028":"\\u2029"},"dueTime":"","period":""}';
  const limit = Buffer.byteLength(envelope);
  assert.deepEqual(prepareReminderData(value, true, limit).data, value);
  assert.throws(() => prepareReminderData(value, true, limit - 1), { code: 'INVALID_MESSAGE' });
});

test('prepareReminderData: sends the validated snapshot without invoking toJSON a second time', () => {
  let serializations = 0;
  const value = Object.create({
    toJSON() {
      return ++serializations === 1 ? 'small' : 'x'.repeat(1000);
    },
  });
  const prepared = prepareReminderData(value, true, 100);
  assert.equal(JSON.stringify(prepared.data), '"small"');
  assert.equal(serializations, 1);
});
