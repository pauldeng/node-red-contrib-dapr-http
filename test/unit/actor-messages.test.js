'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { validateActorSegment, serializeProposal } = require('../../lib/actor-messages');
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
