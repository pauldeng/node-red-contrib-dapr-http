'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { PendingRegistry } = require('../../lib/pending');
const { DaprError, ErrorCodes } = require('../../lib/errors');

test('add returns a promise that resolves when the id is settled', async () => {
  const reg = new PendingRegistry();
  const p = reg.add('a');
  assert.equal(reg.size, 1);
  assert.ok(reg.has('a'));
  reg.settle('a', 'SUCCESS');
  assert.equal(await p, 'SUCCESS');
  assert.equal(reg.size, 0);
});

test('settle is first-wins: a second settle is ignored and returns false', async () => {
  const reg = new PendingRegistry();
  const p = reg.add('a');
  assert.equal(reg.settle('a', 'first'), true);
  assert.equal(reg.settle('a', 'second'), false);
  assert.equal(await p, 'first');
});

test('settling an unknown id returns false (e.g. a stale/foreign id)', () => {
  const reg = new PendingRegistry();
  assert.equal(reg.settle('nope', 'x'), false);
});

test('a timed-out entry resolves with the onTimeout value and is removed', async () => {
  const reg = new PendingRegistry();
  const p = reg.add('a', { timeoutMs: 10, onTimeout: () => 'RETRY' });
  assert.equal(await p, 'RETRY');
  assert.equal(reg.has('a'), false);
  // A settle after timeout is a no-op.
  assert.equal(reg.settle('a', 'SUCCESS'), false);
});

test('settling before timeout clears the timer and wins', async () => {
  const reg = new PendingRegistry();
  const p = reg.add('a', { timeoutMs: 10000, onTimeout: () => 'RETRY' });
  reg.settle('a', 'SUCCESS');
  assert.equal(await p, 'SUCCESS');
});

test('add throws PENDING_CAPACITY once max outstanding is reached', () => {
  const reg = new PendingRegistry({ max: 2 });
  reg.add('a');
  reg.add('b');
  assert.throws(
    () => reg.add('c'),
    (e) => e instanceof DaprError && e.code === ErrorCodes.PENDING_CAPACITY
  );
  // Freeing a slot lets a new add succeed.
  reg.settle('a', 'x');
  assert.doesNotThrow(() => reg.add('c'));
});

test('add throws DUPLICATE_PENDING for an id already registered', () => {
  const reg = new PendingRegistry();
  reg.add('a');
  assert.throws(
    () => reg.add('a'),
    (e) => e instanceof DaprError && e.code === ErrorCodes.DUPLICATE_PENDING
  );
});

test('drain settles every outstanding entry with the given value', async () => {
  const reg = new PendingRegistry();
  const a = reg.add('a');
  const b = reg.add('b', { timeoutMs: 10000, onTimeout: () => 'RETRY' });
  const count = reg.drain('RETRY');
  assert.equal(count, 2);
  assert.equal(reg.size, 0);
  assert.equal(await a, 'RETRY');
  assert.equal(await b, 'RETRY');
});
