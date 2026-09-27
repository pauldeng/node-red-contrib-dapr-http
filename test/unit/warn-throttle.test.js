'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { createWarnThrottle, createActorDiagnosticLogger } = require('../../lib/warn-throttle');

test('the first call for a code is never suppressed and reports 0 suppressed', () => {
  const throttle = createWarnThrottle();
  assert.equal(throttle.next('CODE_A'), 0);
});

test('a second call for the same code inside the window is suppressed', () => {
  let clock = 0;
  const throttle = createWarnThrottle({ now: () => clock });
  assert.equal(throttle.next('CODE_A'), 0);
  clock += 1000;
  assert.equal(throttle.next('CODE_A'), null);
  clock += 1000;
  assert.equal(throttle.next('CODE_A'), null);
});

test('the next allowed call after the window reports how many were suppressed in between', () => {
  let clock = 0;
  const throttle = createWarnThrottle({ windowMs: 60000, now: () => clock });
  assert.equal(throttle.next('CODE_A'), 0);
  clock += 1000;
  assert.equal(throttle.next('CODE_A'), null); // suppressed #1
  clock += 1000;
  assert.equal(throttle.next('CODE_A'), null); // suppressed #2
  clock += 60000; // window elapsed since the first allowed call
  assert.equal(throttle.next('CODE_A'), 2, 'reports the 2 suppressed since the last allowed call');
  // The counter resets once an allowed call is reported.
  clock += 1;
  assert.equal(throttle.next('CODE_A'), null);
  clock += 60000;
  assert.equal(throttle.next('CODE_A'), 1);
});

test('each event code is throttled independently, in a bounded map (one record per code)', () => {
  let clock = 0;
  const throttle = createWarnThrottle({ now: () => clock });
  assert.equal(throttle.next('CODE_A'), 0);
  assert.equal(throttle.next('CODE_B'), 0, 'a different code is never suppressed by CODE_A');
  clock += 1000;
  assert.equal(throttle.next('CODE_A'), null);
  assert.equal(throttle.next('CODE_B'), null);
});

test('actor diagnostics, including the drain backstop, survive a throwing logger and share throttling', () => {
  let calls = 0;
  const log = createActorDiagnosticLogger({
    warn: () => {
      calls++;
      throw Error('logger failed');
    },
  });
  for (const code of ['ACTOR_COMMIT_UNKNOWN', 'ACTOR_TOMBSTONE_HIT', 'ACTOR_DRAIN_BACKSTOP']) {
    assert.doesNotThrow(() => log(code, { actorType: 'T', method: 'M' }));
    log(code);
  }
  assert.equal(calls, 3);
});

test('actor diagnostic text reports suppressed counts without including actor IDs', () => {
  const lines = [];
  let now = 0;
  const log = createActorDiagnosticLogger({ warn: (line) => lines.push(line), now: () => now });
  log('ACTOR_COMMIT_UNKNOWN', { actorType: 'T', method: 'M', actorId: 'secret' });
  log('ACTOR_COMMIT_UNKNOWN');
  now = 60000;
  log('ACTOR_COMMIT_UNKNOWN', { actorType: 'T' });
  assert.deepEqual(lines, [
    '[ACTOR_COMMIT_UNKNOWN] type=T method=M',
    '[ACTOR_COMMIT_UNKNOWN] type=T (1 more suppressed)',
  ]);
});
