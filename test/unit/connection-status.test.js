'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { connectionStatus } = require('../../lib/connection-status');

const base = {
  hasLease: true,
  healthy: true,
  servedFingerprint: 'abc',
  desiredFingerprint: 'abc',
  subscriptionCount: 0,
};

test('no listener yet outranks everything else', () => {
  // Precedence matters: a connection with no lease has nothing meaningful to
  // say about health or discovery yet, so it must not claim "connected" or
  // scare the operator with a restart warning.
  const { status, restartRequired } = connectionStatus({
    ...base,
    hasLease: false,
    healthy: false,
    servedFingerprint: 'stale',
    subscriptionCount: 3,
  });
  assert.equal(status.text, 'connecting');
  assert.equal(restartRequired, false);
});

test('an unhealthy sidecar outranks a stale subscription set', () => {
  const { status, restartRequired } = connectionStatus({
    ...base,
    healthy: false,
    servedFingerprint: 'stale',
  });
  assert.equal(status.text, 'sidecar unavailable');
  assert.equal(restartRequired, false);
});

test('a served set differing from the desired one requires a restart', () => {
  const { status, restartRequired } = connectionStatus({
    ...base,
    servedFingerprint: 'stale',
    desiredFingerprint: 'fresh',
  });
  assert.equal(status.text, 'restart sidecar: subscriptions changed');
  assert.equal(restartRequired, true);
});

test('removing the last subscription still requires a restart', () => {
  // The empty set is a real change: daprd keeps delivering to routes it
  // fetched earlier until it re-reads /dapr/subscribe.
  const { restartRequired } = connectionStatus({
    ...base,
    servedFingerprint: 'had-subscriptions',
    desiredFingerprint: 'empty',
    subscriptionCount: 0,
  });
  assert.equal(restartRequired, true);
});

test('subscriptions registered but never fetched report waiting, not connected', () => {
  const { status, restartRequired } = connectionStatus({
    ...base,
    servedFingerprint: null,
    subscriptionCount: 2,
  });
  assert.equal(status.text, 'waiting for sidecar discovery');
  assert.equal(restartRequired, false);
});

test('no subscriptions and no fetch is simply connected', () => {
  // A connection used only for publish/invoke never gets a discovery fetch,
  // and must not sit on "waiting" forever.
  const { status } = connectionStatus({ ...base, servedFingerprint: null, subscriptionCount: 0 });
  assert.equal(status.text, 'connected');
});

test('a served set matching the desired one is connected', () => {
  const { status, restartRequired } = connectionStatus({ ...base, subscriptionCount: 2 });
  assert.equal(status.text, 'connected');
  assert.equal(restartRequired, false);
});
