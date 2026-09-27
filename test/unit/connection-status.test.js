'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { connectionStatus, actorRuntimeWarningText } = require('../../lib/connection-status');

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

// ---- Actor fingerprint: removing the last advertised type is a change ----

test('only a connection that never advertised actors remains connected without actors', () => {
  // No actors ever advertised is different from removing the last type:
  // daprd retains its previous placement registration until restarted.
  for (const servedActorFingerprint of [null, 'anything']) {
    const { status, restartRequired, actorRestartRequired } = connectionStatus({
      ...base,
      servedActorFingerprint,
      desiredActorFingerprint: null,
    });
    assert.equal(
      status.text,
      servedActorFingerprint === null ? 'connected' : 'restart sidecar: actor types changed'
    );
    assert.equal(restartRequired, false);
    assert.equal(actorRestartRequired, servedActorFingerprint !== null);
  }
});

test('actors desired but never served is not yet a restart warning (first activation)', () => {
  const { status, actorRestartRequired } = connectionStatus({
    ...base,
    servedActorFingerprint: null,
    desiredActorFingerprint: 'fp1',
  });
  assert.equal(status.text, 'connected');
  assert.equal(actorRestartRequired, false);
});

test('a served actor fingerprint differing from the desired one requires a restart', () => {
  const { status, restartRequired, actorRestartRequired } = connectionStatus({
    ...base,
    servedActorFingerprint: 'stale',
    desiredActorFingerprint: 'fresh',
  });
  assert.equal(status.text, 'restart sidecar: actor types changed');
  assert.equal(restartRequired, false);
  assert.equal(actorRestartRequired, true);
});

test('a matching served actor fingerprint is connected', () => {
  const { status, actorRestartRequired } = connectionStatus({
    ...base,
    servedActorFingerprint: 'fp1',
    desiredActorFingerprint: 'fp1',
  });
  assert.equal(status.text, 'connected');
  assert.equal(actorRestartRequired, false);
});

test('a stale subscription set outranks an actor-fingerprint drift', () => {
  const { status, restartRequired, actorRestartRequired } = connectionStatus({
    ...base,
    servedFingerprint: 'stale',
    desiredFingerprint: 'fresh',
    servedActorFingerprint: 'stale-actor',
    desiredActorFingerprint: 'fresh-actor',
  });
  assert.equal(status.text, 'restart sidecar: subscriptions changed');
  assert.equal(restartRequired, true);
  assert.equal(actorRestartRequired, false);
});

test('an unhealthy sidecar outranks an actor-fingerprint drift too', () => {
  const { status, actorRestartRequired } = connectionStatus({
    ...base,
    healthy: false,
    servedActorFingerprint: 'stale-actor',
    desiredActorFingerprint: 'fresh-actor',
  });
  assert.equal(status.text, 'sidecar unavailable');
  assert.equal(actorRestartRequired, false);
});

// ---- milestone 6: observability -- actor readiness warning -----------------

test('actorRuntimeWarningText: healthy hostReady + connected placement clears the warning', () => {
  assert.equal(
    actorRuntimeWarningText({ hostReady: true, placement: 'placement: connected' }),
    null
  );
});

test('actorRuntimeWarningText: hostReady false is reported as host not ready', () => {
  assert.equal(
    actorRuntimeWarningText({ hostReady: false, placement: 'placement: connected' }),
    'actors: host not ready'
  );
});

test('actorRuntimeWarningText: a placement string not reporting connected is placement disconnected', () => {
  assert.equal(
    actorRuntimeWarningText({ hostReady: true, placement: 'placement: disconnected' }),
    'actors: placement disconnected'
  );
});

test('actorRuntimeWarningText: missing/undefined metadata is unknown, never healthy', () => {
  assert.equal(actorRuntimeWarningText(null), 'actors: runtime status unknown');
  assert.equal(actorRuntimeWarningText(undefined), 'actors: runtime status unknown');
});

test('actorRuntimeWarningText: an unrecognized hostReady/placement shape is unknown, not healthy', () => {
  // A non-boolean hostReady never confirms readiness, even alongside a
  // connected placement string.
  assert.equal(
    actorRuntimeWarningText({ hostReady: 'yes', placement: 'placement: connected' }),
    'actors: runtime status unknown'
  );
  // A non-string / absent placement cannot establish its connection state.
  assert.equal(
    actorRuntimeWarningText({ hostReady: true, placement: 42 }),
    'actors: runtime status unknown'
  );
  assert.equal(
    actorRuntimeWarningText({ hostReady: true, placement: undefined }),
    'actors: runtime status unknown'
  );
});

test('connectionStatus shows no actor warning when no actor method is registered, even with bad actorRuntime', () => {
  const { status } = connectionStatus({
    ...base,
    actorMethodsRegistered: false,
    actorRuntime: { hostReady: false },
  });
  assert.equal(status.text, 'connected');
});

test('connectionStatus surfaces the actor warning once actor methods are registered', () => {
  const { status, restartRequired, actorRestartRequired } = connectionStatus({
    ...base,
    actorMethodsRegistered: true,
    actorRuntime: { hostReady: false },
  });
  assert.deepEqual(status, { fill: 'yellow', shape: 'ring', text: 'actors: host not ready' });
  assert.equal(restartRequired, false);
  assert.equal(actorRestartRequired, false);
});

test('connectionStatus clears the actor warning once actorRuntime reports healthy', () => {
  const { status } = connectionStatus({
    ...base,
    actorMethodsRegistered: true,
    actorRuntime: { hostReady: true, placement: 'placement: connected' },
  });
  assert.equal(status.text, 'connected');
});

test('precedence: sidecar unavailable outranks an actor warning', () => {
  const { status } = connectionStatus({
    ...base,
    healthy: false,
    actorMethodsRegistered: true,
    actorRuntime: { hostReady: false },
  });
  assert.equal(status.text, 'sidecar unavailable');
});

test('precedence: a stale subscription restart outranks an actor warning', () => {
  const { status } = connectionStatus({
    ...base,
    servedFingerprint: 'stale',
    desiredFingerprint: 'fresh',
    actorMethodsRegistered: true,
    actorRuntime: { hostReady: false },
  });
  assert.equal(status.text, 'restart sidecar: subscriptions changed');
});

test('precedence: an actor-type restart outranks an actor readiness warning', () => {
  const { status } = connectionStatus({
    ...base,
    servedActorFingerprint: 'stale-actor',
    desiredActorFingerprint: 'fresh-actor',
    actorMethodsRegistered: true,
    actorRuntime: { hostReady: false },
  });
  assert.equal(status.text, 'restart sidecar: actor types changed');
});

test('unrecognized placement strings cannot be mistaken for healthy or disconnected', () => {
  for (const placement of ['', 'unknown', 'not placement: connected', 'placement: connectedness']) {
    assert.equal(
      actorRuntimeWarningText({ hostReady: true, placement }),
      'actors: runtime status unknown'
    );
  }
});
