'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createActorRuntimeMonitor } = require('../../lib/actor-runtime-monitor');

test('metadata has one fetch in flight and no fetch without actor registrations', async () => {
  const held = Promise.withResolvers();
  let calls = 0;
  let changes = 0;
  const monitor = createActorRuntimeMonitor({
    fetch: () => {
      calls++;
      return held.promise;
    },
    onChange: () => changes++,
  });
  await monitor.refresh();
  assert.equal(calls, 0);
  monitor.setScope('T');
  const pending = monitor.refresh();
  await monitor.refresh();
  assert.equal(calls, 1);
  held.resolve({ hostReady: true });
  await pending;
  assert.deepEqual(monitor.value, { hostReady: true });
  assert.equal(changes, 1);
  monitor.setScope('T');
  assert.deepEqual(monitor.value, { hostReady: true });
});

for (const change of ['remove/re-add', 'change type', 'unhealthy', 'close']) {
  test(`a late metadata result cannot restore stale readiness after ${change}`, async () => {
    const held = Promise.withResolvers();
    let signal;
    let changes = 0;
    const monitor = createActorRuntimeMonitor({
      fetch: (s) => {
        signal = s;
        return held.promise;
      },
      onChange: () => changes++,
    });
    monitor.setScope('T');
    const pending = monitor.refresh();
    if (change === 'remove/re-add') {
      monitor.setScope(null);
      monitor.setScope('T');
    } else if (change === 'change type') {
      monitor.setScope('U');
    } else if (change === 'unhealthy') {
      monitor.invalidate();
    } else {
      monitor.close();
    }
    assert.equal(signal.aborted, true);
    held.resolve({ hostReady: true });
    await pending;
    assert.equal(monitor.value, null);
    assert.equal(changes, 0);
    if (change === 'close') {
      await monitor.refresh();
      assert.equal(changes, 0);
    }
  });
}

test('metadata failure and absent fields clear previously healthy readings', async () => {
  let fail = false;
  const monitor = createActorRuntimeMonitor({
    fetch: async () => {
      if (fail) throw Error('offline');
      return { hostReady: true };
    },
    onChange: () => {},
  });
  monitor.setScope('T');
  await monitor.refresh();
  fail = true;
  await monitor.refresh();
  assert.equal(monitor.value, null);
  const missing = createActorRuntimeMonitor({ fetch: async () => undefined, onChange: () => {} });
  missing.setScope('T');
  await missing.refresh();
  assert.equal(missing.value, null);
});
