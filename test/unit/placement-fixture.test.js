'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const docker = require('../helpers/docker');
const http = require('../helpers/http');

// Load the fixture with controlled CLI/health responses; no Docker needed.
function fixture(t, runFails = false) {
  const calls = [];
  t.mock.method(docker, 'ensureImage', async () => {});
  t.mock.method(docker, 'execFileP', async (command, args) => {
    assert.equal(command, 'docker');
    calls.push(args);
    if (args[0] === 'run' && runFails) throw new Error('port publication failed');
    return args[0] === 'logs' ? 'startup diagnostics' : '';
  });
  t.mock.method(http, 'httpRequest', async () => ({ status: 200 }));
  const modulePath = require.resolve('../helpers/integration');
  const previous = require.cache[modulePath];
  delete require.cache[modulePath];
  t.after(() => {
    delete require.cache[modulePath];
    if (previous) require.cache[modulePath] = previous;
  });
  return { startPlacement: require(modulePath).startPlacement, calls };
}

test('Placement publishes both ports only on loopback', async (t) => {
  const { startPlacement, calls } = fixture(t);
  const placement = await startPlacement();
  t.after(() => placement.stop());
  const run = calls.find((args) => args[0] === 'run');
  const bindings = run.filter((_arg, index) => run[index - 1] === '-p');
  assert.equal(bindings.length, 2);
  for (const binding of bindings) assert.match(binding, /^127\.0\.0\.1:\d+:\d+$/);
});

test('Placement cleans up a container even when docker run rejects', async (t) => {
  const { startPlacement, calls } = fixture(t, true);
  await assert.rejects(startPlacement(), /port publication failed[\s\S]*startup diagnostics/);
  const run = calls.find((args) => args[0] === 'run');
  const name = run[run.indexOf('--name') + 1];
  assert.deepEqual(calls.slice(1), [
    ['logs', name],
    ['stop', '--time', '5', name],
    ['rm', '-f', name],
  ]);
});
