'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// The runtime tier drives a FAKE Dapr sidecar (test/helpers/fake-dapr.js) so it
// can produce what a real daprd cannot be commanded to produce on demand: a
// specific 4xx/5xx, a truncated body, a response that never ends (the only way
// to prove an in-flight abort). That speed and control costs something real —
// a fake can encode a belief about daprd that is simply wrong.
//
// This package's own history is the argument: real daprd 1.18.x answers a
// missing secret with 500 (not 204), a missing binding with 500 (not 404), and
// a missing secret STORE with 401 (not 404). Every one of those contradicted
// the obvious guess, and only a real sidecar surfaced it.
//
// So the rule this test enforces: every Dapr building block the fake simulates
// must ALSO be exercised against a real daprd container somewhere in the
// integration tier. Adding a new faked endpoint without a real-daprd
// counterpart fails here, forcing a deliberate choice rather than a silent
// assumption.

const root = path.resolve(__dirname, '../..');
const read = (p) => fs.readFileSync(path.join(root, p), 'utf8');
const listTests = (dir) =>
  fs.readdirSync(path.join(root, dir)).filter((name) => name.endsWith('.test.js'));

// Which real-daprd integration file proves each block. Declared rather than
// inferred: an integration test drives a block through the NODES, so the
// sidecar path never appears literally in its source.
const PROVEN_BY_REAL_DAPRD = {
  healthz: 'shutdown.test.js',
  publish: 'nats-pubsub.test.js',
  invoke: 'dapr-building-blocks.test.js',
  state: 'state.test.js',
  configuration: 'configuration.test.js',
  bindings: 'dapr-building-blocks.test.js',
  secrets: 'dapr-building-blocks.test.js',
  metadata: 'dapr-building-blocks.test.js',
  actors: 'actors.test.js',
};

// Paths the fake serves that are deliberately NOT real Dapr endpoints — used
// to prove this package rejects/ignores what it should. Nothing to verify
// against a real sidecar, because a real sidecar never calls them.
const SYNTHETIC_PATHS = new Set(['not']);

function blocksSimulatedByFake() {
  const blocks = new Set();
  for (const name of listTests('test/runtime')) {
    const source = read(path.join('test/runtime', name));
    for (const [, block] of source.matchAll(/\/v1\.0\/([a-z]+)/g)) {
      blocks.add(block);
    }
  }
  return blocks;
}

test('every Dapr block the fake sidecar simulates is also proven against a real daprd', () => {
  const simulated = [...blocksSimulatedByFake()].filter((b) => !SYNTHETIC_PATHS.has(b));
  assert.ok(simulated.length > 0, 'expected the runtime tier to simulate at least one block');

  for (const block of simulated) {
    const file = PROVEN_BY_REAL_DAPRD[block];
    assert.ok(
      file,
      `the runtime tier fakes /v1.0/${block} but no real-daprd test is declared for it. ` +
        `Add one to test/integration/ and register it in PROVEN_BY_REAL_DAPRD, or add ` +
        `"${block}" to SYNTHETIC_PATHS with a reason if a real sidecar never serves it.`
    );
    const source = read(path.join('test/integration', file));
    assert.match(
      source,
      /startDaprd/,
      `${file} is declared as the real-daprd proof for /v1.0/${block} but never starts a real daprd container`
    );
  }
});

test('every declared real-daprd proof file exists and is wired into the integration gate', () => {
  const pkg = JSON.parse(read('package.json'));
  const gate = Object.entries(pkg.scripts)
    .filter(([name]) => name.startsWith('test:integration'))
    .map(([, script]) => script)
    .join(' ');
  const integrationFiles = new Set(listTests('test/integration'));

  for (const [block, file] of Object.entries(PROVEN_BY_REAL_DAPRD)) {
    assert.ok(integrationFiles.has(file), `declared proof for ${block} is missing: ${file}`);
    // nats-*.test.js files are covered by a glob rather than being named.
    const covered = gate.includes(file) || (file.startsWith('nats-') && gate.includes('nats-*'));
    assert.ok(covered, `${file} proves /v1.0/${block} but no test:integration script runs it`);
  }
});
