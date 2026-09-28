'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { DAPRD_IMAGE: PINNED_DAPRD_IMAGE } = require('../helpers/integration');

const read = (name) => fs.readFileSync(path.resolve(__dirname, '../..', name), 'utf8');

// Derived from the pinned image, never hard-coded: a version literal in an
// assertion is a snapshot that goes stale the moment the pin moves, and then
// the "docs name the pinned runtime" guarantee quietly stops being checked.
// Bump test/helpers/integration.js and this follows automatically — while a
// doc that forgot to follow now fails.
const PINNED_DAPR_VERSION = PINNED_DAPRD_IMAGE.match(/:(\d+\.\d+\.\d+)@/)?.[1];

test('the pinned daprd version is discoverable from the integration helper', () => {
  assert.ok(
    PINNED_DAPR_VERSION,
    'could not read the pinned daprd version from test/helpers/integration.js — ' +
      'if DAPRD_IMAGE was reshaped, update this extraction rather than deleting the check'
  );
});

test('deployment examples use the exact pinned daprd image', () => {
  for (const file of ['docker-compose.yml', 'docs/deployment.md']) {
    assert.ok(read(file).includes(PINNED_DAPRD_IMAGE), `${file} has a stale daprd tag or digest`);
  }
});

test('operator docs warn that JetStream dead-letter topics stall on pinned Dapr', () => {
  const subscribeHelp = read('nodes/dapr-subscribe.html').split(
    'data-help-name="dapr-subscribe"'
  )[1];

  for (const text of [
    read('README.md'),
    read('docs/deployment.md'),
    read('docs/subscriptions.md'),
    // The examples quickstart is JetStream-based, so a beginner who later adds a
    // dead-letter topic hits this exact limitation — it has to warn too.
    read('examples/README.md'),
    subscribeHelp,
  ]) {
    // `\s+`, not a literal space: Prettier reflows prose, so any of these
    // phrases can land either side of a line break (which is exactly why the
    // Dapr-version pattern below was already written this way).
    assert.match(text, /NATS\s+JetStream/);
    assert.match(text, new RegExp(`Dapr\\s+${PINNED_DAPR_VERSION.replace(/\./g, '\\.')}`));
    assert.match(text, /deadLetterTopic/);
    assert.match(text, /do\s+not\s+use/i);
  }

  assert.match(
    read('docs/deployment.md'),
    /stream\s+must\s+exist\s+before\s+the\s+component\s+is\s+used/i
  );
});

test('secret guidance scopes redaction to this package and warns about daprd telemetry', () => {
  const secretHelp = read('nodes/dapr-secret-get.html').split(
    'data-help-name="dapr-secret-get"'
  )[1];

  for (const text of [read('README.md'), read('docs/security.md'), secretHelp]) {
    assert.match(text, /package-owned|this\s+node/i);
    assert.match(text, /daprd[\s\S]{0,240}(?:logs|traces)[\s\S]{0,160}(?:secret(?:'s)?\s+)?key/i);
    assert.match(text, /user-authored\s+node\s+name[\s\S]{0,160}(?:span|telemetry)/i);
  }
});

test('the secret example does not log the retrieved value and creates its development file privately', () => {
  const flow = JSON.parse(read('examples/secret-get.json'));
  const get = flow.find((node) => node.type === 'dapr-secret-get');
  const next = flow.find((node) => node.id === get.wires[0][0]);

  assert.equal(next.type, 'function');
  assert.doesNotMatch(next.func, /node\.(?:warn|error|log)|console\.|RED\.log/);
  assert.ok(
    next.wires[0].some((id) => flow.find((node) => node.id === id)?.type === 'debug'),
    "the example should debug only the function's redacted confirmation"
  );

  const examplesReadme = read('examples/README.md');
  assert.match(examplesReadme, /development-only|not\s+for\s+production/i);
  assert.match(examplesReadme, /umask\s+077/);
});

test('local broker examples keep unauthenticated Redis and NATS ports on loopback', () => {
  const examplesReadme = read('examples/README.md');
  const stateStoreExample = read('examples/redis-statestore-component.yaml');
  const compose = read('docker-compose.yml');

  assert.match(examplesReadme, /nats-server --addr 127\.0\.0\.1 -js/);
  assert.match(examplesReadme, /-p 127\.0\.0\.1:6379:6379 redis:8\.10-alpine/);
  assert.match(stateStoreExample, /-p 127\.0\.0\.1:6379:6379 redis:8\.10-alpine/);
  assert.match(compose, /'127\.0\.0\.1:6379:6379'/);
  assert.match(compose, /'127\.0\.0\.1:4222:4222'/);
});

test('local sidecars and integration fixtures explicitly restrict their listening interfaces', () => {
  for (const file of [
    'docker-compose.yml',
    'docs/deployment.md',
    'examples/README.md',
    'test/helpers/integration.js',
  ]) {
    const source = read(file);
    assert.match(source, /--dapr-listen-addresses[= ]127\.0\.0\.1/, file);
    assert.match(source, /--dapr-internal-grpc-listen-address[= ]127\.0\.0\.1/, file);
    assert.match(source, /--enable-metrics=false/, file);
  }
  for (const [file, port] of [
    ['test/helpers/integration.js', 6379],
    ['test/helpers/nats.js', 4222],
    ['test/helpers/otel-collector.js', 4318],
  ]) {
    assert.ok(read(file).includes('`127.0.0.1:${port}:' + port + '`'), file);
  }
  assert.match(read('docs/deployment.md'), /'127\.0\.0\.1:1880:1880'/);
  for (const file of ['docs/deployment.md', 'examples/README.md']) {
    assert.match(read(file), /npx node-red -D uiHost=127\.0\.0\.1/, file);
  }
});
