'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const read = (name) => fs.readFileSync(path.resolve(__dirname, '../..', name), 'utf8');

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
    assert.match(text, /Dapr\s+1\.18\.1/);
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
