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
