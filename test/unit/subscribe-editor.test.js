'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const read = (name) => fs.readFileSync(path.resolve(__dirname, '../..', name), 'utf8');

test('beginner CEL guidance matches ordinary dapr-publish envelopes', () => {
  const html = read('nodes/dapr-subscribe.html');
  const help = html.split('data-help-name="dapr-subscribe"')[1];
  const example = JSON.parse(read('examples/cel-routing.json'));
  const subscribe = example.find((node) => node.type === 'dapr-subscribe');
  const tab = example.find((node) => node.type === 'tab');
  const testing = read('docs/testing.md');

  assert.match(html, /placeholder: 'CEL match, e\.g\. event\.data\.kind == "order"'/);
  assert.match(help, /event\.data\.kind == "order"/);
  assert.match(help, /event\.type[\s\S]*custom\s+CloudEvent/i);
  assert.deepEqual(
    subscribe.rules.map((rule) => rule.match),
    ['event.data.kind == "order"', 'event.data.kind == "payment"']
  );
  assert.match(tab.info, /JSON payloads[\s\S]*"kind"/i);
  assert.doesNotMatch(testing, /CEL rules must match/);
  assert.match(testing, /ordinary[\s\S]*event\.data/i);
  assert.match(testing, /custom\s+CloudEvent[\s\S]*event\.type/i);
});

test('subscribe help links complex Dapr concepts to pinned reference docs', () => {
  const help = read('nodes/dapr-subscribe.html').split('data-help-name="dapr-subscribe"')[1];

  assert.doesNotMatch(help, /class="mermaid"/);
  assert.match(help, /v1-18\.docs\.dapr\.io\/reference\/api\/pubsub_api/);
  assert.match(
    help,
    /v1-18\.docs\.dapr\.io\/developing-applications\/building-blocks\/pubsub\/subscription-methods/
  );
  assert.match(
    help,
    /v1-18\.docs\.dapr\.io\/developing-applications\/building-blocks\/pubsub\/pubsub-bulk/
  );
  assert.match(help, /github\.com\/google\/cel-spec/);
});

test('subscribe helper text uses Node-RED theme colors', () => {
  const html = read('nodes/dapr-subscribe.html');

  assert.doesNotMatch(html, /color:\s*#888/i);
  assert.ok((html.match(/var\(--red-ui-secondary-text-color\)/g) || []).length >= 1);
});

// As in test/unit/editor-ui.test.js, the dialog's LAYOUT is asserted in the e2e
// tier by measuring real rendered geometry, not by matching CSS literals here.
test('subscribe editor stores blank metadata rather than an empty JSON object', () => {
  const html = read('nodes/dapr-subscribe.html');

  // Both halves of the same contract: the default is '' (an imported flow that
  // omits the key must validate), and a stored '{}' from an older flow is
  // blanked on open so the field does not show noise the runtime ignores.
  assert.match(html, /metadata:\s*\{\s*value:\s*''/);
  assert.match(html, /node-input-metadata'\)\.val\(\) === '\{\}'/);
  assert.match(html, /placeholder='\{"consumerID":"group-1"\}'/);
});
