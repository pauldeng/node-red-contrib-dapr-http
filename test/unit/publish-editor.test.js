'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const editorPath = path.resolve(__dirname, '../../nodes/dapr-publish.html');

test('publish editor registers its dialog, fields, and help', () => {
  const html = fs.readFileSync(editorPath, 'utf8');
  assert.match(html, /registerType\('dapr-publish'/);
  assert.match(html, /data-template-name="dapr-publish"/);
  assert.match(html, /data-help-name="dapr-publish"/);
  for (const field of ['connection', 'pubsubName', 'topic', 'contentType', 'metadata']) {
    assert.match(html, new RegExp(`node-input-${field}`));
  }
});

test('publish help explicitly documents metadata merge precedence and message preservation', () => {
  const html = fs.readFileSync(editorPath, 'utf8');
  assert.match(html, /shallow merge/i);
  assert.match(html, /message keys win/i);
  assert.match(html, /scalar values/i);
  assert.match(html, /original message/i);
  assert.match(html, /payload.*required/i);
  assert.match(html, /undefined.*rejected/i);
  assert.match(html, /application\/cloudevents\+json/);
  assert.match(html, /rawPayload/);
});
