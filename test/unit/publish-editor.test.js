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

test('publish editor leaves optional metadata absent by default', () => {
  const html = fs.readFileSync(editorPath, 'utf8');

  assert.match(html, /metadata:\s*\{\s*value:\s*''/);
  assert.match(html, /node-input-metadata'\)\.val\(\) === '\{\}'/);
  assert.match(html, /placeholder='\{"ttlInSeconds":"60"\}'/);
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
  assert.match(html, /iana\.org\/assignments\/media-types\/media-types\.xhtml/);
  assert.match(html, /v1-18\.docs\.dapr\.io\/reference\/api\/pubsub_api/);
});

test('publish editor exposes a bulk publish toggle, off by default', () => {
  const html = fs.readFileSync(editorPath, 'utf8');
  assert.match(html, /bulkEnabled:\s*\{\s*value:\s*false\s*\}/);
  assert.match(html, /node-input-bulkEnabled/);
});

test('publish help documents the bulk entry contract, its content-type restrictions, and bulkResult', () => {
  const html = fs.readFileSync(editorPath, 'utf8');
  assert.match(html, /entryId/);
  assert.match(html, /BULK_PUBLISH_PARTIAL/);
  assert.match(html, /bulkResult/);
  assert.match(html, /msg\.dapr\.bulk\b/);
  assert.match(html, /1000 entries/);
  assert.match(html, /entire batch/i);
});
