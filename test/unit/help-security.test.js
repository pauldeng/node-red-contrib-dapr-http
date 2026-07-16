'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const readNode = (name) =>
  fs.readFileSync(path.resolve(__dirname, `../../nodes/${name}.html`), 'utf8');

test('connection and service help explain the mTLS requirement for caller authorization', () => {
  for (const name of ['dapr-connection', 'dapr-service']) {
    const html = readNode(name);
    assert.match(html, /mTLS/);
    assert.match(html, /Sentry/);
    assert.match(html, /does not\s+configure|not configured/);
    assert.match(html, /network-level controls/);
  }

  assert.match(readNode('dapr-service'), /callerAppId[\s\S]*only when[\s\S]*mTLS/);
});
