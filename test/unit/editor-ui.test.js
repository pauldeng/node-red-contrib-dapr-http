'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const readNode = (name) =>
  fs.readFileSync(path.resolve(__dirname, `../../nodes/${name}.html`), 'utf8');

const inputById = (html, id) => html.match(new RegExp(`<input(?=[^>]*id="${id}")[^>]*>`))[0];

test('connection editor gives long labels room and lets tips span the dialog', () => {
  const html = readNode('dapr-connection');

  assert.match(html, /class="dapr-config-form"/);
  assert.match(html, /class="form-tips dapr-form-tip"/g);
  assert.match(html, /--dapr-label-width:\s*170px/);
  assert.match(html, /\.dapr-config-form[\s\S]*width:\s*calc\(100%\s*-\s*60px\)/);
  assert.match(html, /addClass\('dapr-connection-tray'\)/);
  assert.match(html, /\.red-ui-tray-body\.dapr-connection-tray/);
  assert.match(html, /\.red-ui-tray-body\.dapr-connection-tray[\s\S]*min-width:\s*0/);
  assert.match(html, /width:\s*calc\(100%\s*-\s*var\(--dapr-label-width\)\s*-\s*10px\)/);
  assert.match(html, /\.dapr-form-tip[\s\S]*width:\s*auto/);
  assert.match(html, /\.dapr-form-tip[\s\S]*max-width:\s*none/);
  assert.match(html, /\.dapr-form-tip[\s\S]*overflow-wrap:\s*anywhere/);
  assert.match(html, /\.dapr-form-tip[\s\S]*margin:\s*6px 0/);
});

test('connection numeric fields use native number inputs and unit text after the field', () => {
  const html = readNode('dapr-connection');

  for (const [id, min, max] of [
    ['node-config-input-daprPort', '1', '65535'],
    ['node-config-input-appPort', '1', '65535'],
    ['node-config-input-bodyLimitMb', '1', '64'],
    ['node-config-input-requestTimeoutSec', '1', '300'],
  ]) {
    const input = inputById(html, id);
    assert.match(input, /type="number"/);
    assert.match(input, new RegExp(`min="${min}"`));
    assert.match(input, new RegExp(`max="${max}"`));
    assert.match(input, /step="1"/);
  }

  assert.match(
    html,
    /id="node-config-input-bodyLimitMb"[\s\S]*<span class="dapr-unit-label">MB \(1-64\)<\/span>/
  );
  assert.match(
    html,
    /id="node-config-input-requestTimeoutSec"[\s\S]*<span class="dapr-unit-label">seconds \(1-300\)<\/span>/
  );
});

test('invoke timeout editor is numeric and hints the default value only', () => {
  const html = readNode('dapr-invoke');
  const input = inputById(html, 'node-input-timeoutSec');

  assert.match(input, /type="number"/);
  assert.match(input, /placeholder="30"/);
  assert.doesNotMatch(html, /placeholder="connection default"/);
});

test('response status editor uses a native number input', () => {
  const html = readNode('dapr-response');
  const input = inputById(html, 'node-input-statusCode');

  assert.match(input, /type="number"/);
  assert.match(input, /min="200"/);
  assert.match(input, /max="599"/);
  assert.match(input, /step="1"/);
});
