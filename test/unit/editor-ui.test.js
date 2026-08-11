'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const readNode = (name) =>
  fs.readFileSync(path.resolve(__dirname, `../../nodes/${name}.html`), 'utf8');

const inputById = (html, id) => html.match(new RegExp(`<input(?=[^>]*id="${id}")[^>]*>`))[0];

// Dialog LAYOUT is deliberately not asserted here. Matching CSS literals
// (`170px`, `calc(100% - 60px)`, `margin: 6px 0`) only proves the stylesheet
// contains a string — it cannot show whether anything is clipped, and it fails
// on any cosmetic tweak. The regression those assertions were written for
// (content measuring wider than its tray wrapper, which clips with no
// scrollbar) is asserted properly in the e2e tier: assertNoHorizontalOverflow
// measures scrollWidth against the wrapper in a real browser, for every node,
// across three viewports and both themes. What stays below is behavior the
// runtime depends on — field bounds that must match lib/options.js, and the JS
// wiring of the bind warning.

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

  // The ranges shown to an operator must be the ranges actually enforced (the
  // min/max above, and lib/options.js's own bounds) — not the exact markup that
  // displays them.
  assert.match(html, /MB \(1-64\)/);
  assert.match(html, /seconds \(1-300\)/);
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

test('state TTL editor uses a non-negative integer input', () => {
  const html = readNode('dapr-state');
  const input = inputById(html, 'node-input-ttlSeconds');

  assert.match(input, /type="number"/);
  assert.match(input, /min="0"/);
  assert.match(input, /step="1"/);
});

test('credential inputs opt out of browser password autofill', () => {
  const html = readNode('dapr-connection');

  // Without an autocomplete token, a browser password manager treats a Dapr API
  // token like a site login: it offers to save it, and can autofill a saved
  // password into the field. "off" is valid HTML but most browsers ignore it for
  // password fields (MDN), so it does not stop either behaviour; "new-password" is
  // the token they honour for "a secret is being set, do not fill an existing
  // credential". html-validate's autocomplete-password rule also flags "off",
  // which is that tool's preference rather than an HTML-validity error.
  for (const id of ['daprApiToken', 'appApiToken']) {
    const input = inputById(html, `node-config-input-${id}`);
    assert.match(input, /type="password"/);
    assert.match(input, /autocomplete="new-password"/, `${id} must opt out of autofill`);
  }
  assert.doesNotMatch(html, /autocomplete="(off|current-password)"/);
});

test('the connection editor wires a live non-loopback bind warning', () => {
  const html = readNode('dapr-connection');

  // The warning element exists, starts hidden, and is toggled from the value —
  // docs/security.md promises the editor warns, so this must not silently
  // become a static paragraph again.
  assert.match(html, /id="dapr-bind-warning"[\s\S]*?style="display: none"/);
  assert.match(html, /Not loopback/);
  assert.match(html, /bindInput\.on\('input change', refresh\)/);
  assert.match(html, /warning\.toggle\(!isLoopback\(bindInput\.val\(\)\)\)/);
  // Blank (the 127.0.0.1 default) and every loopback form must not warn.
  assert.match(html, /v === ''[\s\S]*?v === 'localhost'[\s\S]*?\/\^127\\\./);
});
