'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  sanitizeResponseHeaders,
  validateContentType,
  HOP_BY_HOP,
} = require('../../lib/http-headers');

test('sanitizeResponseHeaders drops hop-by-hop and framing headers', () => {
  const out = sanitizeResponseHeaders({
    'content-type': 'application/json',
    'Content-Length': '999',
    'Transfer-Encoding': 'chunked',
    connection: 'keep-alive',
    'keep-alive': 'timeout=5',
    'x-app': 'ok',
  });
  assert.deepEqual(out, { 'content-type': 'application/json', 'x-app': 'ok' });
});

test('sanitizeResponseHeaders drops entries with invalid names or values', () => {
  const out = sanitizeResponseHeaders({
    'x good': 'v', // space in name → invalid
    'x-bad-value': 'line1\r\nInjected: 1', // CRLF injection → invalid
    'x-ok': 'fine',
  });
  assert.deepEqual(out, { 'x-ok': 'fine' });
});

test('sanitizeResponseHeaders coerces values to strings and tolerates empty input', () => {
  assert.deepEqual(sanitizeResponseHeaders({ 'x-num': 7 }), { 'x-num': '7' });
  assert.deepEqual(sanitizeResponseHeaders(undefined), {});
  assert.deepEqual(sanitizeResponseHeaders(null), {});
});

test('sanitizeResponseHeaders rejects non-object input instead of enumerating it', () => {
  // A string or array would otherwise index into numeric header names ('0','1').
  assert.deepEqual(sanitizeResponseHeaders('abc'), {});
  assert.deepEqual(sanitizeResponseHeaders(['a', 'b']), {});
  assert.deepEqual(sanitizeResponseHeaders(42), {});
});

test('validateContentType accepts a valid string and tolerates absence', () => {
  assert.equal(validateContentType('application/json'), 'application/json');
  assert.equal(validateContentType(undefined), undefined);
  assert.equal(validateContentType(null), undefined);
  assert.equal(validateContentType(''), undefined);
});

test('validateContentType rejects non-string values and illegal header values', () => {
  assert.throws(() => validateContentType(42), /string/i);
  assert.throws(() => validateContentType({ type: 1 }), /string/i);
  assert.throws(() => validateContentType('bad\r\nvalue'), /invalid|contentType/i);
});

test('HOP_BY_HOP covers the full hop-by-hop and framing header set', () => {
  for (const name of [
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade',
    'host',
    'content-length',
  ]) {
    assert.ok(HOP_BY_HOP.has(name), `${name} should be hop-by-hop`);
  }
});
