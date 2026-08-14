'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { prepareSecretGet, resolveSecretProperty } = require('../../lib/secret-messages');
const { DaprError, ErrorCodes } = require('../../lib/errors');
const { normalisePropertyExpression } = require('@node-red/util').util;

const resolveProperty = (value) => resolveSecretProperty(value, normalisePropertyExpression);

const baseConfig = {
  storeName: 'vault',
  key: 'apiKey',
  metadata: '{}',
};

function invalidAssert(fn) {
  assert.throws(fn, (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE);
}

test('prepareSecretGet resolves the configured storeName/key', () => {
  const result = prepareSecretGet(baseConfig, {});
  assert.equal(result.storeName, 'vault');
  assert.equal(result.key, 'apiKey');
  assert.deepEqual(result.metadata, {});
});

test('msg.dapr.storeName overrides the configured storeName', () => {
  const result = prepareSecretGet(baseConfig, { dapr: { storeName: 'other' } });
  assert.equal(result.storeName, 'other');
});

test('msg.dapr.key overrides the configured key', () => {
  const result = prepareSecretGet(baseConfig, { dapr: { key: 'otherKey' } });
  assert.equal(result.key, 'otherKey');
});

test('prepareSecretGet requires a non-empty storeName', () => {
  invalidAssert(() => prepareSecretGet({ ...baseConfig, storeName: '' }, {}));
});

test('prepareSecretGet requires a non-empty key', () => {
  invalidAssert(() => prepareSecretGet({ ...baseConfig, key: '' }, {}));
});

test('prepareSecretGet merges configured and message metadata', () => {
  const config = { ...baseConfig, metadata: JSON.stringify({ a: '1' }) };
  const result = prepareSecretGet(config, { dapr: { metadata: { b: '2' } } });
  assert.deepEqual(result.metadata, { a: '1', b: '2' });
});

test('prepareSecretGet rejects a non-object msg.dapr', () => {
  invalidAssert(() => prepareSecretGet(baseConfig, { dapr: 'x' }));
});

test('prepareSecretGet rejects non-scalar metadata values', () => {
  invalidAssert(() =>
    prepareSecretGet(baseConfig, { dapr: { metadata: { a: { nested: true } } } })
  );
});

// --- resolveSecretProperty ---

test('resolveSecretProperty defaults to "payload" for a blank value', () => {
  for (const blank of [undefined, null, '', '   ']) {
    assert.equal(resolveProperty(blank), 'payload');
  }
});

test('resolveSecretProperty accepts a plain or nested static path, with or without a "msg." prefix', () => {
  assert.equal(resolveProperty('secret'), 'secret');
  assert.equal(resolveProperty('msg.secret'), 'msg.secret');
  assert.equal(resolveProperty('secret.nested'), 'secret.nested');
});

test('resolveSecretProperty rejects msg.dapr and any nested path under it', () => {
  invalidAssert(() => resolveProperty('dapr'));
  invalidAssert(() => resolveProperty('msg.dapr'));
  invalidAssert(() => resolveProperty('dapr.secret'));
});

test('resolveSecretProperty rejects __proto__ anywhere in the path', () => {
  invalidAssert(() => resolveProperty('__proto__'));
  invalidAssert(() => resolveProperty('secret.__proto__.polluted'));
});

test('resolveSecretProperty rejects a dynamic (message-computed) path segment', () => {
  invalidAssert(() => resolveProperty('secret[msg.topic]'));
});

test('resolveSecretProperty rejects a malformed property expression', () => {
  invalidAssert(() => resolveProperty('..'));
  invalidAssert(() => resolveProperty('secret.'));
});
