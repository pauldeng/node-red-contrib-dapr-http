'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  prepareConfigurationGet,
  prepareConfigurationSubscribe,
} = require('../../lib/configuration-messages');
const { DaprError, ErrorCodes } = require('../../lib/errors');

const baseConfig = {
  storeName: 'orders-config',
  keys: 'featureFlag\nmaxRetries',
  metadata: '{}',
};

function invalidAssert(fn) {
  assert.throws(fn, (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE);
}

// --- prepareConfigurationGet ---

test('prepareConfigurationGet parses newline-separated keys from config', () => {
  const result = prepareConfigurationGet(baseConfig, {});
  assert.equal(result.storeName, 'orders-config');
  assert.deepEqual(result.keys, ['featureFlag', 'maxRetries']);
});

test('prepareConfigurationGet trims and drops blank lines from configured keys', () => {
  const result = prepareConfigurationGet({ ...baseConfig, keys: '  a  \n\nb\n' }, {});
  assert.deepEqual(result.keys, ['a', 'b']);
});

test('an empty configured keys field means "all keys"', () => {
  const result = prepareConfigurationGet({ ...baseConfig, keys: '' }, {});
  assert.deepEqual(result.keys, []);
});

test('msg.dapr.keys (an array) overrides the configured keys', () => {
  const result = prepareConfigurationGet(baseConfig, { dapr: { keys: ['x', 'y'] } });
  assert.deepEqual(result.keys, ['x', 'y']);
});

test('msg.dapr.storeName overrides the configured storeName', () => {
  const result = prepareConfigurationGet(baseConfig, { dapr: { storeName: 'other' } });
  assert.equal(result.storeName, 'other');
});

test('prepareConfigurationGet requires a non-empty storeName', () => {
  invalidAssert(() => prepareConfigurationGet({ ...baseConfig, storeName: '' }, {}));
});

test('prepareConfigurationGet rejects a non-array, non-string msg.dapr.keys', () => {
  invalidAssert(() => prepareConfigurationGet(baseConfig, { dapr: { keys: 42 } }));
});

test('prepareConfigurationGet rejects a non-string entry in an array key list', () => {
  invalidAssert(() => prepareConfigurationGet(baseConfig, { dapr: { keys: ['a', 42] } }));
});

test('configuration keys preserve significant surrounding whitespace when given as an array', () => {
  const result = prepareConfigurationGet(baseConfig, { dapr: { keys: [' key '] } });
  assert.deepEqual(result.keys, [' key ']);
});

test('prepareConfigurationGet merges configured and message metadata', () => {
  const config = { ...baseConfig, metadata: JSON.stringify({ a: '1' }) };
  const result = prepareConfigurationGet(config, { dapr: { metadata: { b: '2' } } });
  assert.deepEqual(result.metadata, { a: '1', b: '2' });
});

test('prepareConfigurationGet rejects a non-object msg.dapr', () => {
  invalidAssert(() => prepareConfigurationGet(baseConfig, { dapr: 'x' }));
});

// --- prepareConfigurationSubscribe ---

test('prepareConfigurationSubscribe parses newline-separated keys from config', () => {
  const result = prepareConfigurationSubscribe(baseConfig);
  assert.equal(result.storeName, 'orders-config');
  assert.deepEqual(result.keys, ['featureFlag', 'maxRetries']);
  assert.deepEqual(result.metadata, {});
});

test('prepareConfigurationSubscribe requires a non-empty storeName', () => {
  invalidAssert(() => prepareConfigurationSubscribe({ ...baseConfig, storeName: '' }));
});

test('prepareConfigurationSubscribe requires at least one key to watch', () => {
  invalidAssert(() => prepareConfigurationSubscribe({ ...baseConfig, keys: '' }));
  invalidAssert(() => prepareConfigurationSubscribe({ ...baseConfig, keys: '\n\n' }));
});

test('prepareConfigurationSubscribe resolves configured metadata', () => {
  const result = prepareConfigurationSubscribe({
    ...baseConfig,
    metadata: JSON.stringify({ a: '1' }),
  });
  assert.deepEqual(result.metadata, { a: '1' });
});
