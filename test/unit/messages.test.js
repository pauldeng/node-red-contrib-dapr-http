'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { preparePublish } = require('../../lib/messages');
const { DaprError, ErrorCodes } = require('../../lib/errors');

const baseConfig = {
  pubsubName: 'orders-pubsub',
  topic: 'orders',
  contentType: '',
  metadata: '{}',
};

test('publish payload inference is deterministic for every supported type', () => {
  const binary = Buffer.from([0, 1, 2]);
  const cases = [
    [binary, 'application/octet-stream'],
    ['hello', 'text/plain'],
    ['', 'text/plain'],
    [{ orderId: 1 }, 'application/json'],
    [[1, 2], 'application/json'],
    [42, 'application/json'],
    [0, 'application/json'],
    [true, 'application/json'],
    [false, 'application/json'],
    [null, 'application/json'],
  ];

  for (const [payload, contentType] of cases) {
    const result = preparePublish(baseConfig, { payload });
    assert.equal(result.data, payload);
    assert.equal(result.options.contentType, contentType, `wrong type for ${String(payload)}`);
  }
});

test('msg.dapr overrides destination and content type without mutating the message', () => {
  const msg = {
    payload: { specversion: '1.0', id: '1', source: 'checkout', type: 'order', data: {} },
    marker: 'preserve-me',
    dapr: {
      pubsubName: 'override-pubsub',
      topic: 'priority-orders',
      contentType: 'application/cloudevents+json',
    },
  };
  const before = structuredClone(msg);

  const result = preparePublish(baseConfig, msg);

  assert.equal(result.pubsubName, 'override-pubsub');
  assert.equal(result.topic, 'priority-orders');
  assert.equal(result.options.contentType, 'application/cloudevents+json');
  assert.deepEqual(msg, before);
});

test('message metadata merges over configured metadata without mutating either source', () => {
  const config = {
    ...baseConfig,
    metadata: JSON.stringify({ rawPayload: 'false', ttlInSeconds: '60', partitionKey: 'default' }),
  };
  const messageMetadata = { ttlInSeconds: '10', partitionKey: 'order-42' };
  const msg = { payload: 'hello', dapr: { metadata: messageMetadata } };

  const result = preparePublish(config, msg);

  assert.deepEqual(result.options.metadata, {
    rawPayload: 'false',
    ttlInSeconds: '10',
    partitionKey: 'order-42',
  });
  assert.deepEqual(messageMetadata, { ttlInSeconds: '10', partitionKey: 'order-42' });
  assert.equal(
    config.metadata,
    JSON.stringify({ rawPayload: 'false', ttlInSeconds: '60', partitionKey: 'default' })
  );
});

test('metadata may be supplied as an object by internal callers', () => {
  const result = preparePublish(
    { ...baseConfig, metadata: { rawPayload: 'true' } },
    { payload: 'x' }
  );
  assert.deepEqual(result.options.metadata, { rawPayload: 'true' });
});

test('metadata scalar values are normalized to strings and nested values are rejected', () => {
  const result = preparePublish(
    { ...baseConfig, metadata: { ttlInSeconds: 60, rawPayload: false } },
    { payload: 'x', dapr: { metadata: { ttlInSeconds: 10 } } }
  );
  assert.deepEqual(result.options.metadata, { ttlInSeconds: '10', rawPayload: 'false' });

  assert.throws(
    () => preparePublish(baseConfig, { payload: 'x', dapr: { metadata: { nested: {} } } }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});

test('invalid destination, metadata, or payload fails with INVALID_MESSAGE', () => {
  const invalid = [
    [{ ...baseConfig, pubsubName: '' }, { payload: 'x' }],
    [{ ...baseConfig, topic: '' }, { payload: 'x' }],
    [{ ...baseConfig, metadata: '[]' }, { payload: 'x' }],
    [{ ...baseConfig, metadata: '{bad json' }, { payload: 'x' }],
    [baseConfig, { payload: 'x', dapr: { metadata: [] } }],
    [baseConfig, {}],
    [baseConfig, { payload: 1n }],
  ];

  for (const [config, msg] of invalid) {
    assert.throws(
      () => preparePublish(config, msg),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

test('a blank message override is rejected rather than silently falling back to config', () => {
  assert.throws(
    () => preparePublish(baseConfig, { payload: 'x', dapr: { topic: '' } }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});
