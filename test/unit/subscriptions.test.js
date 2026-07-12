'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildSubscription,
  discoveryEntry,
  fingerprint,
  parseDelivery,
} = require('../../lib/subscriptions');
const { DaprError, ErrorCodes } = require('../../lib/errors');

test('buildSubscription derives a stable route from the node id and normalizes fields', () => {
  const sub = buildSubscription({
    nodeId: 'abc123',
    pubsubName: 'orders-pubsub',
    topic: 'orders',
    ackMode: 'manual',
    deadLetterTopic: 'orders-dlq',
    rawPayload: true,
    metadata: { ttlInSeconds: 30 },
  });
  assert.equal(sub.route, '/node-red-dapr/subscriptions/abc123');
  assert.equal(sub.pubsubName, 'orders-pubsub');
  assert.equal(sub.topic, 'orders');
  assert.equal(sub.ackMode, 'manual');
  assert.equal(sub.deadLetterTopic, 'orders-dlq');
  assert.equal(sub.rawPayload, true);
  assert.deepEqual(sub.metadata, { ttlInSeconds: '30' }); // scalar coerced to string
});

test('buildSubscription defaults ackMode to auto and rawPayload to false', () => {
  const sub = buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't' });
  assert.equal(sub.ackMode, 'auto');
  assert.equal(sub.rawPayload, false);
  assert.equal(sub.deadLetterTopic, undefined);
  assert.deepEqual(sub.metadata, {});
});

test('buildSubscription rejects a missing pubsub or topic', () => {
  assert.throws(
    () => buildSubscription({ nodeId: 'n', pubsubName: '', topic: 't' }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
  assert.throws(
    () => buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: '  ' }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('discoveryEntry produces the Dapr /dapr/subscribe shape, folding rawPayload into metadata', () => {
  const sub = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rawPayload: true,
    deadLetterTopic: 'dlq',
    metadata: { ttlInSeconds: '30' },
  });
  assert.deepEqual(discoveryEntry(sub), {
    pubsubname: 'ps',
    topic: 't',
    route: '/node-red-dapr/subscriptions/n',
    deadLetterTopic: 'dlq',
    metadata: { ttlInSeconds: '30', rawPayload: 'true' },
  });
});

test('discoveryEntry omits empty metadata and dead-letter topic', () => {
  const sub = buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't' });
  assert.deepEqual(discoveryEntry(sub), {
    pubsubname: 'ps',
    topic: 't',
    route: '/node-red-dapr/subscriptions/n',
  });
});

test('fingerprint is stable across ordering and set-equal definitions, and changes with content', () => {
  const a = buildSubscription({ nodeId: 'a', pubsubName: 'ps', topic: 'orders' });
  const b = buildSubscription({ nodeId: 'b', pubsubName: 'ps', topic: 'payments' });
  assert.equal(fingerprint([a, b]), fingerprint([b, a]), 'order-independent');

  const bChanged = buildSubscription({ nodeId: 'b', pubsubName: 'ps', topic: 'refunds' });
  assert.notEqual(
    fingerprint([a, b]),
    fingerprint([a, bChanged]),
    'topic change alters fingerprint'
  );

  const aMeta = buildSubscription({
    nodeId: 'a',
    pubsubName: 'ps',
    topic: 'orders',
    metadata: { x: '1' },
  });
  assert.notEqual(fingerprint([a]), fingerprint([aMeta]), 'metadata alters fingerprint');
});

test('parseDelivery extracts CloudEvent data and preserves the envelope', () => {
  const envelope = {
    specversion: '1.0',
    id: 'evt-1',
    source: 'checkout',
    type: 'order',
    datacontenttype: 'application/json',
    data: { orderId: 7 },
  };
  const result = parseDelivery(Buffer.from(JSON.stringify(envelope)));
  assert.deepEqual(result.payload, { orderId: 7 });
  assert.deepEqual(result.cloudEvent, envelope);
});

test('parseDelivery decodes data_base64 to a Buffer (how raw deliveries arrive)', () => {
  // daprd wraps raw broker messages in a CloudEvent with the bytes in
  // data_base64, so this is exactly what a rawPayload subscription receives.
  const bytes = Buffer.from([0, 1, 2, 255]);
  const envelope = {
    specversion: '1.0',
    id: 'e',
    source: 's',
    type: 't',
    data_base64: bytes.toString('base64'),
  };
  const result = parseDelivery(Buffer.from(JSON.stringify(envelope)));
  assert.ok(Buffer.isBuffer(result.payload));
  assert.deepEqual(result.payload, bytes);
});

test('parseDelivery rejects structurally invalid CloudEvent envelopes', () => {
  const core = { specversion: '1.0', id: 'e', source: 's', type: 't' };
  const bad = [
    '{not json', // not JSON
    JSON.stringify(null), // not an object
    JSON.stringify(42), // not an object
    JSON.stringify({}), // missing required attributes
    JSON.stringify({ specversion: '1.0', id: 'e', source: 's' }), // missing type
    JSON.stringify({ ...core, data_base64: 'not base64!' }), // invalid base64
    JSON.stringify({ ...core, data_base64: 42 }), // non-string base64
    JSON.stringify({ ...core, data: { a: 1 }, data_base64: 'AAAA' }), // both data and data_base64
  ];
  for (const body of bad) {
    assert.throws(
      () => parseDelivery(Buffer.from(body)),
      (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_MESSAGE,
      `expected rejection for ${body}`
    );
  }
});
