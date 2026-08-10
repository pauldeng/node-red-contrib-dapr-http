'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { preparePublish, prepareBulkPublish } = require('../../lib/messages');
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
    // msg.dapr itself must be an object: a string or array would otherwise be
    // read with hasOwn/property access and silently contribute nothing, so a
    // flow that set it wrongly would publish to the configured destination
    // believing it had overridden it.
    [baseConfig, { payload: 'x', dapr: 'orders' }],
    [baseConfig, { payload: 'x', dapr: [] }],
    [baseConfig, { payload: 'x', dapr: 42 }],
    // A non-string contentType would stringify into the header as "42" or
    // "[object Object]" and be sent as-is.
    [baseConfig, { payload: 'x', dapr: { contentType: 42 } }],
    [baseConfig, { payload: 'x', dapr: { contentType: {} } }],
  ];

  for (const [config, msg] of invalid) {
    assert.throws(
      () => preparePublish(config, msg),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

test('a node with no metadata configured at all publishes with empty metadata', () => {
  // The editor omits the key entirely rather than storing '{}' on a
  // hand-authored or re-imported flow, so the unset path must not throw.
  const request = preparePublish({ pubsubName: 'ps', topic: 't' }, { payload: 'x' });
  assert.deepEqual(request.options.metadata, {});
});

test('a blank message override is rejected rather than silently falling back to config', () => {
  assert.throws(
    () => preparePublish(baseConfig, { payload: 'x', dapr: { topic: '' } }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});

test('preparePublish carries validated msg.dapr.headers through for trace propagation', () => {
  const traceparent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
  const request = preparePublish(baseConfig, {
    payload: { a: 1 },
    dapr: { headers: { traceparent } },
  });
  assert.deepEqual(request.headers, { traceparent });

  // No headers at all is the common case and must stay an empty object.
  assert.deepEqual(preparePublish(baseConfig, { payload: 'x' }).headers, {});
});

test('preparePublish rejects headers that are not a valid header object', () => {
  for (const headers of ['[1,2]', 42, { 'x bad': 'v' }, { 'x-a': 'a\r\nb: 1' }]) {
    assert.throws(
      () => preparePublish(baseConfig, { payload: 'x', dapr: { headers } }),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

test('an illegal contentType is rejected as an invalid message, not left for the socket', () => {
  // Reaching node:http with a CRLF or illegal-token value throws ERR_INVALID_CHAR
  // there, which the publish client can only report as a transport failure — a
  // fake sidecar outage for what is really a bad message.
  const illegal = ['bad\r\nvalue', 'application/json\r\nX-Injected: 1'];
  for (const contentType of illegal) {
    assert.throws(
      () => preparePublish(baseConfig, { payload: { a: 1 }, dapr: { contentType } }),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
    // A configured (not per-message) value is validated the same way.
    assert.throws(
      () => preparePublish({ ...baseConfig, contentType }, { payload: 'x' }),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }

  // A legitimate parameterized type still passes through untouched.
  assert.equal(
    preparePublish(baseConfig, {
      payload: { a: 1 },
      dapr: { contentType: 'application/json; charset=utf-8' },
    }).options.contentType,
    'application/json; charset=utf-8'
  );
});

// --- prepareBulkPublish ---

const assertInvalid = (fn) =>
  assert.throws(fn, (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE);

test('prepareBulkPublish shapes entries for the wire: entryId, event, contentType, metadata', () => {
  const request = prepareBulkPublish(baseConfig, {
    payload: [
      { entryId: 'e1', payload: { a: 1 } },
      { entryId: 'e2', payload: 'hello', contentType: 'text/plain', metadata: { x: 'y' } },
    ],
  });
  assert.equal(request.pubsubName, 'orders-pubsub');
  assert.equal(request.topic, 'orders');
  assert.deepEqual(request.entries, [
    { entryId: 'e1', event: { a: 1 }, contentType: 'application/json', metadata: {} },
    { entryId: 'e2', event: 'hello', contentType: 'text/plain', metadata: { x: 'y' } },
  ]);
});

test('prepareBulkPublish base64-encodes a Buffer payload for a binary content type', () => {
  const request = prepareBulkPublish(baseConfig, {
    payload: [
      { entryId: 'e1', payload: Buffer.from([0, 1, 2]), contentType: 'application/octet-stream' },
    ],
  });
  assert.equal(request.entries[0].event, Buffer.from([0, 1, 2]).toString('base64'));
});

test('a non-Buffer payload for a binary content type is rejected', () => {
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, {
      payload: [
        { entryId: 'e1', payload: 'not a buffer', contentType: 'application/octet-stream' },
      ],
    })
  );
});

test('a non-string payload for a text content type is rejected', () => {
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, {
      payload: [{ entryId: 'e1', payload: { a: 1 }, contentType: 'text/plain' }],
    })
  );
});

test('a content type Dapr bulk publish does not support is rejected before contacting daprd', () => {
  // application/xml is accepted (Dapr's own string-content-type category);
  // a vendor +json suffix and application/x-protobuf are not, even though
  // single publish's own isJsonType()/anything-goes rules would allow the
  // first — bulk's server-side ConvertEventToBytes only recognizes the exact
  // application/json and application/cloudevents+json strings.
  for (const contentType of ['application/vnd.example+json', 'application/x-protobuf']) {
    assertInvalid(() =>
      prepareBulkPublish(baseConfig, {
        payload: [{ entryId: 'e1', payload: { a: 1 }, contentType }],
      })
    );
  }
});

test('missing entry contentType falls back to configured content type, then to payload inference', () => {
  const configured = prepareBulkPublish(
    { ...baseConfig, contentType: 'application/cloudevents+json' },
    { payload: [{ entryId: 'e1', payload: { a: 1 } }] }
  );
  assert.equal(configured.entries[0].contentType, 'application/cloudevents+json');

  const inferred = prepareBulkPublish(baseConfig, {
    payload: [{ entryId: 'e1', payload: 'plain text' }],
  });
  assert.equal(inferred.entries[0].contentType, 'text/plain');
});

test('msg.dapr must be an object in bulk mode too', () => {
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, {
      payload: [{ entryId: 'e1', payload: 1 }],
      dapr: 'not an object',
    })
  );
});

test('a non-string entry contentType is rejected', () => {
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, { payload: [{ entryId: 'e1', payload: 1, contentType: 42 }] })
  );
});

test('an empty batch is rejected rather than silently publishing nothing', () => {
  assertInvalid(() => prepareBulkPublish(baseConfig, { payload: [] }));
});

test('msg.payload must be an array in bulk mode', () => {
  assertInvalid(() => prepareBulkPublish(baseConfig, { payload: { entryId: 'e1' } }));
  assertInvalid(() => prepareBulkPublish(baseConfig, { payload: 'not an array' }));
});

test('a batch over the entry-count safety cap is rejected before contacting daprd', () => {
  const payload = Array.from({ length: 1001 }, (_, i) => ({ entryId: `e${i}`, payload: i }));
  assertInvalid(() => prepareBulkPublish(baseConfig, { payload }));
  // Exactly at the cap is fine.
  assert.equal(
    prepareBulkPublish(baseConfig, { payload: payload.slice(0, 1000) }).entries.length,
    1000
  );
});

test('a duplicate entryId is rejected client-side, matching how daprd fails the whole batch', () => {
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, {
      payload: [
        { entryId: 'e1', payload: 1 },
        { entryId: 'e1', payload: 2 },
      ],
    })
  );
});

test('a missing or blank entryId is rejected', () => {
  assertInvalid(() => prepareBulkPublish(baseConfig, { payload: [{ payload: 1 }] }));
  assertInvalid(() => prepareBulkPublish(baseConfig, { payload: [{ entryId: '', payload: 1 }] }));
});

test('a non-object entry descriptor is rejected', () => {
  for (const bad of ['not an object', 42, null, ['nested', 'array']]) {
    assertInvalid(() => prepareBulkPublish(baseConfig, { payload: [bad] }));
  }
});

test('entry metadata overrides request metadata for that entry only, matching Dapr precedence', () => {
  const request = prepareBulkPublish(
    { ...baseConfig, metadata: '{"partitionKey":"shared"}' },
    {
      payload: [
        { entryId: 'e1', payload: 1 },
        { entryId: 'e2', payload: 2, metadata: { partitionKey: 'e2-own' } },
      ],
    }
  );
  assert.deepEqual(request.options.metadata, { partitionKey: 'shared' });
  assert.deepEqual(request.entries[0].metadata, {});
  assert.deepEqual(request.entries[1].metadata, { partitionKey: 'e2-own' });
});

test('msg.dapr can override pubsubName, topic, and merge request metadata for a bulk publish', () => {
  const request = prepareBulkPublish(
    { ...baseConfig, metadata: '{"configured":"kept"}' },
    {
      payload: [{ entryId: 'e1', payload: 1 }],
      dapr: { pubsubName: 'other-pubsub', topic: 'other-topic', metadata: { x: 'y' } },
    }
  );
  assert.equal(request.pubsubName, 'other-pubsub');
  assert.equal(request.topic, 'other-topic');
  assert.deepEqual(request.options.metadata, { configured: 'kept', x: 'y' });
});

test('msg.dapr.contentType supplies the default content type for bulk entries', () => {
  const request = prepareBulkPublish(baseConfig, {
    payload: [{ entryId: 'e1', payload: 'json string' }],
    dapr: { contentType: 'application/json' },
  });
  assert.equal(request.entries[0].contentType, 'application/json');
});

test('a missing or unserializable JSON entry payload is INVALID_MESSAGE', () => {
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, {
      payload: [{ entryId: 'missing', contentType: 'application/json' }],
    })
  );
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, {
      payload: [{ entryId: 'bigint', payload: 1n, contentType: 'application/json' }],
    })
  );

  const circular = {};
  circular.self = circular;
  assertInvalid(() =>
    prepareBulkPublish(baseConfig, {
      payload: [{ entryId: 'circular', payload: circular, contentType: 'application/json' }],
    })
  );
});

test('an encoded body over the connection body limit is rejected before contacting daprd', () => {
  const payload = [{ entryId: 'e1', payload: 'x'.repeat(1000) }];
  assertInvalid(() => prepareBulkPublish(baseConfig, { payload }, { maxBodyBytes: 10 }));
  assert.ok(prepareBulkPublish(baseConfig, { payload }, { maxBodyBytes: 1_000_000 }));
});

test('with no maxBodyBytes passed, body size is not bounded by prepareBulkPublish itself', () => {
  const payload = [{ entryId: 'e1', payload: 'x'.repeat(1000) }];
  assert.ok(prepareBulkPublish(baseConfig, { payload }));
});
