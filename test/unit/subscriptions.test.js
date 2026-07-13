'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildSubscription,
  discoveryEntry,
  fingerprint,
  parseDelivery,
  parseBulkDelivery,
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

test('buildSubscription rejects malformed metadata', () => {
  assert.throws(
    () => buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't', metadata: '{not json' }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
  assert.throws(
    () => buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't', metadata: '[1,2]' }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
  assert.throws(
    () =>
      buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't', metadata: { a: { b: 1 } } }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('buildSubscription rejects a non-array rules value', () => {
  assert.throws(
    () => buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't', rules: { not: 'array' } }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('buildSubscription rejects an out-of-range bulkSubscribe bound', () => {
  assert.throws(
    () =>
      buildSubscription({
        nodeId: 'n',
        pubsubName: 'ps',
        topic: 't',
        bulkSubscribe: { enabled: true, maxMessagesCount: 0 },
      }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
  assert.throws(
    () =>
      buildSubscription({
        nodeId: 'n',
        pubsubName: 'ps',
        topic: 't',
        bulkSubscribe: { enabled: true, maxAwaitDurationMs: 'not-a-number' },
      }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
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

test("buildSubscription derives per-rule routes from each rule's persisted id, not its position", () => {
  const sub = buildSubscription({
    nodeId: 'n1',
    pubsubName: 'ps',
    topic: 't',
    rules: [
      { id: 'ruleA', match: 'event.type == "a"' },
      { id: 'ruleB', match: 'event.type == "b"' },
    ],
  });
  assert.equal(sub.hasRules, true);
  assert.equal(sub.routes.length, 3); // 2 rules + default
  assert.equal(sub.routes[0].path, '/node-red-dapr/subscriptions/n1/ruleA');
  assert.equal(sub.routes[0].match, 'event.type == "a"');
  assert.equal(sub.routes[1].path, '/node-red-dapr/subscriptions/n1/ruleB');
  assert.equal(sub.routes[2].isDefault, true);
  // The default path never depends on whether rules exist (see the next
  // test) — it is always the bare node path, with no "/default" suffix.
  assert.equal(sub.routes[2].path, '/node-red-dapr/subscriptions/n1');
  assert.equal(sub.route, '/node-red-dapr/subscriptions/n1');
});

test('buildSubscription: the default path never changes when rules are added or removed', () => {
  // A stale (not-yet-restarted) sidecar keeps delivering to whatever default
  // path it last fetched. If that path moved when the first rule was added,
  // daprd would 404 against the old path — and Dapr treats 404 as a permanent
  // DROP, not a retry (see https://v1-18.docs.dapr.io/reference/api/pubsub_api/).
  const noRules = buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't' });
  const withRules = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [{ id: 'ruleA', match: 'event.type == "a"' }],
  });
  const withMoreRules = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [
      { id: 'ruleA', match: 'event.type == "a"' },
      { id: 'ruleB', match: 'event.type == "b"' },
    ],
  });
  assert.equal(noRules.route, withRules.route);
  assert.equal(withRules.route, withMoreRules.route);
});

test("buildSubscription keeps every existing rule's route stable when a new rule is inserted", () => {
  const before = buildSubscription({
    nodeId: 'n1',
    pubsubName: 'ps',
    topic: 't',
    rules: [
      { id: 'ruleA', match: 'event.type == "a"' },
      { id: 'ruleB', match: 'event.type == "b"' },
    ],
  });
  const afterInsert = buildSubscription({
    nodeId: 'n1',
    pubsubName: 'ps',
    topic: 't',
    rules: [
      { id: 'ruleNew', match: 'event.type == "new"' },
      { id: 'ruleA', match: 'event.type == "a"' },
      { id: 'ruleB', match: 'event.type == "b"' },
    ],
  });
  const pathById = (sub, id) => sub.routes.find((r) => r.ruleId === id).path;
  assert.equal(pathById(before, 'ruleA'), pathById(afterInsert, 'ruleA'));
  assert.equal(pathById(before, 'ruleB'), pathById(afterInsert, 'ruleB'));
});

test('buildSubscription rejects a rule with a missing, blank, or unsafe id, or a duplicate id', () => {
  assert.throws(
    () => buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't', rules: [{ match: 'x' }] }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS,
    'missing id'
  );
  assert.throws(
    () =>
      buildSubscription({
        nodeId: 'n',
        pubsubName: 'ps',
        topic: 't',
        rules: [{ id: '  ', match: 'x' }],
      }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS,
    'blank id'
  );
  assert.throws(
    () =>
      buildSubscription({
        nodeId: 'n',
        pubsubName: 'ps',
        topic: 't',
        rules: [{ id: 'a/../b', match: 'x' }],
      }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS,
    'unsafe id'
  );
  assert.throws(
    () =>
      buildSubscription({
        nodeId: 'n',
        pubsubName: 'ps',
        topic: 't',
        rules: [
          { id: 'dup', match: 'x' },
          { id: 'dup', match: 'y' },
        ],
      }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS,
    'duplicate id'
  );
});

test('buildSubscription allows a rule id of "default" — the fallback route is always bare, so it cannot collide', () => {
  const sub = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [{ id: 'default', match: 'x' }],
  });
  assert.equal(sub.routes[0].path, '/node-red-dapr/subscriptions/n/default');
  assert.equal(sub.route, '/node-red-dapr/subscriptions/n');
  assert.notEqual(sub.routes[0].path, sub.route);
});

test('buildSubscription with no rules exposes a single default-only route (unchanged behavior)', () => {
  const sub = buildSubscription({ nodeId: 'n2', pubsubName: 'ps', topic: 't' });
  assert.equal(sub.hasRules, false);
  assert.deepEqual(sub.routes, [
    { ruleId: null, match: null, path: '/node-red-dapr/subscriptions/n2', isDefault: true },
  ]);
  assert.equal(sub.route, '/node-red-dapr/subscriptions/n2');
});

test('buildSubscription rejects a rule with an empty match expression', () => {
  assert.throws(
    () =>
      buildSubscription({
        nodeId: 'n',
        pubsubName: 'ps',
        topic: 't',
        rules: [{ id: 'r', match: '  ' }],
      }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('buildSubscription normalizes bulkSubscribe and allows combining it with CEL rules', () => {
  // Dapr's bulk subscribe groups entries by their matched CEL route and
  // delivers a separate bulk batch per route, so the two features compose.
  const sub = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    bulkSubscribe: { enabled: true, maxMessagesCount: 50, maxAwaitDurationMs: 200 },
  });
  assert.deepEqual(sub.bulkSubscribe, {
    enabled: true,
    maxMessagesCount: 50,
    maxAwaitDurationMs: 200,
  });

  const combined = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [{ id: 'ruleA', match: 'event.type == "a"' }],
    bulkSubscribe: { enabled: true },
  });
  assert.equal(combined.hasRules, true);
  assert.deepEqual(combined.bulkSubscribe, { enabled: true });
  const entry = discoveryEntry(combined);
  assert.ok(entry.routes, 'routes are still advertised');
  assert.ok(entry.bulkSubscribe, 'bulkSubscribe is still advertised alongside routes');
});

test('discoveryEntry emits routes.rules/default (no top-level route) when rules are configured', () => {
  const sub = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [{ id: 'ruleA', match: 'event.type == "a"' }],
  });
  assert.deepEqual(discoveryEntry(sub), {
    pubsubname: 'ps',
    topic: 't',
    routes: {
      rules: [{ match: 'event.type == "a"', path: '/node-red-dapr/subscriptions/n/ruleA' }],
      default: '/node-red-dapr/subscriptions/n',
    },
  });
});

test('discoveryEntry emits bulkSubscribe, omitting unset optional fields', () => {
  const sub = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    bulkSubscribe: { enabled: true },
  });
  assert.deepEqual(discoveryEntry(sub).bulkSubscribe, { enabled: true });

  const sub2 = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    bulkSubscribe: { enabled: true, maxMessagesCount: 10, maxAwaitDurationMs: 500 },
  });
  assert.deepEqual(discoveryEntry(sub2).bulkSubscribe, {
    enabled: true,
    maxMessagesCount: 10,
    maxAwaitDurationMs: 500,
  });
});

test('fingerprint changes when rule match text or order changes, or bulkSubscribe changes', () => {
  const base = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [
      { id: 'ruleA', match: 'event.type == "a"' },
      { id: 'ruleB', match: 'event.type == "b"' },
    ],
  });
  const reordered = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [
      { id: 'ruleB', match: 'event.type == "b"' },
      { id: 'ruleA', match: 'event.type == "a"' },
    ],
  });
  assert.notEqual(
    fingerprint([base]),
    fingerprint([reordered]),
    'CEL rule order affects match precedence, so it is part of the fingerprint'
  );

  const changedMatch = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    rules: [
      { id: 'ruleA', match: 'event.type == "a"' },
      { id: 'ruleB', match: 'event.type == "c"' },
    ],
  });
  assert.notEqual(fingerprint([base]), fingerprint([changedMatch]));

  const noBulk = buildSubscription({ nodeId: 'n', pubsubName: 'ps', topic: 't' });
  const withBulk = buildSubscription({
    nodeId: 'n',
    pubsubName: 'ps',
    topic: 't',
    bulkSubscribe: { enabled: true },
  });
  assert.notEqual(fingerprint([noBulk]), fingerprint([withBulk]));
});

test('parseBulkDelivery extracts each CloudEvent entry, isolating a malformed entry from the rest', () => {
  const good1 = {
    entryId: 'e1',
    event: { specversion: '1.0', id: 'e1', source: 's', type: 't', data: { a: 1 } },
    contentType: 'application/json',
    metadata: { partitionKey: '7' },
  };
  const good2 = {
    entryId: 'e2',
    event: { specversion: '1.0', id: 'e2', source: 's', type: 't', data: { a: 2 } },
    contentType: 'application/json',
  };
  const bad = { entryId: 'e3', event: { not: 'a cloudevent' } };
  const body = Buffer.from(JSON.stringify({ entries: [good1, bad, good2] }));

  const results = parseBulkDelivery(body);
  assert.equal(results.length, 3);
  assert.equal(results[0].entryId, 'e1');
  assert.deepEqual(results[0].payload, { a: 1 });
  assert.equal(results[0].contentType, 'application/json');
  assert.deepEqual(results[0].metadata, { partitionKey: '7' });
  assert.equal(results[1].entryId, 'e3');
  assert.ok(results[1].error, 'the malformed entry reports an error instead of throwing');
  assert.equal(results[2].entryId, 'e2');
  assert.deepEqual(results[2].payload, { a: 2 });
  assert.deepEqual(results[2].metadata, {}, 'metadata defaults to an empty object');
});

test("parseBulkDelivery surfaces the envelope's own id and merges its metadata into each entry", () => {
  // Per pinned Dapr 1.18.1 source (bulksubscribe_events.go NewBulkSubscribeEnvelope,
  // and components-contrib/pubsub/envelope.go's IDField/PubsubField constants),
  // the bulk request body has its own top-level "id" and "metadata" alongside
  // "entries" — distinct from each entry's own per-entry metadata.
  const good = {
    entryId: 'e1',
    event: { specversion: '1.0', id: 'e1', source: 's', type: 't', data: { a: 1 } },
    metadata: { partitionKey: '7' }, // entry-level: must win over the batch value
  };
  const body = Buffer.from(
    JSON.stringify({
      id: 'batch-123',
      metadata: { partitionKey: 'batch-default', consumerGroup: 'g1' },
      entries: [good],
    })
  );
  const results = parseBulkDelivery(body);
  assert.equal(results[0].batchId, 'batch-123');
  assert.deepEqual(results[0].metadata, { partitionKey: '7', consumerGroup: 'g1' });
});

test('parseBulkDelivery tolerates a missing envelope id or metadata', () => {
  const good = {
    entryId: 'e1',
    event: { specversion: '1.0', id: 'e1', source: 's', type: 't', data: { a: 1 } },
  };
  const body = Buffer.from(JSON.stringify({ entries: [good] }));
  const results = parseBulkDelivery(body);
  assert.equal(results[0].batchId, undefined);
  assert.deepEqual(results[0].metadata, {});
});

test('parseBulkDelivery({ rawPayload: true }) decodes each entry.event as a base64 string, not a CloudEvent', () => {
  // Per pinned Dapr 1.18.1 source (bulksubscription.go bulkSubscribeTopic), a
  // raw-payload topic's bulk entries carry the raw bytes as a base64 STRING in
  // "event" — never a CloudEvent-wrapped object like a non-raw topic.
  const bytes = Buffer.from([0, 1, 2, 255]);
  const good = {
    entryId: 'e1',
    event: bytes.toString('base64'),
    contentType: 'application/octet-stream',
    metadata: { partitionKey: '3' },
  };
  const bad = { entryId: 'e2', event: 'not valid base64!!' };
  const body = Buffer.from(JSON.stringify({ entries: [good, bad] }));

  const results = parseBulkDelivery(body, { rawPayload: true });
  assert.equal(results.length, 2);
  assert.equal(results[0].entryId, 'e1');
  assert.ok(Buffer.isBuffer(results[0].payload));
  assert.deepEqual(results[0].payload, bytes);
  assert.equal(results[0].cloudEvent, null);
  assert.equal(results[0].contentType, 'application/octet-stream');
  assert.deepEqual(results[0].metadata, { partitionKey: '3' });
  assert.equal(results[1].entryId, 'e2');
  assert.ok(results[1].error, 'invalid base64 reports a per-entry error');
});

test('parseBulkDelivery({ rawPayload: true }) reports a per-entry error for a non-string event', () => {
  const body = Buffer.from(JSON.stringify({ entries: [{ entryId: 'e1', event: { a: 1 } }] }));
  const results = parseBulkDelivery(body, { rawPayload: true });
  assert.equal(results.length, 1);
  assert.equal(results[0].entryId, 'e1');
  assert.ok(results[0].error);
});

test('parseBulkDelivery rejects a body without an entries array', () => {
  assert.throws(
    () => parseBulkDelivery(Buffer.from(JSON.stringify({ notEntries: [] }))),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_MESSAGE
  );
  assert.throws(
    () => parseBulkDelivery(Buffer.from('not json')),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_MESSAGE
  );
});

test('parseBulkDelivery reports a per-entry error for a missing entryId rather than throwing', () => {
  const body = Buffer.from(JSON.stringify({ entries: [{ event: {} }] }));
  const results = parseBulkDelivery(body);
  assert.equal(results.length, 1);
  assert.equal(results[0].entryId, null);
  assert.ok(results[0].error);
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
