'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  resolveOperation,
  prepareStateGet,
  prepareStateSave,
  prepareStateDelete,
  prepareStateBulkGet,
  prepareStateTransaction,
} = require('../../lib/state-messages');
const { DaprError, ErrorCodes } = require('../../lib/errors');

const baseConfig = {
  operation: 'get',
  storeName: 'orders-store',
  key: 'order-1',
  consistency: '',
  concurrency: '',
  ttlSeconds: '',
  metadata: '{}',
};

function invalidAssert(fn) {
  assert.throws(fn, (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE);
}

// --- resolveOperation ---

test('resolveOperation reads the configured operation by default', () => {
  assert.equal(resolveOperation(baseConfig, {}), 'get');
});

test('msg.dapr.operation overrides the configured operation', () => {
  assert.equal(resolveOperation(baseConfig, { dapr: { operation: 'save' } }), 'save');
});

test('resolveOperation rejects anything outside the five known operations', () => {
  invalidAssert(() => resolveOperation(baseConfig, { dapr: { operation: 'query' } }));
  invalidAssert(() => resolveOperation({ ...baseConfig, operation: 'query' }, {}));
  invalidAssert(() => resolveOperation(baseConfig, { dapr: { operation: 42 } }));
});

test('resolveOperation rejects a non-object msg.dapr', () => {
  invalidAssert(() => resolveOperation(baseConfig, { dapr: 'get' }));
});

// --- prepareStateGet ---

test('prepareStateGet resolves storeName/key from config with msg.dapr overrides', () => {
  const result = prepareStateGet(baseConfig, {});
  assert.equal(result.storeName, 'orders-store');
  assert.equal(result.key, 'order-1');
  assert.equal(result.consistency, undefined);
  assert.deepEqual(result.metadata, {});

  const overridden = prepareStateGet(baseConfig, {
    dapr: { storeName: 'other-store', key: 'order-2', consistency: 'strong' },
  });
  assert.equal(overridden.storeName, 'other-store');
  assert.equal(overridden.key, 'order-2');
  assert.equal(overridden.consistency, 'strong');
});

test('prepareStateGet requires a non-empty storeName and key', () => {
  invalidAssert(() => prepareStateGet({ ...baseConfig, storeName: '' }, {}));
  invalidAssert(() => prepareStateGet({ ...baseConfig, key: '' }, {}));
});

test('state keys preserve significant surrounding whitespace', () => {
  assert.equal(prepareStateGet({ ...baseConfig, key: ' key ' }, {}).key, ' key ');
  assert.equal(prepareStateSave({ ...baseConfig, key: ' key ' }, { payload: 1 }).item.key, ' key ');
  assert.equal(prepareStateDelete({ ...baseConfig, key: ' key ' }, {}).key, ' key ');
  assert.deepEqual(prepareStateBulkGet(baseConfig, { payload: [' key '] }).keys, [' key ']);
  assert.equal(
    prepareStateTransaction(baseConfig, {
      payload: [{ operation: 'delete', key: ' key ' }],
    }).operations[0].request.key,
    ' key '
  );
});

test('prepareStateGet rejects a consistency value outside strong/eventual', () => {
  invalidAssert(() => prepareStateGet(baseConfig, { dapr: { consistency: 'weak' } }));
});

test('prepareStateGet merges configured and message metadata', () => {
  const config = { ...baseConfig, metadata: JSON.stringify({ a: '1' }) };
  const result = prepareStateGet(config, { dapr: { metadata: { b: '2' } } });
  assert.deepEqual(result.metadata, { a: '1', b: '2' });
});

// --- prepareStateSave ---

test('prepareStateSave shapes a single item from msg.payload as the value', () => {
  const result = prepareStateSave(baseConfig, { payload: { total: 42 } });
  assert.equal(result.storeName, 'orders-store');
  assert.deepEqual(result.item, { key: 'order-1', value: { total: 42 } });
});

test('prepareStateSave includes etag/metadata/options only when set', () => {
  const result = prepareStateSave(baseConfig, {
    payload: 'v',
    dapr: { etag: 'v1', consistency: 'strong', concurrency: 'first-write', metadata: { a: '1' } },
  });
  assert.deepEqual(result.item, {
    key: 'order-1',
    value: 'v',
    etag: 'v1',
    metadata: { a: '1' },
    options: { concurrency: 'first-write', consistency: 'strong' },
  });
});

test('prepareStateSave rejects a Buffer payload', () => {
  invalidAssert(() => prepareStateSave(baseConfig, { payload: Buffer.from('x') }));
});

test('prepareStateSave rejects a concurrency value outside first-write/last-write', () => {
  invalidAssert(() =>
    prepareStateSave(baseConfig, { payload: 'v', dapr: { concurrency: 'bogus' } })
  );
});

test('prepareStateSave converts ttlSeconds into a string metadata.ttlInSeconds', () => {
  const result = prepareStateSave(baseConfig, { payload: 'v', dapr: { ttlSeconds: 60 } });
  assert.equal(result.item.metadata.ttlInSeconds, '60');
});

test('prepareStateSave rejects a negative or non-integer ttlSeconds', () => {
  invalidAssert(() => prepareStateSave(baseConfig, { payload: 'v', dapr: { ttlSeconds: -1 } }));
  invalidAssert(() => prepareStateSave(baseConfig, { payload: 'v', dapr: { ttlSeconds: 1.5 } }));
});

test('prepareStateSave rejects an unserializable value as INVALID_MESSAGE', () => {
  const circular = {};
  circular.self = circular;
  invalidAssert(() => prepareStateSave(baseConfig, { payload: circular }));
});

test('prepareStateSave rejects values JSON would silently omit', () => {
  for (const payload of [undefined, () => {}, Symbol('value')]) {
    invalidAssert(() => prepareStateSave(baseConfig, { payload }));
  }
});

test("prepareStateSave enforces the caller's maxBodyBytes", () => {
  invalidAssert(() =>
    prepareStateSave(baseConfig, { payload: 'x'.repeat(100) }, { maxBodyBytes: 10 })
  );
});

// --- prepareStateDelete ---

test('prepareStateDelete resolves storeName/key/etag/consistency/concurrency', () => {
  const result = prepareStateDelete(baseConfig, {
    dapr: { etag: 'v1', consistency: 'eventual', concurrency: 'last-write' },
  });
  assert.equal(result.storeName, 'orders-store');
  assert.equal(result.key, 'order-1');
  assert.equal(result.etag, 'v1');
  assert.equal(result.consistency, 'eventual');
  assert.equal(result.concurrency, 'last-write');
});

test('prepareStateDelete leaves etag undefined when not supplied', () => {
  const result = prepareStateDelete(baseConfig, {});
  assert.equal(result.etag, undefined);
});

test('prepareStateDelete requires a non-empty storeName and key', () => {
  invalidAssert(() => prepareStateDelete({ ...baseConfig, key: '' }, {}));
});

// --- prepareStateBulkGet ---

test('prepareStateBulkGet reads keys from msg.payload by default', () => {
  const result = prepareStateBulkGet(baseConfig, { payload: ['a', 'b'] });
  assert.equal(result.storeName, 'orders-store');
  assert.deepEqual(result.keys, ['a', 'b']);
});

test('msg.dapr.keys overrides msg.payload for bulkGet', () => {
  const result = prepareStateBulkGet(baseConfig, {
    payload: 'ignored',
    dapr: { keys: ['x', 'y'] },
  });
  assert.deepEqual(result.keys, ['x', 'y']);
});

test('prepareStateBulkGet rejects a non-array or empty keys list', () => {
  invalidAssert(() => prepareStateBulkGet(baseConfig, { payload: 'not-an-array' }));
  invalidAssert(() => prepareStateBulkGet(baseConfig, { payload: [] }));
});

test('prepareStateBulkGet rejects a non-string entry in keys', () => {
  invalidAssert(() => prepareStateBulkGet(baseConfig, { payload: ['a', 42] }));
});

test('prepareStateBulkGet resolves an optional integer parallelism', () => {
  const result = prepareStateBulkGet(baseConfig, {
    payload: ['a'],
    dapr: { parallelism: 4 },
  });
  assert.equal(result.parallelism, 4);
  assert.equal(prepareStateBulkGet(baseConfig, { payload: ['a'] }).parallelism, undefined);
});

test('prepareStateBulkGet rejects a negative or non-integer parallelism', () => {
  invalidAssert(() =>
    prepareStateBulkGet(baseConfig, { payload: ['a'], dapr: { parallelism: -1 } })
  );
  invalidAssert(() =>
    prepareStateBulkGet(baseConfig, { payload: ['a'], dapr: { parallelism: 1.5 } })
  );
});

// --- prepareStateTransaction ---

test('prepareStateTransaction reads operations from msg.payload by default', () => {
  const result = prepareStateTransaction(baseConfig, {
    payload: [
      { operation: 'upsert', key: 'a', value: 1 },
      { operation: 'delete', key: 'b' },
    ],
  });
  assert.equal(result.storeName, 'orders-store');
  assert.deepEqual(result.operations, [
    { operation: 'upsert', request: { key: 'a', value: 1 } },
    { operation: 'delete', request: { key: 'b' } },
  ]);
});

test('msg.dapr.operations overrides msg.payload for transaction', () => {
  const result = prepareStateTransaction(baseConfig, {
    payload: 'ignored',
    dapr: { operations: [{ operation: 'delete', key: 'z' }] },
  });
  assert.deepEqual(result.operations, [{ operation: 'delete', request: { key: 'z' } }]);
});

test('prepareStateTransaction rejects a non-array or empty operations list', () => {
  invalidAssert(() => prepareStateTransaction(baseConfig, { payload: 'not-an-array' }));
  invalidAssert(() => prepareStateTransaction(baseConfig, { payload: [] }));
});

test('prepareStateTransaction rejects an operation value outside upsert/delete', () => {
  invalidAssert(() =>
    prepareStateTransaction(baseConfig, { payload: [{ operation: 'set', key: 'a', value: 1 }] })
  );
});

test('prepareStateTransaction requires a value for an upsert operation', () => {
  invalidAssert(() =>
    prepareStateTransaction(baseConfig, { payload: [{ operation: 'upsert', key: 'a' }] })
  );
});

test('prepareStateTransaction rejects upsert values JSON would silently omit', () => {
  for (const value of [undefined, () => {}, Symbol('value')]) {
    invalidAssert(() =>
      prepareStateTransaction(baseConfig, {
        payload: [{ operation: 'upsert', key: 'a', value }],
      })
    );
  }
});

test('prepareStateTransaction rejects Buffer upsert values', () => {
  invalidAssert(() =>
    prepareStateTransaction(baseConfig, {
      payload: [{ operation: 'upsert', key: 'a', value: Buffer.from('x') }],
    })
  );
});

test('prepareStateTransaction does not require a value for a delete operation', () => {
  const result = prepareStateTransaction(baseConfig, {
    payload: [{ operation: 'delete', key: 'a' }],
  });
  assert.deepEqual(result.operations[0].request, { key: 'a' });
});

test('prepareStateTransaction requires a non-empty key per operation', () => {
  invalidAssert(() =>
    prepareStateTransaction(baseConfig, { payload: [{ operation: 'delete', key: '' }] })
  );
});

test('prepareStateTransaction includes per-operation etag/metadata/options only when set', () => {
  const result = prepareStateTransaction(baseConfig, {
    payload: [
      {
        operation: 'upsert',
        key: 'a',
        value: 1,
        etag: 'v1',
        consistency: 'strong',
        concurrency: 'first-write',
        metadata: { m: '1' },
      },
    ],
  });
  assert.deepEqual(result.operations[0].request, {
    key: 'a',
    value: 1,
    etag: 'v1',
    options: { concurrency: 'first-write', consistency: 'strong' },
    metadata: { m: '1' },
  });
});

test('prepareStateTransaction rejects a per-operation consistency/concurrency value outside the enum', () => {
  invalidAssert(() =>
    prepareStateTransaction(baseConfig, {
      payload: [{ operation: 'upsert', key: 'a', value: 1, consistency: 'weak' }],
    })
  );
});

test("prepareStateTransaction enforces the caller's maxBodyBytes", () => {
  invalidAssert(() =>
    prepareStateTransaction(
      baseConfig,
      { payload: [{ operation: 'upsert', key: 'a', value: 'x'.repeat(100) }] },
      { maxBodyBytes: 10 }
    )
  );
});

// --- shared msg.dapr validation ---

test('every prepareState* function rejects a non-object msg.dapr', () => {
  invalidAssert(() => prepareStateGet(baseConfig, { dapr: 'x' }));
  invalidAssert(() => prepareStateSave(baseConfig, { payload: 'v', dapr: 'x' }));
  invalidAssert(() => prepareStateDelete(baseConfig, { dapr: 'x' }));
  invalidAssert(() => prepareStateBulkGet(baseConfig, { payload: ['a'], dapr: 'x' }));
  invalidAssert(() => prepareStateTransaction(baseConfig, { payload: [], dapr: 'x' }));
});
