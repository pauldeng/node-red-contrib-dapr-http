'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { prepareBindingRequest } = require('../../lib/binding-messages');
const { DaprError, ErrorCodes } = require('../../lib/errors');

const baseConfig = {
  bindingName: 'orders-binding',
  operation: 'create',
  metadata: '{}',
};

function invalidAssert(fn) {
  assert.throws(fn, (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE);
}

test('prepareBindingRequest resolves the configured bindingName/operation and msg.payload as data', () => {
  const result = prepareBindingRequest(baseConfig, { payload: { id: 1 } });
  assert.equal(result.bindingName, 'orders-binding');
  assert.equal(result.operation, 'create');
  assert.deepEqual(result.data, { id: 1 });
  assert.deepEqual(result.metadata, {});
});

test('msg.dapr.bindingName overrides the configured bindingName', () => {
  const result = prepareBindingRequest(baseConfig, { dapr: { bindingName: 'other' } });
  assert.equal(result.bindingName, 'other');
});

test('msg.dapr.operation overrides the configured operation', () => {
  const result = prepareBindingRequest(baseConfig, { dapr: { operation: 'delete' } });
  assert.equal(result.operation, 'delete');
});

test('prepareBindingRequest requires a non-empty bindingName', () => {
  invalidAssert(() => prepareBindingRequest({ ...baseConfig, bindingName: '' }, {}));
});

test('prepareBindingRequest requires a non-empty operation', () => {
  invalidAssert(() => prepareBindingRequest({ ...baseConfig, operation: '' }, {}));
});

test('prepareBindingRequest rejects a Buffer msg.payload', () => {
  assert.throws(
    () => prepareBindingRequest(baseConfig, { payload: Buffer.from('x') }),
    (err) =>
      err instanceof DaprError &&
      err.code === ErrorCodes.INVALID_MESSAGE &&
      /base64-encode it yourself/.test(err.message)
  );
});

test('an undefined msg.payload resolves data as undefined, not an error', () => {
  const result = prepareBindingRequest(baseConfig, {});
  assert.equal(result.data, undefined);
});

test('prepareBindingRequest merges configured and message metadata', () => {
  const config = { ...baseConfig, metadata: JSON.stringify({ a: '1' }) };
  const result = prepareBindingRequest(config, { dapr: { metadata: { b: '2' } } });
  assert.deepEqual(result.metadata, { a: '1', b: '2' });
});

test('prepareBindingRequest rejects a non-object msg.dapr', () => {
  invalidAssert(() => prepareBindingRequest(baseConfig, { dapr: 'x' }));
});

test('prepareBindingRequest rejects non-scalar metadata values', () => {
  invalidAssert(() =>
    prepareBindingRequest(baseConfig, { dapr: { metadata: { a: { nested: true } } } })
  );
});
