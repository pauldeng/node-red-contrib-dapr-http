'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { DaprError, ErrorCodes } = require('../../lib/errors');

test('DaprError is an Error carrying a stable code and message', () => {
  const err = new DaprError(ErrorCodes.INVALID_OPTIONS, 'bad port');
  assert.ok(err instanceof Error);
  assert.equal(err.name, 'DaprError');
  assert.equal(err.code, ErrorCodes.INVALID_OPTIONS);
  assert.equal(err.message, 'bad port');
});

test('DaprError preserves an underlying cause', () => {
  const cause = new Error('root');
  const err = new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'sidecar down', { cause });
  assert.equal(err.cause, cause);
});

test('ErrorCodes exposes the codes the package relies on, each equal to its key', () => {
  const required = [
    'INVALID_OPTIONS',
    'DUPLICATE_LISTENER',
    'SIDECAR_UNAVAILABLE',
    'INVALID_MESSAGE',
    'PUBLISH_FAILED',
    'PENDING_CAPACITY',
    'DUPLICATE_PENDING',
    'STATE_OPERATION_FAILED',
    'STATE_ETAG_MISMATCH',
    'CONFIGURATION_OPERATION_FAILED',
  ];
  for (const code of required) {
    assert.ok(code in ErrorCodes, `missing code ${code}`);
  }
  // Values are stable strings equal to their keys.
  for (const [key, value] of Object.entries(ErrorCodes)) {
    assert.equal(value, key);
  }
});
