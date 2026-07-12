'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { buildService } = require('../../lib/services');
const { DaprError, ErrorCodes } = require('../../lib/errors');

test('buildService normalizes the verb and leading slash', () => {
  const svc = buildService({ nodeId: 'n1', verb: 'post', path: 'orders/create' });
  assert.deepEqual(svc, { nodeId: 'n1', verb: 'POST', path: '/orders/create' });
});

test('buildService accepts each supported verb', () => {
  for (const verb of ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']) {
    assert.equal(buildService({ nodeId: 'n', verb, path: '/m' }).verb, verb);
  }
});

test('buildService rejects an unsupported verb', () => {
  assert.throws(
    () => buildService({ nodeId: 'n', verb: 'TRACE', path: '/m' }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('buildService rejects reserved, internal, and unsafe paths', () => {
  const bad = [
    '/dapr',
    '/dapr/subscribe',
    '/healthz',
    '/node-red-dapr/subscriptions/x',
    '/a/../b', // traversal
    '//x', // empty segment
    '/has space', // unsafe char
    '', // empty
  ];
  for (const path of bad) {
    assert.throws(
      () => buildService({ nodeId: 'n', verb: 'GET', path }),
      (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS,
      `expected rejection for "${path}"`
    );
  }
});
