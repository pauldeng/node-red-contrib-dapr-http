'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { resolveAckStatus } = require('../../lib/ack');

test('resolveAckStatus reads the configured dropdown only in fixed mode', () => {
  assert.equal(
    resolveAckStatus({ ackStatus: 'DROP' }, { ackStatusSource: 'fixed', ackStatus: 'retry' })
      .status,
    'RETRY'
  );
});

test('resolveAckStatus reads msg.ackStatus only in message mode', () => {
  assert.equal(
    resolveAckStatus({ ackStatus: 'drop' }, { ackStatusSource: 'message', ackStatus: 'SUCCESS' })
      .status,
    'DROP'
  );
});

test('resolveAckStatus rejects a value that is not one of the three outcomes', () => {
  const result = resolveAckStatus({}, { ackStatusSource: 'fixed', ackStatus: 'MAYBE' });
  assert.equal(result.status, undefined);
  assert.match(result.error, /invalid ack status: MAYBE/);
});

test('resolveAckStatus rejects a non-string message value', () => {
  const result = resolveAckStatus(
    { ackStatus: { text: 'success' } },
    { ackStatusSource: 'message', ackStatus: 'SUCCESS' }
  );
  assert.equal(result.status, undefined);
  assert.match(result.error, /must be a string, got object/);
  assert.doesNotMatch(result.error, /\[object Object\]/);
});

test('resolveAckStatus names a missing message status in message mode', () => {
  const result = resolveAckStatus({}, { ackStatusSource: 'message', ackStatus: 'SUCCESS' });
  assert.equal(result.status, undefined);
  assert.match(result.error, /msg\.ackStatus/);
});

test('resolveAckStatus rejects an unknown source', () => {
  const result = resolveAckStatus({}, { ackStatusSource: 'elsewhere', ackStatus: 'SUCCESS' });
  assert.equal(result.status, undefined);
  assert.match(result.error, /invalid ack status source/);
});

test('resolveAckStatus keeps 0.2 flows working until they are edited', () => {
  assert.equal(
    resolveAckStatus({ dapr: { status: 'retry' } }, { status: 'SUCCESS' }).status,
    'RETRY'
  );
  assert.equal(resolveAckStatus({}, { status: 'drop' }).status, 'DROP');
  assert.equal(
    resolveAckStatus({}, { status: { text: 'success', fill: 'green', shape: 'dot' } }).status,
    'SUCCESS'
  );
});
