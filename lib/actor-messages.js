'use strict';

const { DaprError, ErrorCodes } = require('./errors');

const invalid = (message) => new DaprError(ErrorCodes.INVALID_MESSAGE, message);

// True for any C0 control (0x00-0x1f) or DEL (0x7f).
// eslint-disable-next-line no-control-regex -- deliberate control-char match, not a typo
const CONTROL_CHAR_PATTERN = /[\x00-\x1f\x7f]/;

const MAX_SEGMENT_LENGTH = 256;
const MAX_DEPTH = 32;
const ERROR_CODE_PATTERN = /^[A-Z][A-Z0-9_]{0,63}$/;
const MAX_ERROR_MESSAGE_LENGTH = 512;

// An actor type, actor id, or method name: nonempty, bounded, no path
// separator or control character, and never "." or ".." (which would be
// ambiguous once it is a segment of /actors/<type>/<id>/method/<method>).
// Case-sensitive: no case normalization is applied. Shared by connection
// registration (lib/connection-registry.js), inbound route parsing
// (lib/app-channel.js), the actor client (lib/actor-client.js), and the
// (later) call node -- one validator, one set of rules. `code` lets a
// registration-time caller report INVALID_OPTIONS while a per-message/
// per-request caller keeps the default INVALID_MESSAGE.
function validateActorSegment(value, label, code = ErrorCodes.INVALID_MESSAGE) {
  if (typeof value !== 'string' || value === '') {
    throw new DaprError(code, `${label} is required`);
  }
  if (value.length > MAX_SEGMENT_LENGTH) {
    throw new DaprError(code, `${label} must be at most ${MAX_SEGMENT_LENGTH} characters`);
  }
  if (value.includes('/')) {
    throw new DaprError(code, `${label} must not contain "/"`);
  }
  if (CONTROL_CHAR_PATTERN.test(value)) {
    throw new DaprError(code, `${label} must not contain control characters`);
  }
  if (value === '.' || value === '..') {
    throw new DaprError(code, `${label} must not be "." or ".."`);
  }
  return value;
}

// Walk a value the way JSON.stringify would, but reject what JSON.stringify
// would otherwise mangle silently: undefined and functions (dropped from an
// object, turned into null inside an array), a Buffer (serializes as its own
// {type,data} shape, never what an actor proposal means), a BigInt
// (JSON.stringify throws, but with a generic, unhelpful message), a
// non-finite number (silently becomes null), and a circular reference
// (JSON.stringify throws with a V8-internal message). Depth and cycles share
// one mechanism: `ancestors` is a stack, not a set, so the same object
// reachable twice on unrelated branches (a legitimate DAG, not a cycle) is
// only ever "on the stack" while it is actually being visited -- it never
// collides with itself here. A genuine cycle keeps extending depth forever
// and always trips the MAX_DEPTH cap, so no separate cycle counter is needed.
function assertSerializable(value, depth, ancestors) {
  if (depth > MAX_DEPTH) {
    throw invalid(`value nesting exceeds ${MAX_DEPTH} levels`);
  }
  if (value === null) {
    return;
  }
  const type = typeof value;
  if (type === 'string' || type === 'boolean') {
    return;
  }
  if (type === 'number') {
    if (!Number.isFinite(value)) {
      throw invalid('value contains a non-finite number');
    }
    return;
  }
  if (type === 'undefined') {
    throw invalid('value contains undefined');
  }
  if (type === 'bigint') {
    throw invalid('value contains a BigInt');
  }
  if (type === 'function') {
    throw invalid('value contains a function');
  }
  if (Buffer.isBuffer(value)) {
    throw invalid('value contains a Buffer');
  }
  if (type !== 'object') {
    throw invalid('value is not JSON-serializable');
  }
  if (ancestors.has(value)) {
    throw invalid('value contains a circular reference');
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      for (const item of value) {
        assertSerializable(item, depth + 1, ancestors);
      }
    } else {
      for (const key of Object.keys(value)) {
        assertSerializable(value[key], depth + 1, ancestors);
      }
    }
  } finally {
    ancestors.delete(value);
  }
}

// Validate then serialize, so the byte bound is checked against the exact
// bytes that will be stored/sent, not an estimate.
function boundedJson(value, label, maxBodyBytes) {
  assertSerializable(value, 0, new WeakSet());
  const json = JSON.stringify(value);
  if (typeof maxBodyBytes === 'number' && Buffer.byteLength(json, 'utf8') > maxBodyBytes) {
    throw invalid(`${label} exceeds the connection's body limit (${maxBodyBytes} bytes)`);
  }
  return json;
}

// Build the actor reply node's settlement proposal from its input message,
// fully serialized and bounded here so a later branch mutating `msg` cannot
// change what actually gets committed (see docs/architecture.md's "Actor
// request ownership" section). `outcome` is the reply node's own fixed configuration
// ('complete' or 'fail'); it is not inferred from message shape. `maxBodyBytes`
// mirrors lib/state-messages.js's own limits shape and bounds both the
// response and replacement state before the handler can commit anything.
function serializeProposal(msg, outcome, { maxBodyBytes } = {}) {
  const isObj = msg !== null && typeof msg === 'object';
  const dapr = isObj && msg.dapr !== null && typeof msg.dapr === 'object' ? msg.dapr : {};
  const actor = dapr.actor !== null && typeof dapr.actor === 'object' ? dapr.actor : {};

  if (outcome === 'fail') {
    const error = actor.error;
    if (error === undefined || error === null || typeof error !== 'object') {
      throw invalid('msg.dapr.actor.error is required for a fail reply');
    }
    if (typeof error.code !== 'string' || !ERROR_CODE_PATTERN.test(error.code)) {
      throw invalid('msg.dapr.actor.error.code must match /^[A-Z][A-Z0-9_]{0,63}$/');
    }
    if (typeof error.message !== 'string') {
      throw invalid('msg.dapr.actor.error.message must be a string');
    }
    const code = error.code;
    const message = error.message.slice(0, MAX_ERROR_MESSAGE_LENGTH);
    return {
      outcome: 'fail',
      errorBody: boundedJson({ error: { code, message } }, 'error response', maxBodyBytes),
    };
  }

  if (outcome !== 'complete') {
    throw invalid(`unknown actor reply outcome: ${outcome}`);
  }

  const payload = isObj ? msg.payload : undefined;
  if (payload === undefined) {
    throw invalid('msg.payload is required for a complete reply');
  }
  const responseJson = boundedJson(payload, 'response', maxBodyBytes);

  const proposal = { outcome: 'complete', responseJson };
  if (Object.hasOwn(actor, 'nextState')) {
    proposal.nextStateJson = boundedJson(actor.nextState, 'msg.dapr.actor.nextState', maxBodyBytes);
  }
  return proposal;
}

module.exports = { validateActorSegment, serializeProposal };
