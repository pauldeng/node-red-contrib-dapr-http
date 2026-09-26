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
const MAX_SCHEDULE_TIME_LENGTH = 128;

// The reserved actor-method registry key one dapr-actor-method node's
// `trigger: 'reminder'` registers under. Deliberately a value
// validateActorSegment (below) can NEVER accept as a real method name --
// "reminder" alone would NOT be reserved: it is itself a legal method name
// (e.g. an actor with a method literally called "reminder"), so a plain
// English word here would let an ordinary method registration collide with,
// or bypass, the reminder registration. Containing "/" guarantees no
// user-configured method name can ever equal it, and it never reaches an
// actual URL (lib/app-channel.js parses the reminder shape's name out of the
// path itself; this string is only ever a Map key inside this process).
const REMINDER_METHOD = 'remind/';

// The reminder callback envelope's own fixed wrapper: `{"data":` plus
// `,"dueTime":"","period":""}` around the caller's `data` value, 34 bytes for
// a payload with no HTML-escaped ("<", ">", "&") characters -- verified
// against real daprd 1.18.4 by test/integration/actors-schedule-probe.test.js.
const REMINDER_ENVELOPE_BYTES = 34;

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

  // Absent or `false` means "no deletion"; only a literal `true` commits a
  // delete of the actor's `record`. Any other present value (including
  // `null` or a present `undefined`) is a configuration mistake, not a
  // silent no-op.
  const hasDeleteState = Object.hasOwn(actor, 'deleteState');
  const deleteState = hasDeleteState ? actor.deleteState : false;
  if (hasDeleteState && typeof deleteState !== 'boolean') {
    throw invalid('msg.dapr.actor.deleteState must be a boolean');
  }

  if (outcome === 'fail') {
    if (deleteState === true) {
      throw invalid('msg.dapr.actor.deleteState is not valid for a fail reply');
    }
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
  const hasNextState = Object.hasOwn(actor, 'nextState');
  if (deleteState === true) {
    // Checked before nextState is ever serialized: a delete proposal carries
    // no nextStateJson at all, so a conflicting nextState is rejected by its
    // mere presence, not by whatever value it happens to hold.
    if (hasNextState) {
      throw invalid(
        'msg.dapr.actor.nextState must not be present when msg.dapr.actor.deleteState is true'
      );
    }
    proposal.deleteState = true;
  } else if (hasNextState) {
    proposal.nextStateJson = boundedJson(actor.nextState, 'msg.dapr.actor.nextState', maxBodyBytes);
  }
  return proposal;
}

// A reminder's dueTime/period/ttl: absent or an empty string means "omit this
// field"; anything else must be a bounded string. `null` and other non-string values
// are rejected rather than silently treated as absent -- a caller that meant
// to clear a field should send "", not null.
function validateScheduleTime(value, label) {
  if (value === undefined || value === '') {
    return undefined;
  }
  if (typeof value !== 'string') {
    throw invalid(`${label} must be a string`);
  }
  if (value.length > MAX_SCHEDULE_TIME_LENGTH) {
    throw invalid(`${label} must be at most ${MAX_SCHEDULE_TIME_LENGTH} characters`);
  }
  return value;
}

// `overwrite` defaults to true (Dapr's own default) but is always resolved to
// an explicit boolean, never omitted from the wire body -- see
// lib/actor-client.js's setReminder.
function validateOverwrite(value) {
  if (value === undefined) {
    return true;
  }
  if (typeof value !== 'boolean') {
    throw invalid('overwrite must be a boolean');
  }
  return value;
}

// The exact byte length daprd's reminder callback would use for this JSON
// text, HTML-escaped the way Go's json encoder escapes "<", ">" and "&" by
// default (net +5 bytes), and U+2028/U+2029 (net +3 bytes).
// Mirrors boundedJson's byte check above, but against that
// escaped length rather than the raw one, since escaping only ever grows it.
function escapedByteLength(json) {
  let bytes = Buffer.byteLength(json, 'utf8');
  for (const ch of json) {
    if (ch === '<' || ch === '>' || ch === '&') {
      bytes += 5;
    } else if (ch === '\u2028' || ch === '\u2029') {
      bytes += 3;
    }
  }
  return bytes;
}

// Validate and shape a reminder's `data` for lib/actor-client.js's
// setReminder: `hasData` distinguishes "no data key at all" (msg.payload
// absent) from an explicit `null`/other falsy value, matching
// dapr-actor-call's own absent-vs-null handling. Bounds the callback
// envelope's eventual worst-case wire size (data's HTML-escaped JSON plus the
// fixed 34-byte wrapper) against the connection's body limit up front, at
// schedule time, rather than only discovering an oversized reminder when
// daprd eventually calls back.
function prepareReminderData(value, hasData, maxBodyBytes) {
  if (!hasData) {
    return { hasData: false, data: undefined };
  }
  assertSerializable(value, 0, new WeakSet());
  const json = JSON.stringify(value);
  if (
    typeof maxBodyBytes === 'number' &&
    escapedByteLength(json) + REMINDER_ENVELOPE_BYTES > maxBodyBytes
  ) {
    throw invalid(`reminder data exceeds the connection's body limit (${maxBodyBytes} bytes)`);
  }
  // Retain the JSON that was bounded: getters/toJSON must not run again
  // when actor-client serializes the enclosing schedule request.
  return { hasData: true, data: JSON.parse(json) };
}

module.exports = {
  validateActorSegment,
  serializeProposal,
  assertSerializable,
  REMINDER_METHOD,
  validateScheduleTime,
  validateOverwrite,
  prepareReminderData,
};
