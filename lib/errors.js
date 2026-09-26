'use strict';

// Stable internal error codes. Values equal their keys so a code round-trips
// through logs and comparisons without a lookup table.
const ErrorCodes = {
  INVALID_OPTIONS: 'INVALID_OPTIONS',
  DUPLICATE_LISTENER: 'DUPLICATE_LISTENER',
  SIDECAR_UNAVAILABLE: 'SIDECAR_UNAVAILABLE',
  RESPONSE_TOO_LARGE: 'RESPONSE_TOO_LARGE',
  INVALID_MESSAGE: 'INVALID_MESSAGE',
  PUBLISH_FAILED: 'PUBLISH_FAILED',
  BULK_PUBLISH_PARTIAL: 'BULK_PUBLISH_PARTIAL',
  PENDING_CAPACITY: 'PENDING_CAPACITY',
  DUPLICATE_PENDING: 'DUPLICATE_PENDING',
  STATE_OPERATION_FAILED: 'STATE_OPERATION_FAILED',
  STATE_ETAG_MISMATCH: 'STATE_ETAG_MISMATCH',
  CONFIGURATION_OPERATION_FAILED: 'CONFIGURATION_OPERATION_FAILED',
  BINDING_INVOKE_FAILED: 'BINDING_INVOKE_FAILED',
  SECRET_OPERATION_FAILED: 'SECRET_OPERATION_FAILED',
  SECRET_ACCESS_DENIED: 'SECRET_ACCESS_DENIED',
  METADATA_OPERATION_FAILED: 'METADATA_OPERATION_FAILED',
  // Actor host/client (lib/actor-host.js, lib/actor-client.js): admission
  // refused because the same (actorType, actorId) is already being handled, or
  // the connection's active-handler budget is exhausted.
  ACTOR_BUSY: 'ACTOR_BUSY',
  // The flow's reply proposal did not arrive before the invocation's reserved
  // deadline (or the connection drained while still waiting) -- no commit runs.
  ACTOR_REPLY_EXPIRED: 'ACTOR_REPLY_EXPIRED',
  // A call node invoked the same actor type+id it is currently handling a
  // method for -- reserved for nodes/dapr-actor-call.js's guard.
  ACTOR_SELF_CALL: 'ACTOR_SELF_CALL',
  // An actor-state commit's transport failed, timed out, or aborted after the
  // write was sent: the database may or may not have applied it.
  ACTOR_COMMIT_UNKNOWN: 'ACTOR_COMMIT_UNKNOWN',
  // A non-2xx from daprd's actor method-invoke endpoint -- a definite, confirmed
  // failure (unlike ACTOR_COMMIT_UNKNOWN's transport ambiguity). No existing
  // code distinguishes this from a plain state-operation failure, so it is its
  // own code; see lib/actor-client.js.
  ACTOR_INVOKE_FAILED: 'ACTOR_INVOKE_FAILED',
  // A non-2xx from daprd's reminder set/get/delete endpoints -- mirrors
  // ACTOR_INVOKE_FAILED's shape (a definite, confirmed failure with a bounded
  // cause), but kept distinct since a reminder schedule failure (e.g. a 409
  // ERR_ACTOR_REMINDER_ALREADY_EXISTS) is a different business outcome from a
  // failed method call; see lib/actor-client.js's setReminder/getReminder/
  // deleteReminder.
  ACTOR_SCHEDULE_FAILED: 'ACTOR_SCHEDULE_FAILED',
};

// A typed error carrying one of the codes above. Used for internal control
// flow and node status; HTTP responses never echo these — the app-channel maps
// them to fixed, safe messages so nothing internal leaks over the wire.
class DaprError extends Error {
  constructor(code, message, options = {}) {
    super(message, options);
    this.name = 'DaprError';
    this.code = code;
  }
}

module.exports = { DaprError, ErrorCodes };
