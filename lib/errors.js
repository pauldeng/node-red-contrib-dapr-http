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
  PENDING_CAPACITY: 'PENDING_CAPACITY',
  DUPLICATE_PENDING: 'DUPLICATE_PENDING',
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
