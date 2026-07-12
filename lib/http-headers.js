'use strict';

const http = require('node:http');

const { DaprError, ErrorCodes } = require('./errors');

// The canonical hop-by-hop and framing header set. These are meaningful only on
// a single transport hop: node:http computes Content-Length / Transfer-Encoding
// itself, and forwarding or exposing them (a wrong Content-Length, a leaked
// proxy-authorization credential) is either unsafe or corrupts framing. Shared
// by outbound-request filtering, inbound-request exposure, and response writing
// so all three agree on one list.
const HOP_BY_HOP = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
  'host',
  'content-length',
]);

// Build a safe response-header object: reject non-object input (a string or
// array would enumerate into numeric header names), drop framing/hop-by-hop
// headers, and drop any entry whose name or value Node's HTTP validators reject
// (illegal tokens, CRLF header injection), coercing values to strings. Never
// throws.
function sanitizeResponseHeaders(headers) {
  const out = {};
  if (!headers || typeof headers !== 'object' || Array.isArray(headers)) {
    return out;
  }
  for (const [name, value] of Object.entries(headers)) {
    if (HOP_BY_HOP.has(name.toLowerCase())) {
      continue;
    }
    const str = String(value);
    try {
      http.validateHeaderName(name);
      http.validateHeaderValue(name, str);
    } catch {
      continue;
    }
    out[name] = str;
  }
  return out;
}

// Validate a caller-supplied Content-Type: undefined/null/'' means "not set"
// (the caller falls back to an inferred type); otherwise it must be a string
// that Node's header-value validator accepts. Rejects non-string values (a
// number or object would otherwise stringify to "42" or "[object Object]" and
// be sent as-is) and CRLF/illegal-token values. Throws DaprError(INVALID_MESSAGE).
function validateContentType(value) {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  if (typeof value !== 'string') {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `contentType must be a string: ${value}`);
  }
  try {
    http.validateHeaderValue('content-type', value);
  } catch {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid contentType: ${value}`);
  }
  return value;
}

module.exports = { sanitizeResponseHeaders, validateContentType, HOP_BY_HOP };
