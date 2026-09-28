'use strict';

const { DaprError, ErrorCodes } = require('./errors');

// The field validators every per-message request builder shares.
//
// These were previously copy-pasted, byte-identical, into state-, configuration-,
// binding-, and secret-messages.js. That is the shape of bug no linter can see:
// a fix to metadata validation had to land in four places, and nothing caught a
// missed one. One copy, four importers.
//
// Anything genuinely specific to one building block (state's consistency/
// concurrency enums, its TTL and transaction shapes, secret's output-property
// path rules) deliberately stays in that module — this file holds only what is
// actually common.

const { hasOwn } = Object;
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

function invalid(message) {
  return new DaprError(ErrorCodes.INVALID_MESSAGE, message);
}

// A required identifier (store name, binding name, operation, ...). Trimmed:
// surrounding whitespace in a configured identifier is always a typo.
function requiredString(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw invalid(`${field} is required`);
  }
  return value.trim();
}

// A required key. NOT trimmed, unlike requiredString: Dapr treats a state or
// configuration key as arbitrary caller data, where leading/trailing spaces can
// be significant. Only emptiness is rejected.
function requiredKey(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw invalid(`${field} is required`);
  }
  return value;
}

// Dapr component metadata: a flat map of scalars, coerced to the strings the
// wire format requires. Accepts a JSON string (the editor's textarea shape) or
// an already-parsed object (a msg.dapr override).
function metadataObject(value, field) {
  if (value === undefined || value === null || value === '') {
    return {};
  }
  let parsed = value;
  if (typeof value === 'string') {
    try {
      parsed = JSON.parse(value);
    } catch {
      throw invalid(`${field} must be valid JSON`);
    }
  }
  if (!isObject(parsed)) {
    throw invalid(`${field} must be a JSON object`);
  }
  const metadata = {};
  for (const [key, item] of Object.entries(parsed)) {
    if (
      key.trim() === '' ||
      (typeof item !== 'string' && typeof item !== 'number' && typeof item !== 'boolean')
    ) {
      throw invalid(`${field} keys must have scalar values`);
    }
    metadata[key] = String(item);
  }
  return metadata;
}

function daprOverride(msg) {
  const dapr = msg.dapr === undefined ? {} : msg.dapr;
  if (!isObject(dapr)) {
    throw invalid('msg.dapr must be an object');
  }
  return dapr;
}

// Configured metadata with the message's own merged over it (message wins).
function resolvedMetadata(config, dapr) {
  const configuredMetadata = metadataObject(config.metadata, 'configured metadata');
  const messageMetadata = hasOwn(dapr, 'metadata')
    ? metadataObject(dapr.metadata, 'msg.dapr.metadata')
    : {};
  return { ...configuredMetadata, ...messageMetadata };
}

module.exports = {
  hasOwn,
  isObject,
  invalid,
  requiredString,
  requiredKey,
  metadataObject,
  daprOverride,
  resolvedMetadata,
};
