'use strict';

const { DaprError, ErrorCodes } = require('./errors');

// Own local copies of these tiny validators rather than importing from
// lib/messages.js or lib/state-messages.js -- each module in this package
// owns its own (see lib/options.js beside lib/messages.js, and
// lib/state-messages.js beside lib/messages.js).
const hasOwn = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

function invalid(message) {
  return new DaprError(ErrorCodes.INVALID_MESSAGE, message);
}

function requiredString(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw invalid(`${field} is required`);
  }
  return value.trim();
}

// Configuration keys are kept byte-exact (not trimmed), the same as
// lib/state-messages.js's requiredKey -- Dapr treats them as arbitrary
// caller data, distinct from configuration identifiers like the store name.
function requiredKey(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw invalid(`${field} is required`);
  }
  return value;
}

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

// Configured keys are a newline-separated field (the editor's own textarea
// shape); a msg.dapr.keys override is an array. An empty configured field
// means "all keys" (Dapr's own get/subscribe semantics when no key query
// param is sent), so it resolves to an empty array, not an error.
function resolveKeys(raw, field) {
  if (raw === undefined || raw === null || raw === '') {
    return [];
  }
  if (Array.isArray(raw)) {
    return raw.map((key, index) => requiredKey(key, `${field} at index ${index}`));
  }
  if (typeof raw === 'string') {
    return raw
      .split('\n')
      .map((key) => key.trim())
      .filter((key) => key !== '');
  }
  throw invalid(`${field} must be an array or a newline-separated string`);
}

function resolvedMetadata(config, dapr) {
  const configuredMetadata = metadataObject(config.metadata, 'configured metadata');
  const messageMetadata = hasOwn(dapr, 'metadata')
    ? metadataObject(dapr.metadata, 'msg.dapr.metadata')
    : {};
  return { ...configuredMetadata, ...messageMetadata };
}

// GET /v1.0/configuration/<store>?key=...: message-triggered, so msg.dapr
// overrides storeName/keys/metadata for this one call.
function prepareConfigurationGet(config, msg) {
  const dapr = daprOverride(msg);
  const storeName = requiredString(
    hasOwn(dapr, 'storeName') ? dapr.storeName : config.storeName,
    'storeName'
  );
  const keys = resolveKeys(hasOwn(dapr, 'keys') ? dapr.keys : config.keys, 'keys');
  return { storeName, keys, metadata: resolvedMetadata(config, dapr) };
}

// GET /v1.0/configuration/<store>/subscribe?key=...: deploy-time only, no
// msg and no per-call override -- the node activates once from its own
// configured fields, exactly like dapr-subscribe's own construction-time
// validation. At least one key is required: subscribing to "everything" has
// no bounded callback-route set to register ahead of time.
function prepareConfigurationSubscribe(config) {
  const storeName = requiredString(config.storeName, 'storeName');
  const keys = resolveKeys(config.keys, 'keys');
  if (keys.length === 0) {
    throw invalid('subscribe requires at least one key to watch');
  }
  return { storeName, keys, metadata: metadataObject(config.metadata, 'configured metadata') };
}

module.exports = { prepareConfigurationGet, prepareConfigurationSubscribe };
