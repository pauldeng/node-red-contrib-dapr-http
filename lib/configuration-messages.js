'use strict';

const {
  hasOwn,
  invalid,
  requiredString,
  requiredKey,
  metadataObject,
  daprOverride,
  resolvedMetadata,
} = require('./message-fields');

// Configuration keys are kept byte-exact (not trimmed), the same as
// lib/state-messages.js's requiredKey -- Dapr treats them as arbitrary
// caller data, distinct from configuration identifiers like the store name.

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
