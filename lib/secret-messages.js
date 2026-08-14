'use strict';

const { DaprError, ErrorCodes } = require('./errors');

// Own local copy of these tiny validators rather than importing from
// lib/state-messages.js/lib/configuration-messages.js/lib/binding-messages.js
// -- each module in this package owns its own (see lib/options.js beside
// lib/messages.js).
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

function resolvedMetadata(config, dapr) {
  const configuredMetadata = metadataObject(config.metadata, 'configured metadata');
  const messageMetadata = hasOwn(dapr, 'metadata')
    ? metadataObject(dapr.metadata, 'msg.dapr.metadata')
    : {};
  return { ...configuredMetadata, ...messageMetadata };
}

// GET /v1.0/secrets/<store>/<key>: message-triggered, so msg.dapr overrides
// storeName/key/metadata for this one call. The output message property is
// deliberately NOT resolved here -- it's a config-only, non-overridable
// detail the node itself applies, not part of the request shape sent to
// daprd.
function prepareSecretGet(config, msg) {
  const dapr = daprOverride(msg);
  const storeName = requiredString(
    hasOwn(dapr, 'storeName') ? dapr.storeName : config.storeName,
    'storeName'
  );
  const key = requiredString(hasOwn(dapr, 'key') ? dapr.key : config.key, 'key');
  return { storeName, key, metadata: resolvedMetadata(config, dapr) };
}

// The configured output property must be a static message path: not
// msg.dapr (reserved for this node's own result metadata), not a dynamic/
// computed segment (a nested msg[...] lookup, which would reintroduce the
// exact "message picks its own destination" risk of a msg.dapr-overridable
// property), and never __proto__ (defense in depth against prototype
// pollution through RED.util.setMessageProperty's createMissing path).
// normalisePropertyExpression is injected rather than imported, so this
// module stays free of any Node-RED import -- the node passes its own
// RED.util.normalisePropertyExpression.
function resolveSecretProperty(value, normalisePropertyExpression) {
  const property = typeof value === 'string' && value.trim() ? value.trim() : 'payload';
  const expression = property.startsWith('msg.') ? property.slice(4) : property;
  let parts;
  try {
    parts = normalisePropertyExpression(expression);
  } catch {
    throw invalid('property must be a valid static message property path');
  }
  if (parts[0] === 'dapr' || parts.includes('__proto__') || parts.some(Array.isArray)) {
    throw invalid('property must be a static message path outside msg.dapr');
  }
  return property;
}

module.exports = { prepareSecretGet, resolveSecretProperty };
