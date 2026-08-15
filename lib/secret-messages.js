'use strict';

const {
  hasOwn,
  invalid,
  requiredString,
  daprOverride,
  resolvedMetadata,
} = require('./message-fields');

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
