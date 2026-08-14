'use strict';

const { DaprError, ErrorCodes } = require('./errors');

// Own local copy of these tiny validators rather than importing from
// lib/state-messages.js/lib/configuration-messages.js -- each module in this
// package owns its own (see lib/options.js beside lib/messages.js).
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

// POST /v1.0/bindings/<name>: message-triggered, so msg.dapr overrides
// bindingName/operation/metadata for this one call. A Buffer payload is
// rejected rather than silently auto-base64-encoded, the same choice
// lib/state-messages.js's save already makes, since a component's own binary
// convention is not ours to guess. The client JSON-serializes the completed
// request envelope once before transport.
function prepareBindingRequest(config, msg) {
  const dapr = daprOverride(msg);
  const bindingName = requiredString(
    hasOwn(dapr, 'bindingName') ? dapr.bindingName : config.bindingName,
    'bindingName'
  );
  const operation = requiredString(
    hasOwn(dapr, 'operation') ? dapr.operation : config.operation,
    'operation'
  );
  if (Buffer.isBuffer(msg.payload)) {
    throw invalid('binding invoke does not accept a Buffer payload -- base64-encode it yourself');
  }
  return {
    bindingName,
    operation,
    data: msg.payload,
    metadata: resolvedMetadata(config, dapr),
  };
}

module.exports = { prepareBindingRequest };
