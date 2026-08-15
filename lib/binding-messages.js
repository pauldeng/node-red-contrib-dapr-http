'use strict';

const {
  hasOwn,
  invalid,
  requiredString,
  daprOverride,
  resolvedMetadata,
} = require('./message-fields');

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
