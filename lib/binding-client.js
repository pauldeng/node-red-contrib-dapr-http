'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { sidecarRequest } = require('./sidecar-http');

// How many characters of the sidecar's own error body to surface in a failure
// message. Mirrors lib/state-client.js's and lib/configuration-client.js's own
// bound.
const ERROR_BODY_CHARS = 200;

function validateSegment(value, label) {
  const segment = String(value ?? '');
  if (segment === '' || segment === '.' || segment === '..') {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid ${label}: ${value}`);
  }
  return segment;
}

// Build the sidecar binding path from a binding name, rejecting anything that
// could escape /v1.0/bindings/<name> the same way lib/state-client.js's
// buildStatePath protects the state route: encodeURIComponent leaves "." and
// ".." unchanged, so a segment equal to exactly one of those must be rejected
// before encoding, not just encoded.
function buildBindingPath(bindingName) {
  const name = validateSegment(bindingName, 'binding name');
  if (name.includes('/')) {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid binding name: ${bindingName}`);
  }
  return `/v1.0/bindings/${encodeURIComponent(name)}`;
}

function throwBindingError(result) {
  const detail = result.body.toString().slice(0, ERROR_BODY_CHARS).trim();
  throw new DaprError(
    ErrorCodes.BINDING_INVOKE_FAILED,
    `Dapr binding invoke failed with status ${result.status}${detail ? `: ${detail}` : ''}`
  );
}

async function callSidecar(baseUrl, options) {
  try {
    return await sidecarRequest(baseUrl, options);
  } catch (err) {
    if (err instanceof DaprError) {
      throw err;
    }
    throw new DaprError(
      ErrorCodes.SIDECAR_UNAVAILABLE,
      `Dapr binding invoke could not reach the sidecar: ${err.message}`,
      { cause: err }
    );
  }
}

// POST /v1.0/bindings/<name>: {data, metadata, operation}. Every failure mode
// daprd can report for this endpoint -- binding not found, operation
// unsupported by that binding, or the component's own operation failing --
// collapses to the same 500 ERR_INVOKE_OUTPUT_BINDING (confirmed against real
// daprd 1.18.1 source: onOutputBindingMessage has exactly one err!=nil
// branch). Resolves { status, headers, body } on 200 (component response, not
// JSON-enveloped) or 204 (no data, no error) -- the caller decides what a 204
// means, this function only transports.
async function invokeBinding({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const headers = { 'content-type': 'application/json' };
  if (token) {
    headers['dapr-api-token'] = token;
  }

  let body;
  try {
    body = Buffer.from(
      JSON.stringify({
        data: request.data,
        metadata: request.metadata,
        operation: request.operation,
      })
    );
  } catch (err) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `binding data cannot be serialized: ${err.message}`,
      { cause: err }
    );
  }

  const result = await callSidecar(baseUrl, {
    path: buildBindingPath(request.bindingName),
    method: 'POST',
    headers,
    body,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status >= 200 && result.status <= 299) {
    return result;
  }
  throwBindingError(result);
}

module.exports = { buildBindingPath, invokeBinding };
