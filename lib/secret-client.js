'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { sidecarRequest } = require('./sidecar-http');

const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

function validateSegment(value, label) {
  const segment = String(value ?? '');
  if (segment === '' || segment === '.' || segment === '..') {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid ${label}: ${value}`);
  }
  return segment;
}

// Build the sidecar secret path from a store name and key, rejecting
// anything that could escape /v1.0/secrets/<store>/<key> the same way
// lib/state-client.js's buildStatePath protects the state route.
function buildSecretPath(storeName, key) {
  const store = validateSegment(storeName, 'secret store name');
  if (store.includes('/')) {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid secret store name: ${storeName}`);
  }
  const secretKey = validateSegment(key, 'secret key');
  return `/v1.0/secrets/${encodeURIComponent(store)}/${encodeURIComponent(secretKey)}`;
}

// Dapr's own generic "metadata.*" query-param passthrough -- own local copy
// rather than importing lib/configuration-client.js's buildQueryString,
// which also handles repeated "key" params this endpoint has no use for
// (the secret's key is a path segment, not a query param).
function buildMetadataQueryString(metadata) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(metadata || {})) {
    params.append(`metadata.${key}`, value);
  }
  const qs = params.toString();
  return qs ? `?${qs}` : '';
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
      `Dapr secret get could not reach the sidecar: ${err.message}`,
      { cause: err }
    );
  }
}

// GET /v1.0/secrets/<store>/<key>?metadata.*=...: resolves { data, status }.
// data is the component's own map -- Dapr's own proto doc says explicitly a
// secret store may hold more than one key/value pair per secret, so this is
// never assumed to be exactly one. 204 (component returned success with no
// data) resolves { data: null, status: 204 }.
//
// Every non-2xx/204 error below is a FIXED, GENERIC message with NO daprd
// response body included, ever -- a deliberate departure from every other
// *-client.js in this package (which all include a truncated raw error
// body). Real daprd 1.18.2 embeds the requested secret's key/name directly
// in every one of its own secrets error messages (confirmed against
// pkg/messages/predefined.go and secretstores/local/file's own "secret %s
// not found"), so forwarding that text here would be exactly the kind of
// leak this node exists to prevent.
async function getSecret({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const headers = {};
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const result = await callSidecar(baseUrl, {
    path:
      buildSecretPath(request.storeName, request.key) + buildMetadataQueryString(request.metadata),
    method: 'GET',
    headers,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status === 204) {
    return { data: null, status: result.status };
  }
  if (result.status === 403) {
    throw new DaprError(ErrorCodes.SECRET_ACCESS_DENIED, 'Dapr secret access denied');
  }
  if (result.status < 200 || result.status > 299) {
    throw new DaprError(
      ErrorCodes.SECRET_OPERATION_FAILED,
      `Dapr secret get failed with status ${result.status}`
    );
  }
  let data;
  try {
    data = JSON.parse(result.body.toString());
    if (!isPlainObject(data)) {
      throw new TypeError('expected a JSON object');
    }
  } catch {
    throw new DaprError(
      ErrorCodes.SECRET_OPERATION_FAILED,
      'Dapr secret get returned a malformed response'
    );
  }
  return { data, status: result.status };
}

module.exports = { buildSecretPath, getSecret };
