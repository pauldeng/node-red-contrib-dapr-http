'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { sidecarRequest } = require('./sidecar-http');

// How many characters of the sidecar's own error body to surface in a failure
// message. Mirrors lib/dapr-client.js's and lib/state-client.js's own bound.
const ERROR_BODY_CHARS = 200;

const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

function validateSegment(value, label) {
  const segment = String(value ?? '');
  if (segment === '' || segment === '.' || segment === '..') {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid ${label}: ${value}`);
  }
  return segment;
}

// Build the sidecar configuration path from a store name, rejecting anything
// that could escape /v1.0/configuration/<store> the same way
// lib/state-client.js's buildStatePath protects the state route:
// encodeURIComponent leaves "." and ".." unchanged, so a segment equal to
// exactly one of those must be rejected before encoding, not just encoded.
function buildConfigurationPath(storeName) {
  const store = validateSegment(storeName, 'configuration store name');
  if (store.includes('/')) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `invalid configuration store name: ${storeName}`
    );
  }
  return `/v1.0/configuration/${encodeURIComponent(store)}`;
}

// daprd builds callback URLs from the configured store and key. Canonicalize
// the same path before registering it on the app channel, while rejecting the
// URL delimiters and traversal segments that cannot name one stable route.
function buildConfigurationCallbackPath(storeName, key) {
  const store = validateSegment(storeName, 'configuration store name');
  const item = validateSegment(key, 'configuration key');
  const segments = item.split('/');
  if (
    store.includes('/') ||
    /[?#]/.test(store) ||
    /[?#]/.test(item) ||
    segments.some((segment) => segment === '' || segment === '.' || segment === '..')
  ) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `invalid configuration callback route: ${storeName}/${key}`
    );
  }
  return new URL(`/configuration/${store}/${item}`, 'http://localhost').pathname;
}

// Dapr's Get/Subscribe configuration query string: a repeated "key" param per
// watched key, plus "metadata.*" -- lib/sidecar-http.js's own `query` option
// takes a plain object (one value per key via Object.entries), which cannot
// express a repeated param, so the query string is built here and appended
// directly to `path` instead. URLSearchParams percent-encodes every value,
// matching what sidecarRequest's own query handling would have done.
function buildQueryString(keys, metadata) {
  const params = new URLSearchParams();
  for (const key of keys || []) {
    params.append('key', key);
  }
  for (const [key, value] of Object.entries(metadata || {})) {
    params.append(`metadata.${key}`, value);
  }
  const qs = params.toString();
  return qs ? `?${qs}` : '';
}

function throwConfigurationError(operation, result) {
  const detail = result.body.toString().slice(0, ERROR_BODY_CHARS).trim();
  throw new DaprError(
    ErrorCodes.CONFIGURATION_OPERATION_FAILED,
    `Dapr configuration ${operation} failed with status ${result.status}${detail ? `: ${detail}` : ''}`
  );
}

async function callSidecar(operation, baseUrl, options) {
  try {
    return await sidecarRequest(baseUrl, options);
  } catch (err) {
    if (err instanceof DaprError) {
      throw err;
    }
    throw new DaprError(
      ErrorCodes.SIDECAR_UNAVAILABLE,
      `Dapr configuration ${operation} could not reach the sidecar: ${err.message}`,
      { cause: err }
    );
  }
}

// GET /v1.0/configuration/<store>?key=...&metadata.*=...: key is repeated,
// omitted entirely for "all keys". Resolves { items, status }. The response
// is the raw, unwrapped {key: {value, version, metadata}} map -- a 204 (no
// items) resolves an empty object, not an error.
async function getConfiguration({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const headers = {};
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const result = await callSidecar('get', baseUrl, {
    path:
      buildConfigurationPath(request.storeName) + buildQueryString(request.keys, request.metadata),
    method: 'GET',
    headers,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status === 204) {
    return { items: {}, status: result.status };
  }
  if (result.status < 200 || result.status > 299) {
    throwConfigurationError('get', result);
  }
  let items;
  try {
    items = JSON.parse(result.body.toString());
    if (!isPlainObject(items)) {
      throw new TypeError('expected a JSON object');
    }
  } catch (err) {
    throw new DaprError(
      ErrorCodes.CONFIGURATION_OPERATION_FAILED,
      `Dapr configuration get returned a malformed response: ${err.message}`,
      { cause: err }
    );
  }
  return { items, status: result.status };
}

// GET /v1.0/configuration/<store>/subscribe?key=...&metadata.*=...: no
// discovery step -- daprd calls back into the app channel directly and
// immediately when a watched key changes. Resolves { id, status }.
async function subscribeConfiguration(
  { baseUrl, token, timeoutMs, signal, maxResponseBytes },
  request
) {
  const headers = {};
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const result = await callSidecar('subscribe', baseUrl, {
    path:
      `${buildConfigurationPath(request.storeName)}/subscribe` +
      buildQueryString(request.keys, request.metadata),
    method: 'GET',
    headers,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status < 200 || result.status > 299) {
    throwConfigurationError('subscribe', result);
  }
  let id;
  try {
    const parsed = JSON.parse(result.body.toString());
    id = isPlainObject(parsed) ? parsed.id : undefined;
  } catch (err) {
    throw new DaprError(
      ErrorCodes.CONFIGURATION_OPERATION_FAILED,
      `Dapr configuration subscribe returned a malformed response: ${err.message}`,
      { cause: err }
    );
  }
  if (typeof id !== 'string' || id === '') {
    throw new DaprError(
      ErrorCodes.CONFIGURATION_OPERATION_FAILED,
      'Dapr configuration subscribe returned no subscription id'
    );
  }
  return { id, status: result.status };
}

// GET /v1.0/configuration/<store>/<subscriptionId>/unsubscribe -- GET, not
// POST/DELETE (verified against real daprd 1.18.1 source). The v1.0 and
// v1.0-alpha1 prefixes are wired to the identical handler and the same
// underlying component Unsubscribe call, so only the stable v1.0 prefix is
// used here. Resolves { status } on 2xx; a failure body is {"ok":false,
// "message":...} with no errorCode field, unlike get/subscribe's generic
// envelope -- throwConfigurationError only ever includes the truncated raw
// body text, so this needs no special-casing.
async function unsubscribeConfiguration(
  { baseUrl, token, timeoutMs, signal, maxResponseBytes },
  request
) {
  const headers = {};
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const result = await callSidecar('unsubscribe', baseUrl, {
    path: `${buildConfigurationPath(request.storeName)}/${encodeURIComponent(
      validateSegment(request.subscriptionId, 'subscription id')
    )}/unsubscribe`,
    method: 'GET',
    headers,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status < 200 || result.status > 299) {
    throwConfigurationError('unsubscribe', result);
  }
  return { status: result.status };
}

module.exports = {
  buildConfigurationCallbackPath,
  buildConfigurationPath,
  getConfiguration,
  subscribeConfiguration,
  unsubscribeConfiguration,
};
