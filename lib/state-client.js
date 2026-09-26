'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { decodeBody } = require('./invoke-client');
const { sidecarRequest } = require('./sidecar-http');

// How many characters of the sidecar's own error body to surface in a failure
// message. Enough for daprd's errorCode/message, short enough not to dump a
// large body into the Node-RED log. Mirrors lib/dapr-client.js's own bound.
const ERROR_BODY_CHARS = 200;

function validateSegment(value, label) {
  const segment = String(value ?? '');
  if (segment === '' || segment === '.' || segment === '..') {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid ${label}: ${value}`);
  }
  return segment;
}

// Build the sidecar state path from a store name and an optional key,
// rejecting anything that could escape /v1.0/state/<store>[/<key>] the same
// way lib/invoke-client.js's buildInvokePath protects the invoke route:
// encodeURIComponent leaves "." and ".." unchanged, so a segment equal to
// exactly one of those must be rejected before encoding, not just encoded.
// The store name is a single identifier (like an app id or pubsub name
// elsewhere in this package) and may not contain a "/"; a key is arbitrary
// caller data and may, since encodeURIComponent escapes "/" safely.
function buildStatePath(storeName, key) {
  const store = validateSegment(storeName, 'state store name');
  if (store.includes('/')) {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid state store name: ${storeName}`);
  }
  let path = `/v1.0/state/${encodeURIComponent(store)}`;
  if (key !== undefined) {
    path += `/${encodeURIComponent(validateSegment(key, 'state key'))}`;
  }
  return path;
}

// Shared status-code translation for every state operation: a 409 is always
// an etag conflict (an actionable, distinguishable outcome so a Catch node
// can reload and retry with a fresh etag), anything else outside 2xx is a
// generic operation failure. Applied identically across get/save/delete/
// bulkGet/transaction, so a store that surfaces a 409 on an operation real
// daprd's handler code has no confirmed branch for (transaction, save) is
// already handled correctly without touching this file again.
function throwStateError(operation, result) {
  const detail = result.body.toString().slice(0, ERROR_BODY_CHARS).trim();
  const suffix = detail ? `: ${detail}` : '';
  if (result.status === 409) {
    throw new DaprError(
      ErrorCodes.STATE_ETAG_MISMATCH,
      `Dapr state ${operation} failed with status 409 (etag mismatch)${suffix}`
    );
  }
  throw new DaprError(
    ErrorCodes.STATE_OPERATION_FAILED,
    `Dapr state ${operation} failed with status ${result.status}${suffix}`
  );
}

// Shared error-translation point for every sidecar-calling client in this
// package (state, actor): a transport failure/abort/timeout becomes
// SIDECAR_UNAVAILABLE, since the caller cannot tell whether daprd ever
// received the request; a DaprError already thrown (e.g. RESPONSE_TOO_LARGE)
// passes through unchanged. `label` is the full "<domain> <operation>" phrase
// embedded in the message, e.g. "state get" or "actor invoke".
async function callSidecar(label, baseUrl, options) {
  try {
    return await sidecarRequest(baseUrl, options);
  } catch (err) {
    if (err instanceof DaprError) {
      throw err;
    }
    throw new DaprError(
      ErrorCodes.SIDECAR_UNAVAILABLE,
      `Dapr ${label} could not reach the sidecar: ${err.message}`,
      { cause: err }
    );
  }
}

// GET /v1.0/state/<store>/<key>: consistency and metadata.* as query params.
// Resolves { value, etag, status }. A 204 (key not found) is a normal
// outcome -- value: null, etag: undefined -- not an error; only a status
// outside {200, 204} rejects.
async function stateGet({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const query = {};
  if (request.consistency) {
    query.consistency = request.consistency;
  }
  for (const [key, value] of Object.entries(request.metadata || {})) {
    query[`metadata.${key}`] = value;
  }
  const headers = {};
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const result = await callSidecar('state get', baseUrl, {
    path: buildStatePath(request.storeName, request.key),
    method: 'GET',
    headers,
    query,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status === 204) {
    return { value: null, etag: undefined, status: result.status };
  }
  if (result.status >= 200 && result.status <= 299) {
    return {
      value: decodeBody(result.body, result.headers['content-type']),
      etag: result.headers.etag,
      status: result.status,
    };
  }
  throwStateError('get', result);
}

// POST /v1.0/state/<store>: a single-item array, since Dapr's save endpoint
// is array-shaped on the wire but this node's own UX stays single-item.
// request.item is already wire-shaped by lib/state-messages.js (etag/
// metadata/options included only when set) -- this function only transports
// it. Resolves { status } on 2xx.
async function stateSave({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const headers = { 'content-type': 'application/json' };
  if (token) {
    headers['dapr-api-token'] = token;
  }

  let body;
  try {
    body = Buffer.from(JSON.stringify([request.item]));
  } catch (err) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `state save value cannot be serialized: ${err.message}`,
      { cause: err }
    );
  }

  const result = await callSidecar('state save', baseUrl, {
    path: buildStatePath(request.storeName),
    method: 'POST',
    headers,
    body,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status < 200 || result.status > 299) {
    throwStateError('save', result);
  }
  return { status: result.status };
}

// DELETE /v1.0/state/<store>/<key>: concurrency/consistency/metadata.* as
// query params, etag as the If-Match request header (not a query param --
// verified against real daprd's extractEtag reading r.Header["If-Match"]).
// Resolves { status } on 2xx.
async function stateDelete({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const query = {};
  if (request.concurrency) {
    query.concurrency = request.concurrency;
  }
  if (request.consistency) {
    query.consistency = request.consistency;
  }
  for (const [key, value] of Object.entries(request.metadata || {})) {
    query[`metadata.${key}`] = value;
  }
  const headers = {};
  if (request.etag) {
    headers['if-match'] = request.etag;
  }
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const result = await callSidecar('state delete', baseUrl, {
    path: buildStatePath(request.storeName, request.key),
    method: 'DELETE',
    headers,
    query,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status < 200 || result.status > 299) {
    throwStateError('delete', result);
  }
  return { status: result.status };
}

// POST /v1.0/state/<store>/bulk: {keys, parallelism, metadata}. The response
// is one JSON document -- [{key, data, etag, metadata, error}] -- so each
// item's `data` is already a decoded value, never re-decoded here. A
// per-item `error` does not fail the call; only a whole-call non-2xx does
// (real daprd's own ERR_STATE_BULK_GET, a transport-level failure to the
// backing store). Resolves { items, status }.
async function stateBulkGet({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const headers = { 'content-type': 'application/json' };
  if (token) {
    headers['dapr-api-token'] = token;
  }

  let body;
  try {
    body = Buffer.from(
      JSON.stringify({
        keys: request.keys,
        parallelism: request.parallelism,
        metadata: request.metadata,
      })
    );
  } catch (err) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `state bulk get request cannot be serialized: ${err.message}`,
      { cause: err }
    );
  }

  const result = await callSidecar('state bulkGet', baseUrl, {
    path: `${buildStatePath(request.storeName)}/bulk`,
    method: 'POST',
    headers,
    body,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status < 200 || result.status > 299) {
    throwStateError('bulkGet', result);
  }
  let items;
  try {
    items = JSON.parse(result.body.toString());
    if (!Array.isArray(items)) {
      throw new TypeError('expected a JSON array');
    }
  } catch (err) {
    throw new DaprError(
      ErrorCodes.STATE_OPERATION_FAILED,
      `Dapr state bulkGet returned a malformed response: ${err.message}`,
      { cause: err }
    );
  }
  return { items, status: result.status };
}

// POST /v1.0/state/<store>/transaction: {operations, metadata}. operations
// is already wire-shaped by lib/state-messages.js
// ([{operation: "upsert"|"delete", request: {key, value?, etag?, metadata?,
// options?}}]). An empty operations array still sends and still expects 204
// (real daprd short-circuits without touching the store). Resolves
// { status } on 2xx.
async function stateTransaction({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const headers = { 'content-type': 'application/json' };
  if (token) {
    headers['dapr-api-token'] = token;
  }

  let body;
  try {
    body = Buffer.from(
      JSON.stringify({ operations: request.operations, metadata: request.metadata })
    );
  } catch (err) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `state transaction operations cannot be serialized: ${err.message}`,
      { cause: err }
    );
  }

  const result = await callSidecar('state transaction', baseUrl, {
    path: `${buildStatePath(request.storeName)}/transaction`,
    method: 'POST',
    headers,
    body,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status < 200 || result.status > 299) {
    throwStateError('transaction', result);
  }
  return { status: result.status };
}

module.exports = {
  buildStatePath,
  callSidecar,
  stateGet,
  stateSave,
  stateDelete,
  stateBulkGet,
  stateTransaction,
};
