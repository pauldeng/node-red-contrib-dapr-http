'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { callSidecar } = require('./state-client');
const { validateActorSegment } = require('./actor-messages');

// Build /v1.0/actors/<type>/<id>[/<rest>...], validating and percent-encoding
// every segment -- mirrors lib/invoke-client.js's buildInvokePath and
// lib/state-client.js's buildStatePath: a segment equal to "." or ".." (or
// containing "/") is rejected before encoding, never merely encoded, so it
// cannot escape this route.
function buildActorPath(actorType, actorId, ...rest) {
  const type = validateActorSegment(actorType, 'actor type');
  const id = validateActorSegment(actorId, 'actor id');
  let path = `/v1.0/actors/${encodeURIComponent(type)}/${encodeURIComponent(id)}`;
  for (const segment of rest) {
    path += `/${encodeURIComponent(validateActorSegment(segment, 'actor path segment'))}`;
  }
  return path;
}

// The app API token header, present only when configured.
function tokenHeaders(token, base = {}) {
  return token ? { ...base, 'dapr-api-token': token } : base;
}

// POST /v1.0/actors/{type}/{id}/method/{method}: invoke a method on an actor
// through the local sidecar. `body`, when present, is JSON-encoded; absent
// sends no request body. Resolves { status, headers, body } on 2xx; a
// confirmed non-2xx throws ACTOR_INVOKE_FAILED -- distinct from
// callSidecar's SIDECAR_UNAVAILABLE, which lib/actor-host.js relies on to
// tell a definite failure apart from an unknown-outcome transport error.
async function invoke({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const path = buildActorPath(request.actorType, request.actorId, 'method', request.method);
  const headers = tokenHeaders(token);
  let body;
  if (request.body !== undefined) {
    headers['content-type'] = 'application/json';
    body = Buffer.from(JSON.stringify(request.body));
  }
  const result = await callSidecar('actor invoke', baseUrl, {
    path,
    method: 'POST',
    headers,
    body,
    timeoutMs,
    signal,
    maxResponseBytes,
  });
  if (result.status < 200 || result.status > 299) {
    // Node-RED Catch preserves Error.cause, but not arbitrary error fields.
    // Keep only bounded Dapr envelope diagnostics there; the stable package
    // code/message do not depend on daprd's version-specific wording.
    const cause = { statusCode: result.status };
    let remote;
    try {
      remote = JSON.parse(result.body.toString('utf8'));
    } catch {
      // A non-JSON error response still has a useful HTTP status.
    }
    if (typeof remote?.errorCode === 'string') cause.errorCode = remote.errorCode.slice(0, 128);
    if (typeof remote?.message === 'string') cause.message = remote.message.slice(0, 2048);
    throw new DaprError(
      ErrorCodes.ACTOR_INVOKE_FAILED,
      `Dapr actor invoke failed with status ${result.status}`,
      { cause }
    );
  }
  return result;
}

// GET /v1.0/actors/{type}/{id}/state/record: the package's one fixed state
// key ("record" -- v1 has exactly one state key per actor). Resolves
// { exists: false } on 204 (never activated / no record yet)
// or { exists: true, value } on 200 -- value is parsed JSON and may be null
// (Dapr distinguishes "no record" from "a stored null"). The body is parsed
// directly here, not through lib/invoke-client.js's decodeBody, which falls
// back to returning the raw string on a parse failure -- silently treating a
// malformed response as a valid (string) record would be worse than failing.
async function readRecord({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const path = buildActorPath(request.actorType, request.actorId, 'state', 'record');
  const headers = tokenHeaders(token);
  const result = await callSidecar('actor state read', baseUrl, {
    path,
    method: 'GET',
    headers,
    timeoutMs,
    signal,
    maxResponseBytes,
  });
  if (result.status === 204) {
    return { exists: false };
  }
  if (result.status >= 200 && result.status <= 299) {
    let value;
    try {
      value = JSON.parse(result.body.toString('utf8'));
    } catch (err) {
      throw new DaprError(
        ErrorCodes.STATE_OPERATION_FAILED,
        'Dapr actor state read returned a malformed response',
        { cause: err }
      );
    }
    return { exists: true, value };
  }
  throw new DaprError(
    ErrorCodes.STATE_OPERATION_FAILED,
    `Dapr actor state read failed with status ${result.status}`
  );
}

// POST /v1.0/actors/{type}/{id}/state: one upsert transaction for the
// `record` key. `valueJson` is already validated, bounded JSON text (from
// lib/actor-messages.js's serializeProposal) -- spliced directly into the
// transaction body rather than parsed and re-stringified, so it is inserted
// as JSON, not as a string containing JSON. No `signal` parameter: a caller
// disconnect must never abort a commit already under way (see
// docs/architecture.md's "Actor request ownership" section) -- omitting the
// parameter here makes that structurally true rather than a caller
// convention to remember.
async function saveRecord({ baseUrl, token, maxResponseBytes }, request) {
  const path = buildActorPath(request.actorType, request.actorId, 'state');
  const headers = tokenHeaders(token, { 'content-type': 'application/json' });
  const body = Buffer.from(
    `[{"operation":"upsert","request":{"key":"record","value":${request.valueJson}}}]`
  );
  const result = await callSidecar('actor state save', baseUrl, {
    path,
    method: 'POST',
    headers,
    body,
    timeoutMs: request.timeoutMs,
    maxResponseBytes,
  });
  if (result.status < 200 || result.status > 299) {
    throw new DaprError(
      ErrorCodes.STATE_OPERATION_FAILED,
      `Dapr actor state save failed with status ${result.status}`
    );
  }
  return { status: result.status };
}

module.exports = { buildActorPath, invoke, readRecord, saveRecord };
