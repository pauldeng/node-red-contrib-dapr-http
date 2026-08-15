'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { HOP_BY_HOP } = require('./http-headers');
const { sidecarRequest } = require('./sidecar-http');

// Content types Dapr's HTTP pub/sub API treats as JSON documents.
const JSON_TYPES = new Set(['application/json', 'application/cloudevents+json']);

// Is this content type a JSON document? A media type carries parameters and is
// case-insensitive (RFC 9110 §8.3), so `application/json; charset=utf-8` is the
// same type as `application/json`; and any `+json` structured suffix
// (`application/vnd.example+json`, RFC 6839 §3.1) is JSON too. Matching the
// exact string instead would serialize an object payload as "[object Object]".
function isJsonType(contentType) {
  const type = String(contentType ?? '')
    .split(';')[0]
    .trim()
    .toLowerCase();
  return JSON_TYPES.has(type) || type.endsWith('+json');
}

// How many characters of the sidecar's own error body to surface in a failure
// message. Enough for daprd's errorCode/message, short enough not to dump a
// large body into the Node-RED log.
const ERROR_BODY_CHARS = 200;

// Serialize a publish payload for the resolved content type. A Buffer is always
// sent byte for byte; a JSON type is stringified; anything else is stringified
// as text. Falsy values (0, false, null, '') serialize as themselves — there is
// no "empty body" special case, which is exactly what the previous SDK path
// needed a truthy wrapper to work around.
function serializeBody(data, contentType) {
  if (Buffer.isBuffer(data)) {
    return data;
  }
  if (isJsonType(contentType)) {
    const json = JSON.stringify(data);
    return Buffer.from(json === undefined ? '' : json);
  }
  return Buffer.from(data === null || data === undefined ? '' : String(data));
}

// Publish one message through the sidecar's HTTP pub/sub API:
// POST /v1.0/publish/<pubsub>/<topic> with Dapr metadata as `metadata.*` query
// parameters. Both path segments are percent-encoded, so a topic can never
// escape the publish route into another Dapr control-plane API.
//
// Caller-supplied headers (e.g. `traceparent`, to continue a trace across a
// publish) are forwarded, minus hop-by-hop/framing headers, the Dapr API token
// (a message must not be able to replace it), and content-type (owned by the
// resolved publish content type).
//
// Rejects with SIDECAR_UNAVAILABLE when the sidecar cannot be reached at all,
// PUBLISH_FAILED when it answers with a non-2xx status, and RESPONSE_TOO_LARGE
// when its answer exceeds maxResponseBytes.
async function publish({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const { contentType, metadata } = request.options;

  const headers = {};
  for (const [key, value] of Object.entries(request.headers || {})) {
    const lower = key.toLowerCase();
    if (HOP_BY_HOP.has(lower) || lower === 'dapr-api-token' || lower === 'content-type') {
      continue;
    }
    headers[key] = value;
  }
  if (contentType) {
    // preparePublish always resolves one (inferring from the payload when the
    // node configures none); the guard keeps a direct caller from setting an
    // undefined header value, which node:http rejects outright.
    headers['content-type'] = contentType;
  }
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const query = {};
  for (const [key, value] of Object.entries(metadata || {})) {
    query[`metadata.${key}`] = value;
  }

  const path = `/v1.0/publish/${encodeURIComponent(request.pubsubName)}/${encodeURIComponent(request.topic)}`;

  // Serialize BEFORE the transport boundary. An explicit JSON content type skips
  // inferContentType's payload-shape check, so a BigInt or circular payload only
  // fails here — a local defect in the message, which must not be reported as an
  // unreachable sidecar (no socket is opened). Same classification dapr-invoke and
  // dapr-response give their own encodeBody failures.
  let body;
  try {
    body = serializeBody(request.data, contentType);
  } catch (err) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `publish payload cannot be serialized as ${contentType}: ${err.message}`,
      { cause: err }
    );
  }

  let result;
  try {
    result = await sidecarRequest(baseUrl, {
      path,
      method: 'POST',
      headers,
      query,
      body,
      timeoutMs,
      signal,
      maxResponseBytes,
    });
  } catch (err) {
    // A typed failure already classifies itself (e.g. RESPONSE_TOO_LARGE — the
    // sidecar answered, it just answered with too much). Only an untyped
    // transport error means the sidecar could not be reached.
    if (err instanceof DaprError) {
      throw err;
    }
    throw new DaprError(
      ErrorCodes.SIDECAR_UNAVAILABLE,
      `Dapr publish could not reach the sidecar: ${err.message}`,
      { cause: err }
    );
  }

  if (result.status < 200 || result.status > 299) {
    // The sidecar's own error body (errorCode/message) is the most useful thing
    // an operator can see here, and it is not a stack trace, token, or flow
    // state — but it is truncated so a large body cannot flood the log.
    const detail = result.body.toString().slice(0, ERROR_BODY_CHARS).trim();
    throw new DaprError(
      ErrorCodes.PUBLISH_FAILED,
      `Dapr publish failed with status ${result.status}${detail ? `: ${detail}` : ''}`
    );
  }
}

// Publish a batch through Dapr's bulk HTTP endpoint:
// POST /v1.0/publish/bulk/<pubsub>/<topic>. request.entries are already
// shaped for the wire by lib/messages.js's prepareBulkPublish (entryId,
// event, contentType, metadata); this function only handles the transport.
//
// Verified against dapr/dapr source tag v1.18.1
// (pkg/api/http/http.go's onBulkPublish):
// success is always a 2xx with an empty body — daprd never reports a
// partial failure alongside a success status. A non-2xx response carrying a
// non-empty `failedEntries` array is a real per-entry bulk result (some or all
// entries failed at the pubsub component; the request itself was valid) and
// throws BULK_PUBLISH_PARTIAL with that result attached. A non-2xx response
// with no identifiable failed entry — daprd's own error envelope
// ({errorCode, message, details}) — is a request-level rejection (a
// duplicate/missing entryId, an unsupported content type, or anything else
// that fails the whole batch before it ever reaches the pubsub component)
// and follows the existing PUBLISH_FAILED contract, exactly like single
// publish's own non-2xx handling.
//
// Deliberate boundary: an EMPTY `failedEntries: []` array is also possible on
// a non-2xx response — onBulkPublish always marshals `res.FailedEntries` into
// this same field whenever the pubsub adapter's BulkPublish call itself
// returns a non-nil error, even when that adapter identified no specific
// failed entry (a whole-broker-level failure, not a per-entry one). Despite
// coming from the identical bulk-result code path, this is classified as
// PUBLISH_FAILED here, not BULK_PUBLISH_PARTIAL: BULK_PUBLISH_PARTIAL exists
// so a Catch node can retry exactly the entries msg.dapr.bulkResult.failedEntries
// names, and an empty list gives it nothing to retry — reporting it as
// "partial" would claim a retry list that does not exist. This does not
// introduce a second total-failure error code (still only PUBLISH_FAILED and
// BULK_PUBLISH_PARTIAL); it narrows which responses BULK_PUBLISH_PARTIAL
// covers to those carrying at least one identified failed entry.
const BULK_PATH_PREFIX = '/v1.0/publish/bulk';

async function publishBulk({ baseUrl, token, timeoutMs, signal, maxResponseBytes }, request) {
  const headers = {};
  for (const [key, value] of Object.entries(request.headers || {})) {
    const lower = key.toLowerCase();
    if (HOP_BY_HOP.has(lower) || lower === 'dapr-api-token' || lower === 'content-type') {
      continue;
    }
    headers[key] = value;
  }
  // The bulk entries array is always JSON on the wire, regardless of what
  // content type any individual entry declares internally.
  headers['content-type'] = 'application/json';
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const query = {};
  for (const [key, value] of Object.entries(request.options.metadata || {})) {
    query[`metadata.${key}`] = value;
  }

  const path = `${BULK_PATH_PREFIX}/${encodeURIComponent(request.pubsubName)}/${encodeURIComponent(request.topic)}`;
  let body;
  try {
    body = Buffer.from(JSON.stringify(request.entries));
  } catch (err) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `bulk publish entries cannot be serialized: ${err.message}`,
      { cause: err }
    );
  }

  let result;
  try {
    result = await sidecarRequest(baseUrl, {
      path,
      method: 'POST',
      headers,
      query,
      body,
      timeoutMs,
      signal,
      maxResponseBytes,
    });
  } catch (err) {
    if (err instanceof DaprError) {
      throw err;
    }
    throw new DaprError(
      ErrorCodes.SIDECAR_UNAVAILABLE,
      `Dapr bulk publish could not reach the sidecar: ${err.message}`,
      { cause: err }
    );
  }

  if (result.status >= 200 && result.status <= 299) {
    return;
  }

  let parsed = null;
  try {
    parsed = JSON.parse(result.body.toString());
  } catch {
    // Not JSON at all: definitely not a bulk-shaped result.
  }

  if (parsed && Array.isArray(parsed.failedEntries) && parsed.failedEntries.length > 0) {
    const failedEntries = parsed.failedEntries.map((entry) => ({
      entryId: String(entry?.entryId ?? ''),
      error: String(entry?.error ?? '').slice(0, ERROR_BODY_CHARS),
    }));
    const err = new DaprError(
      ErrorCodes.BULK_PUBLISH_PARTIAL,
      `Dapr bulk publish reported ${failedEntries.length} of ${request.entries.length} entries failed`
    );
    err.bulkResult = {
      failedEntries,
      ...(parsed.errorCode
        ? { errorCode: String(parsed.errorCode).slice(0, ERROR_BODY_CHARS) }
        : {}),
    };
    throw err;
  }

  const detail = result.body.toString().slice(0, ERROR_BODY_CHARS).trim();
  throw new DaprError(
    ErrorCodes.PUBLISH_FAILED,
    `Dapr bulk publish failed with status ${result.status}${detail ? `: ${detail}` : ''}`
  );
}

module.exports = { publish, publishBulk };
