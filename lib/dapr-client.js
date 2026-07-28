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

module.exports = { publish, serializeBody };
