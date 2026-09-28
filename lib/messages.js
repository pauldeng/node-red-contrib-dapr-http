'use strict';

const { parseRequestHeaders, validateContentType } = require('./http-headers');
const {
  hasOwn,
  isObject,
  invalid,
  requiredString,
  metadataObject,
  daprOverride,
  resolvedMetadata,
} = require('./message-fields');

function inferContentType(payload) {
  if (Buffer.isBuffer(payload)) {
    return 'application/octet-stream';
  }
  if (typeof payload === 'string') {
    return 'text/plain';
  }
  if (
    payload === null ||
    typeof payload === 'object' ||
    typeof payload === 'number' ||
    typeof payload === 'boolean'
  ) {
    return 'application/json';
  }
  throw invalid(`unsupported payload type: ${typeof payload}`);
}

// Our own defensive cap, not a Dapr-enforced limit — mirrors
// lib/subscriptions.js's BULK_MESSAGES_MAX (same package-wide safety-cap
// convention for a bulk batch size).
const BULK_PUBLISH_ENTRIES_MAX = 1000;

// Dapr's bulk publish endpoint is far more restrictive about content type
// than single publish: verified against dapr/dapr source tag v1.18.1's
// pkg/api/http/util.go ConvertEventToBytes and components-contrib v1.18.0's
// contenttype package. Only these four categories are accepted; anything
// else fails the WHOLE bulk request atomically (before it reaches the
// pubsub component), not just that one entry — so it is rejected here,
// per-entry, with a clear message instead of a confusing whole-batch daprd
// error.
function bulkContentTypeCategory(contentType) {
  const type = String(contentType).split(';')[0].trim().toLowerCase();
  if (type === 'application/json' || type === 'application/cloudevents+json') {
    return 'json';
  }
  if (type === 'application/octet-stream') {
    return 'binary';
  }
  if (type.startsWith('text/') || type === 'application/xml') {
    return 'string';
  }
  return null;
}

// Shapes an entry's payload for the wire's `event` field per its content
// category. json: sent as-is (daprd re-marshals it itself). string: must
// already be a string (Dapr does not stringify arbitrary values for
// text/XML). binary: base64-encoded, since JSON has no byte type and Dapr
// decodes application/octet-stream entries as base64 (ConvertEventToBytes).
function bulkEntryEvent(payload, category, entryId) {
  if (category === 'binary') {
    if (!Buffer.isBuffer(payload)) {
      throw invalid(`entry "${entryId}" payload must be a Buffer for a binary content type`);
    }
    return payload.toString('base64');
  }
  if (category === 'string') {
    if (typeof payload !== 'string') {
      throw invalid(`entry "${entryId}" payload must be a string for a text or XML content type`);
    }
    return payload;
  }
  try {
    const encoded = JSON.stringify(payload);
    if (encoded === undefined) {
      throw new TypeError('value is not JSON-serializable');
    }
    // Normalize once here so getters/toJSON cannot change the value between
    // the size check and the transport's later encoding.
    return JSON.parse(encoded);
  } catch (err) {
    throw invalid(`entry "${entryId}" payload is not valid JSON: ${err.message}`);
  }
}

// Validates and shapes a bulk publish request: msg.payload is an array of
// {entryId, payload, contentType, metadata} descriptors. Entry count, shape,
// entryId uniqueness, and content type are all validated here — before any
// socket opens — so a malformed batch fails as INVALID_MESSAGE rather than
// as a confusing whole-batch daprd rejection. maxBodyBytes is the caller's
// own encoded-size bound (the connection's configured body limit), checked
// against the entries actually built (post base64/JSON shaping), not the
// raw msg.payload — the caller is responsible for passing it; there is no
// internal default, matching this module's other bounds being resolved by
// the connection layer, not invented here.
function prepareBulkPublish(config, msg, { maxBodyBytes } = {}) {
  const dapr = daprOverride(msg);

  const pubsubName = requiredString(
    hasOwn(dapr, 'pubsubName') ? dapr.pubsubName : config.pubsubName,
    'pubsubName'
  );
  const topic = requiredString(hasOwn(dapr, 'topic') ? dapr.topic : config.topic, 'topic');
  const metadata = resolvedMetadata(config, dapr);

  if (!Array.isArray(msg.payload)) {
    throw invalid('bulk publish requires msg.payload to be an array of entries');
  }
  if (msg.payload.length === 0) {
    throw invalid('bulk publish requires at least one entry');
  }
  if (msg.payload.length > BULK_PUBLISH_ENTRIES_MAX) {
    throw invalid(`bulk publish accepts at most ${BULK_PUBLISH_ENTRIES_MAX} entries`);
  }

  const defaultContentType = hasOwn(dapr, 'contentType')
    ? dapr.contentType
    : config.contentType || '';
  const seenIds = new Set();
  const entries = msg.payload.map((descriptor, index) => {
    if (!isObject(descriptor)) {
      throw invalid(`bulk entry at index ${index} must be an object`);
    }
    const entryId = requiredString(descriptor.entryId, `bulk entry at index ${index}'s entryId`);
    if (seenIds.has(entryId)) {
      throw invalid(`duplicate entryId "${entryId}"`);
    }
    seenIds.add(entryId);

    const rawContentType = hasOwn(descriptor, 'contentType')
      ? descriptor.contentType
      : defaultContentType;
    if (typeof rawContentType !== 'string') {
      throw invalid(`entry "${entryId}" contentType must be a string`);
    }
    const contentType = rawContentType.trim() || inferContentType(descriptor.payload);
    const category = bulkContentTypeCategory(contentType);
    if (!category) {
      throw invalid(
        `entry "${entryId}" content type "${contentType}" is not one Dapr's bulk publish API ` +
          'accepts (application/json, application/cloudevents+json, text/*, application/xml, ' +
          'or application/octet-stream)'
      );
    }

    return {
      entryId,
      event: bulkEntryEvent(descriptor.payload, category, entryId),
      contentType,
      metadata: hasOwn(descriptor, 'metadata')
        ? metadataObject(descriptor.metadata, `entry "${entryId}" metadata`)
        : {},
    };
  });

  if (typeof maxBodyBytes === 'number') {
    const encodedBytes = Buffer.byteLength(JSON.stringify(entries), 'utf8');
    if (encodedBytes > maxBodyBytes) {
      throw invalid(
        `bulk publish body (${encodedBytes} bytes) exceeds the connection's body limit (${maxBodyBytes} bytes)`
      );
    }
  }

  return {
    pubsubName,
    topic,
    entries,
    headers: hasOwn(dapr, 'headers') ? parseRequestHeaders(dapr.headers, 'msg.dapr.headers') : {},
    options: {
      metadata,
    },
  };
}

function preparePublish(config, msg) {
  const dapr = daprOverride(msg);

  const pubsubName = requiredString(
    hasOwn(dapr, 'pubsubName') ? dapr.pubsubName : config.pubsubName,
    'pubsubName'
  );
  const topic = requiredString(hasOwn(dapr, 'topic') ? dapr.topic : config.topic, 'topic');
  const metadata = resolvedMetadata(config, dapr);
  const configuredContentType = config.contentType || '';
  const contentType = hasOwn(dapr, 'contentType') ? dapr.contentType : configuredContentType;
  if (typeof contentType !== 'string') {
    throw invalid('contentType must be a string');
  }

  return {
    pubsubName,
    topic,
    data: msg.payload,
    // Outbound request headers a flow supplies, most usefully `traceparent` to
    // continue a W3C trace across the publish. Validated here so a malformed
    // header fails as INVALID_MESSAGE before any socket is opened; content-type
    // is not settable this way (msg.dapr.contentType owns it).
    headers: hasOwn(dapr, 'headers') ? parseRequestHeaders(dapr.headers, 'msg.dapr.headers') : {},
    options: {
      // Validated here (illegal token, CRLF injection) so a bad value fails as
      // INVALID_MESSAGE before any socket is opened — otherwise node:http throws
      // ERR_INVALID_CHAR mid-request and the publish client can only report it as
      // a sidecar that could not be reached, which it was never asked to.
      contentType: validateContentType(contentType.trim()) || inferContentType(msg.payload),
      metadata,
    },
  };
}

module.exports = { inferContentType, preparePublish, prepareBulkPublish };
