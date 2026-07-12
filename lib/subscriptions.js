'use strict';

const crypto = require('node:crypto');

const { DaprError, ErrorCodes } = require('./errors');

const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const invalidOptions = (message) => new DaprError(ErrorCodes.INVALID_OPTIONS, message);
const invalidMessage = (message) => new DaprError(ErrorCodes.INVALID_MESSAGE, message);

function requiredString(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw invalidOptions(`${field} is required`);
  }
  return value.trim();
}

// Normalize editor metadata (a JSON string) or an object into a string map,
// rejecting nested/non-scalar values — Dapr metadata are string query params.
function normalizeMetadata(value, field) {
  if (value === undefined || value === null || value === '') {
    return {};
  }
  let parsed = value;
  if (typeof value === 'string') {
    try {
      parsed = JSON.parse(value);
    } catch {
      throw invalidOptions(`${field} must be valid JSON`);
    }
  }
  if (!isObject(parsed)) {
    throw invalidOptions(`${field} must be a JSON object`);
  }
  const out = {};
  for (const [key, item] of Object.entries(parsed)) {
    if (
      key.trim() === '' ||
      (typeof item !== 'string' && typeof item !== 'number' && typeof item !== 'boolean')
    ) {
      throw invalidOptions(`${field} keys must have scalar values`);
    }
    out[key] = String(item);
  }
  return out;
}

// Build a normalized subscription definition. The delivery route derives from
// the persisted Node-RED node id, so it is stable across redeploys (daprd keeps
// delivering to the same path without a restart).
function buildSubscription({
  nodeId,
  pubsubName,
  topic,
  deadLetterTopic,
  rawPayload,
  metadata,
  ackMode,
} = {}) {
  const id = requiredString(nodeId, 'nodeId');
  const dlt = deadLetterTopic && String(deadLetterTopic).trim();
  return {
    nodeId: id,
    pubsubName: requiredString(pubsubName, 'pubsubName'),
    topic: requiredString(topic, 'topic'),
    route: `/node-red-dapr/subscriptions/${id}`,
    metadata: normalizeMetadata(metadata, 'metadata'),
    deadLetterTopic: dlt || undefined,
    rawPayload: Boolean(rawPayload),
    ackMode: ackMode === 'manual' ? 'manual' : 'auto',
  };
}

// The entry daprd receives from GET /dapr/subscribe. rawPayload is folded into
// metadata (that is how Dapr expects it on a subscription).
function discoveryEntry(sub) {
  const entry = { pubsubname: sub.pubsubName, topic: sub.topic, route: sub.route };
  const metadata = { ...sub.metadata, ...(sub.rawPayload ? { rawPayload: 'true' } : {}) };
  if (Object.keys(metadata).length > 0) {
    entry.metadata = metadata;
  }
  if (sub.deadLetterTopic) {
    entry.deadLetterTopic = sub.deadLetterTopic;
  }
  return entry;
}

function canonical(sub) {
  const entry = discoveryEntry(sub);
  const metadata = entry.metadata
    ? Object.fromEntries(
        Object.keys(entry.metadata)
          .sort()
          .map((k) => [k, entry.metadata[k]])
      )
    : null;
  return {
    pubsubname: entry.pubsubname,
    topic: entry.topic,
    route: entry.route,
    deadLetterTopic: entry.deadLetterTopic ?? null,
    metadata,
  };
}

// A stable hash of exactly the fields daprd consumes, order-independent across
// the subscription set. Changing any of them means daprd must re-fetch
// /dapr/subscribe (i.e. a sidecar restart is required).
function fingerprint(subs) {
  const canon = subs
    .map(canonical)
    .sort((a, b) =>
      `${a.pubsubname}|${a.topic}|${a.route}`.localeCompare(`${b.pubsubname}|${b.topic}|${b.route}`)
    );
  return crypto.createHash('sha256').update(JSON.stringify(canon)).digest('hex');
}

const CLOUDEVENT_REQUIRED = ['specversion', 'id', 'source', 'type'];

function isBase64(value) {
  return (
    typeof value === 'string' && value.length % 4 === 0 && /^[A-Za-z0-9+/]*={0,2}$/.test(value)
  );
}

// Convert a delivery body into { payload, cloudEvent }. daprd always delivers a
// validated CloudEvent — even a rawPayload subscription receives a synthetic
// CloudEvent with the original bytes in data_base64 — so `data` becomes the
// payload and `data_base64` is decoded to a Buffer. A structurally invalid
// envelope is rejected so it can be dropped rather than acknowledged.
function parseDelivery(body) {
  let envelope;
  try {
    envelope = JSON.parse(body.toString());
  } catch {
    throw invalidMessage('delivery body is not valid JSON');
  }
  if (!isObject(envelope)) {
    throw invalidMessage('CloudEvent envelope must be a JSON object');
  }
  for (const attr of CLOUDEVENT_REQUIRED) {
    if (typeof envelope[attr] !== 'string' || envelope[attr] === '') {
      throw invalidMessage(`CloudEvent envelope is missing required attribute "${attr}"`);
    }
  }
  const hasData = 'data' in envelope;
  const hasDataBase64 = 'data_base64' in envelope;
  if (hasData && hasDataBase64) {
    throw invalidMessage('CloudEvent must not set both data and data_base64');
  }
  let payload;
  if (hasDataBase64) {
    if (!isBase64(envelope.data_base64)) {
      throw invalidMessage('CloudEvent data_base64 must be valid base64');
    }
    payload = Buffer.from(envelope.data_base64, 'base64');
  } else {
    payload = envelope.data;
  }
  return { payload, cloudEvent: envelope };
}

module.exports = { buildSubscription, discoveryEntry, fingerprint, parseDelivery };
