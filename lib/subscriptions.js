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

const RULE_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

// Normalize the editor's CEL routing rules (an ordered list of {id, match})
// into routes whose paths derive from each rule's own PERSISTED id, never from
// its array position. The editor assigns an id once, when a rule is first
// created, and keeps it for that rule's lifetime — so inserting, removing, or
// reordering rules never reassigns an existing rule's route (an index-derived
// id would: inserting at position 0 would shift every later rule onto a path
// that a stale, not-yet-restarted daprd still associates with a DIFFERENT
// rule, silently misrouting events during the pending-restart window). Match
// order is still significant (daprd evaluates rules in order, first match
// wins) and is preserved, never sorted — only the path is order-independent.
function normalizeRules(nodeId, rules) {
  if (rules === undefined || rules === null || rules === '') {
    return [];
  }
  const parsed = typeof rules === 'string' ? JSON.parse(rules) : rules;
  if (!Array.isArray(parsed)) {
    throw invalidOptions('rules must be an array');
  }
  const seenIds = new Set();
  return parsed.map((rule, index) => {
    if (!isObject(rule) || typeof rule.id !== 'string' || !RULE_ID_PATTERN.test(rule.id)) {
      throw invalidOptions(
        `rule ${index} must have an id matching ${RULE_ID_PATTERN} (assigned once by the editor)`
      );
    }
    if (typeof rule.match !== 'string' || rule.match.trim() === '') {
      throw invalidOptions(`rule ${index} (${rule.id}) must have a non-empty match expression`);
    }
    if (seenIds.has(rule.id)) {
      throw invalidOptions(`duplicate rule id: ${rule.id}`);
    }
    seenIds.add(rule.id);
    return {
      ruleId: rule.id,
      match: rule.match.trim(),
      path: `/node-red-dapr/subscriptions/${nodeId}/${rule.id}`,
      isDefault: false,
    };
  });
}

// dapr/dapr source tag v1.18.1's BulkSubscribe struct only requires a positive
// int32 (no documented upper bound) — these tighter caps are a deliberate package
// safety choice, not a Dapr requirement: an unbounded maxMessagesCount or
// maxAwaitDurationMs would let a single fat-fingered value force daprd into
// building huge in-memory batches or holding messages for absurd latencies.
// Kept in sync with the matching validate() functions in dapr-subscribe.html.
const BULK_MESSAGES_MIN = 1;
const BULK_MESSAGES_MAX = 1000;
const BULK_AWAIT_MS_MIN = 1;
const BULK_AWAIT_MS_MAX = 60000;

function boundedInt(value, field, min, max) {
  const n = Number(value);
  if (!Number.isInteger(n) || n < min || n > max) {
    throw invalidOptions(`${field} must be an integer between ${min} and ${max}`);
  }
  return n;
}

// Normalize the editor's bulk-subscribe config. Returns undefined when not
// enabled — bulkSubscribe is otherwise omitted from the discovery entry.
function normalizeBulkSubscribe(value) {
  if (!value || value.enabled !== true) {
    return undefined;
  }
  const out = { enabled: true };
  if (value.maxMessagesCount !== undefined && value.maxMessagesCount !== '') {
    out.maxMessagesCount = boundedInt(
      value.maxMessagesCount,
      'bulkSubscribe.maxMessagesCount',
      BULK_MESSAGES_MIN,
      BULK_MESSAGES_MAX
    );
  }
  if (value.maxAwaitDurationMs !== undefined && value.maxAwaitDurationMs !== '') {
    out.maxAwaitDurationMs = boundedInt(
      value.maxAwaitDurationMs,
      'bulkSubscribe.maxAwaitDurationMs',
      BULK_AWAIT_MS_MIN,
      BULK_AWAIT_MS_MAX
    );
  }
  return out;
}

// Build a normalized subscription definition. Every route (one per CEL rule,
// plus one default/fallback) derives its path from the persisted Node-RED node
// id and each rule's own persisted id, so it is stable across redeploys (daprd
// keeps delivering to the same paths without a restart). CEL routing and bulk
// subscribe compose: Dapr groups bulk entries by their matched CEL route and
// delivers a separate bulk batch per route, so a route with rules configured
// gets bulk-delivered exactly like the default route does.
function buildSubscription({
  nodeId,
  pubsubName,
  topic,
  deadLetterTopic,
  rawPayload,
  metadata,
  ackMode,
  rules,
  bulkSubscribe,
} = {}) {
  const id = requiredString(nodeId, 'nodeId');
  const dlt = deadLetterTopic && String(deadLetterTopic).trim();
  const ruleRoutes = normalizeRules(id, rules);
  const hasRules = ruleRoutes.length > 0;
  const normalizedBulk = normalizeBulkSubscribe(bulkSubscribe);
  // Never suffixed with "/default", and never conditional on hasRules: a
  // not-yet-restarted sidecar keeps posting to whatever default path it last
  // fetched, so the default path must be identical whether or not rules are
  // configured — otherwise adding/removing the first/last rule moves it out
  // from under a stale sidecar, which 404s (Dapr treats 404 as a permanent
  // DROP, not a retry) until the operator restarts daprd.
  const defaultPath = `/node-red-dapr/subscriptions/${id}`;
  const routes = [...ruleRoutes, { ruleId: null, match: null, path: defaultPath, isDefault: true }];
  return {
    nodeId: id,
    pubsubName: requiredString(pubsubName, 'pubsubName'),
    topic: requiredString(topic, 'topic'),
    route: defaultPath,
    routes,
    hasRules,
    metadata: normalizeMetadata(metadata, 'metadata'),
    deadLetterTopic: dlt || undefined,
    rawPayload: Boolean(rawPayload),
    ackMode: ackMode === 'manual' ? 'manual' : 'auto',
    bulkSubscribe: normalizedBulk,
  };
}

// The entry daprd receives from GET /dapr/subscribe. rawPayload is folded into
// metadata (that is how Dapr expects it on a subscription). With CEL rules
// configured, "routes" (rules + default) replaces the plain "route" string.
function discoveryEntry(sub) {
  const entry = { pubsubname: sub.pubsubName, topic: sub.topic };
  if (sub.hasRules) {
    entry.routes = {
      rules: sub.routes.filter((r) => !r.isDefault).map((r) => ({ match: r.match, path: r.path })),
      default: sub.route,
    };
  } else {
    entry.route = sub.route;
  }
  const metadata = { ...sub.metadata, ...(sub.rawPayload ? { rawPayload: 'true' } : {}) };
  if (Object.keys(metadata).length > 0) {
    entry.metadata = metadata;
  }
  if (sub.deadLetterTopic) {
    entry.deadLetterTopic = sub.deadLetterTopic;
  }
  if (sub.bulkSubscribe) {
    entry.bulkSubscribe = { ...sub.bulkSubscribe };
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
    // Rule order is significant (first match wins) and is preserved as declared.
    route: entry.route ?? null,
    routes: entry.routes ?? null,
    deadLetterTopic: entry.deadLetterTopic ?? null,
    metadata,
    bulkSubscribe: entry.bulkSubscribe ?? null,
  };
}

// A stable hash of exactly the fields daprd consumes, order-independent across
// the subscription set (but NOT within one subscription's rule list — see
// canonical()). Changing any of them means daprd must re-fetch
// /dapr/subscribe (i.e. a sidecar restart is required).
function fingerprint(subs) {
  const canon = subs.map(canonical).sort((a, b) => {
    const aKey = `${a.pubsubname}|${a.topic}|${a.route ?? a.routes.default}`;
    const bKey = `${b.pubsubname}|${b.topic}|${b.route ?? b.routes.default}`;
    return aKey.localeCompare(bKey);
  });
  return crypto.createHash('sha256').update(JSON.stringify(canon)).digest('hex');
}

const CLOUDEVENT_REQUIRED = ['specversion', 'id', 'source', 'type'];

function isBase64(value) {
  return (
    typeof value === 'string' && value.length % 4 === 0 && /^[A-Za-z0-9+/]*={0,2}$/.test(value)
  );
}

// Extract { payload, cloudEvent } from an already-parsed CloudEvent envelope
// object. daprd always delivers a validated CloudEvent — even a rawPayload
// subscription receives a synthetic CloudEvent with the original bytes in
// data_base64 — so `data` becomes the payload and `data_base64` is decoded to a
// Buffer. A structurally invalid envelope is rejected.
function extractCloudEvent(envelope) {
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

// Convert a single (non-bulk) delivery body into { payload, cloudEvent }. A
// structurally invalid envelope is rejected so it can be dropped rather than
// acknowledged.
function parseDelivery(body) {
  let envelope;
  try {
    envelope = JSON.parse(body.toString());
  } catch {
    throw invalidMessage('delivery body is not valid JSON');
  }
  return extractCloudEvent(envelope);
}

// Parse a bulk delivery body ({ id, metadata, entries: [{ entryId, event,
// contentType, metadata }] }) into one result per entry: { entryId, payload,
// cloudEvent, metadata, contentType, batchId } on success, or { entryId,
// error } for a structurally invalid entry. Per-entry results never throw for
// one bad entry — the point of a bulk batch is that a caller can answer DROP
// for just that entry while the rest of the batch still succeeds.
//
// Unlike a single delivery, a bulk entry's "event" is NOT always a CloudEvent
// envelope: per the pinned Dapr 1.18.1 source
// (pkg/runtime/subscription/bulksubscription.go, bulkSubscribeTopic), a
// rawPayload topic's bulk entries carry the raw bytes as a base64 STRING in
// "event" (isCloudEvent=false), while a non-raw topic's entries carry the full
// parsed CloudEvent object (isCloudEvent=true) — mirroring single delivery.
//
// The envelope itself carries its own top-level "id" and "metadata" (per
// pkg/runtime/pubsub/bulksubscribe_events.go NewBulkSubscribeEnvelope, using
// the IDField/PubsubField constants from components-contrib/pubsub/envelope.go)
// — distinct from each entry's own "metadata". The envelope id is surfaced as
// batchId; envelope metadata is merged in as defaults an entry's own metadata
// overrides, since bulk has no shared per-message HTTP headers to fall back on.
function parseBulkDelivery(body, { rawPayload = false } = {}) {
  let envelope;
  try {
    envelope = JSON.parse(body.toString());
  } catch {
    throw invalidMessage('bulk delivery body is not valid JSON');
  }
  if (!isObject(envelope) || !Array.isArray(envelope.entries)) {
    throw invalidMessage('bulk delivery body must have an "entries" array');
  }
  const batchId = typeof envelope.id === 'string' && envelope.id !== '' ? envelope.id : undefined;
  const batchMetadata = isObject(envelope.metadata) ? envelope.metadata : {};
  return envelope.entries.map((entry, index) => {
    if (!isObject(entry) || typeof entry.entryId !== 'string' || entry.entryId === '') {
      return { entryId: null, error: `entry ${index} is missing a valid entryId` };
    }
    const metadata = { ...batchMetadata, ...(isObject(entry.metadata) ? entry.metadata : {}) };
    const contentType = typeof entry.contentType === 'string' ? entry.contentType : undefined;
    try {
      if (rawPayload) {
        if (!isBase64(entry.event)) {
          throw invalidMessage('raw bulk entry event must be a base64 string');
        }
        return {
          entryId: entry.entryId,
          payload: Buffer.from(entry.event, 'base64'),
          cloudEvent: null,
          metadata,
          contentType,
          batchId,
        };
      }
      const { payload, cloudEvent } = extractCloudEvent(entry.event);
      return { entryId: entry.entryId, payload, cloudEvent, metadata, contentType, batchId };
    } catch (err) {
      return { entryId: entry.entryId, error: err.message };
    }
  });
}

module.exports = {
  buildSubscription,
  discoveryEntry,
  fingerprint,
  parseDelivery,
  parseBulkDelivery,
};
