'use strict';

const {
  hasOwn,
  isObject,
  invalid,
  requiredString,
  requiredKey,
  metadataObject,
  daprOverride,
  resolvedMetadata,
} = require('./message-fields');

function optionalEnum(value, allowed, field) {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  if (typeof value !== 'string' || !allowed.has(value)) {
    throw invalid(`${field} must be one of ${[...allowed].join(', ')}`);
  }
  return value;
}

function nonNegativeInteger(value, field) {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  const n = Number(value);
  if (!Number.isInteger(n) || n < 0) {
    throw invalid(`${field} must be a non-negative integer`);
  }
  return n;
}

function checkEncodedSize(value, maxBodyBytes, label) {
  let encoded;
  try {
    encoded = JSON.stringify(value);
  } catch (err) {
    throw invalid(`${label} is not valid JSON: ${err.message}`);
  }
  if (typeof maxBodyBytes === 'number') {
    const encodedBytes = Buffer.byteLength(encoded, 'utf8');
    if (encodedBytes > maxBodyBytes) {
      throw invalid(
        `${label} (${encodedBytes} bytes) exceeds the connection's body limit (${maxBodyBytes} bytes)`
      );
    }
  }
}

function jsonValue(value, label) {
  try {
    const encoded = JSON.stringify(value);
    if (encoded === undefined) {
      throw new TypeError('value is not JSON-serializable');
    }
    // Normalize once so getters/toJSON cannot change the value between the
    // size check here and the transport's later encoding.
    return JSON.parse(encoded);
  } catch (err) {
    throw invalid(`${label} is not valid JSON: ${err.message}`);
  }
}

const CONSISTENCY_VALUES = new Set(['strong', 'eventual']);
const CONCURRENCY_VALUES = new Set(['first-write', 'last-write']);
const OPERATIONS = new Set(['get', 'save', 'delete', 'bulkGet', 'transaction']);
const TRANSACTION_OPERATION_TYPES = new Set(['upsert', 'delete']);

// Our own defensive caps, not Dapr-enforced limits -- mirror
// lib/messages.js's BULK_PUBLISH_ENTRIES_MAX (same package-wide safety-cap
// convention for a batch size).
const BULK_GET_KEYS_MAX = 1000;
const TRANSACTION_OPERATIONS_MAX = 1000;

// Resolves the dapr-state node's operation for this message: msg.dapr.operation
// overrides the node's configured default, validated against the five known
// operations before anything else in this module runs.
function resolveOperation(config, msg) {
  const dapr = daprOverride(msg);
  const raw = hasOwn(dapr, 'operation') ? dapr.operation : config.operation;
  if (typeof raw !== 'string' || !OPERATIONS.has(raw)) {
    throw invalid(`operation must be one of ${[...OPERATIONS].join(', ')}`);
  }
  return raw;
}

// GET /v1.0/state/<store>/<key>: consistency and metadata are query-shaped.
function prepareStateGet(config, msg) {
  const dapr = daprOverride(msg);
  const storeName = requiredString(
    hasOwn(dapr, 'storeName') ? dapr.storeName : config.storeName,
    'storeName'
  );
  const key = requiredKey(hasOwn(dapr, 'key') ? dapr.key : config.key, 'key');
  const consistency = optionalEnum(
    hasOwn(dapr, 'consistency') ? dapr.consistency : config.consistency,
    CONSISTENCY_VALUES,
    'consistency'
  );
  return { storeName, key, consistency, metadata: resolvedMetadata(config, dapr) };
}

// POST /v1.0/state/<store>: a single wire-shaped item, ready to transport as
// a one-item array. etag/metadata/options are included only when set, so
// lib/state-client.js never has to decide what to omit. A Buffer payload is
// rejected -- Dapr's save is JSON-native, with no documented raw-bytes path
// the way pub/sub has application/octet-stream.
function prepareStateSave(config, msg, { maxBodyBytes } = {}) {
  const dapr = daprOverride(msg);
  const storeName = requiredString(
    hasOwn(dapr, 'storeName') ? dapr.storeName : config.storeName,
    'storeName'
  );
  const key = requiredKey(hasOwn(dapr, 'key') ? dapr.key : config.key, 'key');
  if (Buffer.isBuffer(msg.payload)) {
    throw invalid(
      'save does not accept a Buffer payload -- base64-encode it into the value yourself'
    );
  }
  const value = jsonValue(msg.payload, 'state save value');
  const etag = hasOwn(dapr, 'etag') ? requiredString(dapr.etag, 'etag') : undefined;
  const consistency = optionalEnum(
    hasOwn(dapr, 'consistency') ? dapr.consistency : config.consistency,
    CONSISTENCY_VALUES,
    'consistency'
  );
  const concurrency = optionalEnum(
    hasOwn(dapr, 'concurrency') ? dapr.concurrency : config.concurrency,
    CONCURRENCY_VALUES,
    'concurrency'
  );
  const metadata = resolvedMetadata(config, dapr);

  const ttlRaw = hasOwn(dapr, 'ttlSeconds') ? dapr.ttlSeconds : config.ttlSeconds;
  const ttlSeconds = nonNegativeInteger(ttlRaw, 'ttlSeconds');
  if (ttlSeconds !== undefined) {
    metadata.ttlInSeconds = String(ttlSeconds);
  }

  const options = {};
  if (concurrency) {
    options.concurrency = concurrency;
  }
  if (consistency) {
    options.consistency = consistency;
  }
  const item = { key, value };
  if (etag) {
    item.etag = etag;
  }
  if (Object.keys(metadata).length) {
    item.metadata = metadata;
  }
  if (Object.keys(options).length) {
    item.options = options;
  }

  checkEncodedSize([item], maxBodyBytes, 'state save body');
  return { storeName, item };
}

// DELETE /v1.0/state/<store>/<key>: concurrency/consistency/metadata are
// query-shaped; etag travels as the If-Match request header, built by
// lib/state-client.js -- this function only resolves the value.
function prepareStateDelete(config, msg) {
  const dapr = daprOverride(msg);
  const storeName = requiredString(
    hasOwn(dapr, 'storeName') ? dapr.storeName : config.storeName,
    'storeName'
  );
  const key = requiredKey(hasOwn(dapr, 'key') ? dapr.key : config.key, 'key');
  const etag = hasOwn(dapr, 'etag') ? requiredString(dapr.etag, 'etag') : undefined;
  const consistency = optionalEnum(
    hasOwn(dapr, 'consistency') ? dapr.consistency : config.consistency,
    CONSISTENCY_VALUES,
    'consistency'
  );
  const concurrency = optionalEnum(
    hasOwn(dapr, 'concurrency') ? dapr.concurrency : config.concurrency,
    CONCURRENCY_VALUES,
    'concurrency'
  );
  return {
    storeName,
    key,
    etag,
    consistency,
    concurrency,
    metadata: resolvedMetadata(config, dapr),
  };
}

// POST /v1.0/state/<store>/bulk: {keys, parallelism, metadata}. Keys come
// from msg.payload (an array of key strings) by default, or msg.dapr.keys.
function prepareStateBulkGet(config, msg, { maxBodyBytes } = {}) {
  const dapr = daprOverride(msg);
  const storeName = requiredString(
    hasOwn(dapr, 'storeName') ? dapr.storeName : config.storeName,
    'storeName'
  );
  const rawKeys = hasOwn(dapr, 'keys') ? dapr.keys : msg.payload;
  if (!Array.isArray(rawKeys) || rawKeys.length === 0) {
    throw invalid('bulkGet requires an array of keys (msg.payload or msg.dapr.keys)');
  }
  if (rawKeys.length > BULK_GET_KEYS_MAX) {
    throw invalid(`bulkGet accepts at most ${BULK_GET_KEYS_MAX} keys`);
  }
  const keys = rawKeys.map((key, index) => requiredKey(key, `key at index ${index}`));
  const parallelism = nonNegativeInteger(
    hasOwn(dapr, 'parallelism') ? dapr.parallelism : undefined,
    'parallelism'
  );
  const metadata = resolvedMetadata(config, dapr);

  checkEncodedSize({ keys, parallelism, metadata }, maxBodyBytes, 'state bulk get body');
  return { storeName, keys, parallelism, metadata };
}

// POST /v1.0/state/<store>/transaction: {operations, metadata}. Operations
// come from msg.payload by default, or msg.dapr.operations -- each
// {operation: "upsert"|"delete", key, value?, etag?, metadata?, options?}
// (consistency/concurrency given flat, wire-shaped into options here, same
// as save's own per-item options).
function prepareStateTransaction(config, msg, { maxBodyBytes } = {}) {
  const dapr = daprOverride(msg);
  const storeName = requiredString(
    hasOwn(dapr, 'storeName') ? dapr.storeName : config.storeName,
    'storeName'
  );
  const rawOperations = hasOwn(dapr, 'operations') ? dapr.operations : msg.payload;
  if (!Array.isArray(rawOperations) || rawOperations.length === 0) {
    throw invalid(
      'transaction requires an array of operations (msg.payload or msg.dapr.operations)'
    );
  }
  if (rawOperations.length > TRANSACTION_OPERATIONS_MAX) {
    throw invalid(`transaction accepts at most ${TRANSACTION_OPERATIONS_MAX} operations`);
  }

  const operations = rawOperations.map((op, index) => {
    if (!isObject(op)) {
      throw invalid(`operation at index ${index} must be an object`);
    }
    if (!TRANSACTION_OPERATION_TYPES.has(op.operation)) {
      throw invalid(`operation at index ${index} must have operation "upsert" or "delete"`);
    }
    const key = requiredKey(op.key, `operation at index ${index}'s key`);
    const request = { key };
    if (op.operation === 'upsert') {
      if (!hasOwn(op, 'value')) {
        throw invalid(`upsert operation at index ${index} requires a value`);
      }
      if (Buffer.isBuffer(op.value)) {
        throw invalid(
          `upsert operation at index ${index} does not accept a Buffer value -- base64-encode it yourself`
        );
      }
      request.value = jsonValue(op.value, `upsert operation at index ${index}'s value`);
    }
    if (hasOwn(op, 'etag')) {
      request.etag = requiredString(op.etag, `operation at index ${index}'s etag`);
    }
    const consistency = optionalEnum(
      op.consistency,
      CONSISTENCY_VALUES,
      `operation at index ${index}'s consistency`
    );
    const concurrency = optionalEnum(
      op.concurrency,
      CONCURRENCY_VALUES,
      `operation at index ${index}'s concurrency`
    );
    const options = {};
    if (concurrency) {
      options.concurrency = concurrency;
    }
    if (consistency) {
      options.consistency = consistency;
    }
    if (Object.keys(options).length) {
      request.options = options;
    }
    if (hasOwn(op, 'metadata')) {
      const opMetadata = metadataObject(op.metadata, `operation at index ${index}'s metadata`);
      if (Object.keys(opMetadata).length) {
        request.metadata = opMetadata;
      }
    }
    return { operation: op.operation, request };
  });

  const metadata = resolvedMetadata(config, dapr);
  checkEncodedSize({ operations, metadata }, maxBodyBytes, 'state transaction body');
  return { storeName, operations, metadata };
}

module.exports = {
  resolveOperation,
  prepareStateGet,
  prepareStateSave,
  prepareStateDelete,
  prepareStateBulkGet,
  prepareStateTransaction,
};
