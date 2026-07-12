'use strict';

const { DaprError, ErrorCodes } = require('./errors');

const hasOwn = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

function invalid(message) {
  return new DaprError(ErrorCodes.INVALID_MESSAGE, message);
}

function requiredString(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw invalid(`${field} is required`);
  }
  return value.trim();
}

function metadataObject(value, field) {
  if (value === undefined || value === null || value === '') {
    return {};
  }
  let parsed = value;
  if (typeof value === 'string') {
    try {
      parsed = JSON.parse(value);
    } catch {
      throw invalid(`${field} must be valid JSON`);
    }
  }
  if (!isObject(parsed)) {
    throw invalid(`${field} must be a JSON object`);
  }
  const metadata = {};
  for (const [key, item] of Object.entries(parsed)) {
    if (
      key.trim() === '' ||
      (typeof item !== 'string' && typeof item !== 'number' && typeof item !== 'boolean')
    ) {
      throw invalid(`${field} keys must have scalar values`);
    }
    metadata[key] = String(item);
  }
  return metadata;
}

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

function preparePublish(config, msg) {
  const dapr = msg.dapr === undefined ? {} : msg.dapr;
  if (!isObject(dapr)) {
    throw invalid('msg.dapr must be an object');
  }

  const pubsubName = requiredString(
    hasOwn(dapr, 'pubsubName') ? dapr.pubsubName : config.pubsubName,
    'pubsubName'
  );
  const topic = requiredString(hasOwn(dapr, 'topic') ? dapr.topic : config.topic, 'topic');
  const configuredMetadata = metadataObject(config.metadata, 'configured metadata');
  const messageMetadata = hasOwn(dapr, 'metadata')
    ? metadataObject(dapr.metadata, 'msg.dapr.metadata')
    : {};
  const configuredContentType = config.contentType || '';
  const contentType = hasOwn(dapr, 'contentType') ? dapr.contentType : configuredContentType;
  if (typeof contentType !== 'string') {
    throw invalid('contentType must be a string');
  }

  return {
    pubsubName,
    topic,
    data: msg.payload,
    options: {
      contentType: contentType.trim() || inferContentType(msg.payload),
      metadata: { ...configuredMetadata, ...messageMetadata },
    },
  };
}

module.exports = { inferContentType, preparePublish };
