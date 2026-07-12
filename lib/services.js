'use strict';

const { DaprError, ErrorCodes } = require('./errors');

const SUPPORTED_VERBS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE']);
// Paths owned by daprd or by this package's internal delivery routes.
const RESERVED_PREFIXES = ['/dapr', '/healthz', '/node-red-dapr'];
const SAFE_PATH = /^\/[A-Za-z0-9\-._~/]*$/;

const invalid = (message) => new DaprError(ErrorCodes.INVALID_OPTIONS, message);

function requiredString(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw invalid(`${field} is required`);
  }
  return value.trim();
}

// Validate and normalize one inbound service method route. Rejects unsupported
// verbs and reserved, internal, traversal, or otherwise unsafe paths at deploy
// time. Duplicate (verb, path) detection is the connection hub's responsibility.
function buildService({ nodeId, verb, path } = {}) {
  const id = requiredString(nodeId, 'nodeId');
  const method = requiredString(verb, 'verb').toUpperCase();
  if (!SUPPORTED_VERBS.has(method)) {
    throw invalid(`unsupported HTTP verb: ${verb}`);
  }
  let normalized = requiredString(path, 'path');
  if (!normalized.startsWith('/')) {
    normalized = `/${normalized}`;
  }
  if (!SAFE_PATH.test(normalized) || normalized.includes('..') || normalized.includes('//')) {
    throw invalid(`invalid service path: ${path}`);
  }
  for (const prefix of RESERVED_PREFIXES) {
    if (normalized === prefix || normalized.startsWith(`${prefix}/`)) {
      throw invalid(`service path ${normalized} is reserved`);
    }
  }
  return { nodeId: id, verb: method, path: normalized };
}

module.exports = { buildService, SUPPORTED_VERBS };
