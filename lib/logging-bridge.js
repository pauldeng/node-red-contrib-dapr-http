'use strict';

const { SeverityNumber } = require('@opentelemetry/api-logs');
const { context } = require('@opentelemetry/api');
const telemetry = require('./telemetry');

const MAX_BODY_LENGTH = 4096;
const MAX_ATTRIBUTE_LENGTH = 256;
const MAX_PENDING_RECORDS = 256;
const AUDIT_LEVEL = 98;
const METRIC_LEVEL = 99;

// Node-RED's own numeric log levels (see @node-red/util/lib/log.js) mapped to
// OTel's severity number/text. audit(98) and metric(99) have no OTel-defined
// severity of their own; both are reported as INFO, distinguished only by
// node_red.level, since neither is an error condition.
const LEVELS = {
  10: { severityNumber: SeverityNumber.FATAL, severityText: 'FATAL', name: 'fatal' },
  20: { severityNumber: SeverityNumber.ERROR, severityText: 'ERROR', name: 'error' },
  30: { severityNumber: SeverityNumber.WARN, severityText: 'WARN', name: 'warn' },
  40: { severityNumber: SeverityNumber.INFO, severityText: 'INFO', name: 'info' },
  50: { severityNumber: SeverityNumber.DEBUG, severityText: 'DEBUG', name: 'debug' },
  60: { severityNumber: SeverityNumber.TRACE, severityText: 'TRACE', name: 'trace' },
  98: { severityNumber: SeverityNumber.INFO, severityText: 'INFO', name: 'audit' },
  99: { severityNumber: SeverityNumber.INFO, severityText: 'INFO', name: 'metric' },
};
const UNKNOWN_LEVEL = { severityNumber: SeverityNumber.UNSPECIFIED, severityText: 'UNSPECIFIED' };

function bounded(value, max = MAX_BODY_LENGTH) {
  const text = String(value);
  return text.length > max ? `${text.slice(0, max)}… (truncated)` : text;
}

function ownValue(value, key) {
  try {
    return Object.getOwnPropertyDescriptor(value, key)?.value;
  } catch {
    return undefined;
  }
}

function stringProperty(value, key) {
  try {
    const property = value[key];
    return typeof property === 'string' ? property : undefined;
  } catch {
    return undefined;
  }
}

// Never recurses into an arbitrary object's own properties or invokes its
// accessors/toString (a node author can call node.warn()/node.error() with
// anything). A plain object's own string .message is useful; every other
// arbitrary object gets a fixed placeholder.
function bodyOf(msg) {
  if (msg instanceof Error) {
    return bounded(stringProperty(msg, 'message') || stringProperty(msg, 'name') || 'Error');
  }
  if (typeof msg === 'string') {
    return bounded(msg);
  }
  if (msg && typeof msg === 'object') {
    const message = ownValue(msg, 'message');
    return bounded(typeof message === 'string' ? message : '[object Object]');
  }
  return bounded(msg);
}

function exceptionAttributes(msg) {
  if (!(msg instanceof Error)) {
    return {};
  }
  const name = stringProperty(msg, 'name');
  const message = stringProperty(msg, 'message');
  const stack = stringProperty(msg, 'stack');
  const attrs = { 'exception.type': name ? bounded(name) : 'Error' };
  if (message) {
    attrs['exception.message'] = bounded(message);
  }
  if (stack) {
    attrs['exception.stacktrace'] = bounded(stack);
  }
  return attrs;
}

function addIdentityAttribute(attrs, entry, field, attribute) {
  const value = ownValue(entry, field);
  if (typeof value === 'string' && value) {
    attrs[attribute] = bounded(value, MAX_ATTRIBUTE_LENGTH);
  }
}

// Allowlist, not a denylist: only these fields of a Node-RED log entry are
// ever read, so an audit entry's req-derived msg.user/msg.path/msg.ip (see
// log.audit()) is excluded by construction, not by remembering to strip it.
function attributesOf(entry, levelName) {
  const attrs = { 'node_red.level': levelName };
  addIdentityAttribute(attrs, entry, 'id', 'node_red.id');
  if (levelName === 'metric' && !attrs['node_red.id']) {
    addIdentityAttribute(attrs, entry, 'nodeid', 'node_red.id');
  }
  addIdentityAttribute(attrs, entry, 'type', 'node_red.type');
  addIdentityAttribute(attrs, entry, 'name', 'node_red.name');
  addIdentityAttribute(attrs, entry, 'z', 'node_red.flow_id');
  return Object.assign(attrs, exceptionAttributes(ownValue(entry, 'msg')));
}

function eventNameOf(entry) {
  const event = ownValue(entry, 'event');
  return typeof event === 'string' && event ? bounded(event, MAX_ATTRIBUTE_LENGTH) : 'node_red.log';
}

function recordBody(entry, rawLevel, eventName) {
  if (rawLevel === AUDIT_LEVEL) {
    return eventName;
  }
  if (rawLevel === METRIC_LEVEL) {
    const value = ownValue(entry, 'value');
    return ['string', 'number', 'boolean'].includes(typeof value) ? bounded(value) : eventName;
  }
  return bodyOf(ownValue(entry, 'msg'));
}

function recordOf(entry, activeContext) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const rawLevel = ownValue(entry, 'level');
  const level = LEVELS[rawLevel] ?? {
    ...UNKNOWN_LEVEL,
    name: bounded(rawLevel, MAX_ATTRIBUTE_LENGTH),
  };
  const timestamp = ownValue(entry, 'timestamp');
  const eventName = eventNameOf(entry);
  const record = {
    eventName,
    severityNumber: level.severityNumber,
    severityText: level.severityText,
    body: recordBody(entry, rawLevel, eventName),
    attributes: attributesOf(entry, level.name),
    context: activeContext,
  };
  if (Number.isFinite(timestamp)) {
    record.timestamp = timestamp;
  }
  return record;
}

function emitFailOpen(logger, record) {
  try {
    logger.emit(record);
  } catch {
    // A host-owned provider or malformed third-party value must not break logging.
  }
}

async function releaseFailOpen(lease) {
  try {
    await lease.release();
  } catch {
    // Telemetry shutdown is fail-open.
  }
}

const activeHandlers = new Set();
let shutdownPromise = null;

async function initialize(state, acquire, getLogger) {
  let acquired;
  try {
    acquired = await acquire();
    const logger = getLogger();
    state.lease = acquired;
    state.logger = logger;
    const records = state.pending.splice(0);
    for (const record of records) {
      emitFailOpen(logger, record);
    }
  } catch {
    state.pending.length = 0;
    if (acquired) {
      await releaseFailOpen(acquired);
    }
  }
}

// The settings.js `logging.<key>.handler` contract (see
// @node-red/util/lib/log.js's LogHandler): Node-RED calls this once,
// synchronously, at RED.log.init() — before any flow deploys — with that
// logger's own settings object, and uses the returned function as its log
// sink for every entry it decides (via its own level/audit/metrics gating)
// to report. That timing is why this lease is process-lifetime: nothing in
// the ordinary deploy/redeploy path ever calls release() on it. An operator
// that wants a bounded flush on deliberate process shutdown can call this
// module's own exported shutdown().
//
// `deps` exists only so a test can inject fake telemetry boundaries instead of
// driving the real OTel SDK end to end.
function createHandler(_settings, deps = {}) {
  const acquire = deps.acquire || telemetry.acquireLogs;
  const getLogger = deps.getLogger || telemetry.getLogger;
  const activeContext = deps.activeContext || (() => context.active());
  const state = { lease: null, logger: null, pending: [], closing: false };
  activeHandlers.add(state);
  state.ready = initialize(state, acquire, getLogger);
  void state.ready;

  return function handleLogEntry(entry) {
    if (state.closing) {
      return;
    }
    let record;
    try {
      // Capture both occurrence time and context synchronously. Initialization
      // may complete later, outside the node handler's AsyncLocalStorage scope.
      record = recordOf(entry, activeContext());
    } catch {
      return;
    }
    if (!record) {
      return;
    }
    if (state.logger) {
      emitFailOpen(state.logger, record);
      return;
    }
    if (state.pending.length >= MAX_PENDING_RECORDS) {
      state.pending.shift();
    }
    state.pending.push(record);
  };
}

async function shutdownHandlers() {
  const states = [...activeHandlers];
  activeHandlers.clear();
  for (const state of states) {
    state.closing = true;
  }
  await Promise.all(states.map((state) => state.ready));
  await Promise.all(
    states.map(async (state) => {
      state.pending.length = 0;
      if (state.lease) {
        const lease = state.lease;
        state.lease = null;
        await releaseFailOpen(lease);
      }
    })
  );
}

async function shutdown() {
  if (shutdownPromise) {
    await shutdownPromise;
    return;
  }
  shutdownPromise = shutdownHandlers();
  try {
    await shutdownPromise;
  } finally {
    shutdownPromise = null;
  }
}

module.exports = createHandler;
module.exports.shutdown = shutdown;
