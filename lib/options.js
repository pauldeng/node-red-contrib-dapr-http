'use strict';

const { DaprError, ErrorCodes } = require('./errors');

const MiB = 1024 * 1024;

// Operational bounds for the two editor-configurable limits. Kept in sync with
// the editor validators in nodes/dapr-connection.html.
const BODY_LIMIT_MB_MIN = 1;
const BODY_LIMIT_MB_MAX = 64;
const REQUEST_TIMEOUT_SEC_MIN = 1;
const REQUEST_TIMEOUT_SEC_MAX = 300;

// Fixed limits — not surfaced in the editor until a demonstrated need justifies
// the extra UI and configuration surface.
const FIXED_LIMITS = {
  headerLimitBytes: 16 * 1024,
  headerCountLimit: 100,
  headersTimeoutMs: 10000,
  drainTimeoutMs: 5000,
  leaseGraceMs: 2000,
  maxPending: 1000,
};

const isBlank = (v) => v === undefined || v === null || String(v).trim() === '';

function invalid(message) {
  return new DaprError(ErrorCodes.INVALID_OPTIONS, message);
}

function parsePort(value, field) {
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1 || n > 65535) {
    throw invalid(`${field} must be an integer between 1 and 65535`);
  }
  return n;
}

function parseBounded(value, field, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < min || n > max) {
    throw invalid(`${field} must be a number between ${min} and ${max}`);
  }
  return n;
}

// Build and validate the outbound base URL so a malformed or unsupported
// endpoint is caught here (INVALID_OPTIONS) rather than crashing the health
// poller later. Scope is HTTP over TCP, and the health probe uses node:http, so
// only an http:// origin (no path/query/fragment, no https/file/etc.) is valid.
function validatedBaseUrl(raw, field) {
  let url;
  try {
    url = new URL(String(raw));
  } catch {
    throw invalid(`${field} is not a valid URL`);
  }
  if (url.protocol !== 'http:') {
    throw invalid(`${field} must use the http:// scheme`);
  }
  if ((url.pathname && url.pathname !== '/') || url.search || url.hash) {
    throw invalid(`${field} must be an origin only (no path, query, or fragment)`);
  }
  return url.origin;
}

const firstNonBlank = (...values) => values.find((v) => !isBlank(v)) ?? undefined;

// Loopback bind addresses, for which the app channel is only reachable from this
// host. Anything else (including 0.0.0.0, which covers every interface) exposes
// it to the network and must therefore carry an app API token — without one the
// channel authenticates nobody (see lib/app-channel.js).
function isLoopbackBind(address) {
  const value = address.toLowerCase();
  return (
    value === 'localhost' ||
    value === '::1' ||
    value === '[::1]' ||
    /^127\.\d+\.\d+\.\d+$/.test(value)
  );
}

// Normalize editor config + credentials + environment into the canonical option
// shape the connection node and app-channel consume. Throws DaprError
// (INVALID_OPTIONS) on invalid input.
function resolveOptions({ config = {}, credentials = {}, env = {} } = {}) {
  const hasHost = !isBlank(config.daprHost);
  const hasPort = !isBlank(config.daprPort);

  // Env-endpoint mode only when NEITHER host nor port is configured — passing
  // empty values must not override DAPR_HTTP_ENDPOINT discovery.
  let outbound;
  if (!hasHost && !hasPort) {
    const raw = firstNonBlank(env.DAPR_HTTP_ENDPOINT) ?? 'http://127.0.0.1:3500';
    outbound = { mode: 'env', baseUrl: validatedBaseUrl(raw, 'DAPR_HTTP_ENDPOINT') };
  } else {
    const host = hasHost ? String(config.daprHost).trim() : '127.0.0.1';
    const port = hasPort ? parsePort(config.daprPort, 'daprPort') : 3500;
    outbound = {
      mode: 'explicit',
      host,
      port,
      baseUrl: validatedBaseUrl(`http://${host}:${port}`, 'Dapr endpoint'),
    };
  }

  const bodyLimitMb = isBlank(config.bodyLimitMb)
    ? 4
    : parseBounded(config.bodyLimitMb, 'bodyLimitMb', BODY_LIMIT_MB_MIN, BODY_LIMIT_MB_MAX);
  const requestTimeoutSec = isBlank(config.requestTimeoutSec)
    ? 30
    : parseBounded(
        config.requestTimeoutSec,
        'requestTimeoutSec',
        REQUEST_TIMEOUT_SEC_MIN,
        REQUEST_TIMEOUT_SEC_MAX
      );

  const bindAddress = isBlank(config.bindAddress) ? '127.0.0.1' : String(config.bindAddress).trim();
  const appApiToken = firstNonBlank(credentials.appApiToken, env.APP_API_TOKEN);
  if (!isLoopbackBind(bindAddress) && !appApiToken) {
    throw invalid(
      `an app API token is required when bindAddress (${bindAddress}) is not loopback: set the App API token credential or APP_API_TOKEN`
    );
  }

  return {
    outbound,
    daprApiToken: firstNonBlank(credentials.daprApiToken, env.DAPR_API_TOKEN),
    inbound: {
      bindAddress,
      port: isBlank(config.appPort) ? 3000 : parsePort(config.appPort, 'appPort'),
      appApiToken,
    },
    limits: {
      bodyLimitBytes: Math.round(bodyLimitMb * MiB),
      requestTimeoutMs: Math.round(requestTimeoutSec * 1000),
      ...FIXED_LIMITS,
    },
  };
}

module.exports = { resolveOptions };
