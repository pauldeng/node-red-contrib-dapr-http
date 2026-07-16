'use strict';

const http = require('node:http');

const { invoke, encodeBody, decodeBody } = require('../lib/invoke-client');
const { validateContentType } = require('../lib/http-headers');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { SUPPORTED_VERBS } = require('../lib/services');

const TIMEOUT_SEC_MIN = 1;
const TIMEOUT_SEC_MAX = 300;

// Resolve the outbound deadline: a per-message or configured override (seconds),
// else the connection's request timeout. A value that is present but not a
// number in [1, 300] is rejected (INVALID_MESSAGE) rather than silently ignored,
// so a bad override never disables the deadline or passes unnoticed.
function resolveTimeoutMs(override, configured, fallbackMs) {
  const raw = override ?? configured;
  if (raw === undefined || raw === null || raw === '') {
    return fallbackMs;
  }
  const sec = Number(raw);
  if (!Number.isFinite(sec) || sec < TIMEOUT_SEC_MIN || sec > TIMEOUT_SEC_MAX) {
    throw new DaprError(
      ErrorCodes.INVALID_MESSAGE,
      `timeoutSec must be a number between ${TIMEOUT_SEC_MIN} and ${TIMEOUT_SEC_MAX}`
    );
  }
  return Math.round(sec * 1000);
}

// A query override must be a plain object: Object.entries on a string ('ab')
// or array yields numeric-index pairs ('0'->'a') that silently corrupt the
// query string, so those shapes are rejected rather than coerced.
function parseQuery(value) {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== 'object' || Array.isArray(value) || Buffer.isBuffer(value)) {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, 'query must be a plain object');
  }
  return value;
}

// Validate each name/value with Node's own HTTP validators so a malformed
// header (illegal token, CRLF injection) is rejected here as INVALID_MESSAGE,
// not left to throw synchronously from http.request() later — where it would
// be caught by the outbound try/catch and misclassified as SIDECAR_UNAVAILABLE
// even though the sidecar was never contacted.
function parseHeaders(value) {
  if (value === undefined || value === null || value === '') {
    return {};
  }
  const parsed = typeof value === 'string' ? JSON.parse(value) : value;
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, 'headers must be a JSON object');
  }
  const headers = {};
  for (const [key, item] of Object.entries(parsed)) {
    const str = String(item);
    try {
      http.validateHeaderName(key);
      http.validateHeaderValue(key, str);
    } catch (err) {
      throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid header ${key}: ${err.message}`);
    }
    headers[key] = str;
  }
  return headers;
}

module.exports = function registerDaprInvoke(RED) {
  function DaprInvokeNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (
      !connection?.options ||
      typeof connection.isSidecarHealthy !== 'function' ||
      typeof connection.onSidecarHealth !== 'function'
    ) {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      node.on('input', (_msg, _send, done) => {
        done(new DaprError(ErrorCodes.INVALID_OPTIONS, 'Dapr connection is unavailable'));
      });
      return;
    }

    const removeHealthListener = connection.onSidecarHealth((healthy) =>
      node.status(
        healthy
          ? { fill: 'green', shape: 'dot', text: 'ready' }
          : { fill: 'red', shape: 'ring', text: 'sidecar unavailable' }
      )
    );

    const inflight = new Set(); // AbortControllers for outbound calls in flight

    node.on('input', async (msg, send, done) => {
      if (!connection.isSidecarHealthy()) {
        done(new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'Dapr sidecar is unavailable'));
        return;
      }
      const override = msg.dapr && typeof msg.dapr === 'object' ? msg.dapr : {};
      const appId = override.appId ?? config.appId;
      const methodPath = override.method ?? config.method;
      const verb = String(override.verb ?? config.verb ?? 'GET').toUpperCase();

      if (typeof appId !== 'string' || appId.trim() === '') {
        done(new DaprError(ErrorCodes.INVALID_MESSAGE, 'appId is required'));
        return;
      }
      if (typeof methodPath !== 'string' || methodPath.trim() === '') {
        done(new DaprError(ErrorCodes.INVALID_MESSAGE, 'method is required'));
        return;
      }
      if (!SUPPORTED_VERBS.has(verb)) {
        done(new DaprError(ErrorCodes.INVALID_MESSAGE, `unsupported HTTP verb: ${verb}`));
        return;
      }

      let headers;
      let encoded;
      let timeoutMs;
      let contentType;
      let query;
      try {
        // A message override of headers is validated exactly like the configured
        // headers (object shape + string coercion), never spread raw.
        headers = { ...parseHeaders(config.headers), ...parseHeaders(override.headers) };
        contentType = validateContentType(override.contentType ?? config.contentType);
        query = parseQuery(override.query);
        // encodeBody can throw on a non-serializable payload (BigInt, circular);
        // surface it as a clean INVALID_MESSAGE rather than an uncaught throw.
        encoded = encodeBody(msg.payload);
        timeoutMs = resolveTimeoutMs(
          override.timeoutSec,
          config.timeoutSec,
          connection.options.limits.requestTimeoutMs
        );
      } catch (err) {
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.INVALID_MESSAGE, err.message, { cause: err })
        );
        return;
      }
      const resolvedType = contentType || encoded.contentType;
      if (resolvedType && encoded.body !== undefined) {
        headers['content-type'] = resolvedType;
      }

      const controller = new AbortController();
      inflight.add(controller);
      try {
        const result = await invoke(
          {
            baseUrl: connection.options.outbound.baseUrl,
            token: connection.options.daprApiToken,
          },
          {
            appId: appId.trim(),
            methodPath: methodPath.trim(),
            verb,
            headers,
            query,
            body: encoded.body,
            timeoutMs,
            signal: controller.signal,
          }
        );
        msg.payload = decodeBody(result.body, result.headers['content-type']);
        msg.dapr = {
          ...override,
          appId: appId.trim(),
          method: methodPath.trim(),
          verb,
          statusCode: result.status,
          headers: result.headers,
        };
        node.status({ fill: 'green', shape: 'dot', text: `${result.status}` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'invoke failed' });
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, err.message, { cause: err })
        );
      } finally {
        inflight.delete(controller);
      }
    });

    node.on('close', (_removed, done) => {
      removeHealthListener();
      // Abort any outbound call still in flight so it cannot outlive the node.
      for (const controller of inflight) {
        controller.abort();
      }
      inflight.clear();
      done();
    });
  }

  RED.nodes.registerType('dapr-invoke', DaprInvokeNode);
};
