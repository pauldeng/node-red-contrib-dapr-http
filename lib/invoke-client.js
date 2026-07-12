'use strict';

const http = require('node:http');

const { DaprError, ErrorCodes } = require('./errors');
const { HOP_BY_HOP } = require('./http-headers');

// Build the sidecar invoke path from an app id and method, rejecting anything
// that could escape the /v1.0/invoke/<app>/method/<method> route. `URL.pathname`
// silently normalizes `..` segments, so a raw `../../v1.0/metadata` method would
// otherwise redirect the request — with the app's API token — to another Dapr
// control-plane API. Each segment is validated and percent-encoded.
function buildInvokePath(appId, methodPath) {
  const id = String(appId ?? '').trim();
  if (id === '' || id === '.' || id === '..' || id.includes('/')) {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid app id: ${appId}`);
  }
  const segments = String(methodPath ?? '')
    .split('/')
    .filter((s) => s !== '');
  if (segments.length === 0) {
    throw new DaprError(ErrorCodes.INVALID_MESSAGE, 'method is required');
  }
  for (const segment of segments) {
    if (segment === '.' || segment === '..') {
      throw new DaprError(ErrorCodes.INVALID_MESSAGE, `invalid method segment: ${segment}`);
    }
  }
  const encoded = segments.map(encodeURIComponent).join('/');
  return `/v1.0/invoke/${encodeURIComponent(id)}/method/${encoded}`;
}

function rawRequest(
  baseUrl,
  { appId, methodPath, verb, headers, query, body, timeoutMs = 30000, signal }
) {
  return new Promise((resolve, reject) => {
    const url = new URL(baseUrl);
    // Validated + encoded separately so `..` cannot escape the invoke route.
    const invokePath = buildInvokePath(appId, methodPath);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        url.searchParams.append(key, String(value));
      }
    }
    // Normalize the body and set a definite Content-Length so node:http does not
    // fall back to chunked transfer-encoding.
    const bodyBuf =
      body === undefined || body === null
        ? null
        : Buffer.isBuffer(body)
          ? body
          : Buffer.from(String(body));
    const reqHeaders = { ...headers };
    if (bodyBuf) {
      reqHeaders['content-length'] = String(bodyBuf.length);
    }
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: `${invokePath}${url.search}`,
        method: verb,
        headers: reqHeaders,
        // Use the process-global keep-alive agent (Node's default) so repeated
        // invocations reuse pooled sockets — keep-alive is centrally owned, never
        // a per-node one-shot agent.
        // Node destroys the request if the signal aborts (node close), so an
        // outbound call cannot outlive its node.
        signal,
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('error', (err) => settle(() => reject(err)));
        res.on('aborted', () => settle(() => reject(new Error('response aborted'))));
        res.on('end', () =>
          settle(() =>
            resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) })
          )
        );
      }
    );
    const timer = setTimeout(
      () => req.destroy(new Error(`invoke exceeded ${timeoutMs}ms deadline`)),
      Math.max(1, timeoutMs)
    );
    const settle = (fn) => {
      clearTimeout(timer);
      fn();
    };
    req.on('error', (err) => settle(() => reject(err)));
    if (bodyBuf) {
      req.write(bodyBuf);
    }
    req.end();
  });
}

// Invoke a method on another Dapr app through the local sidecar
// (POST/GET/... /v1.0/invoke/<app-id>/method/<method>) over native node:http, so
// status, headers, query, and binary bodies are preserved. Caller-supplied
// hop-by-hop headers are dropped and the Dapr API token is forced (a message
// override cannot replace it). Resolves { status, headers, body: Buffer }.
function invoke({ baseUrl, token }, request) {
  const headers = {};
  for (const [key, value] of Object.entries(request.headers || {})) {
    const lower = key.toLowerCase();
    if (HOP_BY_HOP.has(lower) || lower === 'dapr-api-token') {
      continue;
    }
    headers[key] = value;
  }
  if (token) {
    headers['dapr-api-token'] = token;
  }
  return rawRequest(baseUrl, { ...request, headers });
}

// Serialize a Node-RED payload into an HTTP body with an inferred content type
// (mirrors publish inference): Buffer -> octet-stream, string -> text/plain,
// null/undefined -> no body, otherwise JSON.
function encodeBody(payload) {
  if (payload === undefined || payload === null) {
    return { body: undefined, contentType: undefined };
  }
  if (Buffer.isBuffer(payload)) {
    return { body: payload, contentType: 'application/octet-stream' };
  }
  if (typeof payload === 'string') {
    return { body: payload, contentType: 'text/plain' };
  }
  return { body: JSON.stringify(payload), contentType: 'application/json' };
}

// Decode an HTTP body Buffer by content type: JSON -> parsed value, text/* ->
// string, empty -> undefined, otherwise the raw Buffer.
function decodeBody(buffer, contentType) {
  if (!buffer || buffer.length === 0) {
    return undefined;
  }
  const type = (contentType || '').toLowerCase();
  if (type.includes('application/json') || type.includes('+json')) {
    try {
      return JSON.parse(buffer.toString());
    } catch {
      return buffer.toString();
    }
  }
  if (type.startsWith('text/')) {
    return buffer.toString();
  }
  return buffer;
}

module.exports = { invoke, buildInvokePath, encodeBody, decodeBody };
