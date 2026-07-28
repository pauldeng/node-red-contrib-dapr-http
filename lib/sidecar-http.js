'use strict';

const http = require('node:http');

const { DaprError, ErrorCodes } = require('./errors');

// Fallback response cap for a direct caller that names none. Every caller in
// this package passes the connection's own configured body limit instead, so
// one operator-visible setting bounds both directions.
const DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

// One HTTP request to the Dapr sidecar's API, shared by every outbound caller
// (publish, service invocation, the health poll). `path` must already be encoded
// by the caller — this function never normalizes it, so `..` cannot be
// introduced here.
//
// Uses the process-global keep-alive agent (Node's default) so repeated calls
// reuse pooled sockets: keep-alive is centrally owned, never a per-node agent.
// `agent: false` opts one call out — the health poll does, so a sidecar going
// down cannot leave a pooled socket behind. The body is normalized to a Buffer
// with a definite Content-Length so node:http does not fall back to chunked
// transfer-encoding. `signal` destroys the request when the owning node closes,
// and `timeoutMs` bounds the whole exchange.
//
// The response body is bounded by `maxResponseBytes` and torn down the moment it
// is exceeded, so an invoked app returning a huge body cannot exhaust memory.
//
// Resolves { status, headers, body: Buffer } for every response status — a
// non-2xx is data, not an error. Rejects only on transport failure, abort, the
// deadline, or an over-size response (RESPONSE_TOO_LARGE).
function sidecarRequest(
  baseUrl,
  {
    path,
    method = 'GET',
    headers = {},
    query,
    body,
    timeoutMs = 30000,
    signal,
    agent,
    maxResponseBytes = DEFAULT_MAX_RESPONSE_BYTES,
  } = {}
) {
  return new Promise((resolve, reject) => {
    const url = new URL(baseUrl);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        url.searchParams.append(key, String(value));
      }
    }
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
        path: `${path}${url.search}`,
        method,
        headers: reqHeaders,
        signal,
        // `undefined` leaves Node on its process-global keep-alive agent.
        agent,
      },
      (res) => {
        const chunks = [];
        let size = 0;
        res.on('data', (chunk) => {
          size += chunk.length;
          if (size > maxResponseBytes) {
            // Tear the exchange down instead of finishing the read: the point is
            // to never hold the whole body, so this must not wait for 'end'.
            req.destroy();
            settle(() =>
              reject(
                new DaprError(
                  ErrorCodes.RESPONSE_TOO_LARGE,
                  `Dapr sidecar response exceeded the ${maxResponseBytes}-byte limit`
                )
              )
            );
            return;
          }
          chunks.push(chunk);
        });
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
      () => req.destroy(new Error(`Dapr sidecar request exceeded ${timeoutMs}ms deadline`)),
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

module.exports = { sidecarRequest };
