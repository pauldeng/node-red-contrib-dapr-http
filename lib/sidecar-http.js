'use strict';

const http = require('node:http');

// One HTTP request to the Dapr sidecar's API, shared by every outbound caller
// (publish, service invocation). `path` must already be encoded by the caller —
// this function never normalizes it, so `..` cannot be introduced here.
//
// Uses the process-global keep-alive agent (Node's default) so repeated calls
// reuse pooled sockets: keep-alive is centrally owned, never a per-node agent.
// The body is normalized to a Buffer with a definite Content-Length so node:http
// does not fall back to chunked transfer-encoding. `signal` destroys the request
// when the owning node closes, and `timeoutMs` bounds the whole exchange.
//
// Resolves { status, headers, body: Buffer } for every response status — a
// non-2xx is data, not an error. Rejects only on transport failure, abort, or
// the deadline.
function sidecarRequest(
  baseUrl,
  { path, method = 'GET', headers = {}, query, body, timeoutMs = 30000, signal } = {}
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
