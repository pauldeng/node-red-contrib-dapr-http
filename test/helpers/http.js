'use strict';

const http = require('node:http');

// Perform one HTTP request over a fresh, non-pooled connection (`agent: false`)
// with an ABSOLUTE wall-clock deadline. No keep-alive socket lingers to keep the
// process alive after tests, and the timer bounds a socket that connects but
// never replies (a socket-inactivity timeout would reset on a slow trickle).
// Resolves { status, headers, text }.
function httpRequest(urlStr, { method = 'GET', headers = {}, body, timeoutMs = 30000 } = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: `${url.pathname}${url.search}`,
        method,
        headers,
        agent: false,
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('error', (err) => settle(() => reject(err)));
        res.on('aborted', () => settle(() => reject(new Error('response aborted'))));
        res.on('end', () =>
          settle(() =>
            resolve({
              status: res.statusCode,
              headers: res.headers,
              text: Buffer.concat(chunks).toString(),
            })
          )
        );
      }
    );
    const timer = setTimeout(
      () => req.destroy(new Error(`request exceeded ${timeoutMs}ms deadline`)),
      Math.max(1, timeoutMs)
    );
    // `settle` (defined after `timer`) is only ever invoked from the async
    // request/response handlers, well after both are initialized.
    const settle = (fn) => {
      clearTimeout(timer);
      fn();
    };
    req.on('error', (err) => settle(() => reject(err)));
    if (body !== undefined && body !== null) {
      req.write(body);
    }
    req.end();
  });
}

module.exports = { httpRequest };
