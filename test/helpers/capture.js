'use strict';

const http = require('node:http');
const { closeHttpServer } = require('./http');

// A throwaway HTTP server that JSON-decodes every POSTed body and appends it
// to `received`, in arrival order. Shared by every integration test that
// wires a flow's "http request" node back to itself as an observability
// hook — the only way a black-box test can see what a flow actually did.
async function startCapture() {
  const received = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try {
        received.push(JSON.parse(Buffer.concat(chunks).toString()));
      } catch {
        received.push(null);
      }
      res.writeHead(200).end('ok');
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    received,
    url: `http://127.0.0.1:${server.address().port}/capture`,
    stop: () => closeHttpServer(server),
  };
}

module.exports = { startCapture };
