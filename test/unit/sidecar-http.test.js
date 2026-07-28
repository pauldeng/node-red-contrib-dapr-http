'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { sidecarRequest } = require('../../lib/sidecar-http');
const { DaprError, ErrorCodes } = require('../../lib/errors');

async function fakeSidecar(handler) {
  const server = http.createServer(handler);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}`,
    stop: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  };
}

test('a response body over maxResponseBytes is rejected, not buffered', async (t) => {
  // Stream well past the cap in many chunks: the request must be torn down part
  // way through rather than accumulating the whole body in memory first.
  const chunk = Buffer.alloc(1024, 0x61);
  let written = 0;
  const sidecar = await fakeSidecar((req, res) => {
    res.writeHead(200, { 'content-type': 'application/octet-stream' });
    const pump = () => {
      if (written >= 64 * 1024) {
        res.end();
        return;
      }
      written += chunk.length;
      if (res.write(chunk)) {
        setImmediate(pump);
      } else {
        res.once('drain', pump);
      }
    };
    pump();
  });
  t.after(() => sidecar.stop());

  await assert.rejects(
    sidecarRequest(sidecar.baseUrl, { path: '/v1.0/invoke/a/method/b', maxResponseBytes: 2048 }),
    (err) => {
      assert.ok(err instanceof DaprError);
      assert.equal(err.code, ErrorCodes.RESPONSE_TOO_LARGE);
      assert.match(err.message, /2048/);
      return true;
    }
  );
});

test('a response body exactly at maxResponseBytes is returned intact', async (t) => {
  const body = Buffer.alloc(2048, 0x62);
  const sidecar = await fakeSidecar((req, res) => {
    res.writeHead(200, { 'content-type': 'application/octet-stream' });
    res.end(body);
  });
  t.after(() => sidecar.stop());

  const result = await sidecarRequest(sidecar.baseUrl, {
    path: '/v1.0/invoke/a/method/b',
    maxResponseBytes: 2048,
  });
  assert.equal(result.status, 200);
  assert.deepEqual(result.body, body);
});

test('agent: false opts a request out of the shared keep-alive pool', async (t) => {
  const seen = [];
  const sidecar = await fakeSidecar((req, res) => {
    seen.push(req.headers.connection);
    res.writeHead(204);
    res.end();
  });
  t.after(() => sidecar.stop());

  // The health poll must not leave a pooled socket behind for a sidecar that is
  // going down, so it opts out; every other caller reuses the global agent.
  await sidecarRequest(sidecar.baseUrl, { path: '/v1.0/healthz/outbound', agent: false });
  await sidecarRequest(sidecar.baseUrl, { path: '/v1.0/healthz/outbound' });

  assert.equal(seen[0], 'close', 'agent: false sends Connection: close');
  assert.equal(seen[1], 'keep-alive', 'the default global agent pools the socket');
});
