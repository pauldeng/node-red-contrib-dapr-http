'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { createFakeDaprStarted } = require('../helpers/fake-dapr');

test('fake daprd: an unregistered route returns 404 but is still recorded', async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());

  const res = await fetch(`${dapr.url}/v1.0/not-registered`, { method: 'POST', body: 'x' });
  assert.equal(res.status, 404, 'unknown routes must fail loudly, not look successful');

  const rec = dapr.requests.find((r) => r.path === '/v1.0/not-registered');
  assert.ok(rec, 'the request should be recorded even when unrouted');
  assert.equal(rec.method, 'POST');
});

test('fake daprd: a registered responder handles its route', async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());

  dapr.respond('GET', '/v1.0/healthz/outbound', (req, res) => {
    res.writeHead(204).end();
  });

  const res = await fetch(`${dapr.url}/v1.0/healthz/outbound`);
  assert.equal(res.status, 204);
});
