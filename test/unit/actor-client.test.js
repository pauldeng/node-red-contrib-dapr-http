'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { once } = require('node:events');

const { buildActorPath, invoke, readRecord, saveRecord } = require('../../lib/actor-client');
const { DaprError, ErrorCodes } = require('../../lib/errors');

async function fakeSidecar(handler) {
  const server = http.createServer(handler);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}`,
    stop: async () => {
      server.closeAllConnections();
      server.close();
      await once(server, 'close');
    },
  };
}

async function recordingSidecar(status, body, extraHeaders = {}) {
  const requests = [];
  const sidecar = await fakeSidecar((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url, 'http://localhost');
      requests.push({
        method: req.method,
        path: url.pathname,
        headers: req.headers,
        body: Buffer.concat(chunks).toString(),
      });
      res.writeHead(status, { 'content-type': 'application/json', ...extraHeaders }).end(body);
    });
  });
  return { ...sidecar, requests };
}

test('buildActorPath percent-encodes and validates every segment', () => {
  assert.equal(buildActorPath('T', 'id'), '/v1.0/actors/T/id');
  assert.equal(buildActorPath('T', 'id', 'method', 'M'), '/v1.0/actors/T/id/method/M');
  assert.throws(() => buildActorPath('..', 'id'), DaprError);
  assert.throws(() => buildActorPath('T', 'a/b'), DaprError);
});

test('invoke posts to /v1.0/actors/{type}/{id}/method/{method} with a JSON body', async (t) => {
  const sidecar = await recordingSidecar(200, '{"ok":true}');
  t.after(sidecar.stop);
  const result = await invoke(
    { baseUrl: sidecar.baseUrl, token: 'tok' },
    { actorType: 'T', actorId: 'a b', method: 'Do', body: { x: 1 } }
  );
  assert.equal(result.status, 200);
  assert.equal(sidecar.requests[0].method, 'POST');
  assert.equal(sidecar.requests[0].path, '/v1.0/actors/T/a%20b/method/Do');
  assert.equal(sidecar.requests[0].headers['dapr-api-token'], 'tok');
  assert.equal(sidecar.requests[0].body, '{"x":1}');
});

test('invoke with an absent body sends no request body', async (t) => {
  const sidecar = await recordingSidecar(200, '');
  t.after(sidecar.stop);
  await invoke({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a', method: 'Do' });
  assert.equal(sidecar.requests[0].body, '');
  assert.equal(sidecar.requests[0].headers['content-type'], undefined);
});

test('invoke throws ACTOR_INVOKE_FAILED on a confirmed non-2xx', async (t) => {
  const sidecar = await recordingSidecar(500, 'boom');
  t.after(sidecar.stop);
  await assert.rejects(
    invoke({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a', method: 'Do' }),
    (err) => {
      assert.ok(err instanceof DaprError);
      assert.equal(err.code, ErrorCodes.ACTOR_INVOKE_FAILED);
      // Never echoes the sidecar's own response body.
      assert.ok(!err.message.includes('boom'));
      return true;
    }
  );
});

test('invoke throws SIDECAR_UNAVAILABLE on a transport failure', async () => {
  await assert.rejects(
    invoke({ baseUrl: 'http://127.0.0.1:1' }, { actorType: 'T', actorId: 'a', method: 'Do' }),
    (err) => {
      assert.ok(err instanceof DaprError);
      assert.equal(err.code, ErrorCodes.SIDECAR_UNAVAILABLE);
      return true;
    }
  );
});

test('invoke retains bounded structured Dapr diagnostics without changing its stable error', async (t) => {
  const remote = {
    errorCode: 'ERR_ACTOR_INVOKE_METHOD',
    message: 'CONFLICT_EXAMPLE ' + 'x'.repeat(3000),
    privateField: 'not forwarded',
  };
  const sidecar = await recordingSidecar(500, JSON.stringify(remote));
  t.after(sidecar.stop);
  await assert.rejects(
    invoke({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a', method: 'Do' }),
    (err) => {
      assert.equal(err.code, ErrorCodes.ACTOR_INVOKE_FAILED);
      assert.equal(err.message, 'Dapr actor invoke failed with status 500');
      assert.deepEqual(err.cause, {
        statusCode: 500,
        errorCode: remote.errorCode,
        message: remote.message.slice(0, 2048),
      });
      return true;
    }
  );
});

test('invoke ignores malformed diagnostic fields and bounds the remote error code', async (t) => {
  for (const body of [
    'not json',
    'null',
    '{"errorCode":{},"message":[]}',
    JSON.stringify({ errorCode: 'x'.repeat(300), message: '' }),
  ]) {
    const sidecar = await recordingSidecar(503, body);
    t.after(sidecar.stop);
    await assert.rejects(
      invoke({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a', method: 'Do' }),
      (err) => {
        assert.equal(err.code, ErrorCodes.ACTOR_INVOKE_FAILED);
        assert.deepEqual(
          err.cause,
          body.includes('xxx')
            ? { statusCode: 503, errorCode: 'x'.repeat(128), message: '' }
            : { statusCode: 503 }
        );
        return true;
      }
    );
  }
});

test('readRecord resolves { exists: false } on 204', async (t) => {
  const sidecar = await recordingSidecar(204, '');
  t.after(sidecar.stop);
  const result = await readRecord({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a' });
  assert.deepEqual(result, { exists: false });
  assert.equal(sidecar.requests[0].method, 'GET');
  assert.equal(sidecar.requests[0].path, '/v1.0/actors/T/a/state/record');
});

test('readRecord resolves { exists: true, value } on 200, including a stored null', async (t) => {
  const sidecar = await recordingSidecar(200, 'null');
  t.after(sidecar.stop);
  const result = await readRecord({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a' });
  assert.deepEqual(result, { exists: true, value: null });
});

test('readRecord parses an ordinary JSON record', async (t) => {
  const sidecar = await recordingSidecar(200, '{"revision":3}');
  t.after(sidecar.stop);
  const result = await readRecord({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a' });
  assert.deepEqual(result, { exists: true, value: { revision: 3 } });
});

for (const body of ['', 'not json']) {
  test(`readRecord rejects malformed JSON ${JSON.stringify(body)} instead of treating it as null`, async (t) => {
    const sidecar = await recordingSidecar(200, body);
    t.after(sidecar.stop);
    await assert.rejects(
      readRecord({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a' }),
      (err) => {
        assert.equal(err.code, ErrorCodes.STATE_OPERATION_FAILED);
        return true;
      }
    );
  });
}

test('readRecord throws STATE_OPERATION_FAILED on a non-2xx/204 status', async (t) => {
  const sidecar = await recordingSidecar(400, 'ERR_ACTOR_INSTANCE_MISSING');
  t.after(sidecar.stop);
  await assert.rejects(
    readRecord({ baseUrl: sidecar.baseUrl }, { actorType: 'T', actorId: 'a' }),
    (err) => {
      assert.equal(err.code, ErrorCodes.STATE_OPERATION_FAILED);
      assert.ok(!err.message.includes('ERR_ACTOR_INSTANCE_MISSING'));
      return true;
    }
  );
});

test('saveRecord posts one upsert transaction with the raw JSON spliced in as JSON, not a string', async (t) => {
  const sidecar = await recordingSidecar(204, '');
  t.after(sidecar.stop);
  const result = await saveRecord(
    { baseUrl: sidecar.baseUrl, token: 'tok' },
    { actorType: 'T', actorId: 'a', valueJson: '{"revision":2}', timeoutMs: 5000 }
  );
  assert.equal(result.status, 204);
  assert.equal(sidecar.requests[0].method, 'POST');
  assert.equal(sidecar.requests[0].path, '/v1.0/actors/T/a/state');
  const sent = JSON.parse(sidecar.requests[0].body);
  assert.deepEqual(sent, [
    { operation: 'upsert', request: { key: 'record', value: { revision: 2 } } },
  ]);
});

test('saveRecord throws STATE_OPERATION_FAILED on a confirmed non-2xx (definite failure)', async (t) => {
  const sidecar = await recordingSidecar(400, 'ERR_ACTOR_INSTANCE_MISSING');
  t.after(sidecar.stop);
  await assert.rejects(
    saveRecord(
      { baseUrl: sidecar.baseUrl },
      { actorType: 'T', actorId: 'a', valueJson: 'null', timeoutMs: 1000 }
    ),
    (err) => {
      assert.equal(err.code, ErrorCodes.STATE_OPERATION_FAILED);
      return true;
    }
  );
});

test('saveRecord throws SIDECAR_UNAVAILABLE on a transport failure (unknown outcome)', async () => {
  await assert.rejects(
    saveRecord(
      { baseUrl: 'http://127.0.0.1:1' },
      { actorType: 'T', actorId: 'a', valueJson: 'null', timeoutMs: 1000 }
    ),
    (err) => {
      assert.equal(err.code, ErrorCodes.SIDECAR_UNAVAILABLE);
      return true;
    }
  );
});
