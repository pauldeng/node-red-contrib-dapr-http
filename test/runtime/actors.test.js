'use strict';

// Real Node-RED + a fake Dapr sidecar (test/helpers/fake-dapr.js) standing in
// for daprd's outbound API (actor state read/save, and the target of
// dapr-actor-call's own outbound invoke). This file plays daprd's OTHER role
// itself: every PUT to this connection's own app-channel port
// (inboundMethodPath) is exactly the callback daprd would make -- see
// test/integration/actors-probe.test.js for the real-wire protocol proof and
// test/integration/actors.test.js for the real-daprd+Placement round trip.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');
const { waitForFast } = require('../helpers/wait-for');

const healthPath = '/v1.0/healthz/outbound';
const NOT_YET = Symbol('not yet settled');

const stateReadPath = (type, id) => `/v1.0/actors/${type}/${id}/state/record`;
const stateSavePath = (type, id) => `/v1.0/actors/${type}/${id}/state`;
// The sidecar-facing shape (what daprd itself exposes, and what a fake dapr
// responder is registered under for dapr-actor-call's own outbound target).
const outboundMethodPath = (type, id, method) => `/v1.0/actors/${type}/${id}/method/${method}`;
// The app-channel shape (what daprd calls INTO the app) -- this is the one a
// test PUTs directly, playing daprd's part.
const inboundMethodPath = (type, id, method) => `/actors/${type}/${id}/method/${method}`;

function connectionNode(appPort, daprPort, extra = {}) {
  return {
    id: 'c1',
    type: 'dapr-connection',
    daprHost: '127.0.0.1',
    daprPort: String(daprPort),
    bindAddress: '127.0.0.1',
    appPort: String(appPort),
    ...extra,
  };
}

function actorPut(appPort, type, id, method, body) {
  return httpRequest(`http://127.0.0.1:${appPort}${inboundMethodPath(type, id, method)}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    timeoutMs: 8000,
  });
}

// Races a real promise against a bounded window that must NOT have fired yet.
// There is no event for "has not happened" to subscribe to, so this is the
// one legitimate use of a fixed wait as a negative assertion (see
// docs/testing.md "A bounded settle after the real signal").
function notYetSettled(promise, ms) {
  return Promise.race([promise, delay(ms, NOT_YET)]);
}

async function startHarness(t, { respondents = [] } = {}) {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
  for (const [method, path, responder] of respondents) {
    dapr.respond(method, path, responder);
  }
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  const appPort = await freePort();
  return { dapr, nr, appPort };
}

test(
  'method -> Function -> reply: a real PUT round trips only after the save actually resolves',
  { timeout: 30000 },
  async (t) => {
    const saveGate = Promise.withResolvers();
    let savedBody = null;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('RT', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('RT', 'd1'),
          async (_req, res, ctx) => {
            savedBody = ctx.body.toString('utf8');
            await saveGate.promise;
            res.writeHead(204).end();
          },
        ],
      ],
    });

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'rt' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'RT',
        method: 'Do',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { echoed: msg.payload };\nmsg.dapr.actor.nextState = { seen: true };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    const pending = actorPut(appPort, 'RT', 'd1', 'Do', { k: 1 });
    await dapr.waitForRequest((r) => r.method === 'POST' && r.path === stateSavePath('RT', 'd1'));

    const race = await notYetSettled(pending, 200);
    assert.equal(race, NOT_YET, 'the caller must not see a response before the save resolves');

    saveGate.resolve();
    const res = await pending;
    assert.equal(res.status, 200, res.text);
    assert.deepEqual(JSON.parse(res.text), { echoed: { k: 1 } });
    assert.equal(
      savedBody,
      JSON.stringify([{ operation: 'upsert', request: { key: 'record', value: { seen: true } } }])
    );
  }
);

test(
  'first reply wins: two replies to the same invocation produce exactly one save',
  { timeout: 30000 },
  async (t) => {
    const saves = [];
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('FW', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('FW', 'd1'),
          (_req, res, ctx) => {
            saves.push(ctx.body.toString('utf8'));
            res.writeHead(204).end();
          },
        ],
      ],
    });

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'fw' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'FW',
        method: 'Do',
        wires: [['fnA', 'fnB']],
      },
      {
        id: 'fnA',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { first: true };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'fnB',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { second: true };\nreturn msg;',
        outputs: 1,
        wires: [['reply2']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
      {
        id: 'reply2',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    const res = await actorPut(appPort, 'FW', 'd1', 'Do', {});
    assert.equal(res.status, 200, res.text);
    assert.equal(saves.length, 1, 'exactly one save must have been issued');
    const envelope = (value) =>
      JSON.stringify([{ operation: 'upsert', request: { key: 'record', value } }]);
    assert.ok(
      saves[0] === envelope({ first: true }) || saves[0] === envelope({ second: true }),
      `unexpected saved body: ${saves[0]}`
    );

    await nr.waitForLog(/no pending actor request/i, { timeoutMs: 5000 });
  }
);

test(
  'a reply that arrives after the invocation deadline settles nothing and produces no save',
  { timeout: 30000 },
  async (t) => {
    let saveCalls = 0;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('EXP', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('EXP', 'd1'),
          (_req, res) => {
            saveCalls += 1;
            res.writeHead(204).end();
          },
        ],
      ],
    });

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'exp' },
      // A short request timeout keeps the pending-reply reserve window
      // (min(drainTimeoutMs, requestTimeoutMs/2)) small, so this test does
      // not have to wait out the default 30s deadline.
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '1' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'EXP',
        method: 'Slow',
        wires: [['dly']],
      },
      {
        id: 'dly',
        type: 'delay',
        z: 'tab',
        pauseType: 'delay',
        timeout: '900',
        timeoutUnits: 'milliseconds',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { lateWrite: true };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    const res = await actorPut(appPort, 'EXP', 'd1', 'Slow', {});
    assert.equal(res.status, 503, res.text);

    await nr.waitForLog(/no pending actor request/i, { timeoutMs: 5000 });
    assert.equal(
      saveCalls,
      0,
      'a reply after expiry must never trigger a save, even with a nextState'
    );
  }
);

test(
  'a non-JSON-serializable nextState fails the reply node as INVALID_MESSAGE, which a scoped Catch can turn into a fail reply, and triggers no save',
  { timeout: 30000 },
  async (t) => {
    let saveCalls = 0;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('BAD', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('BAD', 'd1'),
          (_req, res) => {
            saveCalls += 1;
            res.writeHead(204).end();
          },
        ],
      ],
    });

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bad' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'BAD',
        method: 'Do',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = 10n;\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
      // serializeProposal throws INVALID_MESSAGE (BigInt) before the reply
      // node ever settles the pending proposal, so this Catch fires while the
      // request is still open -- exactly the fail/Catch wiring the reply
      // node's help documents.
      {
        id: 'catch1',
        type: 'catch',
        z: 'tab',
        scope: ['reply1'],
        uncaught: false,
        wires: [['fn2']],
      },
      {
        id: 'fn2',
        type: 'function',
        z: 'tab',
        func: "delete msg.dapr.actor.nextState;\nmsg.dapr.actor.error = { code: msg.error.code, message: 'rejected' };\nreturn msg;",
        outputs: 1,
        wires: [['reply2']],
      },
      {
        id: 'reply2',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'fail',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    const res = await actorPut(appPort, 'BAD', 'd1', 'Do', {});
    assert.equal(res.status, 500, res.text);
    assert.deepEqual(JSON.parse(res.text), {
      error: { code: 'INVALID_MESSAGE', message: 'rejected' },
    });
    assert.equal(saveCalls, 0, 'an invalid nextState must never reach a save');
  }
);

test(
  'a fail reply produces a 500 with the flow-supplied error and no save',
  { timeout: 30000 },
  async (t) => {
    let saveCalls = 0;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('FAIL', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('FAIL', 'd1'),
          (_req, res) => {
            saveCalls += 1;
            res.writeHead(204).end();
          },
        ],
      ],
    });

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'fail' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'FAIL',
        method: 'Bad',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: "msg.dapr.actor.error = { code: 'BAD_REPORT', message: 'nope' };\nreturn msg;",
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'fail',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    const res = await actorPut(appPort, 'FAIL', 'd1', 'Bad', {});
    assert.equal(res.status, 500, res.text);
    assert.deepEqual(JSON.parse(res.text), { error: { code: 'BAD_REPORT', message: 'nope' } });
    assert.equal(saveCalls, 0);
  }
);

test(
  'a duplicate (actorType, method) on one connection is rejected and logged',
  { timeout: 30000 },
  async (t) => {
    const { dapr, nr, appPort } = await startHarness(t);
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'dup' },
      connectionNode(appPort, dapr.port),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'DUP',
        method: 'Do',
        wires: [[]],
      },
      {
        id: 'm2',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'DUP',
        method: 'Do',
        wires: [[]],
      },
    ]);
    // Node status is not observable over this harness's plain HTTP surface
    // (no /comms websocket client); the log line comes from the exact same
    // catch block that also sets the node's red ring status
    // (nodes/dapr-actor-method.js), so it is equivalent evidence that
    // registration was rejected.
    await nr.waitForLog(/duplicate actor method DUP\/Do/i, { timeoutMs: 5000 });
  }
);

test(
  'an unknown reply outcome (a hand-authored/legacy flow bypassing the editor select) is rejected at construction, not silently treated as complete',
  { timeout: 30000 },
  async (t) => {
    const { dapr, nr, appPort } = await startHarness(t);
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bogus' },
      connectionNode(appPort, dapr.port),
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'bogus',
        wires: [],
      },
    ]);
    await nr.waitForLog(/unknown actor reply outcome: bogus/i, { timeoutMs: 5000 });
  }
);

function callFlow({ appPort, daprPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'call' },
    connectionNode(appPort, daprPort, { requestTimeoutSec: '5' }),
    { id: 'in', type: 'http in', z: 'tab', url: '/call', method: 'post', wires: [['before']] },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload.dapr; msg.payload = msg.payload.payload; return msg;',
      outputs: 1,
      wires: [['call1']],
    },
    {
      id: 'call1',
      type: 'dapr-actor-call',
      z: 'tab',
      connection: 'c1',
      actorType: 'ConfigType',
      actorId: 'config-id',
      method: 'ConfigMethod',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200; msg.payload = { payload: msg.payload }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    {
      id: 'errors',
      type: 'catch',
      z: 'tab',
      scope: ['call1'],
      uncaught: false,
      wires: [['failure']],
    },
    {
      id: 'failure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code, cause: msg.error.cause }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab' },
  ];
}

function postCall(nr, dapr, payload) {
  return httpRequest(nr.nodeUrl('/call'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ dapr, payload }),
    timeoutMs: 5000,
  });
}

test(
  'call node: Catch preserves Dapr conflict diagnostics and distinguishes transport failure',
  { timeout: 30000 },
  async (t) => {
    const { dapr, nr, appPort } = await startHarness(t);
    dapr.respond(
      'POST',
      outboundMethodPath('ConfigType', 'config-id', 'ConfigMethod'),
      (req, res, ctx) => {
        const code = JSON.parse(ctx.body.toString()).code;
        if (code === 'OUTAGE') {
          req.socket.destroy();
          return;
        }
        res.writeHead(500, { 'content-type': 'application/json' }).end(
          JSON.stringify({
            errorCode: 'ERR_ACTOR_INVOKE_METHOD',
            message: `error from actor service: (500) {"error":{"code":"${code}"}}`,
          })
        );
      }
    );
    await nr.deploy(callFlow({ appPort, daprPort: dapr.port }));
    await nr.waitForLog(/Dapr sidecar is available/i);
    for (const code of ['INVALID_ARGUMENT', 'CONFLICT_EXAMPLE', 'OUTAGE']) {
      const response = await postCall(nr, {}, { code });
      assert.equal(response.status, 503);
      const error = JSON.parse(response.text);
      assert.equal(error.code, code === 'OUTAGE' ? 'SIDECAR_UNAVAILABLE' : 'ACTOR_INVOKE_FAILED');
      if (code !== 'OUTAGE') {
        assert.equal(error.cause.statusCode, 500);
        assert.equal(error.cause.errorCode, 'ERR_ACTOR_INVOKE_METHOD');
        assert.ok(error.cause.message.includes(code));
      }
    }
  }
);

test(
  'shipped example catches complete-reply validation failure with no state write',
  { timeout: 30000 },
  async (t) => {
    let saves = 0;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('DemoActor', 'test'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('DemoActor', 'test'),
          (_req, res) => {
            saves += 1;
            res.writeHead(204).end();
          },
        ],
      ],
    });
    const flow = JSON.parse(
      require('node:fs').readFileSync(
        require('node:path').resolve(__dirname, '../../examples/actor-demo.json'),
        'utf8'
      )
    );
    Object.assign(
      flow.find((n) => n.type === 'dapr-connection'),
      connectionNode(appPort, dapr.port),
      { id: 'ex-actor-conn' }
    );
    await nr.deploy(flow);
    await dapr.waitForRequest(healthPath);
    let nested = 0;
    for (let i = 0; i < 35; i += 1) nested = [nested];
    const response = await actorPut(appPort, 'DemoActor', 'test', 'SetMyData', {
      extra: nested,
    });
    assert.equal(response.status, 500, response.text);
    assert.deepEqual(JSON.parse(response.text), {
      error: { code: 'INTERNAL', message: 'unexpected error' },
    });
    assert.equal(saves, 0);
    const read = await actorPut(appPort, 'DemoActor', 'test', 'GetMyData', {});
    assert.equal(read.status, 200, 'the actor gate must be released after the failure reply');
  }
);

test(
  'call node: msg.dapr.actorCall overrides the configured target; an absent payload sends no request body',
  { timeout: 30000 },
  async (t) => {
    const invoked = [];
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        [
          'POST',
          outboundMethodPath('OverrideType', 'override-id', 'OverrideMethod'),
          (_req, res, ctx) => {
            invoked.push(ctx.body);
            res
              .writeHead(200, { 'content-type': 'application/json' })
              .end(JSON.stringify({ result: 'overridden' }));
          },
        ],
      ],
    });
    await nr.deploy(callFlow({ appPort, daprPort: dapr.port }));
    await nr.waitForLog(/Dapr sidecar is available/i);

    const res = await postCall(nr, {
      actorCall: { type: 'OverrideType', id: 'override-id', method: 'OverrideMethod' },
    });
    assert.equal(res.status, 200, res.text);
    assert.deepEqual(JSON.parse(res.text).payload, { result: 'overridden' });
    assert.equal(invoked.length, 1);
    assert.equal(invoked[0].length, 0, 'an absent payload must send no request body');
  }
);

test(
  'call node: an explicit null payload sends a literal JSON null body, and a literal null result is preserved',
  { timeout: 30000 },
  async (t) => {
    const invoked = [];
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        [
          'POST',
          outboundMethodPath('ConfigType', 'config-id', 'ConfigMethod'),
          (_req, res, ctx) => {
            invoked.push(ctx.body.toString('utf8'));
            res.writeHead(200, { 'content-type': 'application/json' }).end('null');
          },
        ],
      ],
    });
    await nr.deploy(callFlow({ appPort, daprPort: dapr.port }));
    await nr.waitForLog(/Dapr sidecar is available/i);

    // No override -> the configured ConfigType/config-id/ConfigMethod is used.
    const res = await postCall(nr, {}, null);
    assert.equal(res.status, 200, res.text);
    assert.equal(JSON.parse(res.text).payload, null);
    assert.deepEqual(invoked, ['null']);
  }
);

test(
  'call node: a direct self-call (same actor type+id from within its own live invocation) fails immediately',
  { timeout: 30000 },
  async (t) => {
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [['GET', stateReadPath('Self', 'd1'), (_req, res) => res.writeHead(204).end()]],
    });

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'self' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'Self',
        method: 'Trigger',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: "msg.dapr.actorCall = { type: msg.dapr.actor.type, id: msg.dapr.actor.id, method: 'Other' };\nreturn msg;",
        outputs: 1,
        wires: [['call1']],
      },
      {
        id: 'call1',
        type: 'dapr-actor-call',
        z: 'tab',
        connection: 'c1',
        actorType: 'placeholder',
        actorId: 'placeholder',
        method: 'placeholder',
        wires: [['reply-complete']],
      },
      {
        id: 'reply-complete',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
      {
        id: 'catch1',
        type: 'catch',
        z: 'tab',
        scope: ['call1'],
        uncaught: false,
        wires: [['fnfail']],
      },
      {
        id: 'fnfail',
        type: 'function',
        z: 'tab',
        func: "msg.dapr.actor.error = { code: msg.error.code, message: 'blocked' };\nreturn msg;",
        outputs: 1,
        wires: [['reply-fail']],
      },
      {
        id: 'reply-fail',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'fail',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    const res = await actorPut(appPort, 'Self', 'd1', 'Trigger', {});
    assert.equal(res.status, 500, res.text);
    assert.deepEqual(JSON.parse(res.text), {
      error: { code: 'ACTOR_SELF_CALL', message: 'blocked' },
    });
  }
);

test(
  'an unchanged actor flow survives a full redeploy without a restart warning, and keeps serving',
  { timeout: 30000 },
  async (t) => {
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('RD', 'd1'), (_req, res) => res.writeHead(204).end()],
        ['POST', stateSavePath('RD', 'd1'), (_req, res) => res.writeHead(204).end()],
      ],
    });

    const flow = [
      { id: 'tab', type: 'tab', label: 'rd' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'RD',
        method: 'Do',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { n: 1 };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ];
    await nr.deploy(flow);
    await dapr.waitForRequest(healthPath);

    // Establish daprd's served actor fingerprint (one discovery fetch) BEFORE
    // redeploying -- without this, servedActorFingerprint stays null and
    // connectionStatus() never raises the actor-restart warning regardless of
    // whether the fingerprint actually changed (see lib/connection-status.js),
    // which would make the "no warning" assertion below vacuous.
    const configFetch = await httpRequest(`http://127.0.0.1:${appPort}/dapr/config`, {
      timeoutMs: 2000,
    });
    assert.equal(configFetch.status, 200, configFetch.text);

    const first = await actorPut(appPort, 'RD', 'd1', 'Do', {});
    assert.equal(first.status, 200, first.text);

    await nr.deploy(flow); // identical config, full redeploy
    await delay(300); // allow-timer: bounded negative -- no restart warning should ever appear

    assert.equal(
      /restart the Dapr sidecar/i.test(nr.logText()),
      false,
      'an unchanged actor method set must not request a restart'
    );

    // The new connection instance's lease reacquire (grace window) is async;
    // poll via the real endpoint rather than a second fixed sleep.
    const second = await waitForFast(async () => {
      const r = await actorPut(appPort, 'RD', 'd1', 'Do', {});
      return r.status === 200 ? r : null;
    });
    assert.equal(
      second.status,
      200,
      'the connection must keep serving actor invocations after redeploy'
    );
  }
);

// ---- milestone 3: commit-aware close ---------------------------------------

test(
  'full redeploy mid-commit: the in-flight caller gets its real outcome, no second save starts, and a concurrent call during the drain window is rejected rather than silently waiting',
  { timeout: 30000 },
  async (t) => {
    const saveGate = Promise.withResolvers();
    let saveCalls = 0;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('MC', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('MC', 'd1'),
          async (_req, res) => {
            saveCalls += 1;
            await saveGate.promise;
            res.writeHead(204).end();
          },
        ],
        // A different actor id, so a probe sent during the drain window has
        // somewhere to succeed IF it is not rejected -- proving the rejection
        // is really about the connection closing, not a missing responder.
        ['GET', stateReadPath('MC', 'd2'), (_req, res) => res.writeHead(204).end()],
        ['POST', stateSavePath('MC', 'd2'), (_req, res) => res.writeHead(204).end()],
      ],
    });

    const flow = [
      { id: 'tab', type: 'tab', label: 'mc' },
      // A generous request timeout so the commit's own client-side budget
      // (min(drainTimeoutMs, requestTimeoutMs/2) - COMMIT_MARGIN_MS, capped
      // at ~4.75s by the fixed 5s drainTimeoutMs regardless of how large this
      // is set) comfortably outlasts this test's own probe-then-release
      // choreography below -- otherwise the commit's own client-side timeout
      // (see lib/sidecar-http.js) would settle it first, independently of
      // whether the close path waits correctly at all.
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '30' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'MC',
        method: 'Do',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { n: 1 };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ];
    await nr.deploy(flow);
    await dapr.waitForRequest(healthPath);

    const pending = actorPut(appPort, 'MC', 'd1', 'Do', {});
    await dapr.waitForRequest((r) => r.method === 'POST' && r.path === stateSavePath('MC', 'd1'));
    assert.equal(saveCalls, 1);

    // Redeploy the identical flow (a full deploy stops and restarts every
    // node regardless of config change) WITHOUT awaiting it -- the old
    // connection's close handler is now blocked on the held commit.
    const redeployPromise = nr.deploy(flow);

    // A fresh call to an unrelated actor, sent while the old generation is
    // still closing, must be rejected promptly with a retryable 503 -- never
    // silently served, and never a 404. Node-RED stops every node in the flow
    // concurrently, so the actor-method node's own close can unregister and
    // re-activate the (shared, module-scoped) app-channel entry with an empty
    // actor registry before the connection node's own close finishes waiting
    // on whenCommitsSettled(); without lib/app-channel.js's actor tombstones
    // that briefly 404s (daprd treats an actor-method 404 as a permanent
    // "not found", so an ordinary redeploy would drop the call rather than
    // let it retry) -- the tombstone makes it retryable instead.
    const duringDrain = await waitForFast(async () => {
      const r = await actorPut(appPort, 'MC', 'd2', 'Do', {});
      return r.status === 503 ? r : null;
    });
    assert.equal(duringDrain.status, 503, duringDrain.text);

    // The original caller must still be waiting -- not answered by the
    // release path's generic 503 while its commit is genuinely in flight.
    const race = await notYetSettled(pending, 200);
    assert.equal(race, NOT_YET, 'the caller must not see a response before the save resolves');

    saveGate.resolve();
    const res = await pending;
    assert.equal(res.status, 200, res.text);
    assert.deepEqual(JSON.parse(res.text), { ok: true });
    assert.equal(saveCalls, 1, 'no second save must have started for that actor');

    await redeployPromise;

    // The new generation keeps serving afterward.
    const afterRedeploy = await waitForFast(async () => {
      const r = await actorPut(appPort, 'MC', 'd1', 'Do', {});
      return r.status === 200 ? r : null;
    });
    assert.equal(afterRedeploy.status, 200, afterRedeploy.text);
  }
);

test(
  'a modified deploy touching only the method node keeps the connection serving actor calls',
  { timeout: 30000 },
  async (t) => {
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('MM', 'd1'), (_req, res) => res.writeHead(204).end()],
        ['POST', stateSavePath('MM', 'd1'), (_req, res) => res.writeHead(204).end()],
      ],
    });
    const methodNode = (name) => ({
      id: 'm1',
      type: 'dapr-actor-method',
      z: 'tab',
      name,
      connection: 'c1',
      actorType: 'MM',
      method: 'Do',
      wires: [['fn1']],
    });
    const flow = (name) => [
      { id: 'tab', type: 'tab', label: 'mm' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      methodNode(name),
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { n: 1 };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ];
    await nr.deploy(flow(undefined));
    await dapr.waitForRequest(healthPath);

    const before = await actorPut(appPort, 'MM', 'd1', 'Do', {});
    assert.equal(before.status, 200, before.text);

    // Only m1's own config differs (its `name`) -- a 'nodes' deploy restarts
    // only that node (and, transitively, any config node it references, but
    // not this connection, whose own config is byte-identical).
    await nr.deploy(flow('renamed'), { deploymentType: 'nodes' });

    const after = await waitForFast(async () => {
      const r = await actorPut(appPort, 'MM', 'd1', 'Do', {});
      return r.status === 200 ? r : null;
    });
    assert.equal(after.status, 200, after.text);
  }
);

test(
  'a modified deploy touching only the reply node keeps the connection serving actor calls, and a proposal already queued in an unchanged upstream node still completes normally through the new instance',
  { timeout: 30000 },
  async (t) => {
    let saveCalls = 0;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('MR', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('MR', 'd1'),
          (_req, res) => {
            saveCalls += 1;
            res.writeHead(204).end();
          },
        ],
        ['GET', stateReadPath('MR', 'd2'), (_req, res) => res.writeHead(204).end()],
        ['POST', stateSavePath('MR', 'd2'), (_req, res) => res.writeHead(204).end()],
      ],
    });
    const replyNode = (name) => ({
      id: 'reply1',
      type: 'dapr-actor-reply',
      z: 'tab',
      name,
      connection: 'c1',
      outcome: 'complete',
      wires: [],
    });
    const flow = (name) => [
      { id: 'tab', type: 'tab', label: 'mr' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'MR',
        method: 'Do',
        wires: [['dly']],
      },
      // Holds the message so the reply node can be modified mid-flight. A
      // delay node's own config never changes across these two deploys, so
      // its queue survives a redeploy that only touches reply1.
      {
        id: 'dly',
        type: 'delay',
        z: 'tab',
        pauseType: 'delay',
        timeout: '500',
        timeoutUnits: 'milliseconds',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { n: 1 };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      replyNode(name),
    ];
    await nr.deploy(flow(undefined));
    await dapr.waitForRequest(healthPath);

    const pending = actorPut(appPort, 'MR', 'd1', 'Do', {});
    // No wait needed before redeploying: `dly`'s own config is unchanged, so
    // this 'nodes' deploy (which only touches reply1) never stops/recreates
    // it regardless of exactly when the in-flight message reaches its queue
    // -- the queue survives by construction, not by timing luck.
    await nr.deploy(flow('renamed'), { deploymentType: 'nodes' });

    const res = await pending;
    assert.equal(res.status, 200, res.text);
    assert.deepEqual(JSON.parse(res.text), { ok: true });
    assert.equal(saveCalls, 1, 'the queued proposal must still commit exactly once');

    // The connection keeps serving afterward.
    const after = await actorPut(appPort, 'MR', 'd2', 'Do', {});
    assert.equal(after.status, 200, after.text);
  }
);

test(
  'a modified deploy that replaces the node actually holding an in-flight proposal drops it: the caller gets a 503 with no save',
  { timeout: 30000 },
  async (t) => {
    let saveCalls = 0;
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('MD', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('MD', 'd1'),
          (_req, res) => {
            saveCalls += 1;
            res.writeHead(204).end();
          },
        ],
      ],
    });
    const delayNode = (timeout) => ({
      id: 'dly',
      type: 'delay',
      z: 'tab',
      pauseType: 'delay',
      timeout,
      timeoutUnits: 'milliseconds',
      wires: [['fn1']],
    });
    const flow = (timeout) => [
      { id: 'tab', type: 'tab', label: 'md' },
      // A short request timeout keeps this test's reserve/expiry window
      // small once the queued message is dropped.
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '2' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'MD',
        method: 'Do',
        wires: [['probe']],
      },
      // Logs immediately before handing the message to `dly`, so the test
      // below can wait for a real signal that the message has reached the
      // delay node's queue instead of guessing a fixed duration.
      {
        id: 'probe',
        type: 'function',
        z: 'tab',
        func: "node.warn('probe: MD/d1 queued');\nreturn msg;",
        outputs: 1,
        wires: [['dly']],
      },
      delayNode(timeout),
      // A second, delay-free method on the same actor type, used only to
      // prove the connection keeps serving afterward -- the dropped
      // proposal's own path (m1 -> dly) still points at a 60s-timeout delay
      // node after the redeploy, so re-using it here would just repeat the
      // same drop rather than proving recovery.
      {
        id: 'm2',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'MD',
        method: 'Fast',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { n: 1 };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ];
    await nr.deploy(flow('60000'));
    await dapr.waitForRequest(healthPath);

    const pending = actorPut(appPort, 'MD', 'd1', 'Do', {});
    // The delay node itself IS torn down and recreated below, so (unlike the
    // previous test) this redeploy's correctness genuinely depends on the
    // message already sitting in its queue -- wait for the probe's real log
    // line instead of guessing a fixed duration.
    await nr.waitForLog(/probe: MD\/d1 queued/, { timeoutMs: 5000 });

    // The delay node's own config (its timeout) changes -- it is stopped and
    // recreated, dropping its queued message, even though the reply node
    // downstream is untouched.
    await nr.deploy(flow('60001'), { deploymentType: 'nodes' });

    const res = await pending;
    assert.equal(res.status, 503, res.text);
    assert.equal(saveCalls, 0, 'a dropped proposal must never commit');

    // The connection (and this actor id, via the unaffected fast path) keep
    // serving afterward.
    const after = await actorPut(appPort, 'MD', 'd1', 'Fast', {});
    assert.equal(after.status, 200, after.text);
  }
);

test(
  'an unavailable state store answers a sanitized 500, distinguishing a definite read/save failure from an unknown commit outcome',
  { timeout: 30000 },
  async (t) => {
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        [
          'GET',
          stateReadPath('US', 'r1'),
          (_req, res) => res.writeHead(500, { 'content-type': 'application/json' }).end('boom'),
        ],
        ['GET', stateReadPath('US', 'r2'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('US', 'r2'),
          (_req, res) => res.writeHead(500, { 'content-type': 'application/json' }).end('boom'),
        ],
        ['GET', stateReadPath('US', 'r3'), (_req, res) => res.writeHead(204).end()],
        ['POST', stateSavePath('US', 'r3'), (req) => req.socket.destroy()],
      ],
    });
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'us' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '5' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'US',
        method: 'Do',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { n: 1 };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    // A read failure never reaches the flow, and is never echoed verbatim.
    const readFail = await actorPut(appPort, 'US', 'r1', 'Do', {});
    assert.equal(readFail.status, 500, readFail.text);
    assert.equal(JSON.parse(readFail.text).error.code, 'STATE_OPERATION_FAILED');
    assert.ok(!readFail.text.includes('boom'));

    // A confirmed non-2xx save failure is a definite failure.
    const saveFail = await actorPut(appPort, 'US', 'r2', 'Do', {});
    assert.equal(saveFail.status, 500, saveFail.text);
    assert.equal(JSON.parse(saveFail.text).error.code, 'STATE_OPERATION_FAILED');
    assert.ok(!saveFail.text.includes('boom'));

    // A transport failure mid-commit (connection reset) leaves the write's
    // outcome genuinely unknown -- distinct from the confirmed failure above.
    const saveUnknown = await actorPut(appPort, 'US', 'r3', 'Do', {});
    assert.equal(saveUnknown.status, 500, saveUnknown.text);
    assert.equal(JSON.parse(saveUnknown.text).error.code, 'ACTOR_COMMIT_UNKNOWN');
  }
);

test(
  'Node-RED shutdown during a held commit completes once the commit resolves, well under the drain backstop',
  { timeout: 30000 },
  async (t) => {
    let saveCalls = 0;
    const saveGate = Promise.withResolvers();
    const { dapr, nr, appPort } = await startHarness(t, {
      respondents: [
        ['GET', stateReadPath('SD', 'd1'), (_req, res) => res.writeHead(204).end()],
        [
          'POST',
          stateSavePath('SD', 'd1'),
          async (_req, res) => {
            saveCalls += 1;
            await saveGate.promise; // held until the test releases it below
            res.writeHead(204).end();
          },
        ],
      ],
    });
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'sd' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '30' }),
      {
        id: 'm1',
        type: 'dapr-actor-method',
        z: 'tab',
        connection: 'c1',
        actorType: 'SD',
        method: 'Do',
        wires: [['fn1']],
      },
      {
        id: 'fn1',
        type: 'function',
        z: 'tab',
        func: 'msg.payload = { ok: true };\nmsg.dapr.actor.nextState = { n: 1 };\nreturn msg;',
        outputs: 1,
        wires: [['reply1']],
      },
      {
        id: 'reply1',
        type: 'dapr-actor-reply',
        z: 'tab',
        connection: 'c1',
        outcome: 'complete',
        wires: [],
      },
    ]);
    await dapr.waitForRequest(healthPath);

    const pending = actorPut(appPort, 'SD', 'd1', 'Do', {});
    await dapr.waitForRequest((r) => r.method === 'POST' && r.path === stateSavePath('SD', 'd1'));
    assert.equal(saveCalls, 1);

    const start = Date.now();
    const stopPromise = nr.stop(); // SIGINT; the connection's close blocks on whenCommitsSettled()
    // Wait until Node-RED has actually begun stopping. Releasing immediately
    // after sending SIGINT could let the write finish before close even ran.
    await nr.waitForLog(/Stopping flows/, { timeoutMs: 5000 });
    assert.equal(await notYetSettled(stopPromise, 200), NOT_YET);
    assert.equal(
      await notYetSettled(pending, 200),
      NOT_YET,
      'shutdown must not replace an in-flight commit response with 503'
    );
    saveGate.resolve();
    const response = await pending;
    assert.equal(response.status, 200, response.text);
    assert.deepEqual(JSON.parse(response.text), { ok: true });
    await stopPromise;
    const stoppedMs = Date.now() - start;
    // Bounded well under the 5s drainTimeoutMs backstop (already proven at
    // unit tier in test/unit/actor-host.test.js) -- this proves the real,
    // event-driven path: shutdown completing as soon as the commit resolves,
    // not the backstop itself firing.
    assert.ok(
      stoppedMs < 3000,
      `Node-RED took ${stoppedMs}ms to stop after the held commit resolved`
    );
    assert.equal(
      /unhandledRejection|UnhandledPromiseRejection|uncaughtException/i.test(nr.logText()),
      false,
      'no crash artifact from the held commit during shutdown'
    );

    assert.equal(nr.proc.signalCode, null, 'shutdown must not require SIGKILL');
  }
);
