'use strict';

// Real Node-RED, deployed with the exact exported examples/actor-demo.json
// flow (only its connection node's host/ports are rewritten for this
// harness's dynamically allocated ports; the Function/method/reply/call node
// config is untouched) + real daprd 1.18.4 + Placement + an actor-flagged
// Redis state store. Drives every method through daprd's OWN client-facing
// actor API (POST /v1.0/actors/{type}/{id}/method/{method}), the way an
// external caller would -- never the raw app-channel PUT that
// test/runtime/actors.test.js and test/integration/actors-probe.test.js use.
//
// The example's own dapr-actor-call node IS additionally driven, through its
// "countBy: amount 5, multiplier 2" Inject node, via Node-RED's admin
// POST /inject/:id route -- the side effect it produces (a real actor state
// write, reachable through getCounter on daprd's own API) is observed
// instead of its Debug sidebar output: this repository's integration/runtime
// harnesses have no helper for reading Node-RED's `/comms` debug stream (only
// the Playwright e2e tier drives a real browser's Debug sidebar), and the
// debug node's published payload is further reshaped by
// RED.util.encodeObject's truncation/formatting before it reaches that
// stream -- parsing it reliably would be new, nontrivial harness work, not a
// cheap addition here. The dapr-actor-call node's own contract
// (config/override precedence, self-call guard, absent/null body) is
// separately proven at the runtime tier (test/runtime/actors.test.js) against
// a controlled fake sidecar, which is the cheaper and more precise place to
// assert those specifics.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const {
  startRedis,
  startPlacement,
  startDaprd,
  stateComponentYaml,
} = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { invoke } = require('../../lib/actor-client');
const { ErrorCodes } = require('../../lib/errors');

const FLOW_PATH = path.resolve(__dirname, '../../examples/actor-demo.json');
const APP_ID = 'it-actors-app';

function loadFlow({ appPort, daprHttpPort }) {
  const flow = JSON.parse(fs.readFileSync(FLOW_PATH, 'utf8'));
  const conn = flow.find((n) => n.type === 'dapr-connection');
  assert.ok(conn, 'examples/actor-demo.json must declare a dapr-connection node');
  conn.daprHost = '127.0.0.1';
  conn.daprPort = String(daprHttpPort);
  conn.bindAddress = '127.0.0.1';
  conn.appPort = String(appPort);
  return flow;
}

// Drives the same POST /v1.0/actors/{type}/{id}/method/{method} route an
// external caller would, but through this package's own client
// (lib/actor-client.js) rather than a hand-rolled request builder, so this
// file exercises the exact code path the shipped nodes use. A confirmed
// non-2xx throws ACTOR_INVOKE_FAILED (see the one caller below that expects
// a failure) instead of resolving with a non-2xx status.
async function actorCall(daprBaseUrl, actorType, actorId, method, body) {
  const result = await invoke(
    { baseUrl: daprBaseUrl, timeoutMs: 10000 },
    { actorType, actorId, method, body: body ?? {} }
  );
  return { status: result.status, text: result.body.toString('utf8') };
}

// Mirrors test/integration/actors-probe.test.js's own readiness check:
// GET /v1.0/metadata's actorRuntime block is the real (inspected) shape for
// 1.18.4.
function waitForActorReady(daprBaseUrl, { timeoutMs = 30000 } = {}) {
  return waitFor(
    async () => {
      const res = await httpRequest(`${daprBaseUrl}/v1.0/metadata`, { timeoutMs: 2000 });
      if (res.status !== 200) {
        return null;
      }
      const body = JSON.parse(res.text);
      const runtime = body.actorRuntime;
      const ready =
        runtime &&
        runtime.runtimeStatus === 'RUNNING' &&
        runtime.hostReady === true &&
        typeof runtime.placement === 'string' &&
        /placement:\s*connected/i.test(runtime.placement);
      return ready ? body : null;
    },
    { timeoutMs }
  );
}

async function waitForAppHealthz(appPort) {
  return waitFor(async () => {
    const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
    return r.status === 204 ? true : null;
  });
}

test(
  'the actor demo example, deployed exactly as shipped, answers through real daprd + Placement, survives a restart, and rejects an invalid argument',
  { timeout: 240000 },
  async (t) => {
    let nr;
    let daprd;
    // node:test runs after hooks in registration order. Keep dependencies
    // alive until their clients stop; these hooks also own partial startup
    // and the replacement instances created by the restart below.
    t.after(() => daprd?.stop());
    t.after(() => nr?.stop());
    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());

    const actorStoreComponent = () => [
      {
        filename: 'actorstore.yaml',
        yaml: stateComponentYaml(redis.port, { name: 'actorstore', actorStateStore: true }),
      },
    ];

    let appPort = await freePort();
    let daprHttpPort = await freePort();

    nr = new ContainerNodeRed();
    await nr.start({ flows: loadFlow({ appPort, daprHttpPort }) });
    await waitForAppHealthz(appPort);

    daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: actorStoreComponent(),
      placementAddress: `127.0.0.1:${placement.port}`,
    });

    await waitForActorReady(daprd.baseUrl);
    // The dapr-actor-call node's own outbound health gate (session.isReady())
    // is driven by this same connection's independent health poll; confirm it
    // has already landed before the Inject-driven section below relies on it.
    await nr.waitForLog(/Dapr sidecar is available/i, { timeoutMs: 15000 });

    // --- SetMyData then GetMyData, against a never-provisioned actor -------
    const setRes = await actorCall(daprd.baseUrl, 'DemoActor', 'demo-it-1', 'SetMyData', {
      greeting: 'hello world',
    });
    assert.equal(setRes.status, 200, setRes.text);
    assert.equal(JSON.parse(setRes.text), null);

    const getRes = await actorCall(daprd.baseUrl, 'DemoActor', 'demo-it-1', 'GetMyData', {});
    assert.equal(getRes.status, 200, getRes.text);
    const stored = JSON.parse(getRes.text);
    assert.equal(stored.greeting, 'hello world');
    assert.match(stored.ts, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);

    // --- countBy twice then getCounter ---------------------------------------
    const first = await actorCall(daprd.baseUrl, 'DemoActorCounter', 'counter-it-1', 'countBy', {
      amount: 5,
      multiplier: 2,
    });
    assert.equal(first.status, 200, first.text);
    assert.equal(JSON.parse(first.text), 10);

    const second = await actorCall(daprd.baseUrl, 'DemoActorCounter', 'counter-it-1', 'countBy', {
      amount: 3,
    });
    assert.equal(second.status, 200, second.text);
    assert.equal(JSON.parse(second.text), 13);

    const counterRes = await actorCall(
      daprd.baseUrl,
      'DemoActorCounter',
      'counter-it-1',
      'getCounter',
      {}
    );
    assert.equal(counterRes.status, 200, counterRes.text);
    assert.equal(JSON.parse(counterRes.text), 13);

    // --- An invalid countBy surfaces as a caller 500 whose message contains
    // INVALID_ARGUMENT -- daprd wraps every non-200 app answer as its own 500
    // ERR_ACTOR_INVOKE_METHOD; the app's own error code only survives inside
    // that message's text (see test/integration/actors-probe.test.js's
    // milestone-1 findings). lib/actor-client.js's invoke() throws
    // ACTOR_INVOKE_FAILED for a confirmed non-2xx, carrying the status/message
    // on err.cause.
    await assert.rejects(
      actorCall(daprd.baseUrl, 'DemoActorCounter', 'counter-it-1', 'countBy', {
        amount: 'not-a-number',
      }),
      (err) => {
        assert.equal(err.code, ErrorCodes.ACTOR_INVOKE_FAILED);
        assert.equal(err.cause.statusCode, 500);
        assert.match(err.cause.message, /INVALID_ARGUMENT/);
        return true;
      }
    );

    // The failed countBy must not have changed the counter.
    const unchangedRes = await actorCall(
      daprd.baseUrl,
      'DemoActorCounter',
      'counter-it-1',
      'getCounter',
      {}
    );
    assert.equal(JSON.parse(unchangedRes.text), 13);

    // --- Concurrent calls to two different actor IDs both succeed ----------
    const setOther = await actorCall(daprd.baseUrl, 'DemoActor', 'demo-it-2', 'SetMyData', {
      greeting: 'second actor',
    });
    assert.equal(setOther.status, 200, setOther.text);

    const [concA, concB] = await Promise.all([
      actorCall(daprd.baseUrl, 'DemoActorCounter', 'counter-it-2', 'count', {}),
      actorCall(daprd.baseUrl, 'DemoActor', 'demo-it-2', 'GetMyData', {}),
    ]);
    assert.equal(concA.status, 200, concA.text);
    assert.equal(concB.status, 200, concB.text);
    assert.equal(JSON.parse(concA.text), 1);
    assert.equal(JSON.parse(concB.text).greeting, 'second actor');

    // --- Drive the example's own dapr-actor-call node through its Inject ---
    // "countBy: amount 5, multiplier 2 (expect +10)" targets counter-1
    // (untouched above) via msg.dapr.actorCall, so this proves
    // Inject -> call node -> daprd -> Placement -> method -> reply -> commit
    // on the real stack, and exercises the countBy path at this tier too.
    const injectRes = await httpRequest(nr.adminUrl('/inject/ex-actor-inject-countby'), {
      method: 'POST',
      timeoutMs: 5000,
    });
    assert.equal(injectRes.status, 200, injectRes.text);

    const injectedCounter = await waitFor(async () => {
      const r = await actorCall(daprd.baseUrl, 'DemoActorCounter', 'counter-1', 'getCounter', {});
      if (r.status !== 200) {
        return null;
      }
      const value = JSON.parse(r.text);
      return value === 10 ? value : null;
    });
    assert.equal(injectedCounter, 10);

    // --- Stop Node-RED and daprd (keep Redis + Placement), then start both
    // again on fresh ports, and prove the record survived the restart -------
    await daprd.stop();
    daprd = undefined;
    await nr.stop();
    nr = undefined;

    appPort = await freePort();
    daprHttpPort = await freePort();
    nr = new ContainerNodeRed();
    await nr.start({ flows: loadFlow({ appPort, daprHttpPort }) });
    await waitForAppHealthz(appPort);

    daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: actorStoreComponent(),
      placementAddress: `127.0.0.1:${placement.port}`,
    });

    await waitForActorReady(daprd.baseUrl);

    const getAfterRestart = await actorCall(
      daprd.baseUrl,
      'DemoActor',
      'demo-it-1',
      'GetMyData',
      {}
    );
    assert.equal(getAfterRestart.status, 200, getAfterRestart.text);
    assert.equal(JSON.parse(getAfterRestart.text).greeting, 'hello world');

    const counterAfterRestart = await actorCall(
      daprd.baseUrl,
      'DemoActorCounter',
      'counter-it-1',
      'getCounter',
      {}
    );
    assert.equal(counterAfterRestart.status, 200, counterAfterRestart.text);
    assert.equal(JSON.parse(counterAfterRestart.text), 13);
  }
);

// Test-only nodes appended to the shipped flow (never merged into the
// example itself), reusing its own connection id -- a second dapr-connection
// on the same appPort/bind would be a duplicate app-channel listener. Mirrors
// test/runtime/actors.test.js's callFlow/postCall shape: HTTP-in -> call ->
// Catch -> HTTP response is the cheapest way to observe Catch's captured
// msg.error.code/msg.error.cause without parsing Node-RED's debug/comms
// stream (see the file banner above).
function withCallTestNodes(flow) {
  return [
    ...flow,
    {
      id: 'it-call-in',
      type: 'http in',
      z: 'ex-actor-tab',
      url: '/it-actor-call',
      method: 'post',
      wires: [['it-call']],
    },
    {
      id: 'it-call',
      type: 'dapr-actor-call',
      z: 'ex-actor-tab',
      connection: 'ex-actor-conn',
      actorType: 'DemoActorCounter',
      actorId: 'counter-it-fail',
      method: 'countBy',
      wires: [['it-call-ok']],
    },
    {
      id: 'it-call-ok',
      type: 'function',
      z: 'ex-actor-tab',
      func: 'msg.statusCode = 200;\nmsg.payload = { payload: msg.payload };\nreturn msg;',
      outputs: 1,
      wires: [['it-call-res']],
    },
    {
      id: 'it-call-catch',
      type: 'catch',
      z: 'ex-actor-tab',
      scope: ['it-call'],
      uncaught: false,
      wires: [['it-call-fail']],
    },
    {
      id: 'it-call-fail',
      type: 'function',
      z: 'ex-actor-tab',
      func: 'msg.statusCode = 200;\nmsg.payload = { code: msg.error.code, cause: msg.error.cause };\nreturn msg;',
      outputs: 1,
      wires: [['it-call-res']],
    },
    { id: 'it-call-res', type: 'http response', z: 'ex-actor-tab' },
  ];
}

function callThroughFlow(nr, body) {
  return httpRequest(nr.nodeUrl('/it-actor-call'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body ?? {}),
    timeoutMs: 10000,
  });
}

test(
  "the shipped flow survives an unchanged-flow redeploy without restarting daprd; a failing call's diagnostics reach Catch; concurrent calls to one actor never lose an update",
  { timeout: 240000 },
  async (t) => {
    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());

    const actorStoreComponent = () => [
      {
        filename: 'actorstore.yaml',
        yaml: stateComponentYaml(redis.port, { name: 'actorstore', actorStateStore: true }),
      },
    ];

    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const flow = loadFlow({ appPort, daprHttpPort }); // exactly as shipped, only host/ports rewritten

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({ flows: flow });
    await waitForAppHealthz(appPort);

    const daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: actorStoreComponent(),
      placementAddress: `127.0.0.1:${placement.port}`,
    });
    t.after(() => daprd.stop());
    await waitForActorReady(daprd.baseUrl);
    await nr.waitForLog(/Dapr sidecar is available/i, { timeoutMs: 15000 });

    // Baseline: an actor call succeeds before touching the flow at all.
    const before = await actorCall(daprd.baseUrl, 'DemoActor', 'redeploy-baseline', 'SetMyData', {
      greeting: 'before redeploy',
    });
    assert.equal(before.status, 200, before.text);

    // --- (a) redeploy the IDENTICAL pristine shipped flow (full deploy),
    // with the same daprd still running -- prove no restart warning and that
    // calls keep working. ------------------------------------------------
    await nr.deploy(loadFlow({ appPort, daprHttpPort }));
    assert.equal(
      /restart the Dapr sidecar/i.test(nr.logText()),
      false,
      'redeploying the shipped flow unchanged must not ask for a daprd restart'
    );
    // The reacquired lease's grace window is async; a call right after
    // redeploy may land in that brief gap (a connection-refused/retryable
    // response, which daprd itself would turn into its own error) before the
    // new generation activates -- poll via the real endpoint rather than
    // assuming the first attempt lands.
    const after = await waitFor(async () => {
      const r = await actorCall(daprd.baseUrl, 'DemoActor', 'redeploy-baseline', 'GetMyData', {});
      return r.status === 200 ? r : null;
    });
    assert.equal(JSON.parse(after.text).greeting, 'before redeploy');

    // --- (b)/(c): append the test-only call/Catch flow and redeploy once
    // more (still the same daprd; no new actor type is registered). ---------
    await nr.deploy(withCallTestNodes(loadFlow({ appPort, daprHttpPort })));

    // (b) An invalid argument to countBy fails through daprd as a caller 500;
    // Catch on the call node preserves the stable code and Dapr's diagnostic
    // cause, exactly like test/runtime/actors.test.js's fake-sidecar proof,
    // but here against a real daprd + Placement round trip. The new http-in
    // route can briefly 404 right after a full deploy resolves (the admin API
    // response and the httpNode route mount are not perfectly synchronous) --
    // poll for it rather than assuming the very first attempt lands. The
    // request itself never writes state on this failure path, so a retried
    // attempt is safe.
    const failRes = await waitFor(async () => {
      const r = await callThroughFlow(nr, { amount: 'not-a-number' });
      return r.status === 200 ? r : null;
    });
    const failBody = JSON.parse(failRes.text);
    assert.equal(failBody.code, 'ACTOR_INVOKE_FAILED');
    assert.equal(failBody.cause.statusCode, 500);
    assert.equal(failBody.cause.errorCode, 'ERR_ACTOR_INVOKE_METHOD');
    assert.match(failBody.cause.message, /INVALID_ARGUMENT/);

    // The failed call must not have written any state for that actor id.
    const afterFail = await actorCall(
      daprd.baseUrl,
      'DemoActorCounter',
      'counter-it-fail',
      'getCounter',
      {}
    );
    assert.equal(afterFail.status, 200, afterFail.text);
    assert.equal(JSON.parse(afterFail.text), 0, 'the failing call must never have counted');

    // (c) Two concurrent countBy calls to the SAME actor id, straight through
    // daprd's own client API (Dapr serializes calls to one actor while the
    // app's request is open -- see docs/architecture.md) -- neither update is
    // lost.
    const [c1, c2] = await Promise.all([
      actorCall(daprd.baseUrl, 'DemoActorCounter', 'counter-it-order', 'countBy', { amount: 4 }),
      actorCall(daprd.baseUrl, 'DemoActorCounter', 'counter-it-order', 'countBy', { amount: 6 }),
    ]);
    assert.equal(c1.status, 200, c1.text);
    assert.equal(c2.status, 200, c2.text);
    const total = await actorCall(
      daprd.baseUrl,
      'DemoActorCounter',
      'counter-it-order',
      'getCounter',
      {}
    );
    assert.equal(total.status, 200, total.text);
    assert.equal(
      JSON.parse(total.text),
      10,
      'both concurrent updates must be reflected, none lost'
    );
  }
);
