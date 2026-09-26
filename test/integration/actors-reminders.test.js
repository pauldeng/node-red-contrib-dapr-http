'use strict';

// Real Node-RED + real daprd 1.18.4 + Placement + Scheduler + an
// actor-flagged Redis state store, driving the milestone-4 reminder path
// end to end through the shipped nodes: a dapr-actor-schedule node sets/
// gets/deletes reminders, and a dapr-actor-method node with
// trigger:"reminder" receives every reminder of its actor type and commits
// it into the actor's record through the normal reply/commit path (unchanged
// from method calls -- see lib/actor-host.js). This is the "does the
// shipped node wiring actually work against a real Scheduler" proof;
// test/integration/actors-schedule-probe.test.js already pins the raw wire
// protocol (dueTime/period/data/overwrite semantics, envelope shape,
// non-hosted rejection, retry cadence) against a hand-rolled app, so this
// file does not re-probe that -- it only proves the package's own nodes
// exercise it correctly.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const {
  startRedis,
  startPlacement,
  startScheduler,
  startDaprd,
  stateComponentYaml,
} = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { invoke } = require('../../lib/actor-client');
const { startCapture } = require('../helpers/capture');

const ACTOR_TYPE = 'DemoActor';
const APP_ID = 'it-actors-reminders-app';

// A single reminder-trigger method node (only one reminder registration is
// allowed per actor type on a connection -- see
// lib/connection-registry.js's addActorMethod) routes on the firing
// reminder's own name: anything named "demo_fail_reminder" always
// replies fail (for the retry-count proof below); everything else commits
// the schedule's data into the record and replies complete. A plain
// actor method node answers GetMyData for read-back, exactly the way
// examples/actor-demo.json's own DemoActor does.
function loadFlow({ appPort, daprHttpPort, captureUrl }) {
  return [
    { id: 'tab', type: 'tab', label: 'reminders' },
    {
      id: 'conn',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      requestTimeoutSec: '10',
    },
    // ---- reminder trigger -> route -> reply -------------------------------
    {
      id: 'm-reminder',
      type: 'dapr-actor-method',
      z: 'tab',
      connection: 'conn',
      actorType: ACTOR_TYPE,
      trigger: 'reminder',
      wires: [['route-reminder']],
    },
    {
      id: 'route-reminder',
      type: 'function',
      z: 'tab',
      func: `
        node.send([null, null, { payload: {
          name: msg.dapr.actor.trigger.name, id: msg.dapr.actor.id,
        } }]);
        if (msg.dapr.actor.trigger.name === 'demo_fail_reminder') {
          msg.dapr.actor.error = { code: 'INDUCED_FAILURE', message: 'induced failure' };
          return [null, msg];
        }
        msg.dapr.actor.nextState = Object.assign({}, msg.dapr.actor.state || {}, {
          lastReminder: msg.dapr.actor.trigger.name,
          message: msg.payload === undefined ? null : msg.payload,
        });
        msg.payload = 'ok';
        return [msg, null];
      `,
      outputs: 3,
      wires: [['reply-complete'], ['reply-fail'], ['capture']],
    },
    {
      id: 'capture',
      type: 'http request',
      z: 'tab',
      method: 'POST',
      url: captureUrl,
      ret: 'txt',
      wires: [[]],
    },
    {
      id: 'reply-complete',
      type: 'dapr-actor-reply',
      z: 'tab',
      connection: 'conn',
      outcome: 'complete',
      wires: [],
    },
    {
      id: 'reply-fail',
      type: 'dapr-actor-reply',
      z: 'tab',
      connection: 'conn',
      outcome: 'fail',
      wires: [],
    },
    // ---- ordinary method: read back the record ----------------------------
    {
      id: 'm-get',
      type: 'dapr-actor-method',
      z: 'tab',
      connection: 'conn',
      actorType: ACTOR_TYPE,
      trigger: 'method',
      method: 'GetMyData',
      wires: [['get-response']],
    },
    {
      id: 'get-response',
      type: 'function',
      z: 'tab',
      func: 'msg.payload = msg.dapr.actor.stateExists ? msg.dapr.actor.state : null;\nreturn msg;',
      outputs: 1,
      wires: [['reply-get']],
    },
    {
      id: 'reply-get',
      type: 'dapr-actor-reply',
      z: 'tab',
      connection: 'conn',
      outcome: 'complete',
      wires: [],
    },
    // ---- dapr-actor-schedule, driven over plain HTTP for the test to call -
    {
      id: 'sched-in',
      type: 'http in',
      z: 'tab',
      url: '/it-schedule',
      method: 'post',
      wires: [['sched-before']],
    },
    {
      id: 'sched-before',
      type: 'function',
      z: 'tab',
      func: `
        const body = msg.payload;
        msg.dapr = { actorSchedule: {
          operation: body.operation,
          actorType: ${JSON.stringify(ACTOR_TYPE)},
          actorId: body.actorId,
          scheduleName: body.scheduleName,
          dueTime: body.dueTime || '',
          period: body.period || '',
        } };
        msg.payload = Object.hasOwn(body, 'message') ? body.message : undefined;
        return msg;
      `,
      outputs: 1,
      wires: [['sched1']],
    },
    {
      id: 'sched1',
      type: 'dapr-actor-schedule',
      z: 'tab',
      connection: 'conn',
      operation: 'set',
      actorType: ACTOR_TYPE,
      actorId: '',
      scheduleName: '',
      dueTime: '',
      period: '',
      ttl: '',
      overwrite: true,
      wires: [['sched-success']],
    },
    {
      id: 'sched-success',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200;\nmsg.payload = { ok: true, payload: msg.payload };\nreturn msg;',
      outputs: 1,
      wires: [['sched-res']],
    },
    {
      id: 'sched-catch',
      type: 'catch',
      z: 'tab',
      scope: ['sched1'],
      uncaught: false,
      wires: [['sched-failure']],
    },
    {
      id: 'sched-failure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503;\nmsg.payload = { code: msg.error.code, cause: msg.error.cause };\nreturn msg;',
      outputs: 1,
      wires: [['sched-res']],
    },
    { id: 'sched-res', type: 'http response', z: 'tab' },
  ];
}

function waitForActorReady(daprBaseUrl, { timeoutMs = 30000 } = {}) {
  return waitFor(
    async () => {
      const res = await httpRequest(`${daprBaseUrl}/v1.0/metadata`, { timeoutMs: 2000 });
      if (res.status !== 200) return null;
      const body = JSON.parse(res.text);
      const runtime = body.actorRuntime;
      const ready =
        runtime &&
        runtime.runtimeStatus === 'RUNNING' &&
        runtime.hostReady === true &&
        /placement:\s*connected/i.test(runtime.placement) &&
        Array.isArray(runtime.activeActors) &&
        runtime.activeActors.some((a) => a.type === ACTOR_TYPE);
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

async function getMyData(daprBaseUrl, actorId) {
  const result = await invoke(
    { baseUrl: daprBaseUrl, timeoutMs: 10000 },
    { actorType: ACTOR_TYPE, actorId, method: 'GetMyData', body: {} }
  );
  return JSON.parse(result.body.toString('utf8'));
}

function schedule(nr, body) {
  return httpRequest(nr.nodeUrl('/it-schedule'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    timeoutMs: 10000,
  });
}

test(
  'a reminder set through dapr-actor-schedule fires the reminder-trigger method flow, which commits it; a one-shot reminder is then gone; a past dueTime fires promptly; delete removes it before it fires',
  { timeout: 180000 },
  async (t) => {
    const capture = await startCapture();
    t.after(() => capture.stop());
    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());
    const scheduler = await startScheduler();
    t.after(() => scheduler.stop());

    const actorStoreComponent = () => [
      {
        filename: 'actorstore.yaml',
        yaml: stateComponentYaml(redis.port, { name: 'actorstore', actorStateStore: true }),
      },
    ];

    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({ flows: loadFlow({ appPort, daprHttpPort, captureUrl: capture.url }) });
    await waitForAppHealthz(appPort);

    const daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: actorStoreComponent(),
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });
    t.after(() => daprd.stop());

    await waitForActorReady(daprd.baseUrl);
    await nr.waitForLog(/Dapr sidecar is available/i, { timeoutMs: 15000 });

    // --- (a) an RFC 3339 dueTime a few seconds ahead fires and commits ------
    const dueSoon = new Date(Date.now() + 3000).toISOString();
    const setRes = await schedule(nr, {
      operation: 'set',
      actorId: 'demo-1',
      scheduleName: 'demo_reminder',
      dueTime: dueSoon,
      message: { greeting: 'hello from schedule' },
    });
    assert.equal(setRes.status, 200, setRes.text);

    await capture.waitForMessage((m) => m.id === 'demo-1');
    const committed = await getMyData(daprd.baseUrl, 'demo-1');
    assert.deepEqual(committed.message, { greeting: 'hello from schedule' });

    // --- (b) a one-shot reminder (no period) is gone afterwards -------------
    const getAfterFire = await schedule(nr, {
      operation: 'get',
      actorId: 'demo-1',
      scheduleName: 'demo_reminder',
    });
    assert.equal(getAfterFire.status, 200, getAfterFire.text);
    assert.equal(JSON.parse(getAfterFire.text).payload, null);

    // --- (c) a dueTime in the past fires promptly ---------------------------
    const duePast = new Date(Date.now() - 5000).toISOString();
    const pastRes = await schedule(nr, {
      operation: 'set',
      actorId: 'demo-2',
      scheduleName: 'demo_reminder_past',
      dueTime: duePast,
      message: { note: 'past due' },
    });
    assert.equal(pastRes.status, 200, pastRes.text);
    await capture.waitForMessage((m) => m.id === 'demo-2');
    const committedPast = await getMyData(daprd.baseUrl, 'demo-2');
    assert.deepEqual(committedPast.message, { note: 'past due' });

    // --- (f) get/delete through the node: delete before it can fire --------
    const farFuture = new Date(Date.now() + 60000).toISOString();
    const setDelRes = await schedule(nr, {
      operation: 'set',
      actorId: 'demo-4',
      scheduleName: 'demo_reminder_del',
      dueTime: farFuture,
      message: { should: 'never fire' },
    });
    assert.equal(setDelRes.status, 200, setDelRes.text);
    const getBeforeDelete = await schedule(nr, {
      operation: 'get',
      actorId: 'demo-4',
      scheduleName: 'demo_reminder_del',
    });
    assert.notEqual(JSON.parse(getBeforeDelete.text).payload, null);
    const delRes = await schedule(nr, {
      operation: 'delete',
      actorId: 'demo-4',
      scheduleName: 'demo_reminder_del',
    });
    assert.equal(delRes.status, 200, delRes.text);
    const getAfterDelete = await schedule(nr, {
      operation: 'get',
      actorId: 'demo-4',
      scheduleName: 'demo_reminder_del',
    });
    assert.equal(JSON.parse(getAfterDelete.text).payload, null);
  }
);

test(
  'a reminder set just before Node-RED and daprd stop still fires and commits after both restart',
  { timeout: 180000 },
  async (t) => {
    const capture = await startCapture();
    t.after(() => capture.stop());
    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());
    const scheduler = await startScheduler();
    t.after(() => scheduler.stop());

    const actorStoreComponent = () => [
      {
        filename: 'actorstore.yaml',
        yaml: stateComponentYaml(redis.port, { name: 'actorstore', actorStateStore: true }),
      },
    ];

    let appPort = await freePort();
    let daprHttpPort = await freePort();
    let nr = new ContainerNodeRed();
    const firstNr = nr;
    t.after(() => firstNr.stop());
    await nr.start({ flows: loadFlow({ appPort, daprHttpPort, captureUrl: capture.url }) });
    await waitForAppHealthz(appPort);

    let daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: actorStoreComponent(),
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });

    const firstDaprd = daprd;
    t.after(() => firstDaprd.stop());

    await waitForActorReady(daprd.baseUrl);
    await nr.waitForLog(/Dapr sidecar is available/i, { timeoutMs: 15000 });

    const setRes = await schedule(nr, {
      operation: 'set',
      actorId: 'demo-3',
      scheduleName: 'demo_reminder_restart',
      dueTime: '1s',
      period: '1s',
      message: { note: 'set before restart' },
    });
    assert.equal(setRes.status, 200, setRes.text);

    await daprd.stop();
    await nr.stop();
    capture.received.length = 0;

    appPort = await freePort();
    daprHttpPort = await freePort();
    nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({ flows: loadFlow({ appPort, daprHttpPort, captureUrl: capture.url }) });
    await waitForAppHealthz(appPort);

    daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: actorStoreComponent(),
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });
    t.after(() => daprd.stop());

    await waitForActorReady(daprd.baseUrl);
    await nr.waitForLog(/Dapr sidecar is available/i, { timeoutMs: 15000 });

    await capture.waitForMessage((m) => m.id === 'demo-3', { timeoutMs: 20000 });
    const committed = await getMyData(daprd.baseUrl, 'demo-3');
    assert.deepEqual(committed.message, { note: 'set before restart' });
  }
);

test(
  'a reminder flow that always fails is retried a bounded number of times per occurrence',
  { timeout: 60000 },
  async (t) => {
    const capture = await startCapture();
    t.after(() => capture.stop());
    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());
    const scheduler = await startScheduler();
    t.after(() => scheduler.stop());

    const actorStoreComponent = () => [
      {
        filename: 'actorstore.yaml',
        yaml: stateComponentYaml(redis.port, { name: 'actorstore', actorStateStore: true }),
      },
    ];

    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({ flows: loadFlow({ appPort, daprHttpPort, captureUrl: capture.url }) });
    await waitForAppHealthz(appPort);

    const daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: actorStoreComponent(),
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });
    t.after(() => daprd.stop());

    await waitForActorReady(daprd.baseUrl);
    await nr.waitForLog(/Dapr sidecar is available/i, { timeoutMs: 15000 });

    const setRes = await schedule(nr, {
      operation: 'set',
      actorId: 'demo-fail-1',
      scheduleName: 'demo_fail_reminder',
      dueTime: '1s',
    });
    assert.equal(setRes.status, 200, setRes.text);

    // A separate one-shot marks the end of the observation window. Waiting
    // for its delivery is event-driven; an unbounded 1s retry loop would
    // produce more than four failure deliveries before this marker arrives.
    const marker = await schedule(nr, {
      operation: 'set',
      actorId: 'demo-marker',
      scheduleName: 'demo_reminder_marker',
      dueTime: '7s',
      message: null,
    });
    assert.equal(marker.status, 200, marker.text);
    await capture.waitForMessage((m) => m.name === 'demo_reminder_marker', { timeoutMs: 15000 });
    assert.equal(
      capture.received.filter((m) => m.name === 'demo_fail_reminder').length,
      4,
      'one failed occurrence has one initial delivery and exactly three retries'
    );
  }
);
