'use strict';

// Real-wire multi-replica probe: two raw node:http "apps" (NOT Node-RED, NOT
// this package's own app-channel listener -- exactly like
// test/integration/actors-probe.test.js and
// test/integration/actors-schedule-probe.test.js do for the single-replica
// case) are fronted by TWO real daprd 1.18.4 sidecars sharing one --app-id,
// both pointed at the same Placement/Scheduler/actor-store. Placement's
// consistent-hash ring then splits ReplicaProbeActor ids across both
// replicas regardless of which sidecar's HTTP API a client used to create a
// reminder -- so a reminder created through replica A's API for an id
// Placement assigned to replica B must be delivered by daprd FORWARDING the
// Scheduler's fired job from A's process to B's, which then calls B's own
// app over HTTP exactly as A would have called its own.
//
// The question this file answers: does that forwarded reminder callback
// carry a `dapr-caller-app-id` (or `dapr-caller-namespace`) header?
// lib/app-channel.js's parseActorPath() gives a reminder callback (shape
// 'reminder', method/remind/{name}) route.kind 'internal', and every
// 'internal' route 403s a request carrying dapr-caller-app-id -- see the
// `route.kind === 'internal' && req.headers['dapr-caller-app-id'] !==
// undefined` check -- specifically so a mesh caller cannot spoof a reminder
// delivery by crafting its own PUT to .../method/remind/{name} with a caller
// header naming a trusted app id (which an ordinary, non-internal actor
// method route deliberately preserves so a flow can make its own
// authorization decision -- see actors-probe.test.js observation a). If a
// GENUINE forwarded reminder also carried dapr-caller-app-id, that 403 would
// misfire against every multi-replica deployment of this package on the
// exact ids Placement moved off the replica that created the reminder.
// actors-schedule-probe.test.js already proved a reminder callback carries
// no such header on a SINGLE sidecar (where the callback is always local,
// never forwarded); this file is the two-replica case that single sidecar
// cannot exercise, contrasted against an ordinary (non-reminder) method
// call forwarded the same cross-replica way, which observation (a) already
// says DOES carry the header when the sidecar calls its own local app --
// this file checks whether that holds when the receiving app is the OTHER
// replica too.
//
// Observed and recorded here, against real daprd/Placement/Scheduler 1.18.4:
//  - All 20 one-shot reminders, created only through replica A's HTTP API,
//    are each delivered exactly once, landing on whichever replica actually
//    hosts that id (both A and B receive some -- Placement's ring splits
//    them across both, proving real cross-replica forwarding occurs for a
//    good fraction of ids).
//  - Every reminder callback observed, on BOTH replicas, carries the base
//    header set { accept-encoding, content-length, content-type,
//    dapr-api-token, host, user-agent }, PLUS `traceparent` on a majority
//    but not all of the 20 (present or absent inconsistently on both A and
//    B, uncorrelated with which replica actually owns the id -- incidental
//    to this file's question, not asserted further). NONE of the 20 carries
//    dapr-caller-app-id or dapr-caller-namespace, on the replica that owns
//    the id locally AND on the replica that received it only because the
//    OTHER replica's sidecar forwarded the fired job to it. Forwarding a
//    reminder callback across replicas adds neither header.
//  - An ordinary (non-reminder) method call to the same two replicas, also
//    issued only through replica A's API, DOES carry both dapr-caller-app-id
//    (value: the calling sidecar's own --app-id, "replica-probe" -- same
//    string as the receiving replica's, since both share one --app-id) AND
//    dapr-caller-namespace, on every callback observed, including the one
//    landing on replica B (necessarily forwarded, since every create here
//    went through replica A's own HTTP API; that forwarded call additionally
//    carried `x-dapr-remote`, not otherwise relied on here) -- confirming
//    the contrast: only the reminder (internal) route is caller-header-free;
//    403ing dapr-caller-app-id on a reminder callback does not reject
//    anything a genuine forwarded reminder ever sends.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { text: readBody } = require('node:stream/consumers');
const { once } = require('node:events');
const { EventEmitter } = require('node:events');
const { setTimeout: delay } = require('node:timers/promises');

const { httpRequest, closeHttpServer } = require('../helpers/http');
const { freePort } = require('../helpers/node-red');
const {
  startRedis,
  startPlacement,
  startScheduler,
  startDaprd,
  stateComponentYaml,
} = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

const ACTOR_TYPE = 'ReplicaProbeActor';
const APP_ID = 'replica-probe';
const APP_API_TOKEN = 'actors-replicas-probe-app-token';
const REMINDER_NAME = 'probeRemind';
const REMINDER_COUNT = 20;
const METHOD_NAME = 'probePing';
const METHOD_IDS = ['mid-0', 'mid-1', 'mid-2', 'mid-3', 'mid-4'];

const CALLBACK_HEADER_NAMES = [
  'accept-encoding',
  'content-length',
  'content-type',
  'dapr-api-token',
  'host',
  'user-agent',
];

function waitForEvent(events, name, timeoutMs = 30000) {
  return once(events, name, { signal: AbortSignal.timeout(timeoutMs) });
}

// One raw actor-host "app" per replica. `events`/`requests` are SHARED
// across both replica hosts, tagged with `host`, so a wait keyed only by
// actor id resolves regardless of which replica daprd actually delivers the
// callback to -- exactly the fact under test.
function createReplicaHost(host, sharedEvents, sharedRequests) {
  const remindPath = new RegExp(`^/actors/${ACTOR_TYPE}/([^/]+)/method/remind/([^/]+)$`);
  const methodPath = new RegExp(`^/actors/${ACTOR_TYPE}/([^/]+)/method/([^/]+)$`);

  return http.createServer(async (req, res) => {
    let bodyText;
    try {
      bodyText = await readBody(req);
    } catch {
      bodyText = '';
    }
    const r = {
      host,
      method: req.method,
      path: req.url,
      headerNames: Object.keys(req.headers).sort(),
      hasCallerAppId: Object.hasOwn(req.headers, 'dapr-caller-app-id'),
      callerAppId: req.headers['dapr-caller-app-id'],
      hasCallerNamespace: Object.hasOwn(req.headers, 'dapr-caller-namespace'),
      callerNamespace: req.headers['dapr-caller-namespace'],
      bodyText,
      at: Date.now(),
    };
    sharedRequests.push(r);

    if (req.method === 'GET' && req.url === '/dapr/config') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({
          entities: [ACTOR_TYPE],
          actorIdleTimeout: '30s',
          actorScanInterval: '5s',
        })
      );
      return;
    }
    if (req.method === 'GET' && req.url === '/healthz') {
      res.writeHead(204);
      res.end();
      return;
    }
    const deleteMatch = new RegExp(`^/actors/${ACTOR_TYPE}/([^/]+)$`).exec(req.url);
    if (req.method === 'DELETE' && deleteMatch) {
      res.writeHead(200);
      res.end();
      return;
    }
    const remindMatch = remindPath.exec(req.url);
    if (remindMatch) {
      const [, id] = remindMatch;
      sharedEvents.emit(`reminder:${id}`, r);
      res.writeHead(200);
      res.end();
      return;
    }
    const methodMatch = methodPath.exec(req.url);
    if (methodMatch) {
      const [, id, method] = methodMatch;
      sharedEvents.emit(`method:${id}:${method}`, r);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    res.writeHead(404);
    res.end();
  });
}

async function startHost(host, sharedEvents, sharedRequests) {
  const server = createReplicaHost(host, sharedEvents, sharedRequests);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server;
}

async function waitActorReady(daprd) {
  return waitFor(async () => {
    const res = await httpRequest(`${daprd.baseUrl}/v1.0/metadata`, { timeoutMs: 2000 });
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
  });
}

test(
  'real daprd 1.18.4 + Placement + Scheduler: a reminder forwarded to the OTHER replica of the same app-id',
  { timeout: 120000 },
  async (t) => {
    const events = new EventEmitter();
    // Every pending waitForEvent (one per reminder id) adds an 'error' listener.
    events.setMaxListeners(REMINDER_COUNT);
    const requests = [];

    const hostA = await startHost('A', events, requests);
    t.after(() => closeHttpServer(hostA));
    const hostB = await startHost('B', events, requests);
    t.after(() => closeHttpServer(hostB));

    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());
    const scheduler = await startScheduler();
    t.after(() => scheduler.stop());

    const actorStoreYaml = stateComponentYaml(redis.port, {
      name: 'actorstore',
      actorStateStore: true,
    });
    const components = () => [{ filename: 'actorstore.yaml', yaml: actorStoreYaml }];

    const daprAPort = await freePort();
    const daprdA = await startDaprd({
      appId: APP_ID,
      appPort: hostA.address().port,
      httpPort: daprAPort,
      components: components(),
      appApiToken: APP_API_TOKEN,
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });
    t.after(() => daprdA.stop());

    const daprBPort = await freePort();
    const daprdB = await startDaprd({
      appId: APP_ID,
      appPort: hostB.address().port,
      httpPort: daprBPort,
      components: components(),
      appApiToken: APP_API_TOKEN,
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });
    t.after(() => daprdB.stop());

    const [metaA, metaB] = await Promise.all([waitActorReady(daprdA), waitActorReady(daprdB)]);
    t.diagnostic(`replica A actorRuntime ready: ${JSON.stringify(metaA.actorRuntime)}`);
    t.diagnostic(`replica B actorRuntime ready: ${JSON.stringify(metaB.actorRuntime)}`);

    // --- reminders: 20 distinct ids, one-shot (no period), ALL created
    // through replica A's own HTTP API -----------------------------------

    const reminderIds = Array.from({ length: REMINDER_COUNT }, (_, i) => `rid-${i}`);
    const reminderWaits = reminderIds.map((id) => waitForEvent(events, `reminder:${id}`));
    const creates = await Promise.all(
      reminderIds.map((id) =>
        httpRequest(
          `${daprdA.baseUrl}/v1.0/actors/${ACTOR_TYPE}/${id}/reminders/${REMINDER_NAME}`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ dueTime: '2s', data: { probe: true } }),
            timeoutMs: 5000,
          }
        )
      )
    );
    for (const res of creates) {
      assert.equal(res.status, 204, res.text);
    }
    const reminderCallbacks = (await Promise.all(reminderWaits)).map(([r]) => r);

    // A settled window past every reminder's own dueTime, to catch a
    // duplicate delivery a `once` waiter (which resolves on the FIRST
    // occurrence only) would otherwise miss entirely.
    await delay(2000); // allow-timer: bounded settle window proving a one-shot reminder does not redeliver

    const reminderPath = (id) => `/actors/${ACTOR_TYPE}/${id}/method/remind/${REMINDER_NAME}`;
    const reminderCountsById = reminderIds.map(
      (id) => requests.filter((r) => r.path === reminderPath(id)).length
    );
    assert.deepEqual(
      reminderCountsById,
      reminderIds.map(() => 1),
      `every reminder must be delivered exactly once, got counts ${JSON.stringify(reminderCountsById)}`
    );

    const reminderHostCounts = { A: 0, B: 0 };
    for (const r of reminderCallbacks) {
      reminderHostCounts[r.host] += 1;
    }
    t.diagnostic(`reminder callbacks by host: ${JSON.stringify(reminderHostCounts)}`);
    assert.ok(
      reminderHostCounts.B > 0,
      `at least one of ${REMINDER_COUNT} reminder ids must land on replica B to prove cross-replica ` +
        `forwarding actually happened (all landed on A -- increase REMINDER_COUNT), got ${JSON.stringify(reminderHostCounts)}`
    );

    t.diagnostic(
      `reminder callback details: ${JSON.stringify(
        reminderCallbacks.map((r) => ({
          host: r.host,
          headerNames: r.headerNames,
          hasCallerAppId: r.hasCallerAppId,
          hasCallerNamespace: r.hasCallerNamespace,
        }))
      )}`
    );
    // Observed: the header set is NOT identical across all 20 callbacks --
    // `traceparent` is present on some and absent on others, on BOTH host A
    // and host B, with no clean correlation to which replica actually owns
    // the id (i.e. not simply "present only when forwarded" or vice versa).
    // What IS identical across every single one of the 20, regardless of
    // host or traceparent: the base six headers below, and the total
    // absence of dapr-caller-app-id / dapr-caller-namespace.
    const withoutTraceparent = (names) => names.filter((n) => n !== 'traceparent');
    assert.deepEqual(
      [...new Set(reminderCallbacks.map((r) => JSON.stringify(withoutTraceparent(r.headerNames))))],
      [JSON.stringify(CALLBACK_HEADER_NAMES)],
      'every reminder callback, minus an optionally-present traceparent, carries exactly the same base header set'
    );
    const withTraceparentCount = reminderCallbacks.filter((r) =>
      r.headerNames.includes('traceparent')
    ).length;
    t.diagnostic(
      `reminder callbacks carrying traceparent: ${withTraceparentCount}/${reminderCallbacks.length} ` +
        '(present on both replicas, inconsistently per-id -- unrelated to the caller-app-id/' +
        'caller-namespace question this file targets, so not asserted further)'
    );
    assert.equal(
      reminderCallbacks.every((r) => r.hasCallerAppId === false),
      true,
      'no reminder callback -- on the replica that owns the id locally, or on the replica it was ' +
        'forwarded to -- carries dapr-caller-app-id'
    );
    assert.equal(
      reminderCallbacks.every((r) => r.hasCallerNamespace === false),
      true,
      'no reminder callback -- forwarded or local -- carries dapr-caller-namespace either'
    );

    // --- contrast: an ordinary method call, forwarded the same way ---------

    const methodWaits = METHOD_IDS.map((id) => waitForEvent(events, `method:${id}:${METHOD_NAME}`));
    const methodCreates = await Promise.all(
      METHOD_IDS.map((id) =>
        httpRequest(`${daprdA.baseUrl}/v1.0/actors/${ACTOR_TYPE}/${id}/method/${METHOD_NAME}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: '{}',
          timeoutMs: 10000,
        })
      )
    );
    for (const res of methodCreates) {
      assert.equal(res.status, 200, res.text);
    }
    const methodCallbacks = (await Promise.all(methodWaits)).map(([r]) => r);

    const methodHostCounts = { A: 0, B: 0 };
    for (const r of methodCallbacks) {
      methodHostCounts[r.host] += 1;
    }
    t.diagnostic(`ordinary method callbacks by host: ${JSON.stringify(methodHostCounts)}`);
    t.diagnostic(
      `ordinary method callback details: ${JSON.stringify(
        methodCallbacks.map((r) => ({
          host: r.host,
          headerNames: r.headerNames,
          callerAppId: r.callerAppId,
          hasCallerNamespace: r.hasCallerNamespace,
        }))
      )}`
    );
    assert.equal(
      methodCallbacks.every((r) => r.hasCallerAppId === true),
      true,
      'an ordinary (non-reminder) method callback, forwarded across replicas or not, always carries ' +
        'dapr-caller-app-id -- the contrast the reminder-callback observation above is measured against'
    );
    assert.equal(
      methodCallbacks.every((r) => r.callerAppId === APP_ID),
      true,
      "dapr-caller-app-id on an ordinary method callback is the CALLING sidecar's own --app-id -- " +
        "here the same string as the receiving replica's, since both share one --app-id"
    );
    assert.equal(
      methodCallbacks.every((r) => r.hasCallerNamespace === true),
      true,
      'an ordinary method callback also carries dapr-caller-namespace, forwarded or not -- the reminder ' +
        'callback above carries neither of this pair, on either replica'
    );
    // Also observed on the ordinary-method callbacks above (not asserted --
    // incidental to the caller-app-id/caller-namespace question this file
    // targets): every one carries `dapr-api-call` in addition to the base
    // set, and the one landing on host B (necessarily forwarded, since every
    // create here went through replica A's own HTTP API) additionally
    // carried `x-dapr-remote` -- a marker of cross-replica forwarding this
    // file does not otherwise need to rely on for its own conclusion.
  }
);
