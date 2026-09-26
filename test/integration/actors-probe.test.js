'use strict';

// Real-wire protocol probe: proves real daprd 1.18.4 + Placement wire
// behavior against a tiny raw node:http "app" this test controls directly --
// NOT Node-RED, NOT the package's own app-channel listener -- so
// lib/actor-host.js (see docs/architecture.md's "Actor request ownership"
// section) has a ground-truth contract to match instead of trusting a
// comment's source citations alone. Every assertion here is a regression
// trip-wire: a future Dapr pin bump that changes any of this wire behavior
// should fail this file first.
//
// Observed and recorded here:
//  a. /dapr/config carries the app API token but never dapr-caller-app-id;
//     an actor method callback carries both, with dapr-caller-app-id equal
//     to daprd's own --app-id (a same-process actor call has no other
//     caller identity available).
//  b. App 500/503/404 each surface to the caller as a generic HTTP 500
//     ERR_ACTOR_INVOKE_METHOD (503's real status only survives inside the
//     error message text; 404 gets a distinct "method not found" message
//     with no body echo) -- with no default retry: each hits the app once.
//  c. Reading a key that was never saved is 204 with an empty body and
//     needs no active actor; saving is rejected unless the actor is
//     currently hosted (see d), so every save here is preceded by a method
//     call that (re)activates it. A save issued from *inside* an active
//     callback, before the callback answers, commits like any other save.
//  d. A save succeeds after the turn that activated the actor has already
//     answered (still within the idle window), but fails once daprd has
//     deactivated the actor (observed via the DELETE callback) -- and the
//     record written while still active survives that deactivation.
//  e. Two calls to the same actor ID serialize: a second call does not
//     reach the app until the first (held open) one answers. A call to a
//     different actor ID is unaffected and completes while the first is
//     still held.
//  f. With no app-health-check config, daprd never calls the app's
//     /healthz at all -- confirmed by a zero count at the very end, after
//     every other observation already gave it ample opportunity to fire.

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
  startDaprd,
  stateComponentYaml,
} = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

const ACTOR_TYPE = 'ProbeActor';
const APP_ID = 'it-actors-probe-app';
const APP_API_TOKEN = 'actors-probe-app-token';

// A tiny actor-host "app" this test fully controls: daprd's real HTTP actor
// transport talks to it exactly as it would talk to lib/actor-host.js. Every
// request is recorded (method, path, sorted header names, and the two
// headers this probe cares about) so the assertions below can inspect
// exactly what daprd sent, not what a comment predicted.
function createProbeApp() {
  const events = new EventEmitter();
  const requests = [];
  const healthzHits = [];
  const deactivated = [];
  const holds = new Map(); // actorId -> Promise.withResolvers()
  let daprBaseUrl; // set once daprd exists; only `saveInside` needs it

  const methodPath = new RegExp(`^/actors/${ACTOR_TYPE}/([^/]+)/method/([^/]+)$`);
  const deletePath = new RegExp(`^/actors/${ACTOR_TYPE}/([^/]+)$`);

  function writeJson(res, status, body) {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  }

  async function handleMethod(id, method, res, record) {
    if (method === 'ok') {
      writeJson(res, 200, { ok: true, id });
      return;
    }
    if (method === 'fail500') {
      writeJson(res, 500, { message: 'probe-induced failure' });
      return;
    }
    if (method === 'busy503') {
      writeJson(res, 503, { message: 'probe-induced busy' });
      return;
    }
    if (method === 'missing404') {
      writeJson(res, 404, { message: 'probe-induced not-found' });
      return;
    }
    if (method === 'hold') {
      const deferred = Promise.withResolvers();
      holds.set(id, deferred);
      events.emit(`holding:${id}`);
      await deferred.promise;
      record.releasedBeforeAnswer = true;
      writeJson(res, 200, { held: true, id });
      return;
    }
    if (method === 'saveInside') {
      const saveRes = await httpRequest(`${daprBaseUrl}/v1.0/actors/${ACTOR_TYPE}/${id}/state`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify([
          { operation: 'upsert', request: { key: 'record', value: { insideTurn: true } } },
        ]),
        timeoutMs: 5000,
      });
      writeJson(res, 200, { savedInside: true, saveStatus: saveRes.status });
      return;
    }
    writeJson(res, 404, { message: 'unknown probe method' });
  }

  const server = http.createServer(async (req, res) => {
    const headerNames = Object.keys(req.headers).sort();
    const record = {
      method: req.method,
      path: req.url,
      at: Date.now(),
      headerNames,
      callerAppId: Object.prototype.hasOwnProperty.call(req.headers, 'dapr-caller-app-id')
        ? req.headers['dapr-caller-app-id']
        : undefined,
      hasAppToken: Object.prototype.hasOwnProperty.call(req.headers, 'dapr-api-token'),
    };
    requests.push(record);
    try {
      await readBody(req); // drain; every probe route below ignores the body
      if (req.method === 'GET' && req.url === '/dapr/config') {
        writeJson(res, 200, {
          entities: [ACTOR_TYPE],
          actorIdleTimeout: '4s',
          actorScanInterval: '1s',
          drainOngoingCallTimeout: '5s',
          drainRebalancedActors: true,
          reentrancy: { enabled: false },
        });
        return;
      }
      if (req.method === 'GET' && req.url === '/healthz') {
        healthzHits.push(Date.now());
        res.writeHead(204);
        res.end();
        return;
      }
      const deleteMatch = deletePath.exec(req.url);
      if (req.method === 'DELETE' && deleteMatch) {
        const [, id] = deleteMatch;
        deactivated.push(id);
        res.writeHead(200);
        res.end();
        events.emit(`deactivated:${id}`);
        return;
      }
      const callMatch = methodPath.exec(req.url);
      if (req.method === 'PUT' && callMatch) {
        const [, id, method] = callMatch;
        events.emit(`received:${id}:${method}`, record);
        await handleMethod(id, method, res, record);
        return;
      }
      res.writeHead(404);
      res.end();
    } catch (err) {
      writeJson(res, 500, { message: err.message });
    }
  });

  return {
    server,
    events,
    requests,
    healthzHits,
    deactivated,
    release: (id) => {
      const deferred = holds.get(id);
      if (deferred) {
        holds.delete(id);
        deferred.resolve();
      }
    },
    setDaprBaseUrl: (url) => {
      daprBaseUrl = url;
    },
  };
}

function waitForEvent(events, name, timeoutMs = 20000) {
  return once(events, name, { signal: AbortSignal.timeout(timeoutMs) });
}

// One upsert transaction for the actor's single `record` key.
function saveRecord(daprBaseUrl, id, value) {
  return httpRequest(`${daprBaseUrl}/v1.0/actors/${ACTOR_TYPE}/${id}/state`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify([{ operation: 'upsert', request: { key: 'record', value } }]),
    timeoutMs: 5000,
  });
}

function readRecord(daprBaseUrl, id) {
  return httpRequest(`${daprBaseUrl}/v1.0/actors/${ACTOR_TYPE}/${id}/state/record`, {
    timeoutMs: 5000,
  });
}

function callMethod(daprBaseUrl, id, method) {
  return httpRequest(`${daprBaseUrl}/v1.0/actors/${ACTOR_TYPE}/${id}/method/${method}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: '{}',
    timeoutMs: 15000,
  });
}

test(
  'real daprd 1.18.4 + Placement actor wire protocol: discovery, method semantics, state, deactivation and serialization',
  { timeout: 90000 },
  async (t) => {
    const app = createProbeApp();
    app.server.listen(0, '127.0.0.1');
    await once(app.server, 'listening');
    t.after(() => closeHttpServer(app.server));
    const appPort = app.server.address().port;

    // Redis and Placement are independent of each other and of the app
    // (already listening above, as daprd's /dapr/config fetch requires).
    const [redis, placement] = await Promise.all([startRedis(), startPlacement()]);
    t.after(() => redis.stop());
    t.after(() => placement.stop());

    const daprHttpPort = await freePort();
    const daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: [
        {
          filename: 'actorstore.yaml',
          yaml: stateComponentYaml(redis.port, { name: 'actorstore', actorStateStore: true }),
        },
      ],
      appApiToken: APP_API_TOKEN,
      placementAddress: `127.0.0.1:${placement.port}`,
    });
    t.after(() => daprd.stop());
    app.setDaprBaseUrl(daprd.baseUrl);

    // startDaprd() resolving only proves daprd's OWN /v1.0/healthz/outbound;
    // the actor runtime connects to Placement on its own separate schedule.
    // GET /v1.0/metadata's actorRuntime block is the real (inspected) shape
    // for 1.18.4: poll until placement reports the host ready with
    // ProbeActor registered, rather than guessing a fixed settle time.
    const metadata = await waitFor(async () => {
      const res = await httpRequest(`${daprd.baseUrl}/v1.0/metadata`, { timeoutMs: 2000 });
      if (res.status !== 200) return null;
      const body = JSON.parse(res.text);
      const runtime = body.actorRuntime;
      const ready =
        runtime &&
        runtime.runtimeStatus === 'RUNNING' &&
        runtime.hostReady === true &&
        typeof runtime.placement === 'string' &&
        /placement:\s*connected/i.test(runtime.placement) &&
        Array.isArray(runtime.activeActors) &&
        runtime.activeActors.some((a) => a.type === ACTOR_TYPE);
      return ready ? body : null;
    });
    assert.match(
      metadata.actorRuntime.placement,
      /placement:\s*connected/i,
      'actor readiness requires Placement connected'
    );
    t.diagnostic(`actorRuntime ready: ${JSON.stringify(metadata.actorRuntime)}`);

    // --- (a) discovery and callback headers -----------------------------

    const configReq = app.requests.find((r) => r.method === 'GET' && r.path === '/dapr/config');
    assert.ok(configReq, '/dapr/config must have been fetched at daprd startup');
    assert.equal(
      configReq.callerAppId,
      undefined,
      '/dapr/config is daprd itself calling out, not a mesh caller'
    );
    assert.equal(configReq.hasAppToken, true, 'daprd must send the configured app API token');
    t.diagnostic(`/dapr/config header names: ${JSON.stringify(configReq.headerNames)}`);

    const okA = await callMethod(daprd.baseUrl, 'headers-probe', 'ok');
    assert.equal(okA.status, 200);
    assert.deepEqual(JSON.parse(okA.text), { ok: true, id: 'headers-probe' });
    const callbackReq = app.requests.find(
      (r) => r.method === 'PUT' && r.path === `/actors/${ACTOR_TYPE}/headers-probe/method/ok`
    );
    assert.ok(callbackReq, 'the method callback must have reached the app');
    assert.equal(
      callbackReq.callerAppId,
      APP_ID,
      'an actor method callback carries dapr-caller-app-id, unlike /dapr/config'
    );
    assert.equal(callbackReq.hasAppToken, true);
    assert.ok(
      callbackReq.headerNames.includes('dapr-caller-namespace'),
      'method callbacks carry dapr-caller-namespace'
    );
    assert.ok(
      callbackReq.headerNames.includes('traceparent'),
      'method callbacks carry a W3C traceparent'
    );
    t.diagnostic(`method-callback header names: ${JSON.stringify(callbackReq.headerNames)}`);

    // --- (b) method result semantics and retry count --------------------

    const fail500 = await callMethod(daprd.baseUrl, 'result-probe', 'fail500');
    assert.equal(fail500.status, 500, fail500.text);
    const fail500Body = JSON.parse(fail500.text);
    assert.equal(fail500Body.errorCode, 'ERR_ACTOR_INVOKE_METHOD');
    assert.match(fail500Body.message, /\(500\)/);

    const busy503 = await callMethod(daprd.baseUrl, 'result-probe', 'busy503');
    assert.equal(
      busy503.status,
      500,
      'the app answered 503, but daprd wraps every non-200/404 app status the same way'
    );
    const busy503Body = JSON.parse(busy503.text);
    assert.equal(busy503Body.errorCode, 'ERR_ACTOR_INVOKE_METHOD');
    assert.match(busy503Body.message, /\(503\)/, 'the real 503 only survives in the message text');

    const missing404 = await callMethod(daprd.baseUrl, 'result-probe', 'missing404');
    assert.equal(missing404.status, 500, 'a 404 from the app is not passed through to the caller');
    const missing404Body = JSON.parse(missing404.text);
    assert.equal(missing404Body.errorCode, 'ERR_ACTOR_INVOKE_METHOD');
    assert.match(missing404Body.message, /method not found/);

    for (const method of ['fail500', 'busy503', 'missing404']) {
      const hits = app.requests.filter(
        (r) =>
          r.method === 'PUT' && r.path === `/actors/${ACTOR_TYPE}/result-probe/method/${method}`
      );
      assert.equal(hits.length, 1, `daprd must not retry ${method} by default`);
    }

    // --- (c) state read/save shapes --------------------------------------

    const neverWritten = await readRecord(daprd.baseUrl, 'state-never-written');
    assert.equal(neverWritten.status, 204);
    assert.equal(neverWritten.text, '');

    // A save is rejected unless the actor is currently hosted (see d), so
    // reactivate it with an ordinary method call immediately before saving.
    await callMethod(daprd.baseUrl, 'state-null', 'ok');
    const saveNull = await saveRecord(daprd.baseUrl, 'state-null', null);
    assert.equal(saveNull.status, 204, 'the transaction POST itself answers 204');
    const readNull = await readRecord(daprd.baseUrl, 'state-null');
    assert.equal(readNull.status, 200, 'explicit null is a stored value, not "absent"');
    assert.equal(readNull.text, 'null');

    await callMethod(daprd.baseUrl, 'state-object', 'ok');
    const saveObject = await saveRecord(daprd.baseUrl, 'state-object', { n: 1, label: 'probe' });
    assert.equal(saveObject.status, 204);
    const readObject = await readRecord(daprd.baseUrl, 'state-object');
    assert.equal(readObject.status, 200);
    assert.deepEqual(JSON.parse(readObject.text), { n: 1, label: 'probe' });

    // A transaction issued from *inside* an active turn, before the app
    // answers the callback -- the write must persist like any other save.
    const saveInside = await callMethod(daprd.baseUrl, 'state-save-inside', 'saveInside');
    assert.equal(saveInside.status, 200, saveInside.text);
    assert.equal(JSON.parse(saveInside.text).saveStatus, 204);
    const readInside = await readRecord(daprd.baseUrl, 'state-save-inside');
    assert.equal(readInside.status, 200);
    assert.deepEqual(JSON.parse(readInside.text), { insideTurn: true });

    // --- (d) save while active, then after deactivation ------------------

    const deactivateId = 'state-after-deactivate';
    await callMethod(daprd.baseUrl, deactivateId, 'ok'); // activates it
    const saveWhileActive = await saveRecord(daprd.baseUrl, deactivateId, { activeSave: true });
    assert.equal(
      saveWhileActive.status,
      204,
      'a save after the turn ended, while still active, succeeds'
    );

    // No public API deactivates an actor on demand; it happens only once
    // daprd's own idle timer elapses (actorIdleTimeout=4s/actorScanInterval=1s
    // above). Wait for the app to observe the real DELETE callback instead of
    // sleeping a guessed duration.
    await waitForEvent(app.events, `deactivated:${deactivateId}`);
    const deleteReq = app.requests.find(
      (r) => r.method === 'DELETE' && r.path === `/actors/${ACTOR_TYPE}/${deactivateId}`
    );
    assert.ok(deleteReq, 'the deactivation callback must have been recorded');
    assert.equal(
      deleteReq.hasAppToken,
      true,
      'the deactivation callback also carries the app API token'
    );
    assert.equal(
      deleteReq.callerAppId,
      undefined,
      'like /dapr/config, the deactivation callback is daprd itself, not a mesh caller'
    );
    t.diagnostic(`DELETE callback header names: ${JSON.stringify(deleteReq.headerNames)}`);

    const saveAfterDeactivate = await saveRecord(daprd.baseUrl, deactivateId, { lateSave: true });
    // The orchestrator expects this to fail; assert what daprd actually
    // does rather than assume a stronger guarantee.
    assert.equal(saveAfterDeactivate.status, 400, saveAfterDeactivate.text);
    assert.equal(JSON.parse(saveAfterDeactivate.text).errorCode, 'ERR_ACTOR_INSTANCE_MISSING');

    const readAfterDeactivate = await readRecord(daprd.baseUrl, deactivateId);
    assert.equal(readAfterDeactivate.status, 200);
    assert.deepEqual(
      JSON.parse(readAfterDeactivate.text),
      { activeSave: true },
      'deactivation must not delete the durable record, and the late save above must not have applied'
    );

    // --- (e) same-actor serialization vs. different-actor concurrency ----

    const holdId = 'serial-a';
    const otherId = 'serial-b';
    const holdCall = callMethod(daprd.baseUrl, holdId, 'hold'); // not awaited yet
    await waitForEvent(app.events, `holding:${holdId}`);

    const secondOnHeld = callMethod(daprd.baseUrl, holdId, 'ok'); // not awaited yet either
    const concurrentOther = await callMethod(daprd.baseUrl, otherId, 'ok');
    assert.equal(concurrentOther.status, 200, 'a different actor ID must proceed while A is held');

    // Bounded negative assertion, not a completion signal: prove the second
    // same-actor call has not reached the app while the first is still held.
    // There is no event for "did not happen yet" to wait on.
    await delay(300); // allow-timer: bounded negative -- snapshot before releasing the hold
    const secondReachedAppEarly = app.requests.some(
      (r) => r.method === 'PUT' && r.path === `/actors/${ACTOR_TYPE}/${holdId}/method/ok`
    );
    assert.equal(
      secondReachedAppEarly,
      false,
      'the second call to the same actor must not reach the app while the first is held'
    );

    const releasedAt = Date.now();
    app.release(holdId);
    const heldResult = await holdCall;
    assert.equal(heldResult.status, 200);
    const secondResult = await secondOnHeld;
    assert.equal(secondResult.status, 200);
    const secondReq = app.requests.find(
      (r) => r.method === 'PUT' && r.path === `/actors/${ACTOR_TYPE}/${holdId}/method/ok`
    );
    assert.ok(secondReq, 'the second same-actor call must eventually reach the app');
    assert.ok(
      secondReq.at >= releasedAt,
      'the second same-actor call must not reach the app before the hold was released'
    );

    // --- (f) shared /healthz is never polled with default config ---------
    // Run last, after every other observation above already gave a default
    // health-check policy ample time to have fired at least once.
    assert.equal(
      app.healthzHits.length,
      0,
      'daprd must not call /healthz without app-health-check config'
    );
  }
);
