'use strict';

// Real-wire protocol probe: proves real daprd 1.18.4 + Placement + Scheduler
// actor REMINDER and TIMER behavior against a tiny raw node:http "app" this
// test controls directly -- NOT Node-RED, NOT the package's own app-channel
// listener -- exactly as test/integration/actors-probe.test.js does for
// plain actor method calls. Every assertion here is a regression trip-wire: a
// future Dapr pin bump that changes any of this wire behavior should fail
// this file first.
//
// Observed and recorded here:
//  a. Reminder create is `POST .../reminders/{name}` -> 204 empty body for
//     object, string, null and absent `data`. The callback always lands at
//     `PUT /actors/{type}/{id}/method/remind/{name}` with headers
//     accept-encoding, content-length, content-type, dapr-api-token, host,
//     user-agent, sometimes plus traceparent -- NO dapr-caller-app-id or
//     dapr-caller-namespace (unlike an ordinary method callback -- see
//     actors-probe.test.js). The body envelope is `{"data":<value>,
//     "dueTime":"","period":""}` (absent `data` omits the key entirely);
//     dueTime/period are ALWAYS empty strings here, regardless of what the
//     reminder was created with -- a timer's callback (see g) carries the
//     real values instead. `data` is passed through as real JSON (object key
//     order preserved, not re-sorted) but through Go's default HTML-escaping
//     pass: `<`, `>` and `&` come back as Unicode escape sequences.
//  b. A recurring reminder whose callback answers 200 with header
//     `X-DaprReminderCancel: true` is NOT actually cancelled on 1.18.4: the
//     app-dispatch layer recognises the header and raises an internal
//     "reminder canceled" signal, but dapr/dapr v1.18.4's
//     pkg/runtime/scheduler/internal/cluster/streamer.go maps that signal to
//     WatchJobsRequestResultStatus_SUCCESS with an explicit `// TODO: add
//     support for cancelling a job` (there is no CANCELED status yet) -- so
//     the Scheduler just reschedules the next occurrence and the reminder
//     keeps firing on schedule.
//  c. A reminder whose callback answers 500 gets exactly 4 delivery attempts
//     per occurrence (1 initial + 3 retries, each about 1s apart -- the
//     exact policy behind this was not looked up in source), then waits for
//     its next occurrence at the configured period -- it is never
//     permanently disabled by repeated failure. A callback that never
//     answers at all is bounded by the actor's own `actorIdleTimeout`, not
//     by its period or a fixed request timeout: at the idle deadline the
//     actor is deactivated (`DELETE /actors/{type}/{id}` arrives) WHILE its
//     callback is still pending, that same deactivation tears down the
//     pending callback's own connection (observed via the response socket's
//     `close` event, immediately after the DELETE), and a fresh delivery
//     attempt follows at once, reactivating the actor -- an abandoned turn
//     does not block deactivation; deactivation is what ends it.
//  d. Overwriting an existing reminder (POST the same name again) succeeds
//     (204), the next callback carries the new data, and it fires on a
//     schedule reset from the overwrite call (not the original create).
//     `overwrite: false` against an existing name is rejected with 409
//     `ERR_ACTOR_REMINDER_ALREADY_EXISTS`.
//  e. GET of a missing reminder is 404 `ERR_ACTOR_REMINDER_NOT_FOUND`; GET
//     of an existing one is 200 with `{actorID,actorType,data,dueTime,
//     period}` -- `period` is rendered back as a cron expression
//     (`"10s"` in -> `"@every 10s"` out) while `dueTime` is echoed
//     unchanged. DELETE is 204 for both an existing AND a missing reminder
//     (idempotent, never 404).
//  f. A sidecar that does not host ProbeActor rejects reminder create/get
//     with 403 `ERR_ACTOR_REMINDER_NON_HOSTED`, and timer create with 500
//     `ERR_ACTOR_TIMER_CREATE` ("actor type not registered") -- a different
//     status and error code from the reminder path.
//  g. Timer create is `POST .../timers/{name}` -> 204; its callback lands
//     at `PUT /actors/{type}/{id}/method/timer/{name}` with the same
//     header set as a reminder callback (no dapr-caller-app-id either) and
//     a body carrying the REAL `dueTime`/`period` plus a `callback` field
//     (empty string unless supplied) -- unlike a reminder's callback, which
//     always blanks dueTime/period. A `callback` value does not change the
//     callback path: it always stays `/method/timer/{name}`, never
//     `/method/{callback}`. Deleting the timer stops further callbacks.
//     Both a timer and a reminder reactivate the actor and keep firing on
//     schedule even after daprd deactivates it for being idle (observed via
//     `DELETE /actors/{type}/{id}` on this same probe app) -- neither is
//     discarded by deactivation on this pin.
//  h. A reminder due for an actor whose own method call is being held open
//     by the app does not reach the app until that call is released; a
//     reminder for a different actor id is unaffected -- the same
//     same-actor serialization actors-probe.test.js proves for method
//     calls.
//  i. A reminder callback body is exactly N+34 bytes, where N is
//     `Buffer.byteLength(JSON.stringify(data))`, for a payload with no
//     HTML-escaped characters: the fixed `{"data":` / `,"dueTime":"",
//     "period":""}` wrapper contributes 34 bytes regardless of payload
//     size. Each `<`, `>` or `&` inside `data` becomes a 6-byte `\uXXXX`
//     escape (see a), so an inbound-size budget must use 6N+34 as its
//     worst case, not N+34, unless the payload shape is known to exclude
//     those three characters.
//
// NOT probed: two-replica timer routing/deletion. Adding a second host that
// also registers ProbeActor would have Placement's consistent-hash ring
// redistribute ProbeActor ids across both hosts, which could relocate an id
// a still-running single-host subtest above depends on (e.g. life-timer,
// rem-hang) to the new replica mid-test. Probing it safely likely needs
// either a separate actor type for the two-replica pair, or running it only
// after every single-host subtest above has finished with its ids -- this
// file does neither yet, and which of those two is actually necessary was
// not verified here.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { text: readBody } = require('node:stream/consumers');
const { once } = require('node:events');
const { EventEmitter } = require('node:events');

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

const ACTOR_TYPE = 'ProbeActor';
const APP_ID = 'it-actors-schedule-probe-app';
const APP_ID_OTHER = 'it-actors-schedule-probe-other';
const APP_API_TOKEN = 'actors-schedule-probe-app-token';
// Short enough that the dedicated deactivation-survival subtest (g) observes
// a real idle deactivation within this file's own budget. Every tick or
// retry attempt touches idle again before dispatching (dapr/dapr v1.18.4's
// pkg/actors/targets/app/app.go: InvokeReminder and InvokeTimer both call
// touchIdle() before invoking the app), so an id whose repeated ticks land
// well under this deadline apart (rem-cancel at 1s, rem-fail's occurrences
// at up to ~2s, rem-ttl at 2s) never legitimately goes idle. Two ids are
// deliberate exceptions in the other direction: rem-hang's callback never
// answers, so nothing re-touches idle after an attempt starts, and its actor
// idles out mid-turn -- which is itself what spaces its attempts apart (see
// c); life-timer/life-reminder use a period longer than
// this specifically so (g)'s survival check has a real deactivation to
// observe. Everything else fires once (or twice, for the overwrite check in
// d) and is deleted well before idling would matter either way. A period
// picked close to but not past this deadline is the one choice to avoid: a
// 6s period on (c)'s failure reminder (a 3s inter-occurrence gap, landing
// right at the deadline) once dropped an attempt during development, most
// likely to a real idle-deactivation race -- unconfirmed, since that run
// did not record rem-fail's own DELETE count -- which is why (c) below
// uses 5s (a 2s gap) instead.
const IDLE_TIMEOUT = '3s';
const IDLE_TIMEOUT_MS = 3000;
const SCAN_INTERVAL = '1s';

function waitForEvent(events, name, timeoutMs = 20000) {
  return once(events, name, { signal: AbortSignal.timeout(timeoutMs) });
}

// A raw actor-host "app", parameterized so the same shape serves both the
// hosting sidecar (advertises ProbeActor) and the non-hosting one used by (f)
// (advertises no entities at all). Per-(actorId,name) behavior can be
// installed for reminder/timer callbacks (cancel, fail, hang); anything
// without an installed behavior answers a plain 200.
function createProbeApp({ advertiseActor }) {
  const requests = [];
  const events = new EventEmitter();
  const holds = new Map();
  const behaviors = new Map();

  function record(req, bodyText) {
    const r = {
      method: req.method,
      path: req.url,
      headerNames: Object.keys(req.headers).sort(),
      hasAppToken: Object.hasOwn(req.headers, 'dapr-api-token'),
      hasCallerAppId: Object.hasOwn(req.headers, 'dapr-caller-app-id'),
      bodyText,
      bodyLength: Buffer.byteLength(bodyText),
      at: Date.now(),
    };
    requests.push(r);
    return r;
  }

  const server = http.createServer(async (req, res) => {
    let bodyText;
    try {
      bodyText = await readBody(req);
    } catch {
      bodyText = '';
    }
    const r = record(req, bodyText);
    events.emit('request', r);

    if (req.method === 'GET' && req.url === '/dapr/config') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify(
          advertiseActor
            ? {
                entities: [ACTOR_TYPE],
                actorIdleTimeout: IDLE_TIMEOUT,
                actorScanInterval: SCAN_INTERVAL,
              }
            : { entities: [] }
        )
      );
      return;
    }
    if (req.method === 'GET' && req.url === '/healthz') {
      res.writeHead(204);
      res.end();
      return;
    }
    const deleteMatch = /^\/actors\/ProbeActor\/([^/]+)$/.exec(req.url);
    if (req.method === 'DELETE' && deleteMatch) {
      const [, id] = deleteMatch;
      events.emit(`deactivated:${id}`, r);
      res.writeHead(200);
      res.end();
      return;
    }
    const remindMatch = /^\/actors\/ProbeActor\/([^/]+)\/method\/remind\/([^/]+)$/.exec(req.url);
    const timerMatch = /^\/actors\/ProbeActor\/([^/]+)\/method\/timer\/([^/]+)$/.exec(req.url);
    const methodMatch = /^\/actors\/ProbeActor\/([^/]+)\/method\/([^/]+)$/.exec(req.url);

    if (remindMatch) {
      const [, id, name] = remindMatch;
      events.emit(`reminder:${id}:${name}`, r);
      const behavior = behaviors.get(`${id}:${name}`);
      if (behavior) return behavior(req, res, r);
      res.writeHead(200);
      res.end();
      return;
    }
    if (timerMatch) {
      const [, id, name] = timerMatch;
      events.emit(`timer:${id}:${name}`, r);
      const behavior = behaviors.get(`${id}:${name}`);
      if (behavior) return behavior(req, res, r);
      res.writeHead(200);
      res.end();
      return;
    }
    if (methodMatch) {
      const [, id, method] = methodMatch;
      if (method === 'hold') {
        const deferred = Promise.withResolvers();
        holds.set(id, deferred);
        events.emit(`holding:${id}`);
        await deferred.promise;
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ held: true }));
        return;
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    res.writeHead(404);
    res.end();
  });

  return {
    server,
    events,
    requests,
    setBehavior: (id, name, fn) => behaviors.set(`${id}:${name}`, fn),
    release: (id) => {
      const deferred = holds.get(id);
      if (deferred) {
        holds.delete(id);
        deferred.resolve();
      }
    },
  };
}

async function startApp(advertiseActor) {
  const app = createProbeApp({ advertiseActor });
  app.server.listen(0, '127.0.0.1');
  await once(app.server, 'listening');
  return app;
}

test(
  'real daprd 1.18.4 + Placement + Scheduler actor reminder/timer wire protocol',
  { timeout: 120000 },
  async (t) => {
    const app = await startApp(true);
    t.after(() => closeHttpServer(app.server));
    const appPort = app.server.address().port;

    const otherApp = await startApp(false);
    t.after(() => closeHttpServer(otherApp.server));
    const otherAppPort = otherApp.server.address().port;

    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());
    const scheduler = await startScheduler();
    t.after(() => scheduler.stop());

    const actorStore = () => ({
      filename: 'actorstore.yaml',
      yaml: stateComponentYaml(redis.port, { name: 'actorstore', actorStateStore: true }),
    });

    const daprHttpPort = await freePort();
    const daprd = await startDaprd({
      appId: APP_ID,
      appPort,
      httpPort: daprHttpPort,
      components: [actorStore()],
      appApiToken: APP_API_TOKEN,
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });
    t.after(() => daprd.stop());
    const otherDaprHttpPort = await freePort();
    const otherDaprd = await startDaprd({
      appId: APP_ID_OTHER,
      appPort: otherAppPort,
      httpPort: otherDaprHttpPort,
      components: [actorStore()],
      appApiToken: APP_API_TOKEN,
      placementAddress: `127.0.0.1:${placement.port}`,
      schedulerAddress: `127.0.0.1:${scheduler.port}`,
    });
    t.after(() => otherDaprd.stop());

    const metadata = await waitFor(async () => {
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
    t.diagnostic(`actorRuntime ready: ${JSON.stringify(metadata.actorRuntime)}`);

    function post(base, path, body) {
      return httpRequest(`${base}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        timeoutMs: 5000,
      });
    }
    function get(base, path) {
      return httpRequest(`${base}${path}`, { timeoutMs: 5000 });
    }
    function del(base, path) {
      return httpRequest(`${base}${path}`, { method: 'DELETE', timeoutMs: 5000 });
    }
    function callMethod(base, id, method) {
      return httpRequest(`${base}/v1.0/actors/${ACTOR_TYPE}/${id}/method/${method}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
        timeoutMs: 15000,
      });
    }
    function createReminder(id, name, body) {
      return post(daprd.baseUrl, `/v1.0/actors/${ACTOR_TYPE}/${id}/reminders/${name}`, body);
    }
    function getReminder(id, name) {
      return get(daprd.baseUrl, `/v1.0/actors/${ACTOR_TYPE}/${id}/reminders/${name}`);
    }
    function deleteReminder(id, name) {
      return del(daprd.baseUrl, `/v1.0/actors/${ACTOR_TYPE}/${id}/reminders/${name}`);
    }
    function createTimer(id, name, body) {
      return post(daprd.baseUrl, `/v1.0/actors/${ACTOR_TYPE}/${id}/timers/${name}`, body);
    }
    function deleteTimer(id, name) {
      return del(daprd.baseUrl, `/v1.0/actors/${ACTOR_TYPE}/${id}/timers/${name}`);
    }

    const CALLBACK_HEADER_NAMES = [
      'accept-encoding',
      'content-length',
      'content-type',
      'dapr-api-token',
      'host',
      'user-agent',
    ];

    // --- a. create shapes: object / string / null / absent data ----------
    // Waiters are armed BEFORE the creates that trigger them (dueTime=1s
    // gives real margin, but "own the emitter before the action" is the
    // rule regardless -- see docs/testing.md).

    const objWait = waitForEvent(app.events, 'reminder:rem-obj:r1');
    const strWait = waitForEvent(app.events, 'reminder:rem-str:r1');
    const nullWait = waitForEvent(app.events, 'reminder:rem-null:r1');
    const absentWait = waitForEvent(app.events, 'reminder:rem-absent:r1');

    const objData = { blob: 'x'.repeat(500), n: 1 };
    const [createObj, createStr, createNull, createAbsent] = await Promise.all([
      createReminder('rem-obj', 'r1', { dueTime: '1s', period: '2s', data: objData }),
      createReminder('rem-str', 'r1', { dueTime: '1s', period: '2s', data: 'plain-string' }),
      createReminder('rem-null', 'r1', { dueTime: '1s', period: '2s', data: null }),
      createReminder('rem-absent', 'r1', { dueTime: '1s', period: '2s' }),
    ]);
    for (const res of [createObj, createStr, createNull, createAbsent]) {
      assert.equal(res.status, 204, res.text);
      assert.equal(res.text, '');
    }

    const [[objReq], [strReq], [nullReq], [absentReq]] = await Promise.all([
      objWait,
      strWait,
      nullWait,
      absentWait,
    ]);

    assert.equal(objReq.path, `/actors/${ACTOR_TYPE}/rem-obj/method/remind/r1`);
    // traceparent is present on some callbacks and not others (see
    // actors-replicas-probe.test.js), so it is tolerated, never required.
    const withoutTrace = (names) => names.filter((n) => n !== 'traceparent');
    assert.deepEqual(withoutTrace(objReq.headerNames), CALLBACK_HEADER_NAMES);
    assert.equal(objReq.hasAppToken, true);
    assert.equal(
      objReq.hasCallerAppId,
      false,
      'a reminder callback carries no dapr-caller-app-id, unlike an ordinary method callback'
    );
    assert.equal(objReq.bodyText, `{"data":${JSON.stringify(objData)},"dueTime":"","period":""}`);
    assert.equal(strReq.bodyText, '{"data":"plain-string","dueTime":"","period":""}');
    assert.equal(nullReq.bodyText, '{"data":null,"dueTime":"","period":""}');
    assert.equal(
      absentReq.bodyText,
      '{"dueTime":"","period":""}',
      'absent create-time data omits the "data" key from the callback entirely'
    );
    t.diagnostic(`reminder callback header names: ${JSON.stringify(objReq.headerNames)}`);

    await Promise.all([
      deleteReminder('rem-obj', 'r1'),
      deleteReminder('rem-str', 'r1'),
      deleteReminder('rem-null', 'r1'),
      deleteReminder('rem-absent', 'r1'),
    ]);

    // --- i. envelope overhead (reuses the object-data callback above) -----

    assert.equal(
      objReq.bodyLength,
      Buffer.byteLength(JSON.stringify(objData)) + 34,
      'the reminder envelope adds exactly 34 bytes of JSON wrapper around the raw data, for a payload with no HTML-escaped characters'
    );

    // --- a (continued). data passes through as real JSON, HTML-escaped ----

    const escWait = waitForEvent(app.events, 'reminder:rem-esc:rEsc');
    const trickyData = { z: 1, a: '<&>\u2028\u2029' }; // deliberately unsorted keys + escapable chars
    await createReminder('rem-esc', 'rEsc', { dueTime: '1s', period: '10s', data: trickyData });
    const [escReq] = await escWait;
    assert.equal(
      escReq.bodyText,
      '{"data":{"z":1,"a":"\\u003c\\u0026\\u003e\\u2028\\u2029"},"dueTime":"","period":""}',
      'data is passed through in its original key order, but through the same HTML-escaping pass ' +
        "Go's json encoder applies by default"
    );
    await deleteReminder('rem-esc', 'rEsc');

    // --- d. overwrite semantics: next callback's data, and schedule reset --

    const overV1Wait = waitForEvent(app.events, 'reminder:rem-overwrite:rOver');
    await createReminder('rem-overwrite', 'rOver', {
      dueTime: '1s',
      period: '10s',
      data: { v: 1 },
    });
    const [overV1Req] = await overV1Wait;
    assert.equal(overV1Req.bodyText, '{"data":{"v":1},"dueTime":"","period":""}');

    const overV2Wait = waitForEvent(app.events, 'reminder:rem-overwrite:rOver');
    const overwriteAt = Date.now();
    const overwriteDefault = await createReminder('rem-overwrite', 'rOver', {
      dueTime: '1s',
      period: '10s',
      data: { v: 2 },
    });
    assert.equal(
      overwriteDefault.status,
      204,
      'overwriting an existing reminder by default succeeds'
    );
    const [overV2Req] = await overV2Wait;
    assert.equal(
      overV2Req.bodyText,
      '{"data":{"v":2},"dueTime":"","period":""}',
      'the next callback after an overwrite carries the NEW data'
    );
    const overwriteToCallbackMs = overV2Req.at - overwriteAt;
    assert.ok(
      overwriteToCallbackMs > 700 && overwriteToCallbackMs < 2500,
      "the callback after an overwrite arrives around the overwrite's own 1s dueTime (not immediately, and " +
        `not the original create's ~10s-away next occurrence) -- overwriting resets the schedule, got ${overwriteToCallbackMs}ms`
    );

    const overwriteFalse = await createReminder('rem-overwrite', 'rOver', {
      dueTime: '10s',
      period: '10s',
      data: { v: 3 },
      overwrite: false,
    });
    assert.equal(overwriteFalse.status, 409);
    assert.equal(JSON.parse(overwriteFalse.text).errorCode, 'ERR_ACTOR_REMINDER_ALREADY_EXISTS');
    await deleteReminder('rem-overwrite', 'rOver');

    // --- e. get/delete semantics --------------------------------------------

    const getMissing = await getReminder('rem-getdel', 'missing');
    assert.equal(getMissing.status, 404);
    assert.equal(JSON.parse(getMissing.text).errorCode, 'ERR_ACTOR_REMINDER_NOT_FOUND');
    const deleteMissing = await deleteReminder('rem-getdel', 'missing');
    assert.equal(
      deleteMissing.status,
      204,
      'deleting a reminder that never existed is idempotent, not 404'
    );

    await createReminder('rem-getdel', 'rGet', { dueTime: '10s', period: '10s', data: { x: 1 } });
    const getExisting = await getReminder('rem-getdel', 'rGet');
    assert.equal(getExisting.status, 200);
    const existingBody = JSON.parse(getExisting.text);
    assert.equal(existingBody.actorID, 'rem-getdel');
    assert.equal(existingBody.actorType, ACTOR_TYPE);
    assert.deepEqual(existingBody.data, { x: 1 });
    assert.equal(existingBody.dueTime, '10s', 'dueTime is echoed back unchanged');
    assert.equal(
      existingBody.period,
      '@every 10s',
      'period is rendered back as a cron expression, not the raw input string'
    );
    const deleteExisting = await deleteReminder('rem-getdel', 'rGet');
    assert.equal(deleteExisting.status, 204);
    const getAfterDelete = await getReminder('rem-getdel', 'rGet');
    assert.equal(getAfterDelete.status, 404);

    // --- f. non-host sidecar -------------------------------------------------

    const nonHostCreate = await post(
      otherDaprd.baseUrl,
      `/v1.0/actors/${ACTOR_TYPE}/nh1/reminders/remNH`,
      { dueTime: '10s', period: '10s', data: { n: 1 } }
    );
    assert.equal(nonHostCreate.status, 403);
    assert.equal(JSON.parse(nonHostCreate.text).errorCode, 'ERR_ACTOR_REMINDER_NON_HOSTED');
    const nonHostGet = await get(
      otherDaprd.baseUrl,
      `/v1.0/actors/${ACTOR_TYPE}/nh1/reminders/remNH`
    );
    assert.equal(nonHostGet.status, 403);
    assert.equal(JSON.parse(nonHostGet.text).errorCode, 'ERR_ACTOR_REMINDER_NON_HOSTED');
    const nonHostTimer = await post(
      otherDaprd.baseUrl,
      `/v1.0/actors/${ACTOR_TYPE}/nh1/timers/timNH`,
      { dueTime: '10s', period: '10s', data: { n: 1 } }
    );
    assert.equal(
      nonHostTimer.status,
      500,
      'a non-host timer create fails differently from a reminder create'
    );
    assert.equal(JSON.parse(nonHostTimer.text).errorCode, 'ERR_ACTOR_TIMER_CREATE');
    assert.match(JSON.parse(nonHostTimer.text).message, /actor type not registered/);

    // --- g. timer create/callback shape/delete ------------------------------

    const timerWait = waitForEvent(app.events, 'timer:tim-basic:t1');
    const timerCreate = await createTimer('tim-basic', 't1', {
      dueTime: '1s',
      period: '5s',
      data: { tick: 1 },
      callback: 'unused-does-not-change-the-path',
    });
    assert.equal(timerCreate.status, 204);
    const [timerReq] = await timerWait;
    assert.equal(timerReq.path, `/actors/${ACTOR_TYPE}/tim-basic/method/timer/t1`);
    assert.deepEqual(withoutTrace(timerReq.headerNames), CALLBACK_HEADER_NAMES);
    assert.equal(
      timerReq.hasCallerAppId,
      false,
      'a timer callback carries no dapr-caller-app-id either'
    );
    assert.equal(
      timerReq.bodyText,
      '{"data":{"tick":1},"callback":"unused-does-not-change-the-path","dueTime":"1s","period":"5s"}',
      'a timer callback carries the real dueTime/period, unlike a reminder callback'
    );
    const timerCallCountBeforeDelete = app.requests.filter(
      (r) => r.path === `/actors/${ACTOR_TYPE}/tim-basic/method/timer/t1`
    ).length;
    const timerDelete = await deleteTimer('tim-basic', 't1');
    assert.equal(timerDelete.status, 204);
    await assert.rejects(waitForEvent(app.events, 'timer:tim-basic:t1', 5500), {
      name: 'AbortError',
    });
    const timerCallCountAfterDelete = app.requests.filter(
      (r) => r.path === `/actors/${ACTOR_TYPE}/tim-basic/method/timer/t1`
    ).length;
    assert.equal(
      timerCallCountAfterDelete,
      timerCallCountBeforeDelete,
      'no further timer callbacks arrive after delete'
    );

    // --- b. cancel header is recognized but NOT wired to Scheduler on 1.18.4

    const cancelFirstWait = waitForEvent(app.events, 'reminder:rem-cancel:rCancel');
    app.setBehavior('rem-cancel', 'rCancel', async (req, res) => {
      res.writeHead(200, { 'X-DaprReminderCancel': 'true' });
      res.end();
    });
    await createReminder('rem-cancel', 'rCancel', { dueTime: '1s', period: '1s', data: { c: 1 } });
    await cancelFirstWait;
    const cancelCallbackPath = `/actors/${ACTOR_TYPE}/rem-cancel/method/remind/rCancel`;
    const cancelCountAfterFirst = app.requests.filter((r) => r.path === cancelCallbackPath).length;
    await waitForEvent(app.events, 'reminder:rem-cancel:rCancel');
    const cancelCountLater = app.requests.filter((r) => r.path === cancelCallbackPath).length;
    assert.ok(
      cancelCountLater > cancelCountAfterFirst,
      'X-DaprReminderCancel is recognized by the app-dispatch layer but the Scheduler job handler maps it ' +
        "to SUCCESS (dapr/dapr v1.18.4's pkg/runtime/scheduler/internal/cluster/streamer.go has its own " +
        'TODO on this), so on 1.18.4 the reminder keeps firing rather than being cancelled'
    );
    await deleteReminder('rem-cancel', 'rCancel');

    // --- c. failure and hang redelivery -------------------------------------
    // period=5s: the ~1s-apart attempts within one occurrence stay clear of
    // the next occurrence's (unlike a shorter period, which makes the two
    // indistinguishable in the raw timestamps), while the ~2s gap between
    // occurrences stays safely under IDLE_TIMEOUT (3s) -- a 6s period (a 3s
    // inter-occurrence gap, exactly AT the idle deadline) cost this file one
    // dropped occurrence-2 attempt to a real idle-deactivation race before
    // this was tightened to 5s.

    const failTimes = [];
    app.setBehavior('rem-fail', 'rFail', async (req, res) => {
      failTimes.push(Date.now());
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ message: 'induced failure' }));
    });
    const failStart = Date.now();
    await createReminder('rem-fail', 'rFail', { dueTime: '1s', period: '5s', data: { f: 1 } });
    while (failTimes.length < 6) {
      await waitForEvent(app.events, 'reminder:rem-fail:rFail');
    }
    const failGaps = failTimes.slice(1).map((t, i) => t - failTimes[i]);
    t.diagnostic(
      `remFail offsets (ms, period=5s): ${JSON.stringify(failTimes.map((t) => t - failStart))}`
    );
    assert.ok(
      failTimes.length >= 6,
      `expected occurrence 1's 4 attempts plus at least 2 attempts of occurrence 2 (got ${failTimes.length})`
    );
    assert.ok(
      failGaps.slice(0, 3).every((g) => g > 700 && g < 1500),
      `occurrence 1's own 4 attempts must be spaced about 1s apart, got gaps ${JSON.stringify(failGaps.slice(0, 3))}`
    );
    assert.ok(
      failGaps[3] > 1500 && failGaps[3] < 2600,
      'the 4th gap must be the ~2s inter-occurrence gap (period 5s minus the 3 retry-seconds already spent), ' +
        `which pins the cap at exactly 4 attempts per occurrence -- got ${failGaps[3]}ms`
    );
    await deleteReminder('rem-fail', 'rFail');

    const hangTimes = [];
    const hangSocketCloses = [];
    app.setBehavior('rem-hang', 'rHang', async (req, res) => {
      hangTimes.push(Date.now());
      // res (not req) 'close': fires only if the underlying connection dies
      // before res.end() -- which this handler never calls -- so unlike
      // req's own 'close' (which fires once the REQUEST body is fully read,
      // regardless of whether a response was ever sent) this only fires if
      // daprd itself tears the connection down while still waiting.
      res.on('close', () => hangSocketCloses.push(Date.now()));
      await new Promise(() => {}); // allow-promise: intentionally never resolves -- probing daprd's own timeout behavior
    });
    const hangStart = Date.now();
    // period=10s is well clear of actorIdleTimeout (3s), so a retry cadence
    // that tracks the period (not idle) would be obviously distinguishable.
    await createReminder('rem-hang', 'rHang', { dueTime: '1s', period: '10s', data: { h: 1 } });
    while (hangTimes.length < 3) {
      await waitForEvent(app.events, 'reminder:rem-hang:rHang');
    }
    const hangGaps = hangTimes.slice(1).map((t, i) => t - hangTimes[i]);
    const hangDeactivateTimes = app.requests
      .filter((r) => r.method === 'DELETE' && r.path === `/actors/${ACTOR_TYPE}/rem-hang`)
      .map((r) => r.at - hangStart);
    const hangTimeline = [
      ...hangTimes.map((t) => ({ at: t - hangStart, kind: 'attempt' })),
      ...hangDeactivateTimes.map((at) => ({ at, kind: 'deactivate' })),
    ].sort((a, b) => a.at - b.at);
    t.diagnostic(`remHang timeline (ms, period=10s): ${JSON.stringify(hangTimeline)}`);
    t.diagnostic(
      `remHang socket-close offsets (ms): ${JSON.stringify(hangSocketCloses.map((t) => t - hangStart))} ` +
        '(whether daprd tears down the connection of an abandoned attempt, or just stops waiting on it)'
    );
    assert.ok(
      hangTimes.length >= 2,
      `a hung callback must be retried (got ${hangTimes.length} attempts)`
    );
    assert.ok(
      hangGaps.every((g) => g > IDLE_TIMEOUT_MS * 0.8 && g < IDLE_TIMEOUT_MS * 1.25),
      `a hung callback's retry cadence must track actorIdleTimeout (${IDLE_TIMEOUT_MS}ms), not its 10s period, got gaps ${JSON.stringify(hangGaps)}`
    );
    assert.ok(
      hangDeactivateTimes.length >= 1,
      'the actor must be deactivated at least once while a callback is still pending -- an abandoned turn ' +
        'does not block the idle-scan deactivation'
    );
    await deleteReminder('rem-hang', 'rHang');

    // --- ttl ------------------------------------------------------------

    const ttlTimes = [];
    app.setBehavior('rem-ttl', 'rTtl', async (req, res) => {
      ttlTimes.push(Date.now());
      res.writeHead(200);
      res.end();
    });
    const ttlStart = Date.now();
    await createReminder('rem-ttl', 'rTtl', {
      dueTime: '1s',
      period: '2s',
      data: { t: 1 },
      ttl: '4s',
    });
    // Idle deactivation after the last tick occurs beyond this reminder's TTL.
    await waitForEvent(app.events, 'deactivated:rem-ttl');
    t.diagnostic(
      `remTtl offsets (ms, ttl=4s): ${JSON.stringify(ttlTimes.map((t) => t - ttlStart))}`
    );
    const getAfterTtl = await getReminder('rem-ttl', 'rTtl');
    assert.equal(getAfterTtl.status, 404, 'a reminder past its ttl is removed');

    // --- h. serialization: reminder vs. a held method call ------------------
    // The hold must stay well under IDLE_TIMEOUT (3s): InvokeMethod only
    // touches idle once, at the START of the held call, so a hold lasting
    // close to 3s risks a real idle-scan deactivation racing the release.

    const holding = waitForEvent(app.events, 'holding:ser-a');
    const holdCall = callMethod(daprd.baseUrl, 'ser-a', 'hold'); // not awaited yet
    await holding;
    const serAReminderWait = waitForEvent(app.events, 'reminder:ser-a:remSerA'); // armed before release
    await createReminder('ser-a', 'remSerA', { dueTime: '1s', period: '10s', data: { s: 1 } });
    const serBReminderWait = waitForEvent(app.events, 'reminder:ser-b:remSerB');
    await createReminder('ser-b', 'remSerB', { dueTime: '1s', period: '10s', data: { s: 2 } });
    await serBReminderWait;
    assert.equal(
      app.requests.some((r) => r.path === `/actors/${ACTOR_TYPE}/ser-a/method/remind/remSerA`),
      false,
      'a reminder due for the actor whose method call is held must wait for release'
    );
    app.release('ser-a');
    await Promise.all([holdCall, serAReminderWait]);
    await deleteReminder('ser-a', 'remSerA');
    await deleteReminder('ser-b', 'remSerB');

    // --- g (continued): both a timer and a reminder survive a real idle ----
    //     deactivation. Needs a period LONGER than actorIdleTimeout (3s) so
    //     the actor genuinely goes idle between ticks. Waiters are armed
    //     before the creates that eventually trigger them.

    const timerFirstTickWait = waitForEvent(app.events, 'timer:life-timer:timLife');
    const reminderFirstTickWait = waitForEvent(app.events, 'reminder:life-reminder:remLife');
    await Promise.all([
      createTimer('life-timer', 'timLife', { dueTime: '1s', period: '7s', data: { d: 1 } }),
      createReminder('life-reminder', 'remLife', { dueTime: '1s', period: '7s', data: { d: 2 } }),
    ]);
    await Promise.all([timerFirstTickWait, reminderFirstTickWait]);
    const timerDeactivatedWait = waitForEvent(app.events, 'deactivated:life-timer', 15000);
    const reminderDeactivatedWait = waitForEvent(app.events, 'deactivated:life-reminder', 15000);
    await Promise.all([timerDeactivatedWait, reminderDeactivatedWait]);
    const timerSecondTickWait = waitForEvent(app.events, 'timer:life-timer:timLife', 15000);
    const reminderSecondTickWait = waitForEvent(
      app.events,
      'reminder:life-reminder:remLife',
      15000
    );
    await Promise.all([timerSecondTickWait, reminderSecondTickWait]);
    t.diagnostic(
      'both the timer and the reminder fired again after their actor was deactivated (reactivating it)'
    );
    await deleteTimer('life-timer', 'timLife');
    await deleteReminder('life-reminder', 'remLife');
  }
);
