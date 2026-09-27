'use strict';

// One real-daprd trace through the actor server span: a caller-supplied
// traceparent reaches daprd's own actor-invoke API, daprd calls this app's
// method callback carrying that same trace (see docs/architecture.md's
// "Actor wire behavior": method callbacks carry traceparent), and
// lib/actor-host.js's SERVER span parents on it. Observed against real daprd
// 1.18.4: the actor method callback forwards the caller's traceparent
// UNCHANGED (same trace id AND span id) -- daprd does not mint its own
// intermediate hop span for this path the way it does for some other
// telemetry -- so the exported server span's parent is exactly the caller's
// own span. This test pins that observed behavior rather than assuming one.
//
// Reuses test/integration/actors.test.js's real daprd + Placement + actor-
// flagged Redis store setup and test/integration/telemetry.test.js's real
// OTel collector fixture; this file adds only the tracing-enabled connection
// and the traceparent assertion.

const { test } = require('node:test');
const assert = require('node:assert/strict');

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
const { startOtelCollector } = require('../helpers/otel-collector');
const { invoke } = require('../../lib/actor-client');

function stringSpanAttribute(span, key) {
  const attribute = span.attributes?.find((item) => item.key === key);
  return attribute?.value?.stringValue;
}

const APP_ID = 'it-actors-telemetry-app';
const CALLER_TRACEPARENT = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
const CALLER_TRACE_ID = CALLER_TRACEPARENT.split('-')[1];
const CALLER_SPAN_ID = CALLER_TRACEPARENT.split('-')[2];

function flow({ appPort, daprHttpPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'actor-telemetry' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      tracingEnabled: true,
    },
    {
      // Never named "method": Node-RED's config-node dependency scanner
      // mistakes a node id equal to one of its own property names for a
      // circular self-reference (the same class of pitfall as an id equal to
      // a property value -- see docs/testing skill's flow-JSON rules).
      id: 'm1',
      type: 'dapr-actor-method',
      z: 'tab',
      connection: 'c1',
      actorType: 'Widget',
      method: 'Ping',
      trigger: 'method',
      wires: [['fn']],
    },
    {
      id: 'fn',
      type: 'function',
      z: 'tab',
      func: "msg.dapr.actor.nextState = { calls: 1 }; msg.payload = 'pong'; return msg;",
      outputs: 1,
      wires: [['reply']],
    },
    {
      id: 'reply',
      type: 'dapr-actor-reply',
      z: 'tab',
      connection: 'c1',
      outcome: 'complete',
    },
  ];
}

// Mirrors test/integration/actors-probe.test.js's own readiness check: daprd
// reporting hostReady/placement-connected proves Placement is reachable, but
// not yet that THIS actor type's table entry has propagated -- a call
// landing in that gap fails with a FailedPrecondition "did not find address"
// even though every other readiness signal is green. `activeActors`
// including this type is the one field that actually proves it.
function waitForActorReady(daprBaseUrl, actorType, { timeoutMs = 30000 } = {}) {
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
        /placement:\s*connected/i.test(runtime.placement) &&
        Array.isArray(runtime.activeActors) &&
        runtime.activeActors.some((a) => a.type === actorType);
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

// `/healthz` answers 204 as soon as the listener is bound, independent of
// whether this connection's actor method has finished its own (deferred,
// coalesced-via-setImmediate) registration -- see lib/app-channel.js and
// nodes/dapr-connection.js's scheduleActivation. daprd fetches /dapr/config
// exactly once at ITS OWN startup and never again, so starting it before
// this app's actor registration has actually landed would advertise no
// entities at all, a permanent gap (docs/architecture.md's "actor types
// changed" restart rule) rather than a transient one. Confirm the app itself
// already advertises the type before handing daprd anything to fetch.
async function waitForActorAdvertised(appPort, actorType) {
  return waitFor(async () => {
    const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/config`, { timeoutMs: 1000 });
    if (r.status !== 200) {
      return null;
    }
    const body = JSON.parse(r.text);
    return Array.isArray(body.entities) && body.entities.includes(actorType) ? body : null;
  });
}

test(
  'a real daprd actor method call produces the server span parented on the caller traceparent',
  { timeout: 180000 },
  async (t) => {
    const collector = await startOtelCollector();
    t.after(() => collector.stop());

    const redis = await startRedis();
    t.after(() => redis.stop());
    const placement = await startPlacement();
    t.after(() => placement.stop());

    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: flow({ appPort, daprHttpPort }),
      env: {
        OTEL_TRACES_SAMPLER: 'always_on',
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${collector.port}`,
      },
    });
    await waitForAppHealthz(appPort);
    await waitForActorAdvertised(appPort, 'Widget');

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
      placementAddress: `127.0.0.1:${placement.port}`,
    });
    t.after(() => daprd.stop());
    const daprBaseUrl = `http://127.0.0.1:${daprHttpPort}`;
    await waitForActorReady(daprBaseUrl, 'Widget');

    const result = await invoke(
      { baseUrl: daprBaseUrl, timeoutMs: 15000, headers: { traceparent: CALLER_TRACEPARENT } },
      { actorType: 'Widget', actorId: 'w1', method: 'Ping', body: {} }
    );
    assert.equal(result.status, 200);

    // Woken by a real fs.watch event on the collector's output directory
    // (test/helpers/otel-collector.js's waitForSpan), not a fixed poll.
    const serverSpan = await collector.waitForSpan((s) => s.name === 'actor Widget.Ping');
    assert.equal(serverSpan.traceId, CALLER_TRACE_ID, "daprd forwards the caller's own trace");
    assert.equal(
      serverSpan.parentSpanId,
      CALLER_SPAN_ID,
      "real daprd forwards the actor method callback's traceparent unchanged"
    );
    assert.equal(stringSpanAttribute(serverSpan, 'dapr.actor.outcome'), 'OK');
    assert.equal(stringSpanAttribute(serverSpan, 'dapr.actor.type'), 'Widget');
    const inTrace = (name) => (span) => span.name === name && span.traceId === CALLER_TRACE_ID;
    const read = await collector.waitForSpan(inTrace('actor state read'));
    const save = await collector.waitForSpan(inTrace('actor state save'));
    const fn = await collector.waitForSpan(inTrace('function'));
    const reply = await collector.waitForSpan(inTrace('dapr-actor-reply'));
    assert.equal(read.parentSpanId, serverSpan.spanId);
    assert.equal(save.parentSpanId, serverSpan.spanId);
    assert.equal(fn.parentSpanId, serverSpan.spanId);
    assert.equal(reply.parentSpanId, fn.spanId);
  }
);
