'use strict';

// Redeploy behavior moved from Redis to NATS JetStream per Milestone 3's
// NATS-primary rebalance — these are Dapr-runtime/app-channel-layer
// behaviors (stable delivery routes across redeploy, the restart-sidecar
// fingerprint, and drain-on-close), not broker-specific ones.
//
// All three scenarios below share one NATS container and one Node-RED
// container, redeploying a fresh flow between scenarios instead of starting
// a new container each time. Each scenario still gets its OWN daprd,
// deliberately: every one of these scenarios turns on exactly what daprd's
// own one-time /dapr/subscribe fetch captured at ITS OWN startup, which a
// shared daprd could not represent for all three at once (scenario 2 both
// changes a subscription AFTER its daprd has started and then restarts that
// very daprd — sharing it with scenarios 1/3 would either destroy their
// state or defeat what scenario 2 itself proves). Each scenario also uses
// its own, disjoint topic name(s) so the one shared NATS stream never lets
// one scenario's backlog or consumer state leak into another's.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');
const { setTimeout: delay } = require('node:timers/promises');

function flow({ daprHttpPort, appPort, topic, captureUrl }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-nats-redeploy' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    {
      id: 'sub1',
      type: 'dapr-subscribe',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic,
      ackMode: 'auto',
      metadata: '{}',
      wires: [['fwd']],
    },
    {
      id: 'fwd',
      type: 'function',
      z: 'tab',
      func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ payload: msg.payload, topic: msg.dapr.topic });
return msg;`,
      outputs: 1,
      wires: [['req']],
    },
    { id: 'req', type: 'http request', z: 'tab', method: 'use', ret: 'txt', url: '', wires: [[]] },
  ];
}

async function deployAndWaitHealthy(nr, theFlow, appPort) {
  await nr.deploy(theFlow);
  await waitFor(async () => {
    const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
    return r.status === 204 ? true : null;
  });
}

function pendingAckFlow({ daprHttpPort, appPort, topic, captureUrl }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-nats-redeploy-interruption' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      // Far longer than the test's own timeout: our own ack timeout must be
      // physically unable to fire within the observation window below, so a
      // redelivery observed there can only be caused by the redeploy's own
      // drain leaving the delivery retryable, never by this timeout racing it.
      requestTimeoutSec: '120',
    },
    {
      id: 'sub1',
      type: 'dapr-subscribe',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic,
      ackMode: 'manual',
      metadata: '{}',
      wires: [['route']],
    },
    {
      id: 'route',
      type: 'function',
      z: 'tab',
      func: `const n = msg.payload.n;
const key = 'attempt_' + n;
const attempt = (flow.get(key) || 0) + 1;
flow.set(key, attempt);
const report = Object.assign({}, msg);
report.url = ${JSON.stringify(captureUrl)};
report.method = 'POST';
report.headers = { 'content-type': 'application/json' };
report.payload = JSON.stringify({ n, attempt });
return [report, msg];`,
      outputs: 2,
      wires: [['reportReq'], ['decide']],
    },
    {
      id: 'reportReq',
      type: 'http request',
      z: 'tab',
      method: 'use',
      ret: 'txt',
      url: '',
      wires: [[]],
    },
    {
      id: 'decide',
      type: 'function',
      z: 'tab',
      func: `const n = msg.payload.n;
const attempt = flow.get('attempt_' + n);
if (n === 1 && attempt === 1) {
  return null; // deliberately left pending, for the redeploy below to interrupt
}
msg.ackStatus = 'SUCCESS';
return msg;`,
      outputs: 1,
      wires: [['ack']],
    },
    {
      id: 'ack',
      type: 'dapr-ack',
      z: 'tab',
      connection: 'c1',
      ackStatusSource: 'message',
      ackStatus: 'SUCCESS',
      wires: [[]],
    },
  ];
}

test('real daprd + NATS JetStream: redeploy behaviors', { timeout: 220000 }, async (t) => {
  const nats = await startNats();
  t.after(() => nats.stop());
  // One subject per scenario (2 for the subscription-change scenario), all
  // disjoint, so the one shared stream never lets one scenario's backlog or
  // consumer state leak into another's.
  await provisionStream(nats.port, {
    streamName: 'nrdapr-it',
    subjects: ['redeploy1-orders', 'redeploy2-orders', 'redeploy2-orders-v2', 'redeploy3-orders'],
  });
  const jetstreamComponent = () =>
    jetstreamComponentYaml({ name: 'pubsub', natsPort: nats.port, streamName: 'nrdapr-it' });

  const nr = new ContainerNodeRed();
  t.after(() => nr.stop());

  await t.test(
    'an unchanged full redeploy keeps a real daprd delivering, no sidecar restart needed, backed by NATS JetStream',
    async (st) => {
      const appId = 'it-nats-redeploy-unchanged';
      const appPort = await freePort();
      const daprHttpPort = await freePort();
      const capture = await startCapture();
      st.after(() => capture.stop());

      const theFlow = flow({
        daprHttpPort,
        appPort,
        topic: 'redeploy1-orders',
        captureUrl: capture.url,
      });
      await nr.start({ flows: theFlow });
      await waitFor(async () => {
        const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
        return r.status === 204 ? true : null;
      });

      const daprd = await startDaprd({
        appId,
        appPort,
        httpPort: daprHttpPort,
        components: [{ filename: 'pubsub-jetstream.yaml', yaml: jetstreamComponent() }],
      });
      st.after(() => daprd.stop());

      const publish = (n) =>
        waitFor(async () => {
          const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/redeploy1-orders`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ n }),
            timeoutMs: 4000,
          });
          return r.status === 204 ? r : null;
        });

      await publish(1);
      await waitFor(
        () => capture.received.some((r) => r && r.payload && r.payload.n === 1) || null
      );

      // Same flow definition again — a full redeploy, but nothing about the
      // subscription actually changed, so the delivery route (keyed on sub1's
      // persisted node id) and its fingerprint are identical. daprd is never
      // restarted here — this is exactly what proves it doesn't need to be.
      await nr.deploy(theFlow);
      await waitFor(async () => {
        const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
        return r.status === 204 ? true : null;
      });
      // /healthz can succeed against the OLD listener while it's still inside
      // its leaseGraceMs (lib/options.js: 2000ms) grace window, before the
      // redeployed node has actually reacquired the port. There is no retry
      // policy or dead-letter topic on this flow, so a delivery that lands in
      // that handover gap is lost forever, not just delayed — wait out the
      // grace window itself before publishing, not just one successful probe.
      await delay(3000);

      await publish(2);
      await waitFor(
        () => capture.received.some((r) => r && r.payload && r.payload.n === 2) || null
      );

      assert.doesNotMatch(nr.logText(), /restart the Dapr sidecar/i);
    }
  );

  await t.test(
    'changing a subscription surfaces the restart-sidecar warning, and restarting daprd activates it, backed by NATS JetStream',
    async (st) => {
      const appId = 'it-nats-redeploy-changed';
      const appPort = await freePort();
      const daprHttpPort = await freePort();
      const capture = await startCapture();
      st.after(() => capture.stop());

      const v1 = flow({
        daprHttpPort,
        appPort,
        topic: 'redeploy2-orders',
        captureUrl: capture.url,
      });
      await deployAndWaitHealthy(nr, v1, appPort);

      const daprdComponents = [{ filename: 'pubsub-jetstream.yaml', yaml: jetstreamComponent() }];
      let daprd = await startDaprd({
        appId,
        appPort,
        httpPort: daprHttpPort,
        components: daprdComponents,
      });
      st.after(() => daprd.stop());

      const publishTo = (topic, n) =>
        waitFor(async () => {
          const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/${topic}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ n }),
            timeoutMs: 4000,
          });
          return r.status === 204 ? r : null;
        });

      await publishTo('redeploy2-orders', 1);
      await waitFor(
        () =>
          capture.received.some((r) => r && r.topic === 'redeploy2-orders' && r.payload.n === 1) ||
          null
      );

      // Same node id, different topic: the subscription definition's
      // fingerprint changes even though the delivery route path (keyed off
      // sub1's id) does not. daprd was never told about this — it fetched
      // /dapr/subscribe once, at its own startup, with the old definition.
      const v2 = flow({
        daprHttpPort,
        appPort,
        topic: 'redeploy2-orders-v2',
        captureUrl: capture.url,
      });
      await deployAndWaitHealthy(nr, v2, appPort);
      await waitFor(() => (/restart the Dapr sidecar/i.test(nr.logText()) ? true : null));

      // The new topic has no real subscriber yet — daprd never fetched it —
      // so publishing to it must NOT reach the app. A bounded wait for
      // *absence* is the best a black-box test can do here.
      await publishTo('redeploy2-orders-v2', 2);
      await delay(2000);
      assert.equal(
        capture.received.some((r) => r && r.topic === 'redeploy2-orders-v2'),
        false,
        'the new topic cannot be delivered until daprd restarts and re-fetches /dapr/subscribe'
      );

      // Now actually restart the sidecar: same app, same port, fresh daprd
      // process — it re-fetches /dapr/subscribe against the app that's
      // already running and picks up the new topic.
      await daprd.stop();
      daprd = await startDaprd({
        appId,
        appPort,
        httpPort: daprHttpPort,
        components: daprdComponents,
      });
      st.after(() => daprd.stop());

      await publishTo('redeploy2-orders-v2', 3);
      await waitFor(
        () =>
          capture.received.some(
            (r) => r && r.topic === 'redeploy2-orders-v2' && r.payload.n === 3
          ) || null
      );
    }
  );

  await t.test(
    'a delivery pending at redeploy is left retryable and redelivered by real daprd, without corrupting the subscription, backed by NATS JetStream',
    async (st) => {
      const appId = 'it-nats-redeploy-interruption';
      const appPort = await freePort();
      const daprHttpPort = await freePort();
      const capture = await startCapture();
      st.after(() => capture.stop());

      const theFlow = pendingAckFlow({
        daprHttpPort,
        appPort,
        topic: 'redeploy3-orders',
        captureUrl: capture.url,
      });
      await deployAndWaitHealthy(nr, theFlow, appPort);

      // Without a Resiliency retry policy, a retryable outcome and a terminal
      // one (SUCCESS/DROP) are indistinguishable from outside — none of them
      // get redelivered — so this fixture is what makes actual redelivery the
      // proof that the drain left the delivery retryable, not just that the
      // redeploy completed.
      const daprd = await startDaprd({
        appId,
        appPort,
        httpPort: daprHttpPort,
        components: [{ filename: 'pubsub-jetstream.yaml', yaml: jetstreamComponent() }],
        extraFixtures: ['resiliency-retry.yaml'],
      });
      st.after(() => daprd.stop());

      const publish = (n) =>
        waitFor(async () => {
          const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/redeploy3-orders`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ n }),
            timeoutMs: 4000,
          });
          return r.status === 204 ? r : null;
        });

      // The delivery reaches the flow but is deliberately never acked (see
      // 'decide' above) — it's genuinely pending, and our own ack timeout
      // (120s, set far above this test's own timeout) cannot fire on its own
      // before the redeploy below happens.
      await publish(1);
      await waitFor(() => capture.received.some((r) => r && r.n === 1 && r.attempt === 1) || null);

      // A full redeploy while that delivery is still outstanding must settle
      // it as something retryable and complete cleanly — daprd's own HTTP
      // call for delivery #1 must not be left hanging or crash the sidecar.
      // The close handler drains pending acks with an explicit RETRY, but
      // real daprd could also treat a listener-timing 503 as retryable; this
      // test only needs "retryable and redelivered," not the exact mechanism.
      await nr.deploy(theFlow);
      await waitFor(async () => {
        const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
        return r.status === 204 ? true : null;
      });

      // Real daprd redelivering message #1 within a short window is the
      // actual proof the drain left it retryable, not a coincidence of some
      // other timeout: with requestTimeoutSec: 120, nothing else in this flow
      // could produce a retryable outcome within 10s of the redeploy — only
      // the close handler's own drain can.
      await waitFor(() => capture.received.some((r) => r && r.n === 1 && r.attempt === 2) || null, {
        timeoutMs: 10000,
      });

      // Nothing is left pending: 'decide' routes message #1's second attempt
      // (attempt !== 1) straight to SUCCESS, so it's already acked by the time
      // the wait above resolves. Send + ack a separate message #2 too, proving
      // the interrupted delivery didn't corrupt daprd's subscription state.
      await publish(2);
      await waitFor(() => capture.received.some((r) => r && r.n === 2) || null);
    }
  );
});
