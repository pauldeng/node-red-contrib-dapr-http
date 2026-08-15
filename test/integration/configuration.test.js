'use strict';

// Real daprd 1.18.2 + a real Redis configuration.redis component -- dynamic
// configuration never touches pub/sub broker semantics, so this needs no
// NATS/JetStream and no pubsub component (mirrors state.test.js's own
// broker-agnostic setup). Proves: get returns a real value after a change
// made directly against Redis (not through this package), subscribe
// delivers a real push driven by Redis's own keyspace notifications (not an
// artifact of our own code), and confirms that daprd's v1.0 and
// v1.0-alpha1 unsubscribe prefixes both
// stop subsequent delivery, not merely that they answer with HTTP 200.
//
// Redis's own keyspace notifications are off by default; configuration.redis
// subscribes to them directly (not polling), so the backing Redis container
// is started with notify-keyspace-events enabled -- see
// test/helpers/integration.js's configurationComponentYaml.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd, configurationComponentYaml } = require('../helpers/integration');
const { execFileP } = require('../helpers/docker');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');
const { setTimeout: delay } = require('node:timers/promises');

const STORE = 'configstore';
const KEY = 'feature:flag';
const ALPHA_KEY = 'alphaFlag';

function flow({ appPort, daprHttpPort, captureUrl }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-configuration' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    { id: 'in', type: 'http in', z: 'tab', url: '/get', method: 'post', wires: [['before']] },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload; return msg;',
      outputs: 1,
      wires: [['get']],
    },
    {
      id: 'get',
      type: 'dapr-config-get',
      z: 'tab',
      connection: 'c1',
      storeName: STORE,
      keys: KEY,
      metadata: '{}',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200; msg.payload = { dapr: msg.dapr, payload: msg.payload }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    {
      id: 'errors',
      type: 'catch',
      z: 'tab',
      scope: ['get'],
      uncaught: false,
      wires: [['failure']],
    },
    {
      id: 'failure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab' },
    {
      id: 'sub',
      type: 'dapr-config-subscribe',
      z: 'tab',
      connection: 'c1',
      storeName: STORE,
      keys: KEY,
      metadata: '{}',
      wires: [['fwd']],
    },
    {
      id: 'sub-alpha',
      type: 'dapr-config-subscribe',
      z: 'tab',
      connection: 'c1',
      storeName: STORE,
      keys: ALPHA_KEY,
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
msg.payload = JSON.stringify({ payload: msg.payload, dapr: msg.dapr });
return msg;`,
      outputs: 1,
      wires: [['req']],
    },
    { id: 'req', type: 'http request', z: 'tab', method: 'use', ret: 'txt', url: '', wires: [[]] },
  ];
}

test(
  'a real configuration.redis component supports get, a real keyspace-notification push, restart-resubscribe, and both unsubscribe prefixes',
  { timeout: 120000 },
  async (t) => {
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    await nr.start({ flows: flow({ appPort, daprHttpPort, captureUrl: capture.url }) });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const redis = await startRedis({ notifyKeyspaceEvents: 'KEA' });
    t.after(() => redis.stop());
    const redisSet = (key, value) =>
      execFileP('docker', ['exec', redis.name, 'redis-cli', 'SET', key, value]);
    const logCount = (pattern) => (nr.logText().match(pattern) || []).length;
    const captured = (key, value, predicate = () => true) =>
      capture.received.find(
        (message) => message?.payload?.[key]?.value === value && predicate(message)
      );

    // The subscription must emit values that already exist before daprd starts,
    // not only changes made after the subscription is registered.
    await redisSet(KEY, 'v1');
    await redisSet(ALPHA_KEY, 'alpha-v1');

    let daprd = await startDaprd({
      appId: 'it-configuration-app',
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'configstore.yaml', yaml: configurationComponentYaml(redis.port) }],
    });
    t.after(() => daprd?.stop());

    // startDaprd() resolving only proves daprd's OWN /healthz answers; the
    // dapr-connection node (and the subscribe node's own subscribe attempt,
    // gated on the same health signal) polls independently on its own
    // bounded-backoff schedule and may not have caught up yet.
    await waitFor(() => (logCount(/Dapr sidecar is available/g) > 0 ? true : null));

    const initial = await waitFor(() => captured(KEY, 'v1') || null, { timeoutMs: 15000 });
    const alphaInitial = await waitFor(() => captured(ALPHA_KEY, 'alpha-v1') || null, {
      timeoutMs: 15000,
    });
    const firstSubscriptionId = initial.dapr.subscriptionId;
    const firstAlphaSubscriptionId = alphaInitial.dapr.subscriptionId;
    assert.ok(firstSubscriptionId);
    assert.ok(firstAlphaSubscriptionId);

    // A value set directly against Redis (never through this package) is
    // real: get sees it, proving the round trip is genuine.
    const got = await waitFor(async () => {
      const r = await httpRequest(nr.nodeUrl('/get'), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({}),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    const gotBody = JSON.parse(got.text);
    assert.equal(gotBody.payload[KEY].value, 'v1');

    // A further change, made directly against Redis, arrives as a real push
    // driven by Redis's own keyspace notifications -- not an artifact of
    // this package's own code, since nothing in this package wrote it.
    await redisSet(KEY, 'v2');
    const pushed = await waitFor(() => captured(KEY, 'v2') || null, { timeoutMs: 15000 });
    assert.equal(pushed.dapr.storeName, STORE);
    assert.equal(pushed.dapr.subscriptionId, firstSubscriptionId);

    // Prove the connection actually observes the outage before bringing daprd
    // back. A restart entirely between 10-second health polls is not evidence
    // of the subscribe node's recovery path.
    const pubsubChannels = () =>
      execFileP('docker', ['exec', redis.name, 'redis-cli', 'PUBSUB', 'CHANNELS', '*']);
    const unavailableBefore = logCount(/Dapr sidecar is unavailable/g);
    await daprd.stop();
    daprd = null;
    await waitFor(
      () => (logCount(/Dapr sidecar is unavailable/g) > unavailableBefore ? true : null),
      { timeoutMs: 15000 }
    );

    const availableBefore = logCount(/Dapr sidecar is available/g);
    daprd = await startDaprd({
      appId: 'it-configuration-app',
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'configstore.yaml', yaml: configurationComponentYaml(redis.port) }],
    });
    await waitFor(() => (logCount(/Dapr sidecar is available/g) > availableBefore ? true : null), {
      timeoutMs: 30000,
    });

    const recovered = await waitFor(
      () =>
        captured(KEY, 'v2', (message) => message.dapr.subscriptionId !== firstSubscriptionId) ||
        null,
      { timeoutMs: 15000 }
    );
    const alphaRecovered = await waitFor(
      () =>
        captured(
          ALPHA_KEY,
          'alpha-v1',
          (message) => message.dapr.subscriptionId !== firstAlphaSubscriptionId
        ) || null,
      { timeoutMs: 15000 }
    );
    const recoveredId = recovered.dapr.subscriptionId;
    const alphaRecoveredId = alphaRecovered.dapr.subscriptionId;

    await redisSet(KEY, 'v3');
    await redisSet(ALPHA_KEY, 'alpha-v2');
    await waitFor(() =>
      captured(KEY, 'v3', (message) => message.dapr.subscriptionId === recoveredId)
    );
    await waitFor(() =>
      captured(ALPHA_KEY, 'alpha-v2', (message) => message.dapr.subscriptionId === alphaRecoveredId)
    );

    // Exercise each prefix against an independently live node subscription,
    // then mutate Redis and prove no callback follows.
    const unsubscribeDirect = (prefix, id) =>
      httpRequest(`${daprd.baseUrl}/${prefix}/configuration/${STORE}/${id}/unsubscribe`, {
        timeoutMs: 4000,
      });
    for (const [prefix, key, id, value] of [
      ['v1.0', KEY, recoveredId, 'after-stable-unsubscribe'],
      ['v1.0-alpha1', ALPHA_KEY, alphaRecoveredId, 'after-alpha-unsubscribe'],
    ]) {
      const unsubscribed = await unsubscribeDirect(prefix, id);
      assert.equal(unsubscribed.status, 200);
      assert.equal(JSON.parse(unsubscribed.text).ok, true);
      await waitFor(() =>
        pubsubChannels().then((channels) => (channels.includes(`:${key}`) ? null : true))
      );
      await redisSet(key, value);
      await delay(1000);
      assert.equal(captured(key, value), undefined, `${prefix} must stop subsequent delivery`);
    }
  }
);
