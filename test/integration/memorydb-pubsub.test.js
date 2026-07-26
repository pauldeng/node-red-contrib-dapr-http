'use strict';

// OPTIONAL tier: the full chain against a real AWS MemoryDB for Redis cluster —
// dapr-publish -> daprd -> MemoryDB -> daprd -> dapr-subscribe. Skips itself
// unless the environment supplies the cluster's coordinates (see
// test/helpers/memorydb.js for why it cannot be hermetic).
//
// What this covers that test/integration/pubsub.test.js does not: the component
// configuration a managed cluster requires — TLS in transit, ACL authentication
// with a username, and cluster mode — plus the credential handling that goes with
// it (the password reaches daprd through its own env secret store, never a file).
// Node behavior itself is broker-agnostic and already covered against the local
// Redis and JetStream components, so this file does not re-run that matrix.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');
const {
  PASSWORD_ENV,
  memorydbConfigFromEnv,
  memorydbSkipReason,
  uniqueTopic,
  envSecretStoreYaml,
  memorydbPubsubYaml,
  assertMemorydbReachable,
  deleteTopics,
  command,
} = require('../helpers/memorydb');

// Forwards each delivery out of Node-RED to the capture server, which is the
// only way to observe what actually reached a flow from outside the container.
function forwardingSub(id, topic, extra = {}) {
  return {
    id,
    type: 'dapr-subscribe',
    z: 'tab',
    connection: 'c1',
    pubsubName: 'pubsub',
    topic,
    ackMode: 'auto',
    metadata: '{}',
    wires: [[`fwd-${id}`]],
    ...extra,
  };
}

function forwardingFn(id, captureUrl) {
  return {
    id: `fwd-${id}`,
    type: 'function',
    z: 'tab',
    func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
const payload = Buffer.isBuffer(msg.payload) ? msg.payload.toString('utf8') : msg.payload;
msg.payload = JSON.stringify({
  topic: msg.dapr.topic,
  isBuffer: Buffer.isBuffer(msg.payload),
  payload,
  cloudEvent: msg.dapr.cloudEvent,
});
return msg;`,
    outputs: 1,
    wires: [[`req-${id}`]],
  };
}

function forwardingReq(id) {
  return {
    id: `req-${id}`,
    type: 'http request',
    z: 'tab',
    method: 'use',
    ret: 'txt',
    url: '',
    wires: [[]],
  };
}

// node:test skips whenever a `skip` option is PRESENT, even when its value is
// falsy — and it still runs the body, reporting the result as SKIP. Passing
// `skip: null` would therefore run this whole test against the real cluster and
// hide whether it passed. The option is only included when there is a reason.
const skipReason = memorydbSkipReason();
const options = { timeout: 120000, ...(skipReason ? { skip: skipReason } : {}) };

test(
  'the full publish/subscribe chain works against a real MemoryDB cluster over TLS with ACL auth',
  options,
  async (t) => {
    const config = memorydbConfigFromEnv();
    // Fail early and clearly on an unreachable cluster or bad credentials,
    // rather than waiting for daprd to time out with the reason buried in its
    // container logs.
    await assertMemorydbReachable(config);

    const appId = 'it-memorydb-pubsub';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    // Unique per run: the cluster is shared and long-lived, so a fixed topic
    // would inherit an earlier run's stream and consumer group.
    const topic = uniqueTopic('orders');
    const rawTopic = uniqueTopic('raw');

    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-memorydb-pubsub' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        // An HTTP entry point into a real dapr-publish node, so the test drives
        // this package's own publish path rather than calling daprd directly.
        {
          id: 'in',
          type: 'http in',
          z: 'tab',
          url: '/publish',
          method: 'post',
          wires: [['pub']],
        },
        {
          id: 'pub',
          type: 'dapr-publish',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic,
          contentType: '',
          metadata: '{}',
          wires: [['ok']],
        },
        {
          id: 'ok',
          type: 'function',
          z: 'tab',
          func: 'msg.payload = { published: true }; return msg;',
          outputs: 1,
          wires: [['res']],
        },
        { id: 'res', type: 'http response', z: 'tab', statusCode: '', headers: {} },
        forwardingSub('sub-orders', topic),
        forwardingFn('sub-orders', capture.url),
        forwardingReq('sub-orders'),
        forwardingSub('sub-raw', rawTopic, { rawPayload: true }),
        forwardingFn('sub-raw', capture.url),
        forwardingReq('sub-raw'),
      ],
    });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [
        { filename: 'secretstore-env.yaml', yaml: envSecretStoreYaml() },
        { filename: 'pubsub-memorydb.yaml', yaml: memorydbPubsubYaml({ config }) },
      ],
      // The password reaches daprd's process environment, where its own env
      // secret store resolves the component's secretKeyRef. It is never written
      // into the world-readable /components mount.
      env: { [PASSWORD_ENV]: config.password },
    });
    t.after(() => daprd.stop());
    // Registered AFTER daprd's own teardown, because node:test runs after hooks
    // in registration order: a live Dapr subscriber recreates a deleted stream on
    // its next poll, so this has to be the last thing that happens.
    t.after(() => deleteTopics(config, [topic, rawTopic]));

    // Publish through the dapr-publish node: Node-RED -> daprd -> MemoryDB.
    const published = await waitFor(async () => {
      const r = await httpRequest(nr.nodeUrl('/publish'), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ orderId: 'ORD-1', via: 'memorydb' }),
        timeoutMs: 8000,
      });
      return r.status === 200 ? r : null;
    });
    assert.deepEqual(JSON.parse(published.text), { published: true });

    // ...and back out: MemoryDB -> daprd -> dapr-subscribe -> flow.
    const delivered = await waitFor(
      () => capture.received.find((r) => r && r.topic === topic) || null,
      { timeoutMs: 30000 }
    );
    assert.equal(delivered.isBuffer, false);
    assert.deepEqual(delivered.payload, { orderId: 'ORD-1', via: 'memorydb' });
    for (const attr of ['specversion', 'id', 'source', 'type', 'pubsubname', 'topic']) {
      assert.ok(
        delivered.cloudEvent && typeof delivered.cloudEvent[attr] === 'string',
        `CloudEvent envelope preserves "${attr}"`
      );
    }
    assert.equal(delivered.cloudEvent.topic, topic);

    // Raw payload against the same cluster: published straight through daprd
    // because both the publish and the subscription must be marked raw.
    await waitFor(async () => {
      const r = await httpRequest(
        `${daprd.baseUrl}/v1.0/publish/pubsub/${rawTopic}?metadata.rawPayload=true`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ hello: 'raw' }),
          timeoutMs: 8000,
        }
      );
      return r.status === 204 ? r : null;
    });
    const rawDelivered = await waitFor(
      () => capture.received.find((r) => r && r.topic === rawTopic) || null,
      { timeoutMs: 30000 }
    );
    assert.equal(rawDelivered.isBuffer, true);
    assert.equal(rawDelivered.payload, '{"hello":"raw"}');

    // Dapr keeps one Redis stream per topic, named after the topic — proof the
    // traffic really went through this cluster and not some other component.
    const streamType = await command(config, ['TYPE', topic]);
    assert.match(streamType, /stream/, 'Dapr should have created a Redis stream for the topic');
  }
);
