'use strict';

// Reverifies three payload-handling behaviors that are easy to get wrong and
// that only real daprd can confirm: raw-payload delivery, a declared
// content-type that doesn't match the actual body, and publish-time metadata
// pass-through. See docs/testing.md for how each was empirically confirmed.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

function forwardingSub(id, topic, captureUrl, extra = {}) {
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
  metadata: msg.dapr.metadata,
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

test(
  'raw-payload delivery, a content-type/body mismatch, and publish metadata all behave correctly against real daprd',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-pubsub-payloads';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-pubsub-payloads' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        forwardingSub('sub-raw', 'raw-topic', capture.url, { rawPayload: true }),
        forwardingFn('sub-raw', capture.url),
        forwardingReq('sub-raw'),
        forwardingSub('sub-malformed', 'malformed-topic', capture.url),
        forwardingFn('sub-malformed', capture.url),
        forwardingReq('sub-malformed'),
        forwardingSub('sub-metadata', 'metadata-topic', capture.url),
        forwardingFn('sub-metadata', capture.url),
        forwardingReq('sub-metadata'),
        forwardingSub('sub-malformed-envelope', 'malformed-envelope-topic', capture.url),
        forwardingFn('sub-malformed-envelope', capture.url),
        forwardingReq('sub-malformed-envelope'),
      ],
    });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const redis = await startRedis();
    t.after(() => redis.stop());
    const daprd = await startDaprd({
      appId,
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
    });
    t.after(() => daprd.stop());

    const publish = (topic, body, { query = '', contentType = 'application/json' } = {}) =>
      waitFor(async () => {
        const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/${topic}${query}`, {
          method: 'POST',
          headers: { 'content-type': contentType },
          body,
          timeoutMs: 4000,
        });
        return r.status === 204 ? r : null;
      });

    // Raw payload only strips the CloudEvent envelope end to end if BOTH the
    // publish is marked raw (?metadata.rawPayload=true — otherwise the
    // broker message is already a full CloudEvent, and a raw SUBSCRIPTION
    // just hands that whole envelope over verbatim, which is a real, easy
    // trap) and the subscription itself is configured rawPayload: true.
    await publish('raw-topic', JSON.stringify({ hello: 'raw' }), {
      query: '?metadata.rawPayload=true',
    });
    const rawDelivered = await waitFor(
      () => capture.received.find((r) => r && r.topic === 'raw-topic') || null
    );
    assert.equal(rawDelivered.isBuffer, true);
    assert.equal(rawDelivered.payload, '{"hello":"raw"}');

    // daprd does not validate that a JSON-content-typed publish body is
    // actually valid JSON — it passes the raw text through as a plain
    // CloudEvent "data" string, and our own parseDelivery must expose that
    // as a plain string, not throw and not silently coerce it to a Buffer.
    await publish('malformed-topic', 'not-valid-json{{{');
    const malformedDelivered = await waitFor(
      () => capture.received.find((r) => r && r.topic === 'malformed-topic') || null
    );
    assert.equal(malformedDelivered.isBuffer, false);
    assert.equal(malformedDelivered.payload, 'not-valid-json{{{');

    // Publish-time metadata (ttlInSeconds) must not break ordinary delivery.
    await publish('metadata-topic', JSON.stringify({ ok: true }), {
      query: '?metadata.ttlInSeconds=60',
    });
    const metadataDelivered = await waitFor(
      () => capture.received.find((r) => r && r.topic === 'metadata-topic') || null
    );
    assert.equal(metadataDelivered.isBuffer, false);
    assert.deepEqual(metadataDelivered.payload, { ok: true });

    // The CloudEvent envelope itself must be preserved intact — required
    // attributes present, matching what was actually published — and
    // msg.dapr.metadata (the delivery's own request headers, e.g.
    // traceparent) must be a real, non-empty object, not dropped or stubbed.
    for (const attr of ['specversion', 'id', 'source', 'type', 'pubsubname', 'topic']) {
      assert.ok(
        metadataDelivered.cloudEvent && typeof metadataDelivered.cloudEvent[attr] === 'string',
        `CloudEvent envelope preserves "${attr}"`
      );
    }
    assert.equal(metadataDelivered.cloudEvent.topic, 'metadata-topic');
    assert.ok(
      metadataDelivered.metadata && Object.keys(metadataDelivered.metadata).length > 0,
      "delivery metadata (from daprd's own request headers) is a non-empty object"
    );

    // A malformed CloudEvent envelope — something real daprd would never
    // itself construct, but our own parser must still handle without
    // crashing — is DROPped without ever reaching the flow, and logged.
    const malformedRoute = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) {
        return null;
      }
      const subs = JSON.parse(r.text);
      const sub = subs.find((s) => s.topic === 'malformed-envelope-topic');
      return sub ? sub.route : null;
    });
    const malformedRes = await httpRequest(`http://127.0.0.1:${appPort}${malformedRoute}`, {
      method: 'POST',
      headers: { 'content-type': 'application/cloudevents+json' },
      body: JSON.stringify({ not: 'a valid CloudEvent' }),
      timeoutMs: 4000,
    });
    assert.equal(malformedRes.status, 200);
    assert.deepEqual(JSON.parse(malformedRes.text), { status: 'DROP' });
    await new Promise((resolve) => setTimeout(resolve, 1000));
    assert.equal(
      capture.received.some((r) => r && r.topic === 'malformed-envelope-topic'),
      false,
      'a malformed envelope never reaches the flow'
    );
    assert.match(nr.logText(), /malformed delivery on malformed-envelope-topic/);
  }
);
