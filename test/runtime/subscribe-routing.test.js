'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { startCapture } = require('../helpers/capture');
const { httpRequest } = require('../helpers/http');
const { waitForFast: waitFor } = require('../helpers/wait-for');
const { setTimeout: delay } = require('node:timers/promises');

const healthPath = '/v1.0/healthz/outbound';

const connectionNode = (appPort, daprPort, extra = {}) => ({
  id: 'c1',
  type: 'dapr-connection',
  daprHost: '127.0.0.1',
  daprPort: String(daprPort),
  bindAddress: '127.0.0.1',
  appPort: String(appPort),
  ...extra,
});

function forwardFn(id, captureUrl) {
  return {
    id,
    type: 'function',
    z: 'tab',
    func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.captured = { payload: msg.payload, route: msg.dapr.route, ruleId: msg.dapr.ruleId, entryId: msg.dapr.entryId, batchId: msg.dapr.batchId, metadata: msg.dapr.metadata };
msg.payload = JSON.stringify(msg.captured);
return msg;`,
    outputs: 1,
    wires: [['req_' + id]],
  };
}

function httpReqNode(id, wires = [[]]) {
  return {
    id: 'req_' + id,
    type: 'http request',
    z: 'tab',
    method: 'use',
    ret: 'txt',
    url: '',
    wires,
  };
}

const cloudEvent = (id, type, data) => ({
  specversion: '1.0',
  id,
  source: 'test',
  type,
  data,
});

test(
  'CEL routing: every delivery emits from the single output, with msg.dapr.ruleId identifying the match',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const capture = await startCapture();
    t.after(() => capture.stop());

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'cel' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        rules: [
          { id: 'ruleA', match: 'event.type == "a"' },
          { id: 'ruleB', match: 'event.type == "b"' },
        ],
        wires: [['fwd']],
      },
      forwardFn('fwd', capture.url),
      httpReqNode('fwd'),
    ]);

    const routes = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].routes ? subs[0].routes : null;
    });
    assert.equal(routes.rules.length, 2);
    assert.equal(routes.default, '/node-red-dapr/subscriptions/sub1');

    const deliver = (path, event) =>
      httpRequest(`http://127.0.0.1:${appPort}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/cloudevents+json' },
        body: JSON.stringify(event),
        timeoutMs: 4000,
      });

    const rA = await deliver(routes.rules[0].path, cloudEvent('e-a', 'a', { v: 1 }));
    assert.deepEqual(JSON.parse(rA.text), { status: 'SUCCESS' });
    const rB = await deliver(routes.rules[1].path, cloudEvent('e-b', 'b', { v: 2 }));
    assert.deepEqual(JSON.parse(rB.text), { status: 'SUCCESS' });
    const rDefault = await deliver(routes.default, cloudEvent('e-c', 'c', { v: 3 }));
    assert.deepEqual(JSON.parse(rDefault.text), { status: 'SUCCESS' });

    await capture.waitForCount(3);
    assert.equal(capture.received.length, 3, 'every delivery emits from the one output');
    const byPayload = (v) => capture.received.find((r) => r.payload && r.payload.v === v);
    assert.equal(byPayload(1).ruleId, 'ruleA');
    assert.equal(byPayload(2).ruleId, 'ruleB');
    assert.equal(byPayload(3).ruleId, null, 'unmatched delivery has a null ruleId');
  }
);

test(
  'CEL routing: an unchanged rule set survives a full redeploy without a restart warning',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    const flow = [
      { id: 'tab', type: 'tab', label: 'cel-stable' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        rules: [{ id: 'ruleA', match: 'event.type == "a"' }],
        wires: [[]],
      },
    ];
    const advertised = async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return r.status === 200 && r.text.includes('/ruleA') ? r : null;
    };
    await nr.deploy(flow);
    await waitFor(advertised);
    await nr.deploy(flow); // identical config
    await delay(300);
    assert.equal(
      /restart the Dapr sidecar/i.test(nr.logText()),
      false,
      'an unchanged rule set must not request a restart'
    );
  }
);

test(
  'bulk subscribe (auto ack): every entry is delivered and acknowledged independently',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bulk-auto' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        bulkEnabled: true,
        wires: [['fwd']],
      },
      forwardFn('fwd', capture.url),
      httpReqNode('fwd'),
    ]);

    const deliveryPath = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].bulkSubscribe && subs[0].bulkSubscribe.enabled
        ? subs[0].route
        : null;
    });

    const bulkBody = {
      id: 'batch-77',
      metadata: { partitionKey: 'batch-default', consumerGroup: 'g1' },
      entries: [
        {
          entryId: 'e1',
          event: cloudEvent('e1', 'order', { n: 1 }),
          contentType: 'application/json',
          metadata: { partitionKey: 'e1-specific' }, // entry-level overrides the batch default
        },
        {
          entryId: 'e2',
          event: cloudEvent('e2', 'order', { n: 2 }),
          contentType: 'application/json',
        },
      ],
    };
    const res = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(bulkBody),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    const body = JSON.parse(res.text);
    assert.deepEqual(
      body.statuses.sort((a, b) => a.entryId.localeCompare(b.entryId)),
      [
        { entryId: 'e1', status: 'SUCCESS' },
        { entryId: 'e2', status: 'SUCCESS' },
      ]
    );

    await capture.waitForCount(2);
    const entryIds = capture.received.map((r) => r.entryId).sort();
    assert.deepEqual(entryIds, ['e1', 'e2']);
    const e1 = capture.received.find((r) => r.entryId === 'e1');
    const e2 = capture.received.find((r) => r.entryId === 'e2');
    assert.equal(e1.batchId, 'batch-77', "the envelope's own id is surfaced as batchId");
    assert.deepEqual(
      e1.metadata,
      { partitionKey: 'e1-specific', consumerGroup: 'g1' },
      'entry metadata overrides the batch-level default'
    );
    assert.deepEqual(
      e2.metadata,
      { partitionKey: 'batch-default', consumerGroup: 'g1' },
      'an entry with no metadata of its own falls back to the batch-level metadata'
    );
  }
);

test(
  'bulk subscribe (manual ack): entries resolve independently as a mix of statuses',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    // Router: SUCCESS for e1, DROP for e2 explicitly, e3 gets no ack at all
    // (resolves as RETRY via the connection's short request timeout).
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bulk-manual' },
      connectionNode(appPort, dapr.port, { requestTimeoutSec: '1' }),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'manual',
        metadata: '{}',
        bulkEnabled: true,
        wires: [['router']],
      },
      {
        id: 'router',
        type: 'function',
        z: 'tab',
        func: `if (msg.dapr.entryId === 'e3') { return null; }
msg.dapr.status = msg.dapr.entryId === 'e2' ? 'DROP' : 'SUCCESS';
return msg;`,
        outputs: 1,
        wires: [['ack']],
      },
      { id: 'ack', type: 'dapr-ack', z: 'tab', connection: 'c1', status: 'SUCCESS', wires: [[]] },
    ]);

    const deliveryPath = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].bulkSubscribe ? subs[0].route : null;
    });

    const bulkBody = {
      entries: [
        { entryId: 'e1', event: cloudEvent('e1', 'order', { n: 1 }) },
        { entryId: 'e2', event: cloudEvent('e2', 'order', { n: 2 }) },
        { entryId: 'e3', event: cloudEvent('e3', 'order', { n: 3 }) },
      ],
    };
    const res = await waitFor(
      async () => {
        const r = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(bulkBody),
          timeoutMs: 8000,
        });
        return r.status === 200 ? r : null;
      },
      { timeoutMs: 15000 }
    );
    const body = JSON.parse(res.text);
    assert.deepEqual(
      body.statuses.sort((a, b) => a.entryId.localeCompare(b.entryId)),
      [
        { entryId: 'e1', status: 'SUCCESS' },
        { entryId: 'e2', status: 'DROP' },
        { entryId: 'e3', status: 'RETRY' },
      ]
    );
  }
);

test(
  'bulk subscribe: a malformed entry is DROPped without affecting the rest of the batch',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bulk-bad' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        bulkEnabled: true,
        wires: [['fwd']],
      },
      forwardFn('fwd', capture.url),
      httpReqNode('fwd'),
    ]);

    const deliveryPath = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].bulkSubscribe ? subs[0].route : null;
    });

    const bulkBody = {
      entries: [
        { entryId: 'good', event: cloudEvent('good', 'order', { n: 1 }) },
        { entryId: 'bad', event: { not: 'a cloudevent' } },
      ],
    };
    const res = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(bulkBody),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    const body = JSON.parse(res.text);
    assert.deepEqual(
      body.statuses.sort((a, b) => a.entryId.localeCompare(b.entryId)),
      [
        { entryId: 'bad', status: 'DROP' },
        { entryId: 'good', status: 'SUCCESS' },
      ]
    );
    await capture.waitForCount(1);
    assert.equal(capture.received.length, 1, 'only the good entry reaches the flow');
    assert.equal(capture.received[0].entryId, 'good');
  }
);

test(
  'discoveryEntry combines CEL routes with a dead-letter topic',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'dlq' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        deadLetterTopic: 'orders-dlq',
        rules: [{ id: 'ruleA', match: 'event.type == "a"' }],
        wires: [[]],
      },
    ]);

    const sub = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].routes ? subs[0] : null;
    });
    assert.equal(sub.deadLetterTopic, 'orders-dlq');
    assert.equal(sub.routes.rules.length, 1);
  }
);

test(
  'bulk subscribe composes with CEL routing: a batch delivered to a rule route carries that ruleId',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bulk-cel' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        bulkEnabled: true,
        rules: [{ id: 'ruleA', match: 'event.type == "a"' }],
        wires: [['fwd']],
      },
      forwardFn('fwd', capture.url),
      httpReqNode('fwd'),
    ]);

    const routes = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].routes && subs[0].bulkSubscribe ? subs[0] : null;
    });
    assert.ok(routes.bulkSubscribe.enabled);
    const rulePath = routes.routes.rules[0].path;

    const bulkBody = {
      entries: [
        { entryId: 'e1', event: cloudEvent('e1', 'a', { v: 1 }), contentType: 'application/json' },
      ],
    };
    const res = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}${rulePath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(bulkBody),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    assert.deepEqual(JSON.parse(res.text), { statuses: [{ entryId: 'e1', status: 'SUCCESS' }] });

    const got = await capture.waitForMessage(() => true);
    assert.deepEqual(got.payload, { v: 1 });
    assert.equal(got.ruleId, 'ruleA');
  }
);

test(
  'bulk subscribe on a rawPayload topic decodes each entry as a base64 string, not a CloudEvent',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'bulk-raw' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        rawPayload: true,
        bulkEnabled: true,
        wires: [['fwd']],
      },
      {
        id: 'fwd',
        type: 'function',
        z: 'tab',
        func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({
  isBuffer: Buffer.isBuffer(msg.payload),
  base64: Buffer.isBuffer(msg.payload) ? msg.payload.toString('base64') : null,
  contentType: msg.dapr.contentType,
  partitionKey: msg.dapr.metadata.partitionKey,
});
return msg;`,
        outputs: 1,
        wires: [['req_fwd']],
      },
      httpReqNode('fwd'),
    ]);

    const deliveryPath = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].bulkSubscribe ? subs[0].route : null;
    });

    const bytes = Buffer.from([9, 8, 7, 255]);
    const bulkBody = {
      entries: [
        {
          entryId: 'e1',
          event: bytes.toString('base64'),
          contentType: 'application/octet-stream',
          metadata: { partitionKey: '5' },
        },
      ],
    };
    const res = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(bulkBody),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    assert.deepEqual(JSON.parse(res.text), { statuses: [{ entryId: 'e1', status: 'SUCCESS' }] });

    const got = await capture.waitForMessage(() => true);
    assert.equal(got.isBuffer, true);
    assert.equal(got.base64, bytes.toString('base64'));
    assert.equal(got.contentType, 'application/octet-stream');
    assert.equal(got.partitionKey, '5');
  }
);

test(
  'a caller disconnecting before a manual ack frees the pending correlation at once',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    // The ack node is delayed 600ms; the connection timeout is the default 30s.
    // Without the abort-free the correlation would still be pending when the
    // delayed ack node fires. With the fix it is freed on disconnect, so the
    // delayed ack finds nothing and logs "no pending delivery".
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'disc' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'manual',
        metadata: '{}',
        wires: [['dly']],
      },
      {
        id: 'dly',
        type: 'delay',
        z: 'tab',
        pauseType: 'delay',
        timeout: '600',
        timeoutUnits: 'milliseconds',
        wires: [['ack']],
      },
      { id: 'ack', type: 'dapr-ack', z: 'tab', connection: 'c1', status: 'SUCCESS', wires: [[]] },
    ]);

    const deliveryPath = '/node-red-dapr/subscriptions/sub1';
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return r.status === 200 && r.text.includes(deliveryPath) ? r : null;
    });

    const event = cloudEvent('e1', 'order', { a: 1 });
    // Drop the connection well before the 600ms delayed ack fires.
    await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
      method: 'POST',
      headers: { 'content-type': 'application/cloudevents+json' },
      body: JSON.stringify(event),
      timeoutMs: 150,
    }).catch(() => {});

    await nr.waitForLog('no pending delivery', {
      timeoutMs: 5000,
    });
  }
);

test(
  'inbound delivery headers exclude the full hop-by-hop set, not just an ad-hoc list',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'hdrs' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        wires: [['fwd']],
      },
      {
        id: 'fwd',
        type: 'function',
        z: 'tab',
        func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ keys: Object.keys(msg.dapr.metadata).sort() });
return msg;`,
        outputs: 1,
        wires: [['req_fwd']],
      },
      httpReqNode('fwd'),
    ]);

    const deliveryPath = '/node-red-dapr/subscriptions/sub1';
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return r.status === 200 && r.text.includes(deliveryPath) ? r : null;
    });

    const res = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/cloudevents+json',
          'proxy-authorization': 'Basic SECRET',
          te: 'trailers',
          trailer: 'x',
          upgrade: 'websocket',
          'x-broker-partition': '9',
        },
        body: JSON.stringify(cloudEvent('e1', 'order', { a: 1 })),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    assert.equal(res.status, 200);
    const got = await capture.waitForMessage(() => true);
    assert.ok(got.keys.includes('x-broker-partition'));
    for (const banned of ['proxy-authorization', 'te', 'trailer', 'upgrade']) {
      assert.equal(got.keys.includes(banned), false, `${banned} must not be exposed`);
    }
  }
);

test(
  'removing a CEL rule (without restarting the sidecar) keeps its old path retryable, not a 404 — and the default path and its own delivery never move',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
    const capture = await startCapture();
    t.after(() => capture.stop());
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    const flowWithRule = [
      { id: 'tab', type: 'tab', label: 'rule-removal' },
      connectionNode(appPort, dapr.port),
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        rules: [{ id: 'ruleA', match: 'event.type == "a"' }],
        wires: [['fwd']],
      },
      forwardFn('fwd', capture.url),
      httpReqNode('fwd'),
    ];
    await nr.deploy(flowWithRule);

    const routes = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      if (r.status !== 200) return null;
      const subs = JSON.parse(r.text);
      return subs[0] && subs[0].routes ? subs[0].routes : null;
    });
    const rulePath = routes.rules[0].path;
    const defaultPath = routes.default;

    const deliver = (path, event) =>
      httpRequest(`http://127.0.0.1:${appPort}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/cloudevents+json' },
        body: JSON.stringify(event),
        timeoutMs: 4000,
      });

    // Establish daprd's served fingerprint (one discovery fetch) and confirm
    // the rule delivers before it is removed.
    assert.deepEqual(JSON.parse((await deliver(rulePath, cloudEvent('e1', 'a', { v: 1 }))).text), {
      status: 'SUCCESS',
    });

    // Redeploy the SAME subscription (still active, same topic) with the rule
    // removed — a rules-only edit, not a whole-subscription removal. Crucially,
    // this test must NOT re-fetch /dapr/subscribe after this point: a real,
    // not-yet-restarted daprd never does, and fetching it ourselves would
    // simulate the very discovery fetch this scenario is testing the absence
    // of (which correctly clears the stale-path tracking on a real fetch).
    const flowRuleRemoved = [
      flowWithRule[0],
      flowWithRule[1],
      { ...flowWithRule[2], rules: [] },
      flowWithRule[3],
      flowWithRule[4],
    ];
    await nr.deploy(flowRuleRemoved);

    // The still-active subscription keeps delivering on its default path —
    // using the SAME string fetched before the redeploy, confirming it never
    // moved. Poll via delivery (not /dapr/subscribe) until the redeploy settles.
    const defaultRes = await waitFor(async () => {
      const r = await deliver(defaultPath, cloudEvent('e2', 'anything', { v: 2 }));
      return r.status === 200 ? r : null;
    });
    assert.deepEqual(JSON.parse(defaultRes.text), { status: 'SUCCESS' });

    // The removed rule's OLD path must stay retryable — never a bare 404,
    // which Dapr treats as a permanent DROP rather than a retry — until the
    // operator actually restarts daprd (never simulated in this test, so it
    // must still be retryable now).
    const staleRes = await deliver(rulePath, cloudEvent('e3', 'a', { v: 3 }));
    assert.equal(staleRes.status, 503, "a removed rule's path must be retryable, not 404");

    await nr.waitForLog(/restart the Dapr sidecar/i);
  }
);
