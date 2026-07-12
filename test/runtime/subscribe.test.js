'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');

const healthPath = '/v1.0/healthz/outbound';
const deliveryPath = '/node-red-dapr/subscriptions/sub1';

async function waitFor(fn, { timeoutMs = 10000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) {
        return value;
      }
      last = value;
    } catch (err) {
      last = err;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`waitFor timed out (${last instanceof Error ? last.message : last})`);
}

// A server that records the messages the subscribe node forwards into the flow.
async function startCapture() {
  const received = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try {
        received.push(JSON.parse(Buffer.concat(chunks).toString()));
      } catch {
        received.push(null);
      }
      res.writeHead(200).end('ok');
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    received,
    url: `http://127.0.0.1:${server.address().port}/capture`,
    stop: () => new Promise((resolve) => server.close(resolve)),
  };
}

// subscribe -> function(build capture body, keep msg.dapr) -> http request(capture)
// -> [manual only] dapr-ack.
function subscribeFlow({ appPort, daprPort, captureUrl, ackMode, requestTimeoutSec, withAck }) {
  const forward = {
    id: 'fwd',
    type: 'function',
    z: 'tab',
    func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.captured = { payload: msg.payload, hasCloudEvent: !!(msg.dapr && msg.dapr.cloudEvent), topic: msg.dapr && msg.dapr.topic };
msg.payload = JSON.stringify(msg.captured);
return msg;`,
    outputs: 1,
    wires: [['req']],
  };
  const request = {
    id: 'req',
    type: 'http request',
    z: 'tab',
    method: 'use',
    ret: 'txt',
    url: '',
    wires: [withAck ? ['ack'] : []],
  };
  const flow = [
    { id: 'tab', type: 'tab', label: 'subscribe' },
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
      requestTimeoutSec: requestTimeoutSec ? String(requestTimeoutSec) : '',
    },
    {
      id: 'sub1',
      type: 'dapr-subscribe',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic: 'orders',
      ackMode,
      rawPayload: false,
      metadata: '{}',
      wires: [['fwd']],
    },
    forward,
    request,
  ];
  if (withAck) {
    flow.push({
      id: 'ack',
      type: 'dapr-ack',
      z: 'tab',
      connection: 'c1',
      status: 'SUCCESS',
      wires: [[]],
    });
  }
  return flow;
}

const cloudEvent = {
  specversion: '1.0',
  id: 'e1',
  source: 'test',
  type: 'order',
  data: { orderId: 7 },
};

function deliver(appPort) {
  return httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
    method: 'POST',
    headers: { 'content-type': 'application/cloudevents+json' },
    body: JSON.stringify(cloudEvent),
    timeoutMs: 5000,
  });
}

async function bootstrap(t, flowOpts) {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
  const capture = await startCapture();
  t.after(() => capture.stop());
  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());
  await nr.deploy(
    subscribeFlow({ appPort, daprPort: dapr.port, captureUrl: capture.url, ...flowOpts })
  );
  // Wait until the subscription is advertised (activation completed).
  await waitFor(async () => {
    const res = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
      timeoutMs: 1000,
    });
    return res.status === 200 && res.text.includes(deliveryPath) ? res : null;
  });
  return { appPort, capture };
}

test(
  'auto-ack: delivery is acknowledged SUCCESS and the event reaches the flow',
  { timeout: 60000 },
  async (t) => {
    const { appPort, capture } = await bootstrap(t, { ackMode: 'auto', withAck: false });

    const res = await deliver(appPort);
    assert.equal(res.status, 200);
    assert.deepEqual(JSON.parse(res.text), { status: 'SUCCESS' });

    const captured = await waitFor(() => capture.received[0] || null);
    assert.deepEqual(captured, { payload: { orderId: 7 }, hasCloudEvent: true, topic: 'orders' });
  }
);

test(
  'discovery advertises the subscription with its stable route',
  { timeout: 60000 },
  async (t) => {
    const { appPort } = await bootstrap(t, { ackMode: 'auto', withAck: false });
    const res = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
      timeoutMs: 2000,
    });
    const subs = JSON.parse(res.text);
    assert.deepEqual(subs, [{ pubsubname: 'pubsub', topic: 'orders', route: deliveryPath }]);
  }
);

test(
  'manual ack: a dapr-ack node completes the delivery as SUCCESS',
  { timeout: 60000 },
  async (t) => {
    const { appPort, capture } = await bootstrap(t, { ackMode: 'manual', withAck: true });

    const res = await deliver(appPort);
    assert.equal(res.status, 200);
    assert.deepEqual(JSON.parse(res.text), { status: 'SUCCESS' });
    assert.ok(capture.received.length >= 1, 'the event reached the flow before acknowledgement');
  }
);

test('manual ack: a delivery with no ack times out as RETRY', { timeout: 60000 }, async (t) => {
  const { appPort } = await bootstrap(t, {
    ackMode: 'manual',
    withAck: false,
    requestTimeoutSec: 2,
  });

  const started = Date.now();
  const res = await deliver(appPort);
  assert.deepEqual(JSON.parse(res.text), { status: 'RETRY' });
  const elapsed = Date.now() - started;
  assert.ok(
    elapsed >= 700 && elapsed < 3500,
    `RETRY should arrive near the ack timeout, took ${elapsed}ms`
  );
});

test(
  'an unchanged subscription keeps delivering across a full redeploy',
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

    const flow = subscribeFlow({
      appPort,
      daprPort: dapr.port,
      captureUrl: capture.url,
      ackMode: 'auto',
      withAck: false,
    });
    const advertised = async () => {
      const res = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return res.status === 200 && res.text.includes(deliveryPath) ? res : null;
    };
    // After redeploy, confirm delivery only by delivering (retrying through the
    // brief reacquire-not-ready 503 window) — never by fetching /dapr/subscribe —
    // so this proves deliveries resume without another discovery request.
    const deliverUntilSuccess = () =>
      waitFor(async () => {
        const res = await deliver(appPort);
        return res.status === 200 && JSON.parse(res.text).status === 'SUCCESS' ? res : null;
      });

    await nr.deploy(flow);
    await waitFor(advertised); // one initial discovery fetch establishes the served fingerprint
    assert.deepEqual(JSON.parse((await deliver(appPort)).text), { status: 'SUCCESS' });

    // Redeploy the identical flow: the stable route and listener survive, and no
    // discovery re-fetch is needed — so an unchanged set must not warn to restart.
    await nr.deploy(flow);
    await deliverUntilSuccess();
    assert.equal(
      /restart the Dapr sidecar/i.test(nr.logText()),
      false,
      'an unchanged subscription set must not request a restart'
    );
  }
);

test(
  'a duplicate pubsub/topic subscription on one connection is rejected',
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
      { id: 'tab', type: 'tab', label: 'dup' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
      },
      {
        id: 'subA',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        wires: [[]],
      },
      {
        id: 'subB',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        metadata: '{}',
        wires: [[]],
      },
    ]);

    const res = await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return r.status === 200 ? r : null;
    });
    assert.deepEqual(
      JSON.parse(res.text).length,
      1,
      `exactly one subscription advertised: ${res.text}`
    );
    await waitFor(() => (/duplicate subscription/i.test(nr.logText()) ? true : null));
  }
);

test(
  'removing the last subscription drops its route (restart required)',
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

    await nr.deploy(
      subscribeFlow({
        appPort,
        daprPort: dapr.port,
        captureUrl: capture.url,
        ackMode: 'auto',
        withAck: false,
      })
    );
    // Establish a served fingerprint with one discovery fetch, then confirm the
    // route delivers.
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return r.status === 200 && r.text.includes(deliveryPath) ? r : null;
    });
    assert.deepEqual(JSON.parse((await deliver(appPort)).text), { status: 'SUCCESS' });

    // Redeploy with the subscribe node removed. After re-activation the route is
    // gone, so the previously-cached path now 404s, and — because daprd has a
    // stale served fingerprint — the connection warns (once) to restart.
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'empty' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
      },
    ]);
    await waitFor(async () => {
      const r = await deliver(appPort);
      return r.status === 404 ? r : null;
    });
    await waitFor(() => (/restart the Dapr sidecar/i.test(nr.logText()) ? true : null));
    const warnings = nr.logText().match(/restart the Dapr sidecar/gi) || [];
    assert.equal(warnings.length, 1, 'the restart warning must be rate-limited to one');
  }
);

test('a delivery pending at redeploy completes as RETRY', { timeout: 60000 }, async (t) => {
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  dapr.respond('GET', healthPath, (_req, res) => res.writeHead(204).end());
  const capture = await startCapture();
  t.after(() => capture.stop());
  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());

  const flow = subscribeFlow({
    appPort,
    daprPort: dapr.port,
    captureUrl: capture.url,
    ackMode: 'manual',
    withAck: false,
  });
  await nr.deploy(flow);
  await waitFor(async () => {
    const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, { timeoutMs: 1000 });
    return r.status === 200 && r.text.includes(deliveryPath) ? r : null;
  });

  const deliveryPromise = deliver(appPort); // manual + no ack → stays pending
  await new Promise((resolve) => setTimeout(resolve, 300));
  await nr.deploy(flow); // redeploy while the delivery is pending
  const res = await deliveryPromise;
  assert.deepEqual(JSON.parse(res.text), { status: 'RETRY' });
});

test('a second acknowledgement of the same delivery is rejected', { timeout: 60000 }, async (t) => {
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
    { id: 'tab', type: 'tab', label: 'double-ack' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(dapr.port),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    {
      id: 'sub1',
      type: 'dapr-subscribe',
      z: 'tab',
      connection: 'c1',
      pubsubName: 'pubsub',
      topic: 'orders',
      ackMode: 'manual',
      metadata: '{}',
      wires: [['ack1']],
    },
    {
      id: 'ack1',
      type: 'dapr-ack',
      z: 'tab',
      connection: 'c1',
      status: 'SUCCESS',
      wires: [['ack2']],
    },
    { id: 'ack2', type: 'dapr-ack', z: 'tab', connection: 'c1', status: 'SUCCESS', wires: [[]] },
    { id: 'errcatch', type: 'catch', z: 'tab', scope: ['ack2'], uncaught: false, wires: [['cap']] },
    {
      id: 'cap',
      type: 'function',
      z: 'tab',
      func: `msg.url = ${JSON.stringify(capture.url)}; msg.method='POST'; msg.headers={'content-type':'application/json'}; msg.payload = JSON.stringify({ error: msg.error && msg.error.message }); return msg;`,
      outputs: 1,
      wires: [['req']],
    },
    { id: 'req', type: 'http request', z: 'tab', method: 'use', ret: 'txt', url: '', wires: [[]] },
  ]);

  await waitFor(async () => {
    const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, { timeoutMs: 1000 });
    return r.status === 200 && r.text.includes(deliveryPath) ? r : null;
  });

  const res = await deliver(appPort);
  assert.deepEqual(
    JSON.parse(res.text),
    { status: 'SUCCESS' },
    'the first ack completes the delivery'
  );
  const err = await waitFor(() => capture.received.find((r) => r && r.error) || null);
  assert.match(err.error, /no pending delivery/i);
});

test(
  'raw delivery: Buffer payload, preserved envelope/metadata, token excluded',
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
      { id: 'tab', type: 'tab', label: 'raw' },
      {
        id: 'c1',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
      },
      {
        id: 'sub1',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'c1',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'auto',
        rawPayload: true,
        metadata: '{}',
        wires: [['cap']],
      },
      {
        id: 'cap',
        type: 'function',
        z: 'tab',
        func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({
  isBuffer: Buffer.isBuffer(msg.payload),
  payloadBase64: Buffer.isBuffer(msg.payload) ? msg.payload.toString('base64') : null,
  deliveryId: msg.dapr.deliveryId,
  cloudEventId: msg.dapr.cloudEvent && msg.dapr.cloudEvent.id,
  brokerMeta: msg.dapr.metadata['x-broker-partition'],
  hasToken: 'dapr-api-token' in msg.dapr.metadata,
});
return msg;`,
        outputs: 1,
        wires: [['req']],
      },
      {
        id: 'req',
        type: 'http request',
        z: 'tab',
        method: 'use',
        ret: 'txt',
        url: '',
        wires: [[]],
      },
    ]);

    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return r.status === 200 && r.text.includes(deliveryPath) ? r : null;
    });

    const bytes = Buffer.from([0, 1, 2, 255]);
    const rawEvent = {
      specversion: '1.0',
      id: 'raw-evt-1',
      source: 'broker',
      type: 'raw',
      data_base64: bytes.toString('base64'),
    };
    const res = await httpRequest(`http://127.0.0.1:${appPort}${deliveryPath}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/cloudevents+json',
        'x-broker-partition': '7',
        'dapr-api-token': 'sekret',
      },
      body: JSON.stringify(rawEvent),
      timeoutMs: 5000,
    });
    assert.deepEqual(JSON.parse(res.text), { status: 'SUCCESS' });

    const captured = await waitFor(() => (capture.received[0] ? capture.received[0] : null));
    assert.equal(captured.isBuffer, true, 'raw payload is delivered as a Buffer');
    assert.equal(captured.payloadBase64, bytes.toString('base64'), 'raw bytes decoded correctly');
    assert.equal(captured.cloudEventId, 'raw-evt-1', 'envelope preserved');
    assert.equal(captured.deliveryId, 'raw-evt-1', 'delivery id is the CloudEvent id');
    assert.equal(captured.brokerMeta, '7', 'custom broker metadata is exposed');
    assert.equal(captured.hasToken, false, 'the Dapr API token is excluded from metadata');
  }
);
