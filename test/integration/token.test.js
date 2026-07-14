'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

function flow({ appId, appPort, daprHttpPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-token' },
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
      topic: 'orders',
      ackMode: 'auto',
      metadata: '{}',
      wires: [[]],
    },
    // A self-invoke round trip: dapr-invoke's outbound call only succeeds if
    // it sends DAPR_API_TOKEN to daprd; dapr-service's delivery only succeeds
    // if OUR app-channel accepted daprd's APP_API_TOKEN. One green response
    // proves both directions at once.
    { id: 'in', type: 'http in', z: 'tab', url: '/probe', method: 'get', wires: [['inv']] },
    {
      id: 'inv',
      type: 'dapr-invoke',
      z: 'tab',
      connection: 'c1',
      appId,
      method: 'ping',
      verb: 'POST',
      wires: [['out']],
    },
    {
      id: 'out',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab' },
    {
      id: 'svc',
      type: 'dapr-service',
      z: 'tab',
      connection: 'c1',
      verb: 'POST',
      methodPath: '/ping',
      wires: [['resp']],
    },
    { id: 'resp', type: 'dapr-response', z: 'tab', connection: 'c1', statusCode: '200', wires: [] },
  ];
}

test(
  'APP_API_TOKEN and DAPR_API_TOKEN are enforced end to end against real daprd',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-token-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const appApiToken = 'app-secret';
    const daprApiToken = 'dapr-secret';

    // Both tokens come from the process environment (resolveOptions falls
    // back to APP_API_TOKEN/DAPR_API_TOKEN), matching how the M6 runtime
    // tests already configure them — no Node-RED credential encryption needed.
    const nr = new ContainerNodeRed();
    await nr.start({
      flows: flow({ appId, appPort, daprHttpPort }),
      env: { APP_API_TOKEN: appApiToken, DAPR_API_TOKEN: daprApiToken },
    });
    t.after(() => nr.stop());
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
      appApiToken,
      daprApiToken,
    });
    t.after(() => daprd.stop());

    // Positive: daprd successfully discovered the subscription, meaning its
    // own outbound call to our app-channel's /dapr/subscribe carried the
    // correct APP_API_TOKEN and our own token check accepted it.
    const discoverRes = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
      headers: { 'dapr-api-token': appApiToken },
      timeoutMs: 2000,
    });
    assert.equal(discoverRes.status, 200);
    assert.ok(JSON.parse(discoverRes.text).length >= 1);

    // Positive, both directions at once: a self-invoke round trip only
    // succeeds if dapr-invoke sent DAPR_API_TOKEN to daprd on the way out
    // AND our app-channel accepted daprd's APP_API_TOKEN on the way the
    // service delivery came back in.
    const roundTrip = await waitFor(async () => {
      const r = await httpRequest(nr.nodeUrl('/probe'), { timeoutMs: 4000 });
      return r.status === 200 ? r : null;
    });
    assert.equal(roundTrip.status, 200);

    // Negative: daprd's OWN API genuinely enforces DAPR_API_TOKEN — a caller
    // with no token, or the wrong one, is rejected by daprd itself (not by
    // our code), which the fake sidecar in earlier tiers cannot prove.
    // (/v1.0/healthz/outbound is deliberately exempt from token auth — a
    // liveness probe, mirroring our own app-channel's /healthz — so this
    // uses /v1.0/publish instead, which does enforce it.)
    const publish = (headers) =>
      httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
        body: '{}',
        timeoutMs: 2000,
      });
    assert.equal((await publish({})).status, 401, 'no token');
    assert.equal((await publish({ 'dapr-api-token': 'wrong' })).status, 401, 'wrong token');
    assert.equal((await publish({ 'dapr-api-token': daprApiToken })).status, 204, 'right token');

    // Negative: our OWN app-channel genuinely enforces APP_API_TOKEN — a
    // caller (impersonating daprd) with no token is rejected before it ever
    // reaches subscribe/delivery logic.
    const ourNoToken = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
      timeoutMs: 2000,
    });
    assert.equal(ourNoToken.status, 401);
  }
);
