'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

// A minimal stand-in "caller" app: just enough for its own daprd sidecar to
// consider it ready (a liveness probe and an empty subscription list). It
// never receives anything — it only exists so a distinct app-id can make
// outbound invoke calls into the app under test.
async function startStubApp() {
  const server = http.createServer((req, res) => {
    if (req.url === '/healthz') {
      return res.writeHead(204).end();
    }
    if (req.url === '/dapr/subscribe') {
      return res.writeHead(200, { 'content-type': 'application/json' }).end('[]');
    }
    res.writeHead(200).end('{}');
  });
  const port = await freePort();
  await new Promise((resolve) => server.listen(port, '127.0.0.1', resolve));
  return { port, stop: () => new Promise((resolve) => server.close(resolve)) };
}

// Without mTLS, daprd cannot read a SPIFFE ID off a client cert to learn the
// real caller identity, so every request is evaluated as caller id "" —
// the per-appId allow rule for "allowed-caller" in config-acl.yaml is never
// even consulted, and every caller gets the same defaultAction (deny) no
// matter what app-id it claims. This suite proves that current, documented
// gap (see AGENTS.md's security section) rather than a working allow/deny
// split, which would require standing up Sentry + mTLS — out of scope for
// now. Confirmed via daprd debug logs: "Error while reading spiffe id from
// client cert. applying default global policy action".
test(
  "without mTLS, Dapr's access-control policy denies every caller identically, allow-listed or not",
  { timeout: 60000 },
  async (t) => {
    const targetAppId = 'target-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    // The app under test: dapr-service exposes POST /orders. The ACL policy
    // (test/integration/fixtures/config-acl.yaml) is configured on THIS
    // sidecar — it is the callee that enforces who may invoke it.
    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-acl' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        {
          id: 'svc',
          type: 'dapr-service',
          z: 'tab',
          connection: 'c1',
          verb: 'POST',
          methodPath: '/orders',
          wires: [['resp']],
        },
        {
          id: 'resp',
          type: 'dapr-response',
          z: 'tab',
          connection: 'c1',
          statusCode: '200',
          wires: [],
        },
      ],
    });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const redis = await startRedis();
    t.after(() => redis.stop());
    const targetDaprd = await startDaprd({
      appId: targetAppId,
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
      configFixture: 'config-acl.yaml',
    });
    t.after(() => targetDaprd.stop());

    // "allowed-caller" matches config-acl.yaml's explicit allow rule for
    // POST /orders; "disallowed-caller" matches nothing. Both sidecars run
    // with mTLS off (the harness default), so neither call actually carries
    // a verifiable identity daprd can match against the policy.
    const allowedStub = await startStubApp();
    t.after(() => allowedStub.stop());
    const allowedDaprd = await startDaprd({
      appId: 'allowed-caller',
      appPort: allowedStub.port,
      redisPort: redis.port,
    });
    t.after(() => allowedDaprd.stop());

    const disallowedStub = await startStubApp();
    t.after(() => disallowedStub.stop());
    const disallowedDaprd = await startDaprd({
      appId: 'disallowed-caller',
      appPort: disallowedStub.port,
      redisPort: redis.port,
    });
    t.after(() => disallowedDaprd.stop());

    const invoke = (daprd) =>
      waitFor(async () => {
        const r = await httpRequest(`${daprd.baseUrl}/v1.0/invoke/${targetAppId}/method/orders`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: '{}',
          timeoutMs: 4000,
        });
        return r.status !== undefined ? r : null;
      });

    const allowedRes = await invoke(allowedDaprd);
    const disallowedRes = await invoke(disallowedDaprd);
    assert.equal(
      allowedRes.status,
      403,
      'the nominally allow-listed caller is denied too — its identity was never verified'
    );
    assert.equal(
      disallowedRes.status,
      403,
      'the nominally disallowed caller is denied, as expected'
    );
  }
);
