'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

test(
  'stopping Node-RED closes the app-channel listener while daprd itself, and its outbound healthz, stay up',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-shutdown-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-shutdown' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
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

    // Baseline: both the app and the sidecar's own outbound health are up.
    assert.equal(
      (await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 })).status,
      204
    );
    assert.equal(
      (await httpRequest(`${daprd.baseUrl}/v1.0/healthz/outbound`, { timeoutMs: 1000 })).status,
      204
    );

    // Stopping Node-RED closes only the app-channel listener. It must not
    // take daprd down with it — the sidecar is a separate process/container,
    // and /v1.0/healthz/outbound deliberately excludes the app channel (see
    // AGENTS.md: "the right probe" for this package's own fail-fast health
    // check, precisely because it must stay meaningful even when the app is
    // the thing that's down).
    await nr.stop();

    await assert.rejects(httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 }));
    assert.equal(
      (await httpRequest(`${daprd.baseUrl}/v1.0/healthz/outbound`, { timeoutMs: 1000 })).status,
      204
    );
  }
);
