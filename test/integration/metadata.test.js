'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd, secretComponentYaml } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

function flow({ appPort, daprHttpPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-metadata' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
  ];
}

test(
  'Test Connection returns only curated metadata from real daprd 1.18.2',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const nr = new ContainerNodeRed();
    await nr.start({ flows: flow({ appPort, daprHttpPort }) });
    t.after(() => nr.stop());

    await waitFor(async () => {
      const response = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, {
        timeoutMs: 1000,
      });
      return response.status === 204 ? true : null;
    });

    const daprd = await startDaprd({
      appId: 'it-metadata-app',
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'secretstore.yaml', yaml: secretComponentYaml() }],
      extraFixtures: ['secrets.json'],
    });
    t.after(() => daprd.stop());
    await waitFor(() => (nr.logText().includes('Dapr sidecar is available') ? true : null));

    const response = await httpRequest(nr.adminUrl('/dapr-connection/c1/metadata'));
    assert.equal(response.status, 200);
    assert.equal(response.headers['cache-control'], 'no-store');
    assert.deepEqual(JSON.parse(response.text), {
      ok: true,
      id: 'it-metadata-app',
      runtimeVersion: '1.18.2',
      components: [{ name: 'secretstore', type: 'secretstores.local.file' }],
      componentCount: 1,
      subscriptionCount: 0,
    });
  }
);
