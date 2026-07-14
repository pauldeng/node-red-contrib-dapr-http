'use strict';

const base = require('@playwright/test');

const { NodeRed, freePort } = require('../../helpers/node-red');

// Every e2e spec needs its own real Node-RED process (the same black-box
// harness the runtime tier uses) — a config-node dialog test doesn't need a
// real daprd behind it, only the editor's own validation and rendering, so
// no fake-dapr sidecar is started here.
const test = base.test.extend({
  nr: async ({}, use) => {
    const nr = new NodeRed();
    await nr.start({});
    await use(nr);
    await nr.stop();
  },
  appPort: async ({}, use) => {
    await use(await freePort());
  },
  daprPort: async ({}, use) => {
    await use(await freePort());
  },
});

module.exports = { test, expect: base.expect };
