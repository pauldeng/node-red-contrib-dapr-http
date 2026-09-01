'use strict';

const path = require('node:path');
const fsp = require('node:fs/promises');
const { setTimeout: delay } = require('node:timers/promises');

// A minimal Node-RED storage module used only to reproduce the startup-load
// race in the black-box harness. The first getFlows() snapshots the file before
// pausing, just like a slow filesystem read that was already in flight when an
// Admin API deployment arrived.
function delayedStorage(delayMs) {
  let flowsPath;
  let delayFirstLoad = true;

  return {
    async init(settings) {
      flowsPath = path.join(settings.userDir, settings.flowFile);
    },

    async getFlows() {
      const snapshot = JSON.parse(await fsp.readFile(flowsPath, 'utf8'));
      if (delayFirstLoad) {
        delayFirstLoad = false;
        await delay(delayMs);
      }
      return snapshot;
    },

    async saveFlows(flows) {
      await fsp.writeFile(flowsPath, JSON.stringify(flows));
    },

    async getCredentials() {
      return {};
    },

    async saveCredentials() {},
  };
}

module.exports = { delayedStorage };
