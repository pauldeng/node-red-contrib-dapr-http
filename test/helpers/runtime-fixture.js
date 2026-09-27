'use strict';

const { after } = require('node:test');
const { NodeRed } = require('./node-red');

// For serial input/output tables only. Each case deploys fresh flows, and
// removes them before its fake sidecar closes. Tests of process startup,
// shutdown, credentials or global telemetry keep their own NodeRed instance.
function createRuntimeFixture() {
  const nr = new NodeRed();
  let started;
  after(() => nr.stop());
  return async (t, { fresh = false } = {}) => {
    if (fresh) {
      const dedicated = new NodeRed();
      await dedicated.start();
      t.after(() => dedicated.stop());
      return dedicated;
    }
    started ??= nr.start();
    await started;
    t.after(() => nr.deploy([]));
    return nr;
  };
}

module.exports = { createRuntimeFixture };
