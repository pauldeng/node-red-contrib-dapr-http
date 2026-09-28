'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { NodeRed } = require('../helpers/node-red');

test('log readiness can ignore a previous deployment without discarding diagnostics', async () => {
  const nr = new NodeRed();
  nr._appendLog('old: Started flows\n');
  const started = nr.waitForLog('Started flows', { after: nr.logText().length });
  queueMicrotask(() => nr._appendLog('new: Started flows\n'));
  assert.equal(await started, 'old: Started flows\nnew: Started flows\n');
});
