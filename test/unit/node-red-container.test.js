'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const { ContainerNodeRed } = require('../helpers/node-red-container');

test('container readiness discovers the bound port and waits for flows to start', async () => {
  const nr = new ContainerNodeRed();
  nr._appendLog('1 Sep 10:00:00 - [info] Server now running at http://127.0.0.1:45678/\n');
  queueMicrotask(() => nr._appendLog('1 Sep 10:00:00 - [info] Started flows\n'));

  await nr._waitReady(1000);
  assert.equal(nr.port, 45678);
});

test('stop treats container-owned temp-dir cleanup as best effort', async (t) => {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'nrdapr-unremovable-'));
  const nested = path.join(dir, 'nested');
  t.after(async () => {
    await fsp.chmod(nested, 0o755).catch(() => {});
    await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
  });

  await fsp.mkdir(nested);
  await fsp.writeFile(path.join(nested, 'state.json'), '{}');
  await fsp.chmod(nested, 0o555);

  const nr = new ContainerNodeRed();
  nr.userDir = dir;

  await assert.doesNotReject(() => nr.stop());
  assert.equal(nr.userDir, null);
});
