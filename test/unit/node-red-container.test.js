'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const { ContainerNodeRed } = require('../helpers/node-red-container');

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
