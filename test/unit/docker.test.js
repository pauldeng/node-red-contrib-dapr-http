'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { inspect } = require('node:util');
const { execFileP } = require('../helpers/docker');

test('command failures do not copy credential-bearing arguments into diagnostics', async () => {
  const secret = 'fake-password-for-regression-only';
  await assert.rejects(
    execFileP(process.execPath, ['-e', 'process.exit(1)', '--', `MEMORYDB_PASSWORD=${secret}`]),
    (err) => {
      assert.doesNotMatch(inspect(err), new RegExp(secret));
      assert.match(err.message, /failed/);
      return true;
    }
  );
});

test('command environment overrides reach the child without changing the parent', async () => {
  const key = 'NR_DAPR_SECURITY_TEST_VALUE';
  const before = process.env[key];
  const result = await execFileP(
    process.execPath,
    ['-e', `process.stdout.write(process.env.${key})`],
    {
      env: { [key]: 'fake-environment-value' },
    }
  );
  assert.equal(result, 'fake-environment-value');
  assert.equal(process.env[key], before);
});
