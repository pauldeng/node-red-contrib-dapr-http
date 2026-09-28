'use strict';

// Shared low-level docker CLI plumbing for test/helpers/integration.js
// (daprd + Redis) and test/helpers/node-red-container.js (the pinned
// nodered/node-red image) — both drive real Docker the same way.

const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileAsync = promisify(execFile);

// A fresh CI runner has none of these images cached, and pulling one can take
// minutes on a slow link — far past a timeout sized for a local CLI call.
// ensureImage() gives the (rare, first-time) pull its own generous budget,
// keeping every other docker command's timeout tight.
const PULL_TIMEOUT_MS = 5 * 60 * 1000;

async function execFileP(cmd, args, { timeout = 15000, env = {} } = {}) {
  try {
    const { stdout } = await execFileAsync(cmd, args, { timeout, env: { ...process.env, ...env } });
    return stdout.trim();
  } catch (err) {
    // execFile's message includes argv. Never repeat arguments or retain that
    // error as a cause: a caller may have passed credentials on the command line.
    // eslint-disable-next-line preserve-caught-error -- The original error contains credential-bearing argv.
    throw new Error(`${cmd} failed: ${err.stderr?.trim() || err.code || 'unknown error'}`);
  }
}

async function ensureImage(image) {
  try {
    await execFileP('docker', ['image', 'inspect', image]);
    return;
  } catch {
    // Not present locally — fall through to pull it.
  }
  await execFileP('docker', ['pull', image], { timeout: PULL_TIMEOUT_MS });
}

module.exports = { execFileP, ensureImage, PULL_TIMEOUT_MS };
