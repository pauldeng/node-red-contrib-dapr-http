'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');

const { freePort } = require('../helpers/node-red');

// The OS assigns ephemeral ports from this range on its own, which is exactly
// why the test harness must not draw from it: a port handed out here is bound
// by a child process moments later, and anything the OS assigns in between wins
// the race. A Dapr app channel that loses it fails with EADDRINUSE *inside a
// deployed flow*, which does not stop Node-RED — the test then hangs until an
// unrelated waiter times out without ever naming the port.
function ephemeralFloor() {
  try {
    const low = Number(
      fs.readFileSync('/proc/sys/net/ipv4/ip_local_port_range', 'utf8').trim().split(/\s+/)[0]
    );
    if (Number.isInteger(low) && low > 1024) {
      return low;
    }
  } catch {
    // Not Linux: the assertion below still pins the conservative default.
  }
  return 32768;
}

test('freePort draws below the range the OS assigns spontaneously', async () => {
  const floor = ephemeralFloor();
  for (let i = 0; i < 20; i += 1) {
    const port = await freePort();
    assert.ok(
      port < floor,
      `freePort returned ${port}, inside the OS ephemeral range (floor ${floor}) — ` +
        'the caller can lose this port before it binds'
    );
  }
});

test('freePort never hands the same port out twice', async () => {
  const seen = new Set();
  for (let i = 0; i < 30; i += 1) {
    const port = await freePort();
    assert.equal(seen.has(port), false, `freePort repeated ${port}`);
    seen.add(port);
  }
});

test('a port freePort returns is actually bindable', async () => {
  const port = await freePort();
  const srv = net.createServer();
  await new Promise((resolve, reject) => {
    srv.once('error', reject);
    srv.listen(port, '127.0.0.1', resolve);
  });
  await new Promise((resolve) => srv.close(resolve));
});
