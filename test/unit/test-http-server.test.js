'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { startCapture } = require('../helpers/capture');

test('test HTTP servers stop promptly with an active request', async () => {
  const capture = await startCapture();
  const req = http.request(capture.url, { method: 'POST' });
  req.on('error', () => {});
  const connected = new Promise((resolve) =>
    req.once('socket', (socket) => socket.once('connect', resolve))
  );
  req.flushHeaders();
  req.write('{');
  await connected;
  await new Promise((resolve) => setTimeout(resolve, 50));

  const stopped = capture.stop().then(() => true);
  const stoppedPromptly = await Promise.race([
    stopped,
    new Promise((resolve) => setTimeout(() => resolve(false), 500)),
  ]);

  req.destroy();
  await stopped;
  assert.equal(stoppedPromptly, true);
});
