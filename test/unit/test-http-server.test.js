'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { httpRequest, closeHttpServer } = require('../helpers/http');
const { startCapture } = require('../helpers/capture');
const { NodeRed } = require('../helpers/node-red');
const { setTimeout: delay } = require('node:timers/promises');

test('Node-RED log collection does not rebuild the full log without a waiter', () => {
  const nr = new NodeRed();
  let reads = 0;
  const logText = nr.logText.bind(nr);
  nr.logText = () => {
    reads += 1;
    return logText();
  };

  nr._appendLog('one');
  nr._appendLog('two');

  assert.equal(reads, 0);
});

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
  await delay(50);

  const stopped = capture.stop().then(() => true);
  const stoppedPromptly = await Promise.race([
    stopped,
    new Promise((resolve) => setTimeout(() => resolve(false), 500)),
  ]);

  req.destroy();
  await stopped;
  assert.equal(stoppedPromptly, true);
});

test('test HTTP callers can cancel after observing the event they needed', async (t) => {
  const received = Promise.withResolvers();
  const server = http.createServer(() => received.resolve());
  const listening = Promise.withResolvers();
  server.listen(0, '127.0.0.1', listening.resolve);
  await listening.promise;
  t.after(() => closeHttpServer(server));
  const controller = new AbortController();
  const failed = assert.rejects(
    httpRequest(`http://127.0.0.1:${server.address().port}`, {
      signal: controller.signal,
      timeoutMs: 200,
    }),
    { name: 'AbortError' }
  );
  await received.promise;
  controller.abort();
  await failed;
});
