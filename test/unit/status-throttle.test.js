'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { createThrottledPusher } = require('../../lib/status-throttle');

test('the first push renders immediately', () => {
  const rendered = [];
  const throttle = createThrottledPusher({ render: (v) => rendered.push(v) });
  throttle.push(1);
  assert.deepEqual(rendered, [1]);
  throttle.close();
});

test('pushes within the interval coalesce to the latest value, rendered once after it elapses', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const rendered = [];
  const throttle = createThrottledPusher({ intervalMs: 30, render: (v) => rendered.push(v) });
  throttle.push(1);
  throttle.push(2);
  throttle.push(3);
  assert.deepEqual(rendered, [1], 'only the first push rendered so far');
  t.mock.timers.tick(30);
  assert.deepEqual(rendered, [1, 3], 'the trailing render uses the latest value, not every one');
  throttle.close();
});

test('a return to zero is never dropped, even mid-burst', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const rendered = [];
  const throttle = createThrottledPusher({ intervalMs: 30, render: (v) => rendered.push(v) });
  throttle.push(1);
  throttle.push(0);
  t.mock.timers.tick(30);
  assert.deepEqual(rendered, [1, 0]);
  throttle.close();
});

test('a push after the interval has elapsed renders immediately again', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const rendered = [];
  const throttle = createThrottledPusher({ intervalMs: 20, render: (v) => rendered.push(v) });
  throttle.push(1);
  t.mock.timers.tick(20);
  throttle.push(2);
  assert.deepEqual(rendered, [1, 2]);
  throttle.close();
});

test('close() clears the pending trailing timer; a scheduled push never renders after close', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const rendered = [];
  const throttle = createThrottledPusher({ intervalMs: 30, render: (v) => rendered.push(v) });
  throttle.push(1);
  throttle.push(2); // scheduled as a trailing push
  throttle.close();
  t.mock.timers.tick(30);
  assert.deepEqual(rendered, [1], 'the closed timer must never fire');
});

test('unchanged rendered counts do not publish again, including a burst back to zero', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const rendered = [];
  const throttle = createThrottledPusher({ intervalMs: 30, render: (v) => rendered.push(v) });
  throttle.push(0);
  throttle.push(1);
  throttle.push(0);
  t.mock.timers.tick(30);
  throttle.push(0);
  assert.deepEqual(rendered, [0]);
  throttle.close();
  throttle.push(2);
  t.mock.timers.tick(30);
  assert.deepEqual(rendered, [0], 'closed owners cannot publish again');
});
