'use strict';

// A generic "push at most once per interval, coalescing to the latest value"
// helper. Used by nodes/dapr-actor-method.js for its own "listening · N
// active" status, so a burst of admit/settle changes never churns the editor
// status faster than once a second, while the FINAL value in a burst (in
// particular a return to zero) is never dropped -- it is always the last
// call's value that eventually renders, whether pushed immediately or after
// the trailing timer. Owns exactly one timer, cleared by `close()`.
function createThrottledPusher({ intervalMs = 1000, now = Date.now, render }) {
  let lastRenderedAt = -Infinity;
  let timer = null;
  let pending;
  let lastValue;
  let hasRendered = false;
  let closed = false;

  function flush(value) {
    pending = undefined;
    if (hasRendered && Object.is(value, lastValue)) return;
    render(value);
    lastValue = value;
    hasRendered = true;
    lastRenderedAt = now();
  }

  return {
    push(value) {
      if (closed) return;
      if (timer) {
        // A trailing push is already scheduled; let it pick up this newer
        // value when it fires rather than racing a second timer.
        pending = value;
        return;
      }
      const elapsed = now() - lastRenderedAt;
      if (elapsed >= intervalMs) {
        flush(value);
        return;
      }
      pending = value;
      timer = setTimeout(() => {
        timer = null;
        flush(pending);
      }, intervalMs - elapsed);
      timer.unref?.();
    },
    close() {
      closed = true;
      pending = undefined;
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
    },
  };
}

module.exports = { createThrottledPusher };
