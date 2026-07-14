'use strict';

// Polls fn() until it returns a truthy value or timeoutMs elapses. Shared by
// every integration test file — each polls for HTTP readiness, delivery
// capture, or log content on its own bounded deadline.
async function waitFor(fn, { timeoutMs = 20000, intervalMs = 200 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) {
        return value;
      }
      last = value;
    } catch (err) {
      last = err;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`waitFor timed out (${last instanceof Error ? last.message : last})`);
}

module.exports = { waitFor };
