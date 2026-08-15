'use strict';

const { setTimeout: delay } = require('node:timers/promises');

// Polls fn() until it returns a truthy value or timeoutMs elapses.
//
// Reach for this ONLY when there is nothing to subscribe to: a child
// process's HTTP readiness, a container's state, log text on a stream we do
// not own. For anything this repository's own test doubles produce, prefer
// their event-driven waiters — test/helpers/fake-dapr.js's waitForRequest and
// test/helpers/capture.js's waitForMessage both resolve the moment the thing
// happens instead of up to one interval late.
//
// A throwing fn() is retried, not propagated: the conditions polled here are
// exactly the ones that legitimately fail mid-transition (a route 404s during
// a redeploy gap, a container refuses a connection while starting). The last
// value or error is reported on timeout, so a failure says why.
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
    await delay(intervalMs);
  }
  throw new Error(`waitFor timed out (${last instanceof Error ? last.message : last})`);
}

// Same implementation, tuned for the runtime tier: its polls are cheap
// (loopback HTTP, a child process's own stdout) so it can afford to look more
// often, and it should give up sooner because nothing here waits on Docker.
// The integration tier keeps the patient defaults above — its polls shell out
// to `docker exec` and talk to containers that are still starting.
const waitForFast = (fn, options) => waitFor(fn, { timeoutMs: 10000, intervalMs: 50, ...options });

module.exports = { waitFor, waitForFast };
