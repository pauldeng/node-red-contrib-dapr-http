'use strict';

// A one-shot, awaitable flag for an event a test needs to do two things with:
// assert it has NOT happened yet, then wait for it to happen.
//
// Replaces polling a plain boolean. The producer (an 'close'/'error' handler,
// a fake responder) calls fire(); the consumer awaits `fired` and resolves the
// instant it happens instead of up to one polling interval later. Bounding is
// left to the test's own `{ timeout }` — there is no second deadline to keep
// in step with it here.
function createSignal() {
  let fire;
  const fired = new Promise((resolve) => {
    fire = resolve;
  });
  let hasFired = false;
  return {
    fired,
    get hasFired() {
      return hasFired;
    },
    fire() {
      hasFired = true;
      fire();
    },
  };
}

module.exports = { createSignal };
