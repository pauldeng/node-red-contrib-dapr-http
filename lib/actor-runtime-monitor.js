'use strict';

// Called by the existing health poll: no timer and at most one request.
// Scope changes invalidate a reading even if a previous fetch finishes later.
function createActorRuntimeMonitor({ fetch, onChange }) {
  let scope = null;
  let generation = 0;
  let inFlight = null;
  let closed = false;
  let value = null;

  function invalidate() {
    generation += 1;
    value = null;
    inFlight?.abort();
  }

  return {
    get value() {
      return value;
    },
    setScope(next) {
      if (scope !== next) {
        scope = next;
        invalidate();
      }
    },
    invalidate,
    async refresh() {
      if (closed || scope === null || inFlight) return;
      const current = generation;
      const controller = new AbortController();
      inFlight = controller;
      let next = null;
      try {
        next = (await fetch(controller.signal)) ?? null;
      } catch {
        // A failed fetch is unknown, never evidence of readiness.
      } finally {
        inFlight = null;
      }
      if (!closed && current === generation) {
        value = next;
        onChange();
      }
    },
    close() {
      closed = true;
      invalidate();
    },
  };
}

module.exports = { createActorRuntimeMonitor };
