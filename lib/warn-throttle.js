'use strict';

// Rate-limits a connection's own diagnostic warnings, one small record per
// event code -- bounded by the (small, fixed) set of codes this package ever
// emits, never by how many times or how fast an event fires. Shared by
// lib/actor-host.js's ACTOR_COMMIT_UNKNOWN, lib/app-channel.js's tombstone
// hit, and nodes/dapr-connection.js's own close-drain backstop, so the "at
// most one per code per window, report what was suppressed" rule lives in
// exactly one place rather than three.
function createWarnThrottle({ windowMs = 60000, now = Date.now } = {}) {
  const last = new Map(); // code -> { at, suppressed }

  return {
    // Returns null when this call should be suppressed (and counts it
    // towards the next allowed one), else the number of calls suppressed
    // since the last allowed one for this code (0 the first time, or once
    // the window has passed with nothing suppressed in between).
    next(code) {
      const at = now();
      const entry = last.get(code);
      if (entry && at - entry.at < windowMs) {
        entry.suppressed += 1;
        return null;
      }
      const suppressed = entry ? entry.suppressed : 0;
      last.set(code, { at, suppressed: 0 });
      return suppressed;
    },
  };
}

// One fail-open boundary for all actor warnings, including shutdown.
function createActorDiagnosticLogger({ warn, ...options }) {
  const throttle = createWarnThrottle(options);
  return (code, { actorType, method } = {}) => {
    try {
      const suppressed = throttle.next(code);
      if (suppressed === null) return;
      const detail =
        code === 'ACTOR_DRAIN_BACKSTOP'
          ? 'connection close drain backstop fired before every started commit settled; its outcome is unknown to this close, not necessarily to the caller'
          : `type=${actorType}${method === undefined ? '' : ` method=${method}`}`;
      const suffix = suppressed > 0 ? ` (${suppressed} more suppressed)` : '';
      warn(`[${code}] ${detail}${suffix}`);
    } catch {
      // Diagnostics must never interrupt a response or connection close.
    }
  };
}

module.exports = { createWarnThrottle, createActorDiagnosticLogger };
