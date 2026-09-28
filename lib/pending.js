'use strict';

const { DaprError, ErrorCodes } = require('./errors');

// A bounded, first-wins correlation store. A delivery/request registers an id
// and awaits its settlement (by a matching ack/response node, or a timeout);
// the first settlement wins and later ones are ignored. Stale or foreign ids
// simply miss (settle returns false), and a redeploy drains all outstanding
// entries with a retryable value.
class PendingRegistry {
  constructor({ max = 1000 } = {}) {
    this._max = max;
    this._entries = new Map(); // id -> { resolve, timer }
  }

  get size() {
    return this._entries.size;
  }

  has(id) {
    return this._entries.has(id);
  }

  // Register an id and return a promise for its eventual value. Throws
  // synchronously if the id is already registered or capacity is reached, so a
  // caller can translate either into a retry.
  add(id, { timeoutMs, onTimeout } = {}) {
    if (this._entries.has(id)) {
      throw new DaprError(ErrorCodes.DUPLICATE_PENDING, 'correlation id already registered');
    }
    if (this._entries.size >= this._max) {
      throw new DaprError(ErrorCodes.PENDING_CAPACITY, `pending capacity ${this._max} reached`);
    }

    const { promise, resolve } = Promise.withResolvers();
    const entry = { resolve, timer: null };

    if (timeoutMs !== undefined) {
      entry.timer = setTimeout(
        () => {
          // Only fire if this exact entry is still outstanding.
          if (this._entries.get(id) === entry) {
            this._entries.delete(id);
            resolve(onTimeout ? onTimeout() : undefined);
          }
        },
        Math.max(1, timeoutMs)
      );
    }

    this._entries.set(id, entry);
    return promise;
  }

  // First-wins settlement. Returns true only for the call that settled it.
  settle(id, value) {
    const entry = this._entries.get(id);
    if (!entry) {
      return false;
    }
    this._entries.delete(id);
    if (entry.timer) {
      clearTimeout(entry.timer);
    }
    entry.resolve(value);
    return true;
  }

  // Settle every outstanding entry with one value (shutdown / generation swap).
  drain(value) {
    let count = 0;
    for (const id of this._entries.keys()) {
      if (this.settle(id, value)) {
        count += 1;
      }
    }
    return count;
  }
}

module.exports = { PendingRegistry };
