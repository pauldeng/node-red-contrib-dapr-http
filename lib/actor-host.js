'use strict';

const crypto = require('node:crypto');

const { ErrorCodes } = require('./errors');
const { PendingRegistry } = require('./pending');

// The margin subtracted from the request's absolute deadline before starting
// a commit, reserving time to respond before the outer watchdog. This is a
// budget, not a guarantee against event-loop stalls or a forced close.
const COMMIT_MARGIN_MS = 250;
const JSON_HEADERS = { 'content-type': 'application/json' };

// Internal markers for a settled-but-not-a-real-proposal outcome. Never
// leaves this module.
const EXPIRED = Symbol('actor-reply-expired');
const ABORTED = Symbol('actor-reply-aborted');

// Fixed, sanitized 5xx bodies -- never a raw exception, a daprd response body,
// or Node-RED configuration (mirrors lib/app-channel.js's "never echo
// internals" rule, applied to actor read/commit failures). `code` is one of
// this package's own stable ErrorCodes, safe to disclose on its own.
const SANITIZED_MESSAGE = {
  [ErrorCodes.SIDECAR_UNAVAILABLE]: 'could not reach the Dapr sidecar',
  [ErrorCodes.STATE_OPERATION_FAILED]: 'actor state operation failed',
  [ErrorCodes.RESPONSE_TOO_LARGE]: 'the sidecar response was too large',
  [ErrorCodes.ACTOR_BUSY]: 'actor is busy',
  [ErrorCodes.ACTOR_REPLY_EXPIRED]: 'actor reply expired',
  [ErrorCodes.ACTOR_COMMIT_UNKNOWN]: 'actor state commit outcome is unknown',
  [ErrorCodes.INVALID_MESSAGE]: 'invalid request body',
};

function sanitizedBody(code) {
  return JSON.stringify({ error: { code, message: SANITIZED_MESSAGE[code] || 'internal error' } });
}

// One actor-invocation host per dapr-connection. Admits at most one in-flight
// handler per (actorType, actorId), loads state, hands the invocation to the
// registered method node's `emit`, waits for its reply proposal, and only
// then commits any proposed next state -- see docs/architecture.md's "Actor
// request ownership" section.
//
// `client` is injected (rather than importing lib/actor-client.js directly)
// so this module is unit-testable with a fake read/save pair: it carries the
// connection's already-resolved outbound { baseUrl, token } plus the two
// functions themselves, `readRecord` and `saveRecord`, called with the same
// (opts, request) shape lib/actor-client.js exports. `limits` is the
// connection's already-resolved lib/options.js limits; `now` is injectable so
// tests can drive the remaining-time checks with a fake clock instead of real
// timers.
function createActorHost({ limits, client, now = Date.now }) {
  const active = new Map(); // `${type}\0${id}` -> true, while a handler is in flight
  const pending = new PendingRegistry({ max: limits.maxPending });
  const identities = new Map(); // requestId -> { actorType, actorId }, live only while awaiting a reply
  let closing = false;

  function clientOpts(extra) {
    return {
      baseUrl: client.baseUrl,
      token: client.token,
      maxResponseBytes: limits.bodyLimitBytes,
      ...extra,
    };
  }

  // `ctx` is the app-channel's per-request context (body, signal, deadlineAt);
  // `emit` is the registered method node's emit function, already resolved by
  // the caller (a 404 for an unknown actorType/method never reaches here).
  async function invoke({ actorType, actorId, method, emit, ctx }) {
    const key = `${actorType}\0${actorId}`;
    if (closing || active.has(key) || active.size >= limits.maxPending) {
      return { status: 503, headers: JSON_HEADERS, body: sanitizedBody(ErrorCodes.ACTOR_BUSY) };
    }
    active.set(key, true);
    const reserveMs = Math.min(limits.drainTimeoutMs, limits.requestTimeoutMs / 2);
    const flowDeadline = ctx.deadlineAt - reserveMs;
    let requestId;
    let onAbort;
    const expired = () => closing || ctx.signal.aborted || now() >= flowDeadline;
    const expiredResult = () => ({
      status: 503,
      headers: JSON_HEADERS,
      body: sanitizedBody(ErrorCodes.ACTOR_REPLY_EXPIRED),
    });
    try {
      if (expired()) return expiredResult();
      let record;
      try {
        record = await client.readRecord(
          clientOpts({ timeoutMs: flowDeadline - now(), signal: ctx.signal }),
          {
            actorType,
            actorId,
          }
        );
      } catch (err) {
        return { status: 500, headers: JSON_HEADERS, body: sanitizedBody(err.code) };
      }
      if (expired()) return expiredResult();

      let payload;
      if (ctx.body && ctx.body.length > 0) {
        try {
          payload = JSON.parse(ctx.body.toString('utf8'));
        } catch {
          return {
            status: 400,
            headers: JSON_HEADERS,
            body: sanitizedBody(ErrorCodes.INVALID_MESSAGE),
          };
        }
      }

      requestId = crypto.randomUUID();
      const timeoutMs = flowDeadline - now();
      if (timeoutMs <= 0) return expiredResult();
      const proposalPromise = pending.add(requestId, { timeoutMs, onTimeout: () => EXPIRED });
      identities.set(requestId, { actorType, actorId });

      // Settle our own pending entry the moment the caller disconnects, rather
      // than only on the (much longer) reserve timeout -- otherwise a
      // disconnected caller would hold this actor's gate until the flow
      // eventually replies or the reserve expires.
      if (ctx.signal.aborted) {
        pending.settle(requestId, ABORTED);
      } else {
        onAbort = () => pending.settle(requestId, ABORTED);
        ctx.signal.addEventListener('abort', onAbort, { once: true });
        emit({
          _msgid: crypto.randomUUID(),
          payload,
          dapr: {
            actor: {
              type: actorType,
              id: actorId,
              method,
              stateExists: record.exists,
              // A private deep copy: mutating msg.dapr.actor.state in the flow
              // must never alias (or retarget) what will actually be read back.
              state: record.exists ? JSON.parse(JSON.stringify(record.value)) : null,
            },
            actorRequestId: requestId,
          },
        });
      }

      const proposal = await proposalPromise;
      if (onAbort) {
        ctx.signal.removeEventListener('abort', onAbort);
      }
      identities.delete(requestId);

      if (proposal === EXPIRED) {
        return {
          status: 503,
          headers: JSON_HEADERS,
          body: sanitizedBody(ErrorCodes.ACTOR_REPLY_EXPIRED),
        };
      }
      if (proposal === ABORTED) {
        // The caller is already gone; the app channel has (or will have)
        // finished this response on its own. Nothing to write, nothing to
        // commit.
        return { status: 503 };
      }
      // Timers may not have run yet when an already-queued reply settles.
      // Recheck the absolute budget even for read-only and failure replies.
      if (expired()) return expiredResult();

      if (proposal.outcome === 'fail') {
        return { status: 500, headers: JSON_HEADERS, body: proposal.errorBody };
      }
      if (proposal.nextStateJson === undefined) {
        return { status: 200, headers: JSON_HEADERS, body: proposal.responseJson };
      }

      // About to commit: a caller disconnect or an exhausted deadline *before*
      // the commit starts means do not commit at all (respond only, no
      // write) -- see section 4, "Commit ownership", step 5. Once past this
      // point the commit runs to completion regardless of ctx.signal; the
      // active-handler gate above is released exactly once, in `finally`,
      // whether or not a commit ran.
      if (ctx.signal.aborted) {
        return { status: 503 };
      }
      const commitTimeoutMs = Math.min(reserveMs, ctx.deadlineAt - now()) - COMMIT_MARGIN_MS;
      if (commitTimeoutMs <= 0) {
        return {
          status: 503,
          headers: JSON_HEADERS,
          body: sanitizedBody(ErrorCodes.ACTOR_REPLY_EXPIRED),
        };
      }

      try {
        await client.saveRecord(clientOpts({}), {
          actorType,
          actorId,
          valueJson: proposal.nextStateJson,
          timeoutMs: commitTimeoutMs,
        });
      } catch (err) {
        if (err.code === ErrorCodes.SIDECAR_UNAVAILABLE) {
          return {
            status: 500,
            headers: JSON_HEADERS,
            body: sanitizedBody(ErrorCodes.ACTOR_COMMIT_UNKNOWN),
          };
        }
        return { status: 500, headers: JSON_HEADERS, body: sanitizedBody(err.code) };
      }
      return { status: 200, headers: JSON_HEADERS, body: proposal.responseJson };
    } finally {
      if (onAbort) ctx.signal.removeEventListener('abort', onAbort);
      if (requestId) {
        pending.settle(requestId, ABORTED);
        identities.delete(requestId);
      }
      active.delete(key);
    }
  }

  return {
    invoke,
    // First-wins settlement of a live reply, called by the reply node with an
    // already-serialized proposal (lib/actor-messages.js's serializeProposal).
    // Returns false for a missing, foreign, expired, or already-settled id.
    settleActorReply(requestId, proposal) {
      return pending.settle(requestId, proposal);
    },
    // { actorType, actorId } for a live, not-yet-completed invocation, else
    // null -- read from this live handler map, never from message fields (a
    // flow cannot retarget storage by editing msg.dapr.actor). Used by the
    // (later) call node's direct self-call guard.
    actorIdentity(requestId) {
      return identities.get(requestId) ?? null;
    },
    // Unblock every handler still waiting on a flow reply so a connection
    // close/redeploy answers them (503 ACTOR_REPLY_EXPIRED) promptly instead
    // of after their own reserve timeout. A handler already past this point
    // (running or awaiting its commit) is unaffected -- its own gate is
    // released in `finally` once the commit settles.
    //
    // milestone 3: waiting for started commits to finish before this
    // connection is considered fully drained, tombstones for actor types
    // whose method nodes were removed, and retry/replay behavior for a
    // replayed daprd request that overlaps an already-completed handler.
    drain() {
      closing = true;
      return pending.drain(EXPIRED);
    },
  };
}

module.exports = { createActorHost };
