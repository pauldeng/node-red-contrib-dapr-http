'use strict';

const crypto = require('node:crypto');
const { setTimeout: delay } = require('node:timers/promises');

const { context, propagation, trace, SpanKind, ROOT_CONTEXT } = require('@opentelemetry/api');

const { ErrorCodes } = require('./errors');
const { PendingRegistry } = require('./pending');
const { getTracer, endSpan } = require('./telemetry');

// Bounds the actor id attribute on the server span the same way
// lib/metadata-client.js bounds its own curated text fields -- an id is
// operator/caller data, never assumed short.
const MAX_ATTRIBUTE_LENGTH = 256;
const boundedAttr = (value) =>
  typeof value === 'string' ? value.slice(0, MAX_ATTRIBUTE_LENGTH) : value;

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

// `code` rides alongside the HTTP response fields so the span wrapper (below)
// can label the outcome without reparsing the sanitized JSON body it never
// otherwise inspects. Harmless on the wire: lib/app-channel.js's response
// writer only ever reads status/headers/body.
const fail = (status, code) => ({ status, headers: JSON_HEADERS, body: sanitizedBody(code), code });

// One actor-invocation host per dapr-connection. Admits at most one in-flight
// handler per (actorType, actorId), loads state, hands the invocation to the
// registered method node's `emit`, waits for its reply proposal, and only
// then commits any proposed next state -- see docs/architecture.md's "Actor
// request ownership" section.
//
// `client` is injected (rather than importing lib/actor-client.js directly)
// so this module is unit-testable with a fake read/save/delete client; `now` is
// injectable so tests can drive the remaining-time checks with a fake clock.
function createActorHost({ limits, client, now = Date.now, onDiagnostic }) {
  const active = new Map(); // `${type}\0${id}` -> true, while a handler is in flight
  const pending = new PendingRegistry({ max: limits.maxPending });
  const identities = new Map(); // requestId -> { actorType, actorId }, live only while awaiting a reply
  // Promises for handlers that have already started their commit (added just
  // before client.saveRecord/deleteRecord, removed in invoke()'s outer
  // `finally`). `closing`
  // always wins the race against a new entry here: `expired()` (which checks
  // `closing`) is rechecked right before a commit starts, with no await in
  // between.
  const committing = new Set();
  let closing = false;

  // Per-(actorType, registryMethod) count of admitted, not-yet-finished
  // handlers -- including one still mid-commit, since that only stops being
  // "active" once the outer finally below runs (after the commit settles).
  // Pushed to listeners on every change; nodes/dapr-actor-method.js is the
  // only subscriber, one per (actorType, registryMethod) registration, so
  // this stays a small, bounded map for the life of the connection.
  const registrationCounts = new Map();
  const activeListeners = new Map(); // regKey -> Set<listener>

  function bumpActive(regKey, delta) {
    const next = (registrationCounts.get(regKey) || 0) + delta;
    if (next <= 0) {
      registrationCounts.delete(regKey);
    } else {
      registrationCounts.set(regKey, next);
    }
    const listeners = activeListeners.get(regKey);
    if (!listeners) {
      return;
    }
    for (const listener of listeners) {
      try {
        listener(next);
      } catch {
        // A subscriber's own callback must never break invocation handling.
      }
    }
  }

  // Diagnostics (ACTOR_COMMIT_UNKNOWN today) never change a request's own
  // outcome: a throwing or missing onDiagnostic is swallowed here, at the one
  // call site, rather than trusted to every future caller.
  function diagnostic(code, detail) {
    if (!onDiagnostic) {
      return;
    }
    try {
      onDiagnostic(code, detail);
    } catch {
      // Logging must never affect the response.
    }
  }

  function clientOpts(extra) {
    return {
      baseUrl: client.baseUrl,
      token: client.token,
      maxResponseBytes: limits.bodyLimitBytes,
      ...extra,
    };
  }

  // `ctx` is the app-channel's per-request context (body, signal, deadlineAt,
  // headers); `emit` is the registered method node's emit function, already
  // resolved by the caller (a 404 for an unknown actorType/method never
  // reaches here). `trigger` is present only for a reminder callback
  // (`{ kind: 'reminder', name }`, from lib/app-channel.js's parsed route) --
  // never derived from the request body, so a forged envelope cannot spoof
  // which reminder fired. `span` is this invocation's SERVER span, opened by
  // the outer `invoke()` wrapper below (even for a busy rejection) so every
  // admission decision is observable; this function only adds attributes and
  // child spans to it, never ends it.
  async function invokeBody({ actorType, actorId, method, emit, ctx, trigger }, span) {
    const key = `${actorType}\0${actorId}`;
    const regKey = `${actorType}\0${method}`;
    if (closing || active.has(key) || active.size >= limits.maxPending) {
      return fail(503, ErrorCodes.ACTOR_BUSY);
    }
    active.set(key, true);
    bumpActive(regKey, 1);
    const serverSpanContext = trace.setSpan(context.active(), span);
    const reserveMs = Math.min(limits.drainTimeoutMs, limits.requestTimeoutMs / 2);
    const flowDeadline = ctx.deadlineAt - reserveMs;
    let requestId;
    let onAbort;
    let commitDeferred; // set just before a commit starts; see `committing` above
    const expired = () => closing || ctx.signal.aborted || now() >= flowDeadline;
    const expiredResult = () => fail(503, ErrorCodes.ACTOR_REPLY_EXPIRED);
    try {
      if (expired()) return expiredResult();
      let record;
      const readSpan = getTracer().startSpan(
        'actor state read',
        {
          kind: SpanKind.CLIENT,
          attributes: {
            'rpc.system': 'dapr',
            'dapr.actor.type': actorType,
            'dapr.actor.id': boundedAttr(actorId),
          },
        },
        serverSpanContext
      );
      const readHeaders = {};
      propagation.inject(trace.setSpan(serverSpanContext, readSpan), readHeaders);
      try {
        record = await client.readRecord(
          clientOpts({
            timeoutMs: flowDeadline - now(),
            signal: ctx.signal,
            headers: readHeaders,
          }),
          {
            actorType,
            actorId,
          }
        );
        endSpan(readSpan);
      } catch (err) {
        endSpan(readSpan, err);
        return fail(500, err.code);
      }
      if (expired()) return expiredResult();

      let payload;
      if (trigger) {
        // A reminder callback's body is daprd's own fixed envelope
        // (`{"data":<value>,"dueTime":"","period":""}`, `data` omitted
        // entirely when the reminder was created with none) -- never the raw
        // argument an ordinary method call sends. An absent body is an empty
        // envelope (no `data` key), not a parse failure.
        let envelope = {};
        if (ctx.body && ctx.body.length > 0) {
          try {
            envelope = JSON.parse(ctx.body.toString('utf8'));
          } catch {
            return fail(400, ErrorCodes.INVALID_MESSAGE);
          }
        }
        if (envelope === null || typeof envelope !== 'object' || Array.isArray(envelope)) {
          return fail(400, ErrorCodes.INVALID_MESSAGE);
        }
        payload = Object.hasOwn(envelope, 'data') ? envelope.data : undefined;
      } else if (ctx.body && ctx.body.length > 0) {
        try {
          payload = JSON.parse(ctx.body.toString('utf8'));
        } catch {
          return fail(400, ErrorCodes.INVALID_MESSAGE);
        }
      }

      requestId = crypto.randomUUID();
      const timeoutMs = flowDeadline - now();
      if (timeoutMs <= 0) return expiredResult();
      const proposalPromise = pending.add(requestId, { timeoutMs, onTimeout: () => EXPIRED });
      identities.set(requestId, { actorType, actorId });

      // Settle our own pending entry the moment the caller disconnects, rather
      // than on the (much longer) reserve timeout.
      if (ctx.signal.aborted) {
        pending.settle(requestId, ABORTED);
      } else {
        onAbort = () => pending.settle(requestId, ABORTED);
        ctx.signal.addEventListener('abort', onAbort, { once: true });
        // Run inside the server span's context so Node-RED's per-node flow
        // spans (lib/telemetry.js's onSend/onReceive hooks, active for every
        // node once tracing is enabled) become descendants of this
        // invocation -- mirrors nodes/dapr-service.js's own emission.
        context.with(serverSpanContext, () =>
          emit({
            _msgid: crypto.randomUUID(),
            payload,
            dapr: {
              actor: {
                type: actorType,
                id: actorId,
                // For a reminder, `method` is the reminder's own name (never
                // the fixed registry key 'remind/') -- trigger identity comes
                // from the parsed route (`trigger`), never the request body.
                method: trigger ? trigger.name : method,
                ...(trigger ? { trigger: { kind: trigger.kind, name: trigger.name } } : {}),
                stateExists: record.exists,
                // client.readRecord (lib/actor-client.js) parses fresh JSON per
                // request, so this is already a private copy -- mutating
                // msg.dapr.actor.state in the flow cannot alias what a later
                // request reads.
                state: record.exists ? record.value : null,
              },
              actorRequestId: requestId,
            },
          })
        );
      }

      const proposal = await proposalPromise;
      if (onAbort) {
        ctx.signal.removeEventListener('abort', onAbort);
      }
      identities.delete(requestId);

      if (proposal === EXPIRED) {
        return expiredResult();
      }
      if (proposal === ABORTED) {
        // The caller is already gone; the app channel has (or will have)
        // finished this response on its own. Nothing to write, nothing to
        // commit.
        return { status: 503, code: 'ABORTED' };
      }
      // Timers may not have run yet when an already-queued reply settles.
      // Recheck the absolute budget even for read-only and failure replies.
      if (expired()) return expiredResult();

      if (proposal.outcome === 'fail') {
        return {
          status: 500,
          headers: JSON_HEADERS,
          body: proposal.errorBody,
          code: 'ACTOR_REPLY_FAILED',
        };
      }
      const willDelete = proposal.deleteState === true;
      if (!willDelete && proposal.nextStateJson === undefined) {
        return { status: 200, headers: JSON_HEADERS, body: proposal.responseJson };
      }

      // A caller disconnect or exhausted deadline *before* the commit starts
      // means do not commit at all -- already caught by the `expired()`
      // recheck above (which itself tests `ctx.signal.aborted`), with no
      // await in between, so there is no separate abort check to repeat
      // here. Once past this point the commit runs to completion regardless
      // of ctx.signal.
      const commitTimeoutMs = Math.min(reserveMs, ctx.deadlineAt - now()) - COMMIT_MARGIN_MS;
      if (commitTimeoutMs <= 0) {
        return expiredResult();
      }

      commitDeferred = Promise.withResolvers();
      committing.add(commitDeferred.promise);
      // A commit's own CLIENT span, parented off the server span directly
      // captured from the handler context, including across the detached
      // reply-await above. Kept open
      // for exactly as long as the commit itself: an in-flight commit at
      // close/redeploy keeps its span until whenCommitsSettled() lets it
      // finish.
      const commitSpan = getTracer().startSpan(
        willDelete ? 'actor state delete' : 'actor state save',
        {
          kind: SpanKind.CLIENT,
          attributes: {
            'rpc.system': 'dapr',
            'dapr.actor.type': actorType,
            'dapr.actor.id': boundedAttr(actorId),
          },
        },
        serverSpanContext
      );
      const commitHeaders = {};
      propagation.inject(trace.setSpan(serverSpanContext, commitSpan), commitHeaders);
      try {
        if (willDelete) {
          await client.deleteRecord(clientOpts({ headers: commitHeaders }), {
            actorType,
            actorId,
            timeoutMs: commitTimeoutMs,
          });
        } else {
          await client.saveRecord(clientOpts({ headers: commitHeaders }), {
            actorType,
            actorId,
            valueJson: proposal.nextStateJson,
            timeoutMs: commitTimeoutMs,
          });
        }
        endSpan(commitSpan);
      } catch (err) {
        endSpan(commitSpan, err);
        // A transport failure/timeout leaves the write's outcome genuinely
        // unknown -- distinct from a confirmed non-2xx.
        if (err.code === ErrorCodes.SIDECAR_UNAVAILABLE) {
          diagnostic('ACTOR_COMMIT_UNKNOWN', {
            actorType,
            method: trigger ? trigger.name : method,
            trigger: trigger ? 'reminder' : 'method',
          });
          return fail(500, ErrorCodes.ACTOR_COMMIT_UNKNOWN);
        }
        return fail(500, err.code);
      }
      return { status: 200, headers: JSON_HEADERS, body: proposal.responseJson };
    } finally {
      if (onAbort) ctx.signal.removeEventListener('abort', onAbort);
      if (requestId) {
        pending.settle(requestId, ABORTED);
        identities.delete(requestId);
      }
      // Runs as the very last synchronous step before this handler's own
      // promise settles -- see the `committing` comment above for why a
      // close's drain can never race past this point.
      if (commitDeferred) {
        committing.delete(commitDeferred.promise);
        commitDeferred.resolve();
      }
      active.delete(key);
      // Not "active" any more -- including while a commit was still
      // in-flight, since this whole finally block runs only after the
      // outer try (which awaits the commit) has settled.
      bumpActive(regKey, -1);
    }
  }

  // Extracts the caller's parent context (or a fresh root, on a missing or
  // malformed traceparent -- propagation.extract() falls back to what it was
  // given, so ROOT_CONTEXT here guarantees "malformed -> root" regardless of
  // any ambient context the HTTP server happened to be running under), opens
  // one SERVER span per invocation -- covering everything invokeBody does,
  // including a busy rejection -- and ends it exactly once with the final
  // outcome, whatever path produced it.
  async function invoke(args) {
    const { actorType, actorId, method, ctx, trigger } = args;
    const parentContext = propagation.extract(ROOT_CONTEXT, ctx.headers ?? {});
    const span = getTracer().startSpan(
      trigger ? `actor ${actorType} reminder` : `actor ${actorType}.${method}`,
      {
        kind: SpanKind.SERVER,
        attributes: {
          'rpc.system': 'dapr',
          'dapr.actor.type': actorType,
          'dapr.actor.method': trigger ? trigger.name : method,
          'dapr.actor.trigger': trigger ? 'reminder' : 'method',
          'dapr.actor.id': boundedAttr(actorId),
        },
      },
      parentContext
    );
    try {
      const result = await context.with(trace.setSpan(parentContext, span), () =>
        invokeBody(args, span)
      );
      const outcome = result.status >= 200 && result.status < 300 ? 'OK' : result.code || 'UNKNOWN';
      span.setAttribute('dapr.actor.outcome', outcome);
      endSpan(span, outcome === 'OK' ? undefined : new Error(outcome));
      return result;
    } catch (err) {
      span.setAttribute('dapr.actor.outcome', err.code || 'ERROR');
      endSpan(span, err);
      throw err;
    }
  }

  return {
    invoke,
    // The current count of admitted (including committing) handlers for one
    // (actorType, registryMethod) registration, pushed to `onChange` on every
    // change (seeded once with the count at subscription time). Returns an
    // unsubscribe function. Used by nodes/dapr-actor-method.js for its own
    // throttled status text; the reminder registration key is whatever the
    // caller already resolved (lib/connection-registry.js's REMINDER_METHOD).
    watchActive(actorType, method, onChange) {
      const regKey = `${actorType}\0${method}`;
      let listeners = activeListeners.get(regKey);
      if (!listeners) {
        listeners = new Set();
        activeListeners.set(regKey, listeners);
      }
      listeners.add(onChange);
      onChange(registrationCounts.get(regKey) || 0);
      return () => {
        listeners.delete(onChange);
        if (listeners.size === 0) {
          activeListeners.delete(regKey);
        }
      };
    },
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
    // Unblock every handler still waiting on a flow reply so a close/redeploy
    // answers them (503 ACTOR_REPLY_EXPIRED) promptly instead of after their
    // own reserve timeout. A handler already past this point is unaffected --
    // its gate releases in `finally` once its commit settles. Sets `closing`
    // first, so no handler still waiting here can go on to start a commit
    // afterward -- call this before `whenCommitsSettled()`, not after.
    drain() {
      closing = true;
      return pending.drain(EXPIRED);
    },
    // Resolves once every handler that had already started a commit when this
    // was called has settled -- so a caller mid-commit gets its real outcome
    // instead of the close path's generic 503. Call `drain()` first so no new
    // commit can start in between. Resolves `true` when the bounded backstop
    // fired first (a commit was still unsettled) and `false` otherwise, so a
    // caller can log/report the backstop without inferring it from elapsed
    // time.
    async whenCommitsSettled() {
      if (committing.size === 0) {
        return false;
      }
      const settled = (async () => {
        await Promise.all([...committing]);
        return false;
      })();
      // allow-timer: bounded backstop on a connection close/redeploy so a
      // never-settling commit (stalled sidecar call, event-loop stall)
      // cannot hold it open forever.
      return Promise.race([settled, delay(limits.drainTimeoutMs, true, { ref: false })]);
    },
  };
}

module.exports = { createActorHost };
