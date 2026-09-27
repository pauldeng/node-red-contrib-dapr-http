'use strict';

// The dapr-connection node's status decision, as a pure function of what the
// connection currently knows. Extracted from the node so the precedence
// between "no listener yet", "sidecar down", "daprd needs restarting" and
// "waiting for discovery" is directly testable — it was previously only
// reachable by driving a real Node-RED child process.
//
// Returns the status to display plus `restartRequired`, which the caller uses
// to decide whether to also emit the (rate-limited) restart warning. Emitting
// is the caller's job: this function has no side effects.
const CONNECTING = { fill: 'grey', shape: 'ring', text: 'connecting' };
const UNAVAILABLE = { fill: 'red', shape: 'ring', text: 'sidecar unavailable' };
const RESTART_REQUIRED = {
  fill: 'yellow',
  shape: 'ring',
  text: 'restart sidecar: subscriptions changed',
};
const ACTOR_RESTART_REQUIRED = {
  fill: 'yellow',
  shape: 'ring',
  text: 'restart sidecar: actor types changed',
};
const AWAITING_DISCOVERY = { fill: 'yellow', shape: 'ring', text: 'waiting for sidecar discovery' };
const CONNECTED = { fill: 'green', shape: 'dot', text: 'connected' };

// Derives the actor-readiness warning text from a metadata poll's curated
// `actorRuntime` (lib/metadata-client.js's getMetadata) -- or `null`/
// `undefined` when nothing has been fetched yet. Missing or unrecognized
// fields mean "unknown", never "healthy": only a metadata block that
// positively confirms `hostReady: true` AND a placement string reporting
// "connected" clears the warning entirely. The caller is responsible for
// never calling this at all when no actor method is registered.
function actorRuntimeWarningText(actorRuntime) {
  if (actorRuntime === null || actorRuntime === undefined || typeof actorRuntime !== 'object') {
    return 'actors: runtime status unknown';
  }
  if (actorRuntime.hostReady === false) {
    return 'actors: host not ready';
  }
  // Dapr v1.18.4 pkg/actors/actors.go RuntimeStatus() emits these exact values.
  // Unknown values cannot establish Placement state.
  const placement = actorRuntime.placement;
  if (placement === 'placement: disconnected') {
    return 'actors: placement disconnected';
  }
  if (placement !== 'placement: connected') {
    return 'actors: runtime status unknown';
  }
  if (actorRuntime.hostReady !== true) {
    return 'actors: runtime status unknown';
  }
  return null;
}

function connectionStatus({
  hasLease,
  healthy,
  servedFingerprint,
  desiredFingerprint,
  subscriptionCount,
  servedActorFingerprint = null,
  desiredActorFingerprint = null,
  // Set only when at least one actor method is currently registered; a
  // connection with none never shows an actor warning, regardless of any
  // stale `actorRuntime` a caller might still be holding from before the
  // last registration was removed.
  actorMethodsRegistered = false,
  actorRuntime = null,
}) {
  if (!hasLease) {
    return { status: CONNECTING, restartRequired: false, actorRestartRequired: false };
  }
  if (!healthy) {
    return { status: UNAVAILABLE, restartRequired: false, actorRestartRequired: false };
  }
  // A restart is required whenever the set last SERVED from /dapr/subscribe
  // differs from the current one — including removing the last subscription
  // (an empty set). "Served", not "fetched by daprd": the app channel cannot
  // attribute a fetch to a caller, so a non-daprd fetch (an operator's curl, a
  // probe) clears this too while daprd stays stale. That is a documented
  // operator footgun, not something this decision can detect — see
  // lib/app-channel.js and docs/subscriptions.md.
  if (servedFingerprint !== null && servedFingerprint !== desiredFingerprint) {
    return { status: RESTART_REQUIRED, restartRequired: true, actorRestartRequired: false };
  }
  // Removing the last advertised actor type also requires a restart. A
  // connection that never advertised actors has no served fingerprint.
  if (servedActorFingerprint !== null && servedActorFingerprint !== desiredActorFingerprint) {
    return { status: ACTOR_RESTART_REQUIRED, restartRequired: false, actorRestartRequired: true };
  }
  if (servedFingerprint === null && subscriptionCount > 0) {
    return { status: AWAITING_DISCOVERY, restartRequired: false, actorRestartRequired: false };
  }
  // Precedence below this point: invalid config/listen failure and sidecar
  // unavailable are handled above (or by the caller, before this is ever
  // reached); restart-required (subscriptions or actor types) already
  // returned above too. An actor warning outranks the plain "connected" text
  // but never blocks or delays it -- this function has no side effects and
  // gates nothing else, so ordinary pub/sub/service traffic is unaffected
  // either way.
  if (actorMethodsRegistered) {
    const warningText = actorRuntimeWarningText(actorRuntime);
    if (warningText) {
      return {
        status: { fill: 'yellow', shape: 'ring', text: warningText },
        restartRequired: false,
        actorRestartRequired: false,
      };
    }
  }
  return { status: CONNECTED, restartRequired: false, actorRestartRequired: false };
}

module.exports = { connectionStatus, actorRuntimeWarningText };
