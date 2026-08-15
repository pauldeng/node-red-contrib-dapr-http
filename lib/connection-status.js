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
const AWAITING_DISCOVERY = { fill: 'yellow', shape: 'ring', text: 'waiting for sidecar discovery' };
const CONNECTED = { fill: 'green', shape: 'dot', text: 'connected' };

function connectionStatus({
  hasLease,
  healthy,
  servedFingerprint,
  desiredFingerprint,
  subscriptionCount,
}) {
  if (!hasLease) {
    return { status: CONNECTING, restartRequired: false };
  }
  if (!healthy) {
    return { status: UNAVAILABLE, restartRequired: false };
  }
  // A restart is required whenever the set last SERVED from /dapr/subscribe
  // differs from the current one — including removing the last subscription
  // (an empty set). "Served", not "fetched by daprd": the app channel cannot
  // attribute a fetch to a caller, so a non-daprd fetch (an operator's curl, a
  // probe) clears this too while daprd stays stale. That is a documented
  // operator footgun, not something this decision can detect — see
  // lib/app-channel.js and docs/subscriptions.md.
  if (servedFingerprint !== null && servedFingerprint !== desiredFingerprint) {
    return { status: RESTART_REQUIRED, restartRequired: true };
  }
  if (servedFingerprint === null && subscriptionCount > 0) {
    return { status: AWAITING_DISCOVERY, restartRequired: false };
  }
  return { status: CONNECTED, restartRequired: false };
}

module.exports = { connectionStatus };
