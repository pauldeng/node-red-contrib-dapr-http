# Subscription runbook

When a deploy needs a `daprd` restart, and how to do it safely. See
`docs/architecture.md` for why the listener survives most redeploys on its
own.

## Why this is needed at all

Dapr fetches programmatic subscriptions from `GET /dapr/subscribe` exactly
once, at `daprd` startup, and caches them for the sidecar's lifetime. Dapr
1.18.1 has no endpoint to make it re-fetch. If a flow changes what it would
now report at `/dapr/subscribe`, daprd keeps acting on the stale definition
until it's restarted — there is no way to push the update to a running
sidecar.

## What's fingerprinted

`lib/subscriptions.js` canonically serializes and hashes every field that
feeds `/dapr/subscribe`: pubsub name, topic, metadata (including the
`rawPayload` flag), dead-letter topic, bulk-subscribe settings, CEL rule
matches and their order, and route paths (default and per-rule). This
fingerprint — not the flow's layout, wiring, or any other node's config — is
what decides whether a restart is needed.

## NATS JetStream limitation

With the pinned Dapr 1.18.1 runtime, do not use `deadLetterTopic` with NATS
JetStream. The real integration test observes one delivery and then a stall
before the dead-letter message arrives. Use Redis or broker-side dead-letter
handling instead. The JetStream stream must also exist before the component
is used; see `docs/deployment.md`.

## Decision table

| You changed...                                                                          | Restart needed? |
| --------------------------------------------------------------------------------------- | --------------- |
| Publish node, invoke node, service handler, or any downstream flow logic                | No              |
| Node/tab layout, labels, or wiring that doesn't touch a subscribe node's fields         | No              |
| Adding, removing, or renaming a `dapr-subscribe` node                                   | Yes             |
| A subscribe node's pubsub name or topic                                                 | Yes             |
| A subscribe node's metadata, raw-payload flag, or dead-letter topic                     | Yes             |
| A subscribe node's CEL rules or their order                                             | Yes             |
| A subscribe node's bulk-subscribe settings                                              | Yes             |
| A subscribe node's ack mode, or anything downstream of it (`dapr-ack`, a Function node) | No              |

If in doubt, deploy and check the `dapr-connection` node's status (next
section) — it tells you definitively, rather than requiring you to reason
about which fields matter.

## Reading the connection node's status

- **connected** (green) — the fingerprint daprd last fetched matches the
  current desired one. No restart needed.
- **restart sidecar: subscriptions changed** (yellow) — the desired
  fingerprint changed since daprd last fetched `/dapr/subscribe`. Routes stay
  operational where possible, but daprd is acting on stale subscription
  definitions until restarted. The warning is rate-limited, not repeated on
  every poll.
- **waiting for sidecar discovery** (yellow) — a fresh Node-RED process with
  no observed `/dapr/subscribe` fetch yet. This is not a claim that daprd is
  out of date; it's simply that this process has no record of what daprd
  last saw (e.g. right after a Node-RED restart). It clears when daprd (or
  your own probe) next fetches `/dapr/subscribe`.

  **This state can persist indefinitely on a perfectly healthy system.** daprd
  fetches `/dapr/subscribe` once, at its own startup — so if Node-RED restarts
  while daprd keeps running, delivery continues working (the routes are derived
  from persisted ids and never move) but nothing re-fetches, and the status stays
  yellow until daprd itself is next restarted. Do not alert on it as a fault, and
  do not restart daprd just to clear it: check whether deliveries are arriving.
  `curl` the app channel's `/dapr/subscribe` yourself if you want the status to
  reflect the current set immediately.

The warning only clears after daprd actually re-fetches `/dapr/subscribe` and
receives the new definition — never optimistically, right after you deploy.

## Restart procedure

1. Deploy your flow change as normal.
2. If the connection node shows **restart sidecar: subscriptions changed**,
   restart the `daprd` process/container for that sidecar (e.g.
   `docker restart <daprd-container>`, or let your orchestrator recreate the
   pod).
3. On restart, daprd calls `GET /dapr/subscribe` again against the
   already-running app-channel listener — Node-RED does not need to be
   redeployed or restarted itself, only daprd.
4. Confirm the status returns to **connected**. If it doesn't, check that the
   app-channel listener is actually reachable at the app port daprd is
   configured with (`docs/deployment.md` covers topology).

Routine flow work — everything in the "No" column above — never requires
this procedure; just deploy.
