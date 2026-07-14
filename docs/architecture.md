# Architecture

Trust boundaries and runtime lifecycle. Layout and module list live in
`AGENTS.md`; this document explains why the pieces are shaped the way they
are.

## Two independent HTTP paths

A `dapr-connection` node manages two things that never share a listener:

- **Outbound** — this app calling the sidecar's own HTTP API
  (`lib/dapr-client.js` for pub/sub, `lib/invoke-client.js` for service
  invocation). Plain outgoing HTTP requests; no server or Node-RED HTTP
  infrastructure is involved on this side.
- **Inbound app channel** (`lib/app-channel.js`) — a dedicated `node:http`
  server the connection node starts itself, for the sidecar to call back into
  this app. It exposes exactly:
  - `GET /healthz` — unauthenticated app health/readiness.
  - `GET /dapr/subscribe` — the current programmatic subscription list.
  - `/node-red-dapr/subscriptions/<stable-id>` — pub/sub delivery routes.
  - Registered `dapr-service` method paths.
  - Everything else: 404. A registered path called with the wrong verb: 405
    with an `Allow` header.

This listener is never attached to `RED.httpAdmin`, `RED.httpNode`, or the
editor port. Doing so would expose the Node-RED Admin API (remote flow
read/deploy) to the Dapr mesh, and would let admin routes shadow service
methods. See `docs/security.md` for the auth rules this listener enforces.

## Listener lifecycle across redeploy

Dapr fetches programmatic subscriptions once at `daprd` startup and caches
them; there is no refresh endpoint in Dapr 1.18.1. A `dapr-connection` node
is a transient Node-RED object recreated on every deploy, so the listener
itself is owned by a module-scoped registry (`lib/app-channel.js`)
independent of any one node instance:

1. Delivery paths are derived from persisted node/rule IDs, not array
   position or deploy generation, so they survive an unchanged-flow redeploy.
2. When a connection closes during deploy, its registry lease enters a short
   bounded grace period instead of closing the socket immediately.
3. The replacement connection reacquires the same listener, cancels the idle
   shutdown, installs a new handler generation, and atomically swaps in the
   new routing table.
4. Pub/sub acknowledgements and service requests pending from the old
   generation complete as `RETRY` / `503` respectively, rather than hanging
   or landing on stale flow nodes.
5. Requests arriving in the gap between generations get a retryable 503.
6. If nothing reacquires the lease, the registry drains and closes the
   server and its sockets.

Net effect: a routine flow redeploy — publish nodes, invoke nodes, service
handlers, downstream logic, layout, wiring — needs no sidecar restart. Only a
change to what daprd itself reads from `/dapr/subscribe` does. See
`docs/subscriptions.md` for the fingerprint mechanism that detects this and
the exact restart procedure.

## Module layout

```text
nodes/dapr-connection.js   config-node lifecycle, credentials, health polling, restart-status
nodes/dapr-publish.js      publish wrapper
nodes/dapr-subscribe.js    subscription wrapper (routing rules, bulk config)
nodes/dapr-ack.js          acknowledgement wrapper
nodes/dapr-invoke.js       outbound invocation wrapper
nodes/dapr-service.js      inbound service wrapper
nodes/dapr-response.js     invocation response wrapper
lib/options.js             config/env precedence and validation
lib/messages.js            content-type inference and CloudEvent conversion
lib/dapr-client.js         SDK publish client and shared HTTP-agent lifecycle
lib/invoke-client.js       native outbound invocation adapter
lib/http-headers.js        header allow-listing/normalization shared by both directions
lib/app-channel.js         listener registry, router, auth, limits, sockets
lib/subscriptions.js       subscription definitions, canonical fingerprints, generations
lib/services.js            registered dapr-service method table
lib/pending.js             bounded first-wins ack/response correlation
lib/errors.js              stable internal error types and safe (no-stack-trace) messages
```

`nodes/*.js` files are thin Node-RED wrappers; behavior lives in `lib/` as
modules with no Node-RED import, so they're directly unit-testable. See
`docs/testing.md` for how each tier exercises this split.

## Why HTTP-only and one keep-alive policy

The `@dapr/dapr` SDK's HTTP agents are process-global, not per-node — so
keep-alive is owned centrally in `lib/dapr-client.js` rather than configured
per node instance. The SDK version is pinned (no caret) because
`lib/dapr-client.js` relies on specific 3.18.0 HTTP-client behavior (fail-fast
readiness, falsy-body handling, agent reuse); any upgrade needs re-verifying
those three behaviors against the new version's source plus a real `daprd`
before the pin moves.
