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
lib/sidecar-http.js        the one outbound HTTP request path to the sidecar
lib/dapr-client.js         pub/sub publish client (paths, metadata, serialization)
lib/invoke-client.js       outbound service-invocation client
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

## Why HTTP-only, no SDK, and one keep-alive policy

Everything this package sends to the sidecar is a handful of documented HTTP
endpoints: `POST /v1.0/publish/<pubsub>/<topic>`,
`/v1.0/invoke/<app-id>/method/<method>`, and `GET /v1.0/healthz/outbound`. All
three go through `lib/sidecar-http.js`, so there is exactly one place where
deadlines, aborts, Content-Length framing, response bounds, and error mapping
are implemented.

Publish and invoke ride Node's process-global keep-alive agent, so keep-alive is
owned centrally rather than per node instance. The health poll is the single
caller that opts out (`agent: false`): it runs every 10 s against a sidecar that
may be on its way down, and a pooled socket to a dead sidecar is only something
the next poll would have to discover is dead.

Response bodies are bounded in both directions. The app channel has always
capped what it will buffer from an inbound request; an outbound response is
capped by that same configured limit (**Body limit** on the connection node),
because an invoked app's response is the one body an operator does not control.
Exceeding it tears the exchange down mid-read rather than after it, and fails as
`RESPONSE_TOO_LARGE` — deliberately a different code from `SIDECAR_UNAVAILABLE`,
because the sidecar did answer.

Publishing used to go through the `@dapr/dapr` SDK. That cost 140 transitive
runtime packages (including `express`, `@grpc/grpc-js`, `protobufjs`, and
`node-fetch@2`) for a single call, plus a private-API poke to skip the SDK's
readiness wait and a truthy-wrapper workaround for its falsy-body handling — and
it made every SDK bump a re-verification exercise against real `daprd`. Calling
the endpoint directly removed all of it: the package now has **zero runtime
dependencies**. The wire format is pinned by the runtime tier (exact bodies,
headers, and query parameters against a fake sidecar) and by the integration
tier (the same publishes against real `daprd`).
