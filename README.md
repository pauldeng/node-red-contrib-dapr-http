# node-red-contrib-dapr

Node-RED nodes for Dapr pub/sub and service invocation, talking to a Dapr
sidecar over its HTTP API only. Private, internal package — not published to
npm.

## Prerequisites

- Node.js >= 24.
- Node-RED `>=5.0.1 <6`.
- A Dapr sidecar (`daprd`) reachable from this app, and this app reachable
  from that sidecar — see [Listener topology](#listener-topology) below.

## Install

This is a private, unpublished package — `npm install node-red-contrib-dapr`
will 404. Install it from a local path instead, run from your Node-RED user
directory (typically `~/.node-red`):

```bash
npm install /path/to/node-red-contrib-dapr
```

Then restart Node-RED. Six of the seven nodes below appear in the palette
under **network**; `dapr-connection` is a config node, created from another
node's own dialog rather than dragged from the palette.

| Node              | Purpose                                                                                  |
| ----------------- | ---------------------------------------------------------------------------------------- |
| `dapr-connection` | Shared config node: one sidecar connection.                                              |
| `dapr-publish`    | Publish a message to a pub/sub topic.                                                    |
| `dapr-subscribe`  | Receive messages for a pub/sub topic, with CEL routing rules and optional bulk delivery. |
| `dapr-ack`        | Explicitly acknowledge a subscribed message (SUCCESS/RETRY/DROP).                        |
| `dapr-invoke`     | Call a method on another Dapr app-id.                                                    |
| `dapr-service`    | Expose a method other Dapr apps can invoke.                                              |
| `dapr-response`   | Reply to a `dapr-service` invocation.                                                    |

## Scope

Two Dapr building blocks, over HTTP only, with **zero runtime dependencies** —
every call to the sidecar is a plain `node:http` request:

- **Pub/sub** — publish, subscribe (CEL routing rules, bulk delivery, raw
  payloads, dead-letter topics), and explicit SUCCESS/RETRY/DROP
  acknowledgement.
- **Service invocation** — calling another app-id's method, and exposing methods
  other apps can call.

Deliberately **not** covered: state management, bindings, secrets,
configuration, actors, workflows, distributed lock, jobs, the conversation API,
gRPC transport, and bulk _publish_ (bulk _subscribe_ is supported; the bulk
publish API is still alpha in Dapr 1.18.1). A flow that needs one of those can
reach it with a `dapr-invoke` node or a core `http request` node against the
sidecar's own API in the meantime.

## A minimal flow

Import `examples/basic-pubsub.json` (menu → Import → paste or select the
file) for the smallest complete flow: publish a message, receive it back on
the same topic. `examples/README.md` has the NATS JetStream + `daprd`
quickstart. All five example flows are in `examples/`:

- `basic-pubsub.json` — publish and subscribe.
- `cel-routing.json` — subscribe with CEL routing rules.
- `bulk-acknowledgement.json` — bulk subscribe with per-entry acknowledgement.
- `outbound-invocation.json` — call another app-id's method.
- `inbound-service.json` — expose a method and reply with `dapr-response`.

`examples/nats-jetstream-pubsub-component.yaml` is the beginner pub/sub
component. `examples/memorydb-pubsub-component.yaml` is for AWS MemoryDB
operators. These are Dapr **components**, not flows: the nodes never see the
broker, so `basic-pubsub.json` works against either component unchanged. See
`docs/deployment.md`.

## NATS JetStream

The JetStream stream must exist before the component is used; see
`docs/deployment.md` for the repository's provisioning command. With the
pinned Dapr 1.18.1 runtime, do not use `deadLetterTopic` with NATS JetStream:
the real integration test observes delivery stall before the dead-letter
message arrives. Use Redis or broker-side dead-letter handling instead;
details are in `docs/subscriptions.md`.

## Listener topology

Each `dapr-connection` node owns two independent HTTP paths, configured on
the same config node:

- **Outbound** (this app → sidecar): the sidecar's own HTTP API. Leave Dapr
  host/port blank to use the sidecar's `DAPR_HTTP_ENDPOINT` environment
  discovery; otherwise they default to `127.0.0.1:3500`.
- **Inbound app channel** (sidecar → this app): a small, dedicated
  `node:http` listener this node starts itself for subscription discovery,
  pub/sub delivery, and service methods. It is never attached to the
  Node-RED editor port or Admin API — the Dapr mesh can never reach Node-RED's
  own flow-deploy endpoints through it. Point the sidecar's `--app-port` flag
  at this node's **app port** (default `3000`). The **bind address** defaults
  to `127.0.0.1`; only change it to something non-loopback if the sidecar
  runs on a different host, and pair that with an app API token (see below)
  plus network-level access controls, since a non-loopback bind exposes the
  listener beyond this host.

For daprd running as a separate container, the simplest topology puts daprd
in Node-RED's own network namespace (Docker's `network_mode: service:<name>`
or `--network host`) so both processes can reach each other over `127.0.0.1`.
See `docs/deployment.md` for worked examples.

## Tokens

Two independent tokens, each set either as a credential on the
`dapr-connection` node or via an environment variable:

- **Dapr API token** (credential, else `DAPR_API_TOKEN`) — optional; sent on
  outbound calls to the sidecar, if the sidecar requires one.
- **App API token** (credential, else `APP_API_TOKEN`) — enforced on every
  inbound request except `/healthz` when it is set. It authenticates the
  sidecar to the app; it does **not** authorize a calling application.

  **If it is not set, the app channel authenticates nobody**: anything that can
  reach the listener can read the subscription list and post deliveries into
  flows. That is allowed only on a loopback bind (and the connection node warns
  on every deploy); a non-loopback bind without a token is rejected and the
  listener does not start. Set it for anything past local development.

## Dapr access-control guidance

Two independent Dapr mechanisms, for two independent things — neither
configured by this package's nodes:

- **Service invocation** (`dapr-invoke`/`dapr-service`): Dapr's
  `spec.accessControl` policy authorizes _which app-id_ may call a method,
  but only works when mTLS is enabled between sidecars — without it, daprd
  can't read a caller's identity from a client certificate and evaluates
  every caller as `id: ""`. This package does not stand up mTLS/Sentry, so
  **`accessControl` is not currently a usable caller-authorization
  mechanism** with a default deployment. Use network-level controls instead
  until mTLS is configured.
- **Pub/sub** (`dapr-publish`/`dapr-subscribe`): topic access is controlled
  by the pubsub component's own `subscriptionScopes`, `publishingScopes`,
  and `protectedTopics` metadata (set in the component YAML, not by this
  package) — a completely separate mechanism from `accessControl` that does
  not need mTLS.

See `docs/security.md` for the full picture.

## More documentation

- `docs/architecture.md` — how the pieces fit together.
- `docs/security.md` — tokens, ACLs, and the mTLS gap in detail.
- `docs/deployment.md` — container/sidecar topology recipes.
- `docs/subscriptions.md` — when a subscription change needs a sidecar
  restart, and how to do it safely.
- `docs/development.md` — setup, commands, and the commit workflow.
- `docs/testing.md` — the test tiers and what each one covers.
- `AGENTS.md` — the durable engineering guide this project follows.
