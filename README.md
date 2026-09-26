# @pauldeng/node-red-contrib-dapr-http

Node-RED nodes for Dapr pub/sub, service invocation, state management,
dynamic configuration, output bindings, and secrets, talking to a Dapr
sidecar over its HTTP API only, with optional first-party OpenTelemetry
tracing and application-log export.

## Prerequisites

- Node.js >= 22.9 (Node-RED 5's own minimum). Tested on 22, 24, and 26.
- Node-RED `>=5.0.1 <6`.
- A Dapr sidecar (`daprd`) reachable from this app, and this app reachable
  from that sidecar — see [Listener topology](#listener-topology) below.

## Install

From the Node-RED editor: **menu → Manage palette → Install**, then search for
`dapr` and pick `@pauldeng/node-red-contrib-dapr-http`.

Or from your Node-RED user directory (typically `~/.node-red`):

```bash
npm install @pauldeng/node-red-contrib-dapr-http
```

Then restart Node-RED. The nodes below appear in the palette under
**network**; `dapr-connection` is a config node, created from another node's
own dialog rather than dragged from the palette.

| Node                    | Purpose                                                                                  |
| ----------------------- | ---------------------------------------------------------------------------------------- |
| `dapr-connection`       | Shared config node: one sidecar connection.                                              |
| `dapr-publish`          | Publish a message to a pub/sub topic.                                                    |
| `dapr-subscribe`        | Receive messages for a pub/sub topic, with CEL routing rules and optional bulk delivery. |
| `dapr-ack`              | Explicitly acknowledge a subscribed message (SUCCESS/RETRY/DROP).                        |
| `dapr-invoke`           | Call a method on another Dapr app-id.                                                    |
| `dapr-service`          | Expose a method other Dapr apps can invoke.                                              |
| `dapr-response`         | Reply to a `dapr-service` invocation.                                                    |
| `dapr-state`            | Get, save, delete, bulk get, or transactionally update state-store values.               |
| `dapr-config-get`       | Read one or more values from a configuration store.                                      |
| `dapr-config-subscribe` | Watch configuration keys and emit a message whenever any of them change.                 |
| `dapr-binding-out`      | Invoke an output binding with a component-specific operation.                            |
| `dapr-secret-get`       | Read one scoped secret from a secret store.                                              |
| `dapr-actor-method`     | Register an actor method; emits its argument and loaded state.                           |
| `dapr-actor-reply`      | Settle an actor method's response and optional next state.                               |
| `dapr-actor-call`       | Invoke an actor method through the local sidecar.                                        |

## Scope

Supported Dapr building blocks, over HTTP only. Every call to the sidecar is
a plain `node:http` request; the only runtime dependencies are the pinned
official OpenTelemetry packages used by optional tracing and application-log
export:

- **Pub/sub** — publish (single and bulk), subscribe (CEL routing rules, bulk
  delivery, raw payloads, dead-letter topics), and explicit SUCCESS/RETRY/DROP
  acknowledgement.
- **Service invocation** — calling another app-id's method, and exposing methods
  other apps can call.
- **State management** — get, save, delete, bulk get, and transaction, with
  ETag-based optimistic concurrency. Dapr's state query API is not
  supported: it remains alpha and depends on store-specific query
  capabilities.
- **Dynamic configuration** — a one-shot get, and a long-lived subscribe with
  its own connecting/subscribed/retrying/failed state, driven by the same
  sidecar health signal every other node already uses. A subscription emits
  the current values once it connects, retires its previous subscription after
  a health recovery, and then resumes change delivery with a fresh subscription.
- **Output bindings** — invoke a binding component with an explicit,
  component-specific operation. Input bindings are not covered: daprd probes
  input-binding app routes at startup, so wiring one up needs a sidecar
  restart and a lifecycle design of its own.
- **Scoped secret retrieval** — one secret at a time, by store and key.
  Bulk secret retrieval is not exposed: Dapr's own bulk endpoint applies
  secret-scoping per returned name and silently drops denied entries rather
  than failing, an unnecessarily large accidental-disclosure surface for a
  Debug node or message branch. This node does not automatically attach the
  key or value to its status, generated errors, or secret boundary span, and
  never forwards daprd's response text. The user-authored node name appears in
  generic flow telemetry, so keep secret names out of that name. daprd itself
  still receives the key in the request path;
  daprd's own logs and traces can contain the secret key, so configure those
  sinks as sensitive data too. The returned value is part of the flow message:
  do not send it to Debug or application logs.

- **Actors** — one JSON `record` per actor, read and replaced whole. A method
  node registers an actor type/method and emits the caller's argument plus the
  loaded record; a reply node proposes the response and, optionally, the
  replacement record (the reply node never touches storage itself — the
  request handler commits after the reply is accepted); a call node invokes a
  method on any actor through the local sidecar. Needs a Dapr Placement
  service and a state-store component configured with
  `actorStateStore: "true"`. See `examples/actor-demo.json`.

Deliberately **not** covered: input bindings, bulk secrets, actor reminders,
timers, and reentrancy, workflows, distributed lock, jobs, the conversation
API, and gRPC transport.
A flow that needs one of those can reach it with a `dapr-invoke` node or a
core `http request` node against the sidecar's own API in the meantime.

## OpenTelemetry tracing

Tracing is disabled by default. Enable **Tracing** on any `dapr-connection` to
export Dapr publish, subscribe, invoke, service, state, configuration,
output-binding, and secret-get boundary spans (the secret-get span carries
only the store name, never the secret's own key) plus spans for the
Node-RED nodes traversed by each message.
The integration propagates
W3C `traceparent` and `tracestate` automatically and uses standard `OTEL_*`
environment variables for service identity, the OTLP/HTTP collector endpoint,
authentication, and sampling. If Node-RED already owns a global OpenTelemetry
provider, the nodes reuse it without replacing or shutting it down.

Export is batched and off the message path; an unavailable collector never
changes message delivery. See [Telemetry](docs/architecture.md#telemetry) for
the lifecycle and span model.

## OpenTelemetry application logs

A separate opt-in, installed through Node-RED's own `settings.js`
`logging` configuration rather than a node or connection field — that keeps
exporter credentials out of flow JSON and lets it capture Node-RED's own
startup logs before any flow deploys:

```js
// settings.js
module.exports = {
  // ...
  logging: {
    console: { level: 'info' }, // unaffected; keep or drop independently
    otel: {
      level: 'info', // Node-RED's own gating: fatal/error/warn/info/debug/trace
      metrics: false, // opt in to export RED.log metric() events too
      audit: false, // opt in to export audit events too
      handler: require('@pauldeng/node-red-contrib-dapr-http/logging'),
    },
  },
};
```

Every `node.warn()`/`node.error()`/etc. call and Node-RED's own runtime logs
export over OTLP/HTTP using the same standard `OTEL_*` environment variables
as tracing, and reuse the same resource identity — but logging is an
independent lease: enabling it never enables tracing or flow spans, and
disabling or redeploying a traced connection never stops it. A log emitted
from inside a traced node's own handler carries that span's trace and span
IDs automatically; an out-of-flow log (including everything logged before the
first flow deploys) has neither. Only a bounded, safe subset of each log
entry is exported — node id/type/name, flow id, level, a bounded message body,
and bounded exception details for an `Error`. The bridge never inspects or
automatically attaches a flow message, payload, Dapr token, request header, or
arbitrary object shape. Log text is still application-authored: explicitly
logging a secret string exports that string, so flows must not log secrets.

`OTEL_LOGS_EXPORTER` supports `otlp` (the default) and `none`;
`OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` and `OTEL_EXPORTER_OTLP_LOGS_HEADERS`
configure the OTLP/HTTP destination, and `OTEL_BLRP_*` controls the bounded
batch queue, batch size, schedule delay, and export timeout. `OTEL_SDK_DISABLED`
also disables export. Records produced during asynchronous provider startup use
a bounded startup buffer and retain their original Node-RED timestamp and
active trace context.

The OpenTelemetry JavaScript Logs API/SDK is still classified as Development,
so its packages are pinned exactly and are upgraded only with the complete log
contract passing.

This lease is process-lifetime: nothing in the ordinary deploy/redeploy path
releases it. An operator managing their own graceful shutdown can call
`require('@pauldeng/node-red-contrib-dapr-http/logging').shutdown()` to flush
and release it deliberately.

## A minimal flow

Import `examples/basic-pubsub.json` (menu → Import → paste or select the
file) for the smallest complete flow: publish a message, receive it back on
the same topic. `examples/README.md` has the NATS JetStream + `daprd`
quickstart. The example flows are in `examples/`:

- `basic-pubsub.json` — publish and subscribe.
- `cel-routing.json` — subscribe with CEL routing rules.
- `bulk-acknowledgement.json` — bulk subscribe with per-entry acknowledgement.
- `outbound-invocation.json` — call another app-id's method.
- `inbound-service.json` — expose a method and reply with `dapr-response`.
- `state-management.json` — save a value to a state store, then read it back.
- `dynamic-configuration.json` — read and watch a dynamic configuration value.
- `output-binding.json` — invoke an output binding and inspect its response.
- `secret-get.json` — read one scoped secret into a message property.
- `actor-demo.json` — two actor types modelled on Dapr's own SDK samples:
  `DemoActor` (`SetMyData` / `GetMyData`) and `DemoActorCounter` (`count` /
  `countBy` / `getCounter`).

`examples/nats-jetstream-pubsub-component.yaml` is the beginner pub/sub
component. `examples/memorydb-pubsub-component.yaml` is for AWS MemoryDB
operators. `examples/redis-statestore-component.yaml` backs
`state-management.json`. These are Dapr **components**, not flows: the nodes
never see the broker or store, so a flow works against any component of the
matching name/type unchanged. See `docs/deployment.md`.

## NATS JetStream

The JetStream stream must exist before the component is used; see
`docs/deployment.md` for the repository's provisioning command. With the
pinned Dapr 1.18.4 runtime, do not use `deadLetterTopic` with NATS JetStream:
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

A **Test Connection** button on the `dapr-connection` dialog calls the
sidecar's own `GET /v1.0/metadata` and shows the app id, runtime version, the
first 20 loaded component names/types, and total component and subscription
counts — it tests the _deployed_ connection, not whatever is currently typed
in the still-open dialog. Only that bounded set is shown; arbitrary extended
metadata, the sidecar's raw response, an API token, and raw error text never
reach the editor.

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

- `docs/invariants.md` — the rules this package will not break, and why.
- `docs/architecture.md` — how the pieces fit together.
- `docs/security.md` — tokens, ACLs, and the mTLS gap in detail.
- `docs/deployment.md` — container/sidecar topology recipes.
- `docs/subscriptions.md` — when a subscription change needs a sidecar
  restart, and how to do it safely.
- `docs/development.md` — setup, commands, and the commit workflow.
- `docs/testing.md` — the test tiers and what each one covers.
- `AGENTS.md` — the durable engineering guide this project follows.
