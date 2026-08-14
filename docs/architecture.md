# Architecture

Trust boundaries and runtime lifecycle. Layout and module list live in
`AGENTS.md`; this document explains why the pieces are shaped the way they
are.

## Two independent HTTP paths

A `dapr-connection` node manages two things that never share a listener:

- **Outbound** — this app calling the sidecar's own HTTP API
  (`lib/dapr-client.js` for pub/sub, `lib/invoke-client.js` for service
  invocation, `lib/state-client.js` for state management,
  `lib/configuration-client.js` for dynamic configuration,
  `lib/binding-client.js` for output bindings, `lib/secret-client.js` for
  scoped secret retrieval, `lib/metadata-client.js` for read-only sidecar
  metadata). Plain outgoing HTTP requests; no server or Node-RED HTTP
  infrastructure is involved on this side.
- **Inbound app channel** (`lib/app-channel.js`) — a dedicated `node:http`
  server the connection node starts itself, for the sidecar to call back into
  this app. It exposes exactly:
  - `GET /healthz` — unauthenticated app health/readiness.
  - `GET /dapr/subscribe` — the current programmatic subscription list.
  - `/node-red-dapr/subscriptions/<stable-id>` — pub/sub delivery routes.
  - Registered `dapr-service` method paths.
  - Internal `dapr-config-subscribe` callback routes
    (`POST /configuration/<store>/<key>`, one per watched key). These use
    `kind: 'internal'`, so a mesh service caller cannot address them.
  - Everything else: 404. A registered path called with the wrong verb: 405
    with an `Allow` header.

This listener is never attached to `RED.httpAdmin`, `RED.httpNode`, or the
editor port. Doing so would expose the Node-RED Admin API (remote flow
read/deploy) to the Dapr mesh, and would let admin routes shadow service
methods. See `docs/security.md` for the auth rules this listener enforces.

A completely separate thing, not a violation of the rule above: the
`RED.httpAdmin` route (`GET
/dapr-connection/:id/metadata`, guarded by
`RED.auth.needsPermission('dapr-connection.read')`) backs `dapr-connection`'s
own **Test Connection** editor button. It is Node-RED editor-support tooling,
not an app-channel route; whether the Admin API is network-reachable is an
operator deployment and `adminAuth` concern. It calls `GET /v1.0/metadata`
against the _deployed_ connection's already-resolved options, caps names,
types, and the displayed component list, and excludes every other metadata
field (including `extended`), the sidecar's raw response, tokens, and raw
errors. Browser disconnect and connection-node shutdown both abort the
outbound request.

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
nodes/dapr-state.js        state management wrapper (get/save/delete/bulk get/transaction)
nodes/dapr-config-get.js   configuration get wrapper (message-triggered)
nodes/dapr-config-subscribe.js  configuration subscribe wrapper (deploy-time, long-lived)
nodes/dapr-binding-out.js  output-binding invoke wrapper (message-triggered)
nodes/dapr-secret-get.js   scoped single-secret retrieval wrapper (message-triggered)
lib/options.js             config/env precedence and validation
lib/messages.js            content-type inference and CloudEvent conversion
lib/state-messages.js      per-message state request validation and shaping
lib/configuration-messages.js  configuration get/subscribe request validation and shaping
lib/binding-messages.js    per-message binding invoke request validation and shaping
lib/secret-messages.js     per-message secret get request validation and shaping
lib/sidecar-http.js        the one outbound HTTP request path to the sidecar
lib/dapr-client.js         pub/sub publish client (paths, metadata, serialization)
lib/invoke-client.js       outbound service-invocation client
lib/state-client.js        state management client (get/save/delete/bulk get/transaction)
lib/configuration-client.js  configuration client (get/subscribe/unsubscribe)
lib/binding-client.js      output-binding invoke client
lib/secret-client.js       secret get client (never forwards daprd's own error text)
lib/metadata-client.js     read-only sidecar metadata client (GET /v1.0/metadata)
lib/http-headers.js        header allow-listing/normalization shared by both directions
lib/app-channel.js         listener registry, router, auth, limits, sockets
lib/subscriptions.js       subscription definitions, canonical fingerprints, generations
lib/services.js            registered dapr-service method table
lib/pending.js             bounded first-wins ack/response correlation
lib/errors.js              stable internal error types and safe (no-stack-trace) messages
lib/telemetry.js           OpenTelemetry provider lifecycle, boundary spans, flow-span hooks
lib/logging-bridge.js      settings.js-installed OTel application-log bridge
```

`nodes/*.js` files are thin Node-RED wrappers; behavior lives in `lib/` as
modules with no Node-RED import, so they're directly unit-testable. See
`docs/testing.md` for how each tier exercises this split.

## Why HTTP-only, no SDK, and one keep-alive policy

Everything this package sends to the sidecar is a handful of documented HTTP
endpoints: `POST /v1.0/publish/<pubsub>/<topic>`,
`POST /v1.0/publish/bulk/<pubsub>/<topic>`,
`/v1.0/invoke/<app-id>/method/<method>`, `/v1.0/state/<store>/...`,
`/v1.0/configuration/<store>/...`, `/v1.0/bindings/<name>`,
`/v1.0/secrets/<store>/<key>`, `GET /v1.0/metadata`, and
`GET /v1.0/healthz/outbound`. All go through `lib/sidecar-http.js`, so there
is exactly one place where deadlines, aborts, Content-Length framing,
response bounds, and error mapping are implemented.

Publish, invoke, state, configuration, binding, secret, and metadata calls
ride Node's process-global keep-alive agent, so keep-alive is owned centrally
rather than per node instance. The health poll is the single caller that
opts out
(`agent: false`): it runs
every 10 s against a sidecar that may be on its way down, and a pooled
socket to a dead sidecar is only something the next poll would have to
discover is dead.

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
the endpoint directly removed all of it. The wire format is pinned by the
runtime tier (exact bodies, headers, and query parameters against a fake
sidecar) and by the integration tier (the same publishes against real
`daprd`).

This package's own runtime dependencies are the official OpenTelemetry
packages (see "Telemetry" below) — pinned and deliberately minimal, not zero:
see `AGENTS.md`'s "Stack (pinned)" for the policy. Talking to the sidecar
itself is unaffected: every publish, invoke, state, and health-poll call still goes
through `lib/sidecar-http.js` alone, with no HTTP client of its own.

## Telemetry

Opt-in, disabled by default, and process-wide once any one `dapr-connection`
enables it — there is nothing to configure per connection (`lib/telemetry.js`).
Env-var driven throughout, matching the standard OpenTelemetry SDK contract
(`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_SAMPLER*`,
...), so this package adds no configuration surface of its own beyond the one
checkbox.

If Node-RED was started with an application-owned OpenTelemetry provider, this
integration reuses it. It never replaces or shuts down a provider installed by
the host; the checkbox only manages the package-owned fallback provider.

Three layers, each independently useful:

- **Boundary spans.** A producer span on `dapr-publish`, client spans on
  `dapr-invoke`, `dapr-state`, `dapr-config-get`, `dapr-binding-out`, and
  `dapr-secret-get` (the store name only, deliberately never the secret's
  own key — generic flow spans still include the user-authored node name,
  and daprd's own tracing can record the key-bearing request path), consumer spans on
  `dapr-subscribe` (single and bulk, extracting the W3C trace context from
  the delivery's headers or, for a bulk entry, its CloudEvent with metadata
  as a fallback — bulk has no per-message HTTP headers) and
  `dapr-config-subscribe` (extracting from the callback POST's own
  headers, though real daprd is unlikely to set one today), and a server
  span on `dapr-service`. Each injects or extracts `traceparent` /
  `tracestate` through the connection's own header path, so a manually
  forwarded `msg.dapr.headers.traceparent` (the pre-tracing way to keep one
  trace across a subscribe → publish hop) still works unchanged when tracing
  is disabled — `propagation.inject()`/`extract()` are true no-ops with
  nothing registered — and is superseded by the real mechanism once enabled.
- **Flow spans.** Every node in every flow gets its own span, via the four
  Node-RED runtime hooks built for exactly this (`onSend`, `preDeliver`,
  `onReceive`, `onComplete`) — not only this package's own nodes. `onSend`
  captures the active context on each `SendEvent` (a property on the event
  object, never on `msg`); `preDeliver` re-enters it before Node-RED's own
  (by default asynchronous) delivery, so the destination node's `onReceive`
  sees the sender's context regardless of what ran in between. `onReceive`
  starts the node's span and mints a delivery token bounded by
  `lib/pending.js`'s same registry pattern the app channel already uses for
  acks and responses (a fresh, process-wide instance — a node span applies
  to every flow, not only Dapr ones); `onComplete` reads the token back and
  settles it. `_msgid` is deliberately never used as this key: Node-RED
  preserves one `_msgid` across cloned branches and successive nodes, so it
  cannot identify one node's one delivery. A node whose input handler never
  calls `done()` (most existing nodes — this needs the modern 3-argument
  `(msg, send, done)` form) has its span force-closed after a bounded timeout
  and marked `node_red.span.incomplete`, rather than left open forever.
- **Export.** Batched OTLP/HTTP, off the message path entirely — nothing a
  flow does ever awaits an export. A stopped or unreachable collector is
  fail-open at both ends: mid-operation, the SDK's own batch processor
  swallows export failures (routed to OTel's `diag` logger, never thrown);
  at shutdown, `BasicTracerProvider.shutdown()`'s flush has no timeout of its
  own, so `lib/telemetry.js` bounds and swallows it itself (3s) — otherwise
  an unreachable collector could hang a connection's close handler for as
  long as the exporter's own retry/backoff takes.

Verified against real infrastructure, not only the fake-sidecar runtime tier:
the integration tier pins `otel/opentelemetry-collector-contrib` (see
`docs/testing.md`) configured with a file exporter, and asserts on the
exported OTLP spans directly — real daprd + Redis delivery, through a plain
function node, into a real outbound bulk publish, checked for one shared trace
ID, correct span nesting, and bounded batch/failure-count attributes.

### Application logs

A second, independent lease (`lib/telemetry.js`'s `acquireLogs()`/
`getLogger()`), sharing only resource detection with the trace lease above.
Enabling it never registers a tracer or activates flow spans; disabling or
redeploying a traced connection never stops it. Unlike tracing, there is no
connection checkbox: it is installed once, before any flow deploys, through
Node-RED's own `settings.js` `logging` configuration — the supported
extension point for a custom log sink — because that is what lets it capture
Node-RED's own startup logs and keeps exporter credentials out of flow JSON.
`lib/logging-bridge.js` is the bridge: a factory Node-RED calls once
(`settings.logging.<key>.handler`, see `@node-red/util/lib/log.js`'s
`LogHandler`) that returns the function Node-RED then calls with every log
entry it decides — via its own level/audit/metrics gating — to report. The
package exposes it through one stable subpath (`package.json`'s `exports`
map, `require('@pauldeng/node-red-contrib-dapr-http/logging')`) rather than a
node, since a node cannot run before the first flow deploy.

Each Node-RED log entry (`{level, msg, [id, type, name, z]}`) is normalized,
never passed through: fatal/error/warn/info/debug/trace map deterministically
to their OTel severity; `audit`/`metric` (Node-RED-specific, off by default,
gated by the operator's own `settings.js`) both report as INFO, distinguished
by `node_red.level`. Their real Node-RED `event` field becomes the bounded OTel
event name; an audit record repeats that name as its body, while a metric uses
its bounded scalar `value` and maps `nodeid` to `node_red.id`. Metric `msgid`
and all other audit/metric fields remain excluded. Only an allowlisted set of
attributes is ever attached —
bounded `node_red.id`/`type`/`name`/`flow_id`/`level` — so an audit entry's
req-derived `msg.user`/`msg.path`/`msg.ip` is excluded by construction, not by
remembering to strip it. The body is bounded and never recurses into an
arbitrary logged object's own properties (mirroring
`@node-red/util`'s own console handler): a string is used as-is, an `Error`
contributes bounded `exception.*` attributes plus its message, and any other
object uses an own string `.message` or the fixed `[object Object]` placeholder;
arbitrary-object accessors and custom `toString()` implementations are never
invoked. This does not sanitize text an application explicitly logs: a secret
passed as a string is still a secret in the exported body, so flows must not log
one.

The handler captures Node-RED's occurrence timestamp and `context.active()`
synchronously, before asynchronous provider initialization can leave the
node's AsyncLocalStorage scope. A log inside a traced node's handler is therefore
correlated with that node's own span; an out-of-flow log gets neither. Startup
records wait in a small bounded buffer until the process-lifetime provider is
ready, after which the handler caches its Logger rather than resolving one on
every hot-path emission. Normalization, provider, and emission failures are all
fail-open and cannot become an unhandled rejection.

The provider supports `OTEL_LOGS_EXPORTER=otlp` (default) or `none`, the standard
OTLP log endpoint/header variables, `OTEL_SDK_DISABLED`, and the four
`OTEL_BLRP_*` batch-processor controls. Invalid numeric settings fall back to
the OpenTelemetry defaults, and the configured batch size is capped at the
queue size. The JavaScript Logs API/SDK remains Development status, so all log
packages are pinned exactly and upgrades must pass the complete log contract.

Since Node-RED never calls back into this bridge to release it, the lease is
process-lifetime by construction — the same subpath exports a `shutdown()`
an operator's own graceful-shutdown code can call to flush and release it
deliberately; nothing in the ordinary deploy/redeploy path does.

Verified the same way as the trace pipeline: the integration tier's collector
fixture now runs both a traces and a logs pipeline (two file exporters,
`test/helpers/otel-collector.js`), and one test proves the correlation
directly — a real `node.warn()` call inside a real traced flow span, exported
to the real collector, whose log record carries that exact span's trace and
span IDs.
