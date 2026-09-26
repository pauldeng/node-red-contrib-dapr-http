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
  - With actor methods registered: `GET /dapr/config`,
    `PUT /actors/<type>/<id>/method/<method>` (including the reminder callback
    shape `method/remind/<name>`, routed to the one node registered with
    `trigger: 'reminder'` for that type), and the deactivation callback
    `DELETE /actors/<type>/<id>`. Removed registrations remain retryable;
    see "Actor request ownership" below.
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
them; there is no refresh endpoint in Dapr 1.18.2. A `dapr-connection` node
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
nodes/dapr-ack.js          acknowledgement wrapper (status resolution in lib/ack.js)
nodes/dapr-invoke.js       outbound invocation wrapper
nodes/dapr-service.js      inbound service wrapper
nodes/dapr-response.js     invocation response wrapper
nodes/dapr-state.js        state management wrapper (get/save/delete/bulk get/transaction)
nodes/dapr-config-get.js   configuration get wrapper (message-triggered)
nodes/dapr-config-subscribe.js  configuration subscribe wrapper (deploy-time, long-lived)
nodes/dapr-binding-out.js  output-binding invoke wrapper (message-triggered)
nodes/dapr-secret-get.js   scoped single-secret retrieval wrapper (message-triggered)
nodes/dapr-actor-method.js inbound actor method registration (or, trigger:'reminder', every reminder of a type)
nodes/dapr-actor-reply.js  validates and settles an actor reply proposal
nodes/dapr-actor-call.js   outbound actor invocation wrapper
nodes/dapr-actor-schedule.js  set/get/delete one actor reminder
lib/actor-host.js          actor turn ownership, state read, reply wait and commit
lib/actor-client.js        sidecar actor invocation, record storage, and reminder set/get/delete APIs
lib/actor-messages.js      actor identity, reply proposal, and reminder schedule validation
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
see `AGENTS.md`'s "Stack" for the policy. Talking to the sidecar
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

  Why the CloudEvent wins over the headers: the two carriers name different
  spans. A delivery's request headers carry the _subscribing_ sidecar's own
  delivery span, while the CloudEvent carries the _publishing_ side's span.
  Parenting a consumer span on the headers therefore puts both ends of the hop
  inside the subscribing app, and anything that derives a topology from spans —
  Grafana's service graph, Tempo's `service_graphs` processor — draws that app
  calling itself instead of the hop that actually happened. Parenting on the
  CloudEvent makes the consumer span a child of the publisher's span; the trace
  is the same either way, only the parentage differs.

  The envelope is remote data while the delivery carrier came from the local
  sidecar, so the envelope only wins where its `traceparent` actually parses as
  a W3C trace context. An unusable one extracts to nothing, which would start a
  fresh trace rather than continue the publisher's — strictly worse than the
  misparenting the preference exists to correct. `test/integration/nats-trace-carrier.test.js`
  pins the daprd behaviour the whole preference rests on.

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

## Actor request ownership

`lib/actor-host.js` owns each invocation from state read through commit. The
reply node submits a serialized proposal; only the handler writes the actor's
`record`. State reads and flow work share an absolute deadline, leaving a
bounded commit reserve. Expiry or drain prevents new flow work, and every
exit removes the pending reply and captured identity. An already-started
commit retains its per-actor gate until the local HTTP operation settles.

A commit timeout does not prove that storage rejected the write. The response
margin is a budget, not a guarantee against event-loop stalls or forced close.
`dapr-connection`'s close handler awaits `actorHost.whenCommitsSettled()`
(after `drain()`, before releasing the app-channel lease), so a caller whose
commit had already started gets its real outcome -- 200, or a definite/
`ACTOR_COMMIT_UNKNOWN` failure -- rather than the release path's generic
"connection restarting" 503, on both a redeploy and a full shutdown. That wait
is itself bounded by `limits.drainTimeoutMs` as a backstop: a commit that
never settles (a stalled sidecar call past its own timeout, an event-loop
stall, or a forced close) still cannot hold the redeploy/shutdown open
indefinitely, and in that case the caller does get the release path's generic
503 with its commit's true outcome unknown.

`lib/app-channel.js` tombstones a dropped (actorType, method) pair, and a
whole actor type left with no method at all, the same way it already
tombstones a removed pub/sub delivery path (`staleRoutes`): daprd treats a
404 from an actor method PUT as a permanent "not found" and never retries
it, so a registration this connection drops -- even only for the moment
between one node's own close and the connection's own reactivation during a
redeploy -- must answer retryable (503), never 404. A tombstone is bounded by
registrations ever made (never by actor ids), persists for this listener's
whole life across reacquire, and is cleared only when a real registration
wins it back.

The live method registry removes a closing node before the next coalesced
activation. During that gap, a missing handler for a previously installed
pair also answers 503; waiting for the tombstone alone would briefly return
a permanent 404.

A daprd replay (only under a declared actor retry policy) that overlaps a live
handler for the same actor gets `ACTOR_BUSY` and starts no second read or
commit. One that arrives after the handler finished is an ordinary new turn,
so application methods must tolerate a repeated call.

### Actor wire behavior

Observed against real daprd 1.18.4 by `test/integration/actors-probe.test.js`,
which asserts each point so a pin bump that changes one fails:

- `/dapr/config` is served only while an actor method is registered, so
  connections without actors are unchanged. Actor-type changes need a sidecar
  restart and surface a `restart sidecar` status; "no actors" never warns.
- Method callbacks (`PUT /actors/{type}/{id}/method/{method}`) carry
  `dapr-caller-app-id` and are accepted with it; `/dapr/config` and the
  deactivation `DELETE /actors/{type}/{id}` do not carry it and keep the
  internal-route rule that rejects it. The header is not exposed to flows and
  is not caller authorization.
- daprd treats only 200 as method success. An app 500, 503 or 404 reaches the
  caller as a 500 `ERR_ACTOR_INVOKE_METHOD`, with the app status only inside
  the message text, and is not retried by default.
- An absent `record` reads as 204; a stored `null` reads as 200 `null`, which
  is how `stateExists` tells them apart.
- A save through a sidecar that no longer hosts the actor fails with 400
  `ERR_ACTOR_INSTANCE_MISSING`. Method calls and state need Placement but not
  the Scheduler service.

### Actor reminders

Observed against real daprd 1.18.4 + Placement + Scheduler by
`test/integration/actors-schedule-probe.test.js` (raw wire protocol) and
`test/integration/actors-reminders.test.js` (the shipped nodes end to end).
Timers exist on the same wire protocol but are not exposed by this package
(no node schedules one); the probe covers them only to document their
existence, not to support them.

- **Reminder callback routing shares actor invocation entirely.** `lib/app-channel.js`
  parses `PUT /actors/{type}/{id}/method/remind/{name}` into
  `{ shape: 'reminder', type, id, name }` and hands it to the exact same
  `lib/actor-host.js` turn (gate, deadline, commit) as an ordinary method
  call; only the registry key and the emitted trigger identity differ. One
  `dapr-actor-method` node's `trigger: 'reminder'` registers under
  `lib/actor-messages.js`'s `REMINDER_METHOD` (`'remind/'`) instead of a
  configured method name — a key `validateActorSegment` can never accept for
  an ordinary method (it contains `/`), so a plain method registration can
  never collide with, or be forged as, the reminder registration; there is
  exactly one such registration per actor type on a connection (a duplicate is
  rejected the same way a duplicate ordinary method pair is).
- **The callback body is daprd's own fixed envelope**, never the raw
  scheduling call's argument: `{"data":<value>,"dueTime":"","period":""}`,
  with `data` omitted entirely when the reminder was created with none.
  `lib/actor-host.js` unwraps `data` onto `msg.payload` and sets
  `msg.dapr.actor.trigger = { kind: 'reminder', name }` (also mirrored onto
  `msg.dapr.actor.method`) from the parsed route — never from this body — so
  a forged envelope cannot spoof which reminder fired. A reminder callback
  never carries `dapr-caller-app-id`, whether delivered locally or forwarded
  to the replica hosting the actor (`actors-replicas-probe.test.js`), and
  carries `traceparent` only sometimes. Reminder routes reject `dapr-caller-app-id` as internal
  callbacks; ordinary actor method calls still accept it.
- **A failing, expired, or commit-unknown reminder occurrence is retried by
  daprd itself**, up to 3 more times about 1 second apart, before it gives up
  on that occurrence and waits for the next one — reminder flows must
  tolerate being run more than once for the same firing. Nothing disables a
  reminder that keeps failing.
- **Setting an existing name with `overwrite: true` (the default) resets the
  whole schedule**, timing the next firing from the new `dueTime` rather than
  the original one; `overwrite: false` against an existing name fails with
  409 `ERR_ACTOR_REMINDER_ALREADY_EXISTS` instead. Delete is idempotent (204
  whether or not the name existed). Get resolves the reminder Dapr reports
  or, specifically for a 404 body carrying `ERR_ACTOR_REMINDER_NOT_FOUND`,
  `{ found: false }`; any other non-2xx is an ordinary confirmed failure.
- **Reminder management must go to a sidecar that hosts the target actor
  type**; any other sidecar answers 403 `ERR_ACTOR_REMINDER_NON_HOSTED` for
  set/get alike. A remote app reaches a reminder by calling an actor method
  that sets it locally, not by addressing this package's schedule node at
  another app's sidecar.
- **Setting a reminder inside a method flow is not transactional with that
  flow.** It takes effect as soon as the sidecar call completes and is never
  rolled back by a later reply or commit failure — unlike
  `dapr.actor.nextState`, there is no proposal/commit step for a schedule
  call to participate in.
- The reply's `X-DaprReminderCancel` header is recognized by daprd's
  app-dispatch layer but not wired to the Scheduler on 1.18.4 (a known
  upstream TODO): a reminder keeps firing on schedule regardless of a reply's
  outcome. This package's reply node exposes no "cancel" outcome for that
  reason; stop a reminder with a schedule delete instead.

### Actor limits

- There is no storage-level fence. The actor HTTP API does not expose the
  store's ETag, so a write already accepted by the store can still land after
  its turn ended during rebalance or forced close. Dapr's hosted-instance check
  and the local gate reduce this window; they do not close it.
- A caller-side timeout or `ACTOR_COMMIT_UNKNOWN` does not prove the write did
  not happen. The next call reads the authoritative record.
- A method flow must finish within the connection's request timeout minus the
  commit reserve. Long delays and retries belong outside the method flow;
  scheduling a reminder from inside one is an ordinary bounded sidecar call,
  but the reminder's own callback always runs as a later, separate turn.

Actor-call errors retain the stable `ACTOR_INVOKE_FAILED` code and generic
message. For non-2xx responses, `Error.cause` carries `statusCode` and any
string `errorCode`/`message` from Dapr, capped at 128/2048 characters. Node-RED
5's Catch preserves this as `msg.error.cause`. Dapr's message can contain an
actor fail envelope, but its wording is remote diagnostic text, not a stable
business-error protocol; it is never automatically echoed by the actor host.
