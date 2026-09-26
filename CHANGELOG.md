# Changelog

All notable changes to this package. This project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); the version in
`package.json` is what a consumer installs, so bump it and add an entry here in
the same change that ships behavior.

## Unreleased

### Added

- Actor deletion requires an own `deleteState: true` property; inherited
  properties cannot request a destructive state operation.

- Actor mode: `dapr-actor-method`, `dapr-actor-reply`, and `dapr-actor-call`
  nodes host Dapr actors in flows and call actors hosted elsewhere, over the
  existing app channel and sidecar HTTP client. One JSON `record` per actor;
  needs a Dapr Placement service and a state store configured with
  `actorStateStore: "true"`. Reentrancy is not supported.
- Actor reminders: a `dapr-actor-schedule` node sets, gets, or deletes a
  reminder; a `dapr-actor-method` node with Trigger set to Reminder receives
  every reminder of its actor type through the same gate/deadline/commit path
  as an ordinary method call, named on each firing by
  `msg.dapr.actor.trigger.name`. Needs a Dapr Scheduler service
  (`--scheduler-host-address`); reminders survive a Node-RED and daprd
  restart. Timers are not supported.
- `msg.dapr.actor.deleteState: true` on a Complete reply deletes the actor's
  entire record through the same commit path as an ordinary replacement,
  with `nextState` absent. The next invocation reports `stateExists: false`;
  deleting an already-absent record is not an error, and reminders are
  unaffected.
- The request handler, not the reply node, commits actor state, and only
  after the reply. Calls to one actor take turns; its gate and daprd's request
  stay open until the commit settles, including across a redeploy or
  shutdown, bounded by the drain timeout. A commit whose outcome cannot be confirmed fails as
  `ACTOR_COMMIT_UNKNOWN`, never as success.
- Removed actor methods and types answer a retryable 503, never a 404,
  including the gap before a deferred routing update, because daprd treats
  an actor 404 as permanent.
- Call failures keep bounded Dapr diagnostics on `msg.error.cause` for Catch.
- `examples/actor-demo.json` adapts Dapr's own SDK samples (Python
  `DemoActor`, including its reminder methods, and JavaScript
  `DemoActorCounter`; attribution in `examples/DAPR-SAMPLES.md`), with an
  actor-enabled Redis component example and setup steps (including a Dapr
  Scheduler container for the reminder Injects) in `examples/README.md`.

### Changed

- Reminder callbacks reject mesh caller headers. Schedule validation rejects
  non-boolean overwrite values and bounds a snapshot of the data, including
  Unicode escaping in the callback. Malformed get responses fail instead of
  looking like a missing reminder; editor schedule validation matches runtime.
- Closing an outbound node prevents a pending health probe from starting a
  new sidecar call. The actor demo tolerates user-supplied reminder count fields.

- Pin the Dapr integration runtime and deployment examples to 1.18.4 by
  image digest. The full real-daprd integration suite passes on 1.18.4, and
  the NATS JetStream `deadLetterTopic` stall still reproduces there.

## 0.2.1 - 2026-08-22

### Breaking

- **`dapr-ack` now requires an explicit status source:** either a fixed
  `SUCCESS`/`RETRY`/`DROP` selected in the editor, or `msg.ackStatus`. The former
  implicit `msg.dapr.status` override remains compatible only while an old 0.2.0
  node is left unedited. When editing one, choose the message source and change
  upstream Function nodes to write `msg.ackStatus`, or keep the fixed `SUCCESS`
  default selected by the editor.

### Changed

- `dapr-ack` no longer sends a green status update to every connected editor for
  every successful acknowledgement. Missing connections and rejected stale ids
  still show their low-volume diagnostic statuses.

### Fixed

- `dapr-ack` no longer stores configuration under Node-RED's own `status` key,
  which a runtime badge could replace with `{text, fill, shape}` during redeploy.
  Fixed configuration now uses `ackStatus`; non-string outcomes are rejected
  without interpolating their contents.
- **A `dapr-subscribe` consumer span now parents on the publisher's span, not on
  the subscribing sidecar's delivery span.** A single delivery took its trace
  context from the request headers, which carry the span of the sidecar
  delivering the message — a span belonging to the subscribing app itself. The
  consumer span was therefore a child of its own service, so a topology derived
  from spans (Grafana's service graph, Tempo's `service_graphs` processor) drew
  the subscribing app calling itself and the publish hop vanished entirely.
  Dapr carries the publishing side's `traceparent`/`tracestate` in the
  CloudEvent, and that is now the preferred carrier, with the delivery's own
  carrier as the fallback: request headers for a single delivery, entry
  metadata for a bulk one. Bulk deliveries already preferred their entry's
  CloudEvent, so only the single-delivery parent changes; both paths now share
  `lib/subscriptions.js` `traceCarrier`. Traces are unaffected in shape or
  content beyond the corrected parent, and disabled tracing is unaffected
  entirely. See `docs/architecture.md`'s "Telemetry" section.
- A CloudEvent carrying a `traceparent` that is not a valid W3C trace context
  no longer displaces the delivery's own. Such a value extracts to nothing, so
  a bulk delivery quietly started a fresh trace instead of continuing the
  publisher's; the delivery carrier is now used whenever the envelope's is
  unusable.

## 0.2.0 - 2026-08-15

### Added

- **First-party OpenTelemetry tracing, opt-in and off by default.** A new
  **Tracing** checkbox on `dapr-connection` activates one process-wide
  tracer shared by every connection and node — there is nothing to configure
  per connection; service identity, the OTLP endpoint, exporter
  authentication, and sampling all come from the standard `OTEL_*`
  environment variables (unset, sampling defaults to
  `parentbased_traceidratio` at `0.1`). `dapr-publish` gets a producer span,
  `dapr-invoke` a client span, `dapr-subscribe` a consumer span (single and
  bulk), and `dapr-service` a server span, each injecting or extracting the
  W3C `traceparent`/`tracestate` headers automatically. Every other node in
  a flow — not only this package's own — gets its own span too, via
  Node-RED's `onSend`/`preDeliver`/`onReceive`/`onComplete` runtime hooks.
  See `docs/architecture.md`'s "Telemetry" section.
- A stopped or unreachable OTLP collector never fails, delays, or retries a
  message: export happens off the message path entirely, and a failed
  export or a slow shutdown flush is only ever bounded and swallowed.
  Disabling tracing again leaves every other behavior, and the public
  `msg.dapr` contract, unchanged — a manually forwarded
  `msg.dapr.headers.traceparent` (the pre-tracing way to keep one trace
  across a subscribe → publish hop) still works exactly as before when
  tracing is off.

- **First-party OpenTelemetry application-log export, opt-in.** Installed
  through Node-RED's own `settings.js` `logging` configuration — not a node
  or connection field — via one stable subpath,
  `require('@pauldeng/node-red-contrib-dapr-http/logging')`, so it can be set
  up before any flow deploys and keeps exporter credentials out of flow JSON.
  Every `node.warn()`/`node.error()`/etc. call and Node-RED's own runtime logs
  export over OTLP/HTTP, sharing the same standard `OTEL_*` environment
  variables and resource identity as tracing but as an independent lease:
  enabling it never enables tracing, and disabling or redeploying a traced
  connection never stops it. A log emitted inside a traced node's own handler
  carries that span's trace and span IDs automatically. Only a bounded, safe
  subset of each entry is exported — node/flow identity, level, a bounded
  message body, and bounded exception details. It never automatically inspects
  or attaches a flow message, payload, Dapr token, request header, or arbitrary
  object shape; application-authored log text remains the operator's
  responsibility. See `docs/architecture.md`'s "Application logs" section.

- **Bulk publish on `dapr-publish`.** A new **Bulk publish** checkbox (or a
  per-message `msg.dapr.bulk` override) switches `msg.payload` to an array of
  `{entryId, payload, contentType, metadata}` entries, published through
  Dapr's stable `POST /v1.0/publish/bulk/<pubsub>/<topic>` endpoint. Single
  publish is unaffected and remains byte-for-byte compatible when bulk mode
  is disabled. Entry count, shape, `entryId` uniqueness, content type (Dapr's
  bulk endpoint accepts only `application/json`, `application/cloudevents+json`,
  `text/*`, `application/xml`, or `application/octet-stream` — stricter than
  single publish), and encoded body size are all validated as
  `INVALID_MESSAGE` before any request is sent. On success,
  `msg.dapr.bulkResult = { failedEntries: [], entryCount }` is added before
  the message is sent on; if Dapr identifies specific failed entries —
  including every entry in the batch — the node calls `done(error)` with code
  `BULK_PUBLISH_PARTIAL` and a populated `failedEntries` list instead,
  without resubmitting the batch. A bulk failure with no entries identified
  (nothing for a Catch node to retry) is reported as the existing
  `PUBLISH_FAILED`, not an empty `BULK_PUBLISH_PARTIAL`. One producer span
  covers the whole request, carrying only the batch and failure counts.

- **A new `dapr-state` node** for Dapr's State Management HTTP API: get,
  save, delete, bulk get, and transaction, selected by a configured
  **Operation** field or a per-message `msg.dapr.operation` override. `get`
  resolves `msg.payload` to the stored value (or `null` when the key does
  not exist — an ordinary outcome, not an error) and `msg.dapr.etag` to the
  store's ETag; `save`/`delete`/`transaction` pass `msg` through, adding only
  `msg.dapr`; `bulkGet` resolves `msg.payload` to the store's own per-key
  result array (`{key, data, etag, error}`). Consistency, concurrency, and
  TTL (`metadata.ttlInSeconds`, sent as a string) are validated before any
  request is sent, and an enforced stale ETag on save/delete fails with a
  distinct `STATE_ETAG_MISMATCH` (HTTP 409) rather than the generic
  `STATE_OPERATION_FAILED`, so a Catch node can reload and retry specifically.
  Dapr 1.18.2 surfaces a Redis transactional ETag conflict as a generic 500,
  so transaction retains `STATE_OPERATION_FAILED` rather than guessing from
  store-specific error text. `save` and transactional upsert do not accept a
  Buffer value (Dapr's state API is JSON-native, with no raw-bytes path the way pub/sub has
  `application/octet-stream`). Dapr's state query API remains unsupported —
  it is alpha and depends on store-specific query capabilities.

- **Two new nodes for Dapr's Configuration API: `dapr-config-get` and
  `dapr-config-subscribe`.** `dapr-config-get` is a one-shot, message-triggered
  read: `msg.payload` resolves to the store's own per-key response
  (`{key: {value, version, metadata}}`); a key absent from the store is
  simply absent from the response, not an error. `dapr-config-subscribe` is
  a deploy-time, long-lived watch over one or more keys. It emits the current
  values immediately after subscribing, then emits later changes. Unlike
  `dapr-subscribe`'s pub/sub subscriptions, it needs no sidecar restart to
  take effect, since Dapr's configuration subscribe call has no discovery
  step: the node calls it directly at deploy time and again automatically
  whenever the sidecar recovers from an outage, driven by the same health
  signal `dapr-connection` already exposes. Its status reflects
  `connecting`/`subscribed`/`retrying`/`failed`; while retrying or failed,
  no message is sent and nothing is cleared — the last-known values are
  simply left alone. A single underlying change can touch more than one
  watched key at once; real Dapr delivers one message per changed key in
  that case (each carrying every item that changed together), which this
  node passes through rather than deduplicating. On redeploy or delete, the
  node calls Dapr's unsubscribe API, bounded by a short timeout so a slow
  or unreachable sidecar never delays the redeploy itself. Verified against
  real daprd 1.18.2 with a Redis configuration store
  (`configuration.redis`): a full sidecar restart makes the resubscribe
  attempt retire the prior subscription and establish a new one; subsequent
  Redis keyspace notifications resume on the new subscription. Configuration
  callbacks are registered as internal app-channel routes, so mesh service
  callers carrying `dapr-caller-app-id` cannot forge configuration updates.

- **A new `dapr-binding-out` node for Dapr's output bindings.** Message-triggered:
  `msg.payload` is sent as the binding's own `data` field; `bindingName` and
  `operation` are configured, message-overridable fields (operation is a plain
  text field, not a closed enum — bindings define their own open-ended
  operation vocabulary). On success, `msg.payload` is the component's own
  response body and `msg.dapr = { bindingName, operation, statusCode, metadata }`,
  where `metadata` is the component's response metadata (Dapr carries it as
  `metadata.<key>` response headers; Node's own HTTP client lower-cases every
  header name, so a component-set `statusCode` key arrives as `statuscode`).
  Verified against real daprd 1.18.2 and the `bindings.http` component source:
  every failure mode this endpoint can report — an unconfigured binding name,
  an operation the binding does not support, or the component's own operation
  failing — collapses to the same `500`/`ERR_INVOKE_OUTPUT_BINDING`, with no
  dedicated not-found status the way state has for a missing key; and on an
  HTTP-type binding's non-2xx target response, the component's real response
  body is discarded by daprd's own error handling before it reaches this node.
  Both are documented plainly in the node's help rather than hidden. Input
  bindings remain out of scope: daprd probes input-binding app routes once at
  startup, so wiring one up needs a sidecar restart and a lifecycle design of
  its own.

- **A new `dapr-secret-get` node for scoped, single-secret retrieval.**
  Message-triggered: `storeName` and `key` are configured, message-overridable
  fields; the result is written to a configured message property
  (`payload` by default), never a fixed one, and that property is
  deliberately not message-overridable — letting an upstream message
  redirect where a secret lands is the wrong direction to leave open. Bulk
  secret retrieval is not exposed: Dapr's own bulk endpoint applies
  secret-scoping per returned name and silently drops denied entries rather
  than failing, a materially larger accidental-disclosure surface than one
  deny-or-allow lookup. **This node does not automatically attach the key or
  value to its status text, generated errors, or secret boundary span** — it
  deliberately never forwards daprd's response text. Generic flow telemetry
  still includes the user-authored node name, so that name must not contain a
  secret or sensitive key name. Real daprd 1.18.2 embeds the requested
  secret's key directly in its own error messages. daprd itself sees the key in
  the request path and its own logs and traces can contain it; that
  sidecar-owned telemetry is outside this package's control. Verified end to
  end against a real daprd 1.18.2 + `secretstores.local.file` component and
  real Dapr secret-scoping (`spec.secrets.scopes`): a store-scoping denial
  (`403`/`SECRET_ACCESS_DENIED`) happens before the component is ever
  called, so it never overlaps with "key not found in an otherwise-valid
  store" (`500`/`SECRET_OPERATION_FAILED`, indistinguishable from any other
  component-level failure — Dapr itself doesn't distinguish them).

- **A new "Test Connection" button on `dapr-connection`.** Calls the
  sidecar's own `GET /v1.0/metadata` against the _deployed_ connection (never
  the still-open dialog's own unsaved fields) and shows the app id, runtime
  version, the first 20 loaded component names/types, and total component and
  subscription counts. Dapr's component entries do not contain component
  configuration values, and the route excludes every other metadata field —
  including the arbitrary top-level `extended` map — plus raw bodies, tokens,
  and raw errors. The permission-guarded `RED.httpAdmin` route is separate from
  the Dapr-facing app channel, aborts when the editor disconnects or the
  connection redeploys, and is not a new palette node.

### Changed

- **The pinned Dapr runtime moved from 1.18.1 to 1.18.2**
  (`daprio/daprd:1.18.2`, re-pinned by digest). The full integration gate was
  re-run against the new runtime, so every version-specific behaviour this
  package documents still holds on 1.18.2: a missing secret answering `500`
  rather than `204`, a missing secret store answering `401`, output bindings
  collapsing every failure mode into one `500`/`ERR_INVOKE_OUTPUT_BINDING`,
  configuration resubscribe-after-restart resuming delivery, and both the
  `v1.0` and `v1.0-alpha1` unsubscribe prefixes stopping delivery.

- **Runtime dependencies are no longer zero.** The official OpenTelemetry
  packages (`@opentelemetry/api`, `api-logs`, `sdk-trace-node`, `sdk-logs`,
  `exporter-trace-otlp-http`, `exporter-logs-otlp-http`, `resources`) are now
  pinned, direct dependencies — see `AGENTS.md`'s "Stack (pinned)" for the
  policy this replaces (deliberately minimal, not zero; any other new runtime
  dependency needs the maintainer's explicit approval before it is added).
  Talking to the sidecar itself is unaffected: every publish, invoke, and
  health-poll call still goes through `lib/sidecar-http.js` alone.

## 0.1.1 - 2026-07-29

### Fixed

- **The `dapr-invoke` node's Headers field accepted values the runtime rejects.**
  Its editor validation checked only that the JSON parsed to an object, never the
  header names or values — so an illegal header name (`{"bad name":"x"}`) or a CRLF
  in a value (`{"x":"a\r\nb"}`) showed the node as valid, and then failed _every_
  message at runtime with `INVALID_MESSAGE`. The CRLF case is the header-injection
  input `lib/http-headers.js` exists to block, and the editor gave no warning. The
  field now applies the same rules: RFC 9110's token grammar for names, and the
  character range Node accepts in a value.
- `dapr-service`'s Method path rejected paths with surrounding whitespace
  (`" /orders"`) that the runtime accepts, because the runtime trims and the editor
  did not; and its validator returned valid for a blank path that the runtime
  rejects. Both now match.
- The `dapr-connection` Dapr port and App port fields rejected whitespace-padded
  numbers (`"  5  "`) the runtime accepts. They now trim. They remain deliberately
  stricter than the runtime in one respect — digits only, so exponent notation like
  `1e3` is still refused for a port.

## 0.1.0 - 2026-07-29

First public release, and the first version of this package published to npm at
all. Everything below is part of it. The _Changed_ and _Fixed_ entries describe
work done before that first publish and are kept for provenance — no earlier
version was ever released, so none of them is a change a consumer has to migrate
for.

### Added

- Seven nodes over the Dapr HTTP sidecar API: `dapr-connection`, `dapr-publish`,
  `dapr-subscribe`, `dapr-ack`, `dapr-invoke`, `dapr-service`, and
  `dapr-response` — with pub/sub CEL routing, bulk subscription, explicit
  SUCCESS/RETRY/DROP acknowledgement, and service invocation in both directions.
- Published to npm under the **MIT** license. The name is scoped, per Node-RED's
  guidance for modules first published after 2022-01-31, and `-http` names the
  transport: this package speaks only Dapr's HTTP sidecar API, and gRPC is
  permanently out of scope. `publishConfig.access` is `public` because npm
  defaults scoped packages to restricted.
- Releases publish from CI only, never from a developer machine, with provenance
  attested. Later releases authenticate via GitHub OIDC (npm trusted publishing)
  with no token anywhere; this first one could not, because npm configures trusted
  publishing on a package's own settings page and the package did not exist yet, so
  it used a single-use scope-granular token that was revoked immediately after.
- Listing in the Node-RED flow library is a separate manual submission, not an
  automatic consequence of publishing; see `docs/development.md`.
- `dapr-publish` accepts `msg.dapr.headers`, so a flow can carry request headers
  (most usefully `traceparent`) through a publish and keep one W3C trace across
  a subscribe → publish hop.
- The `dapr-connection` node logs one line per sidecar health transition, so a
  sidecar going down is visible in a headless deployment rather than only as a
  node status in an open editor.
- The connection editor warns, live, when the app-channel bind address is not
  loopback.
- `examples/memorydb-pubsub-component.yaml` — a Dapr pub/sub component for AWS
  MemoryDB, carrying the TLS, Redis ACL, and cluster-mode settings a managed
  cluster needs, with the password supplied through Dapr's own env secret store
  rather than the component file. Flows need no changes: the nodes never see the
  broker, so `examples/basic-pubsub.json` works against it unchanged. See
  `docs/deployment.md`, and `docs/testing.md` for the optional
  `npm run test:integration:memorydb` tier that verifies it against a real
  cluster (it skips itself without credentials).

### Changed

- **The supported Node floor is now `>=22.9.0`**, down from `>=24`, matching the
  minimum Node-RED 5.0.1 itself requires. `>=24` turned away users on Node 22.x
  that Node-RED fully supports, for no reason the code needed — nothing in `lib/`
  or `nodes/` uses an API newer than Node 18.11. CI now runs the unit, coverage,
  lint, format, and runtime tiers on Node 22, 24, and 26, so the floor is tested
  rather than merely declared, and a repository gate asserts `engines.node` and
  the CI matrix cannot drift apart.
- **Outbound response bodies are now bounded.** `lib/sidecar-http.js` caps what it
  buffers from the sidecar at the connection node's **Body limit** (the same
  setting that already bounded inbound requests) and tears the exchange down
  mid-read when it is exceeded. Previously an invoked app could return an
  arbitrarily large body and exhaust Node-RED's memory — the one body an operator
  does not control was the only unbounded one. Over-size fails as the new
  `RESPONSE_TOO_LARGE` code rather than `SIDECAR_UNAVAILABLE`, since the sidecar
  did answer and a retry cannot help.
- The connection node's health poll goes through `lib/sidecar-http.js` like every
  other sidecar call instead of its own inline `http.request`, so deadlines,
  framing, and response bounds are implemented once. It passes `agent: false` to
  stay out of the keep-alive pool, as the inline version did.
- **Publishing no longer uses the `@dapr/dapr` SDK.** `lib/dapr-client.js` calls
  the sidecar's HTTP pub/sub API directly through the same `node:http` path every
  other outbound call already used. The package now has **zero runtime
  dependencies** (previously 140 transitive packages, including `express`,
  `@grpc/grpc-js`, `protobufjs`, and `node-fetch@2`). Wire behavior is unchanged
  and is covered by the real-daprd integration tier.
- A non-loopback `bindAddress` now **requires** an app API token
  (`INVALID_OPTIONS` otherwise). Without one the app channel authenticates
  nobody; on loopback it stays optional but the connection node warns when it is
  unset.
- `dapr-publish` and `dapr-invoke` wait for the connection's first health probe
  before failing a message as `SIDECAR_UNAVAILABLE`, so a message sent
  immediately after deploy is no longer failed against a healthy sidecar.
- A publish still in flight when its node closes is aborted (same contract as
  `dapr-invoke`) instead of being torn down by a shared agent shutdown.
- The published tarball now contains only `lib/`, `nodes/`, `examples/`, `docs/`,
  `README.md`, `CHANGELOG.md`, and `LICENSE`.

### Fixed

- A pub/sub delivery route removed from a flow now stays retryable (503) for the
  life of the app-channel listener, instead of being cleared the first time
  anything fetches `/dapr/subscribe`. The listener cannot tell daprd's own
  startup fetch from an operator's `curl` or a monitoring probe, so clearing on
  the wrong one turned a still-stale sidecar's next delivery into a 404 — which
  Dapr treats as a permanent DROP, losing the message rather than retrying it.
  `docs/subscriptions.md` no longer suggests fetching that endpoint by hand, and
  now states what the connection node's status does and does not prove.
- Publishing with any JSON media type — `application/json; charset=utf-8`,
  `application/vnd.example+json`, or a differently-cased variant — now serializes
  object payloads as JSON. Matching the exact type string (as the SDK path also
  did) sent `[object Object]` for anything but the two bare types.
- An illegal `contentType` (CRLF injection, illegal token) is rejected as
  `INVALID_MESSAGE` before a socket is opened, instead of surfacing as a sidecar
  that could not be reached.
- A payload that cannot be serialized (BigInt, circular structure) under an
  explicit JSON content type is also `INVALID_MESSAGE` rather than
  `SIDECAR_UNAVAILABLE` — matching how `dapr-invoke` and `dapr-response` classify
  the same failure.
- The release workflow is tag-gated as a whole, so a manual dispatch from a branch
  cannot reach `npm publish` without passing the tag/version check.
- `docs/security.md` and the connection help no longer claim the app API token is
  always required; they describe what the code actually enforces.
- The connection node clears its lease reference on close, so a subscribe or
  service node closing afterwards cannot re-activate a released app-channel
  generation.
- The integration harness allocates daprd's internal gRPC port explicitly and
  retries once on port contention, instead of failing a test when daprd loses a
  port race and exits fatally.
