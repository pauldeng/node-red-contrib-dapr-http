# Invariants and why each exists

`AGENTS.md` states these rules in one line each, because a rule an agent has
not loaded is a rule it will break. This file holds the reasoning: what each
invariant prevents, and what went wrong when it was not there.

The reasoning is the justification. Do not relax an invariant because it looks
incidental — read the failure it prevents first. Every one was verified against
a real daprd rather than assumed.

## Architecture

**Dedicated app-channel listener.** Each `dapr-connection` owns a small
`node:http` server for inbound Dapr traffic (subscription discovery, pub/sub
delivery, service methods). Never attach Dapr routes to `RED.httpAdmin`,
`RED.httpNode`, or the editor port — doing so would expose the Node-RED Admin
API to the Dapr mesh (remote flow read/deploy) and let admin routes shadow
service methods.

**Loopback by default.** The listener binds `127.0.0.1`. A non-loopback bind is
allowed only by explicit configuration, requires an app API token
(`lib/options.js` fails closed without one), and must warn in the editor and
docs.

**One listener per connection.** Reject a duplicate bind address/port with a
clear node status, never a crash.

**Stable delivery paths.** Derive delivery routes from persisted node/rule IDs,
never from array position or deployment generation, so an unchanged-flow
redeploy keeps working without restarting daprd.

**Subscription changes need a sidecar restart.** Dapr fetches programmatic
subscriptions once at startup and cannot refresh them in place. Fingerprint the
subscription definition; when it changes, surface a `restart sidecar` status —
never claim daprd is current without observing a `/dapr/subscribe` fetch.

**Fail fast when the sidecar is down.** Do not queue; fail the current message
via `done(error)` and drive status from a bounded-backoff health poll of
`/v1.0/healthz/outbound` (outbound excludes the app channel — the right probe).

**HTTP-only, one outbound path.** Every outbound call — publish, service
invocation, state management, dynamic configuration, output bindings, scoped
secret retrieval, sidecar metadata, and the health poll — goes through
`lib/sidecar-http.js`, so there is one place where deadlines, aborts, framing,
and response bounds are correct. Those calls use Node's process-global
keep-alive agent (centrally owned, never a per-node agent); the health poll is
the only caller that passes `agent: false`, so a sidecar going down leaves no
pooled socket behind. Do not add a second HTTP client or a runtime dependency
to talk to the sidecar — the wire format is a handful of documented endpoints,
and the previous `@dapr/dapr` dependency cost 140 transitive packages plus two
workarounds for one call.

OTLP span and application-log export (when enabled) use separate,
telemetry-only egress paths to a collector, not to the sidecar, so they do not
run through `lib/sidecar-http.js` — but they must still fail open: an exporter
or collector failure never fails, delays, or retries a Node-RED message.

**Bodies are bounded in both directions.** The connection's configured body
limit caps what the app channel buffers from an inbound request _and_ what an
outbound call accepts back — an invoked app's response is the one body an
operator does not control. Over-size fails as `RESPONSE_TOO_LARGE`, never as
`SIDECAR_UNAVAILABLE`: the sidecar answered. Actor reply proposals also bound
the response and replacement record individually before settlement, so an
oversized response cannot trigger a state write first.

**Sidecar paths are built, never interpolated.** Any app id, method, pubsub
name, topic, state store/key, configuration store, binding name, or secret
store/key that reaches a URL is validated and percent-encoded
(`buildInvokePath`, `publish`, `buildStatePath`, `buildConfigurationPath`,
`buildBindingPath`, `buildSecretPath`), so a `..` segment can never redirect a
token-bearing request to another Dapr control-plane API. `GET /v1.0/metadata`
is the one exception with no dynamic segment at all — nothing to interpolate.

**Node configuration never uses a key Node-RED itself writes.** `dapr-ack`
originally stored its acknowledgement outcome under `status`. The editor assigns a
node's runtime badge (`{text, fill, shape}`) to that same property and then exports
every `defaults` key off that same object, so a redeploy serialised the badge over
the configured value: every acknowledgement failed with
`invalid ack status: [object Object]`, deliveries redelivered until `maxDeliver`
was exhausted, and the editor still showed a valid selection throughout. The field
is `ackStatus` as of 0.2.1, paired with `ackStatusSource` so a flow states whether
the outcome is that fixed value or `msg.ackStatus`. Nothing declares `status` any
more, so a badge can never be exported as configuration; the runtime honours a
legacy string only while `ackStatusSource` is absent, and treats a badge object
there as the historical `SUCCESS` default. The failure this prevents is the worst
shape available — an acknowledgement path that looks correct in the editor and
silently stops settling deliveries.

A successful acknowledgement deliberately sets no badge at all: that would publish
one status event per delivery to every connected editor. The rare
missing-connection and stale-id diagnostics remain.

## Actors

**Only the actor request handler writes actor state, and only after the
reply.** The reply node validates and serializes a proposal; `lib/actor-host.js`
commits it. A reply node or branch that wrote directly could commit after the
caller had already seen a timeout, or two branches could both write.

**The per-actor gate and daprd's request stay open until the commit settles.**
daprd releases an actor's turn when the app's request ends. Answering before
the commit settles, whether on a caller disconnect, a redeploy or a shutdown,
would let the next turn for that actor run while the old write is still
landing. The commit never receives the caller's abort signal, and the
connection's close awaits started commits, bounded by the drain timeout.

**One deadline covers the whole turn.** The state read and the flow wait end at
the request deadline minus a commit reserve; the commit gets the reserve minus
a margin. This reserves time to answer before the app-channel watchdog;
event-loop stalls or forced close can still interrupt the response with an
unknown commit outcome.

**Removed actor registrations answer 503, never 404.** daprd treats an actor
404 as a permanent "method not found". A method or type dropped even for the
moment between two nodes' closes during a redeploy is tombstoned, like a
removed delivery route.

**Actor discovery exists only when actors do.** `/dapr/config` and the
`/actors` namespace are claimed only while an actor method is registered, so
existing service routes and connections without actors behave exactly as
before, and "no actors" never raises a restart warning. The one exception is a
tombstoned actor type: its own paths keep answering 503 after its last method
is removed; every other `/actors` path falls through to service routes.

**A reminder callback is an ordinary actor turn, under one reserved key that
no method name can ever equal.** It shares the exact same gate, deadline, and
reply/commit path as a method call — there is no second commit mechanism to
keep consistent with the first. One `dapr-actor-method` node's
`trigger: 'reminder'` registers under a fixed internal key
(`lib/actor-messages.js`'s `REMINDER_METHOD`) that `validateActorSegment`
rejects for any configured method name, so an ordinary registration can never
collide with, or be forged as, the reminder registration.

## Security

**App API token.** Enforce it (configured credential, else `APP_API_TOKEN`) on
discovery, delivery, and service routes; compare in constant time. `/healthz`
stays unauthenticated for app health probes. **With no token configured the app
channel authenticates nobody** — allowed only on a loopback bind, and the
connection node warns every deploy. A non-loopback bind without a token is
rejected outright. Say this plainly in docs and help; never write that the
token is "required" without that qualification.

**Caller header.** Reject `dapr-caller-app-id` on `/dapr/subscribe` and
internal delivery routes — a mesh caller must not treat internal endpoints as
service methods. Preserve it for registered service methods so flows can
authorize.

**The token authenticates daprd to the app; it does not authorize a caller.**
Caller authorization for service invocation requires Dapr's `spec.accessControl`
policy — **and that policy requires mTLS between sidecars.** Without mTLS, daprd
cannot read a caller's identity from a client cert, evaluates every caller as
`id: ""`, and every policy collapses to its `defaultAction` regardless of the
caller's real app-id (confirmed against real daprd in
`test/integration/acl.test.js`). This package does not stand up mTLS/Sentry, so
**`accessControl` is not a usable caller-authorization mechanism as currently
deployed** — document that gap to operators rather than presenting it as a
working control. Wiring up mTLS (a pinned `daprio/sentry` service plus
trust-bundle config) is future work. Pub/sub topic authorization is a separate,
mTLS-independent mechanism (a pubsub component's own `subscriptionScopes` /
`publishingScopes` / `protectedTopics` metadata), not configured by these nodes.

**Never return internals over HTTP.** No stack traces, tokens, Node-RED
configuration, or correlation state.

**Editor-support endpoints** belong on `RED.httpAdmin`, guarded by the
narrowest `RED.auth.needsPermission` permission. Validate that a path id
resolves to the expected deployed node type, bound every returned
collection/string, and abort outbound work when the browser disconnects or the
owning node closes. `dapr-connection`'s "Test Connection" endpoint follows that
rule: it never accepts unsaved credentials from the editor and returns only app
id, runtime version, bounded component names/types, and counts. Dapr's
component metadata entries do not contain component configuration values, while
its top-level `extended` map can contain arbitrary operator-written values;
exclude that map and every other raw metadata field.
