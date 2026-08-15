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
`SIDECAR_UNAVAILABLE`: the sidecar answered.

**Sidecar paths are built, never interpolated.** Any app id, method, pubsub
name, topic, state store/key, configuration store, binding name, or secret
store/key that reaches a URL is validated and percent-encoded
(`buildInvokePath`, `publish`, `buildStatePath`, `buildConfigurationPath`,
`buildBindingPath`, `buildSecretPath`), so a `..` segment can never redirect a
token-bearing request to another Dapr control-plane API. `GET /v1.0/metadata`
is the one exception with no dynamic segment at all — nothing to interpolate.

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
