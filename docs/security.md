# Security

Tokens, caller identity, request limits, and the honest gap in caller
authorization. See `docs/architecture.md` for how the app channel listener
fits into the rest of the system.

## App API token

When an app API token is configured (credential on the `dapr-connection` node,
else `APP_API_TOKEN`), every app-channel route except `GET /healthz` requires
it, compared with `crypto.timingSafeEqual` rather than `===` so a timing
side-channel can't leak the token byte by byte. `/healthz` stays
unauthenticated because container/orchestrator health probes generally can't
attach a token.

**With no token configured, the app channel authenticates nobody.** Subscription
discovery (`GET /dapr/subscribe`) answers with the full subscription set, and
anything that can reach the listener can POST a forged CloudEvent to a delivery
route and inject it into a flow. That is why:

- a **non-loopback** bind without a token is rejected outright at deploy
  (`INVALID_OPTIONS` from `lib/options.js`) — the listener never starts;
- on **loopback** it is allowed, because the reachable set is this host only and
  local development needs a zero-config path, but the connection node logs a
  warning on every deploy so it is never a silent default.

Treat "loopback and untokenized" as trusting every process in the host or pod
network namespace — which, in the documented topology, includes anything sharing
a namespace with `daprd`. Set the token for anything beyond local development.

This token authenticates **daprd to the app** — it proves the caller knows a
secret shared with this app's operator. It does not authorize a specific
calling application; anyone who reaches this listener with the right token
gets in, regardless of which Dapr app-id they claim to be. Caller
authorization is a separate mechanism (see below).

## `dapr-caller-app-id`

The sidecar attaches a `dapr-caller-app-id` header identifying who's calling.
This package treats it differently depending on the route:

- **Rejected** on `/dapr/subscribe`, internal pub/sub delivery routes
  (`/node-red-dapr/subscriptions/...`), and dynamic-configuration callback
  routes (`/configuration/<store>/<key>`). These are daprd-to-app internal
  plumbing, not service methods a mesh caller may address directly.
- **Preserved** on registered `dapr-service` method routes and handed to the
  flow as `msg.dapr.callerAppId`, so a flow can make its own authorization
  decision (e.g. only honor specific app-ids for a sensitive method).

## Dapr access-control — two separate mechanisms

Dapr has two independent authorization mechanisms, for two independent
things. Neither is configured by this package's nodes — both live in the
Dapr Configuration/Component resources an operator deploys alongside it.

### Service invocation: `accessControl` — currently not usable here

`spec.accessControl` in the Dapr Configuration CRD is the documented way to
authorize _which app-id_ may invoke a `dapr-service` method. It only works
when mTLS is enabled between sidecars: without it, daprd cannot read a
caller's identity from a client certificate, evaluates every caller as
`id: ""`, and every policy collapses to its `defaultAction` regardless of the
caller's real app-id. This was confirmed against real daprd 1.18.1 in
`test/integration/acl.test.js`.

This package does not stand up mTLS or a Sentry service anywhere, so
**`accessControl` is not a usable caller-authorization mechanism for service
invocation as currently deployed**. Until mTLS is wired up (a pinned
`daprio/sentry` service plus trust-bundle config — not yet implemented), rely
on network-level controls instead: keep the app-channel listener on
loopback, restrict which sidecars can reach this host/port, and use the app
API token to keep unrelated callers out entirely.

### Pub/sub: component topic scopes — a separate, mTLS-independent mechanism

`accessControl` does **not** govern `dapr-publish`/`dapr-subscribe` at all —
pub/sub authorization is a different mechanism entirely: the pubsub
component's own `subscriptionScopes`, `publishingScopes`, and
`protectedTopics` metadata fields, set in the Component YAML an operator
deploys (not by this package, and not reachable from the Node-RED editor).
Unlike `accessControl`, these do not require mTLS — they're evaluated from
each request's own app-id, independent of the invocation ACL/mTLS gap above.
If topic-level authorization matters, configure these on the pubsub
component directly; this package's nodes have no visibility into or control
over them.

## Non-loopback binding

The app-channel listener binds `127.0.0.1` by default. A non-loopback bind is
only reachable through explicit configuration, and:

- the editor shows a warning next to the field as soon as the value stops being
  loopback (`127.0.0.0/8`, `localhost`, `::1`);
- the runtime **requires** an app API token for it, and refuses to start the
  listener without one;
- binding beyond loopback still exposes the listener to whatever can reach that
  interface, so pair it with the network-level controls described above.

`0.0.0.0` is not loopback: it binds every interface, including public ones.

## Request limits

Before buffering any request data, the listener enforces:

- a header-count limit (`server.maxHeadersCount`);
- a header-size limit (`maxHeaderSize`);
- a body-size limit, rejecting oversized bodies with `BODY_TOO_LARGE` before
  reading the rest of the stream;
- a request-duration timeout, after which the handler is aborted and the
  socket destroyed rather than left half-open.

On shutdown, the registry tracks open sockets and drains in-flight requests
for a bounded window before destroying whatever's left — a slow or stalled
client can't block a redeploy indefinitely.

## What's never returned over HTTP

Stack traces, tokens, Node-RED configuration, and internal correlation state
(pending ack/response ids) never appear in an HTTP response body or header —
errors are mapped through `lib/errors.js` to stable, safe messages before
they reach a client.
