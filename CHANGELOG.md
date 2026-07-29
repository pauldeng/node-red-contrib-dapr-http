# Changelog

All notable changes to this package. This project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); the version in
`package.json` is what a consumer installs, so bump it and add an entry here in
the same change that ships behavior.

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
