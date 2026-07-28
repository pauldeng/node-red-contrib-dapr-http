# Changelog

All notable changes to this package. This project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); the version in
`package.json` is what a consumer installs, so bump it and add an entry here in
the same change that ships behavior.

## Unreleased

### Added

- `dapr-publish` accepts `msg.dapr.headers`, so a flow can carry request headers
  (most usefully `traceparent`) through a publish and keep one W3C trace across
  a subscribe → publish hop.
- The `dapr-connection` node logs one line per sidecar health transition, so a
  sidecar going down is visible in a headless deployment rather than only as a
  node status in an open editor.
- The connection editor warns, live, when the app-channel bind address is not
  loopback.

### Changed

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

## 0.1.0

- Initial internal release: `dapr-connection`, `dapr-publish`, `dapr-subscribe`,
  `dapr-ack`, `dapr-invoke`, `dapr-service`, and `dapr-response` nodes over the
  Dapr HTTP sidecar API, with pub/sub CEL routing, bulk subscription, explicit
  acknowledgement, and service invocation in both directions.
