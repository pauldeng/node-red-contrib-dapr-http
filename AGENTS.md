# @pauldeng/node-red-contrib-dapr-http — Engineering Guide

Node-RED nodes that publish and receive Dapr pub/sub messages and invoke and
expose Dapr services through a Dapr sidecar. HTTP over TCP only. **Published
publicly** to npm as `@pauldeng/node-red-contrib-dapr-http` under the MIT license,
and listed in the Node-RED library — so the README, node help, and examples are
consumer-facing documentation, not internal notes, and a breaking change to a
node's config fields or `msg.dapr` contract is a breaking change for strangers.

This file is the single, provider-neutral source of durable instructions for
any engineer or coding agent working in this repository. `CLAUDE.md` imports it
verbatim (`@AGENTS.md`) — do not duplicate guidance elsewhere.

## Stack (pinned)

Node.js >= 22.9 · Node-RED 5.0.1 (`>=5.0.1 <6`) · Dapr runtime 1.18.1 · **zero
runtime dependencies** — every call to the sidecar goes over `node:http`
(`lib/sidecar-http.js`). Tests use the native `node:test` runner (no Mocha/Jest/
Vitest/Sinon/Supertest and no `node-red-node-test-helper`).

## Layout

- `nodes/*.js` + `nodes/*.html` — thin Node-RED wrappers (runtime + editor/help).
- `lib/*.js` — all behavior, as directly-testable modules with no Node-RED import.
- `test/unit/` — module contract tests. `test/runtime/` — real Node-RED
  child-process black-box tests. `test/integration/` — real daprd + Redis or
  NATS JetStream.
  `test/e2e/` — Playwright editor tests. `test/helpers/`, `test/fixtures/`.
- `docs/*.md` — architecture, development, testing, security, deployment,
  subscription runbook.

Create a file only when something first needs it. The layout is a target, not
permission to scaffold empty files.

There is deliberately **no versioned design or plan document**. The original
implementation plan and its review notes are local working notes, gitignored and
never shipped (see `.gitignore`). They record how the package was built, which is
history, not a contract. **This file is the contract** — anything durable belongs
here or in `docs/`. Do not add a plan/design document to the repository, and do
not cite one: a reference to a file a fresh clone does not contain is worse than
no reference.

## Commands

- `npm install` — install dependencies.
- `npm test` — native unit tests (`node --test "test/unit/**/*.test.js"`).
- `npm run test:coverage` — unit tests with native coverage thresholds on `lib/`.
- `npm run test:runtime` — real Node-RED child-process harness tests.
- `npm run test:runtime:parallel` — same runtime tests without serialized
  test-file execution, for local iteration when the host has enough headroom.
- `npm run test:integration` — real daprd + Redis/NATS JetStream Docker tests.
- `npm run test:integration:memorydb` — **optional** tier against a real AWS
  MemoryDB cluster (TLS, Redis ACL auth, cluster mode). Skips itself unless
  `MEMORYDB_ENDPOINT`/`MEMORYDB_USERNAME`/`MEMORYDB_PASSWORD` are set, because it
  needs a live cluster and VPC routing this repository cannot create. Never commit
  those credentials: pass them through the environment, never a fixture.
- `npm run test:e2e` — Playwright tests against the real Node-RED editor.
- `npm run lint` / `npm run lint:fix` — ESLint 9 (correctness + security rules).
- `npm run format` / `npm run format:check` — Prettier.

Keep this list in step with `package.json`.

## Architecture invariants (do not violate)

These are load-bearing. Each was verified against real daprd 1.18.1 rather than
assumed, and each states the failure it prevents — that reasoning is the
justification, so do not relax one because it looks incidental.

- **Dedicated app-channel listener.** Each `dapr-connection` owns a small
  `node:http` server for inbound Dapr traffic (subscription discovery, pub/sub
  delivery, service methods). Never attach Dapr routes to `RED.httpAdmin`,
  `RED.httpNode`, or the editor port — doing so would expose the Node-RED Admin
  API to the Dapr mesh (remote flow read/deploy) and let admin routes shadow
  service methods.
- **Loopback by default.** The listener binds `127.0.0.1`. A non-loopback bind is
  allowed only by explicit configuration, requires an app API token
  (`lib/options.js` fails closed without one), and must warn in the editor and
  docs.
- **One listener per connection.** Reject duplicate bind address/port with a
  clear node status, never a crash.
- **Stable delivery paths.** Derive delivery routes from persisted node/rule
  IDs, never from array position or deployment generation, so an unchanged-flow
  redeploy keeps working without restarting daprd.
- **Subscription changes need a sidecar restart.** Dapr fetches programmatic
  subscriptions once at startup and cannot refresh them in place. Fingerprint
  the subscription definition; when it changes, surface a `restart sidecar`
  status — never claim daprd is current without observing a `/dapr/subscribe`
  fetch.
- **Fail fast when the sidecar is down.** Do not queue; fail the current message
  via `done(error)` and drive status from a bounded-backoff health poll of
  `/v1.0/healthz/outbound` (outbound excludes the app channel — the right probe).
- **HTTP-only, one outbound path.** Every outbound call — publish, service
  invocation, and the health poll — goes through `lib/sidecar-http.js`, so there
  is one place where deadlines, aborts, framing, and response bounds are correct.
  Publish and invoke use Node's process-global keep-alive agent (centrally owned,
  never a per-node agent); the health poll is the only caller that passes
  `agent: false`, so a sidecar going down leaves no pooled socket behind. Do not
  add a second HTTP client or a runtime dependency to talk to the sidecar — the
  wire format is a handful of documented endpoints, and the previous `@dapr/dapr`
  dependency cost 140 transitive packages plus two workarounds for one call.
- **Bodies are bounded in both directions.** The connection's configured body
  limit caps what the app channel buffers from an inbound request _and_ what an
  outbound call accepts back — an invoked app's response is the one body an
  operator does not control. Over-size fails as `RESPONSE_TOO_LARGE`, never as
  `SIDECAR_UNAVAILABLE`: the sidecar answered.
- **Sidecar paths are built, never interpolated.** Any app id, method, pubsub
  name, or topic that reaches a URL is validated and percent-encoded
  (`buildInvokePath`, `publish`), so a `..` segment can never redirect a
  token-bearing request to another Dapr control-plane API.

## Security boundaries

- Enforce the app API token (configured credential, else `APP_API_TOKEN`) on
  discovery, delivery, and service routes; compare in constant time. `/healthz`
  stays unauthenticated for app health probes. **With no token configured the app
  channel authenticates nobody** — that is allowed only on a loopback bind, and
  the connection node warns every deploy. A non-loopback bind without a token is
  rejected outright. Say this plainly in docs and help; never write that the token
  is "required" without that qualification.
- Reject `dapr-caller-app-id` on `/dapr/subscribe` and internal delivery routes
  (a mesh caller must not treat internal endpoints as service methods). Preserve
  it for registered service methods so flows can authorize.
- The app API token authenticates daprd to the app; it does not authorize a
  caller. Caller authorization for **service invocation** requires Dapr's
  `spec.accessControl` policy — **and that policy requires mTLS to be enabled
  between sidecars.** Without mTLS, daprd cannot read a caller's identity from
  a client cert, evaluates every caller as `id: ""`, and every policy
  collapses to its `defaultAction` regardless of the caller's real app-id
  (confirmed against real daprd 1.18.1 in `test/integration/acl.test.js`).
  This package does not currently stand up mTLS/Sentry anywhere, so
  **`accessControl` is not a usable caller-authorization mechanism for
  service invocation as currently deployed** — document this gap to operators
  rather than presenting it as a working control. Wiring up mTLS (a pinned
  `daprio/sentry` service plus trust-bundle config) is future work, not yet
  implemented. Pub/sub topic authorization is a separate, mTLS-independent
  mechanism (a pubsub component's own `subscriptionScopes`/
  `publishingScopes`/`protectedTopics` metadata) — not configured by this
  package's nodes.
- Never return stack traces, tokens, Node-RED configuration, or correlation
  state over HTTP.

## Test-driven workflow

Red → green → refactor, every change: write a focused failing test, run it and
record the expected failure, implement the smallest passing change, run focused
then affected tests, format and lint. Put behavior in `lib/` modules and test
them directly; keep node wrappers thin and cover them with real Node-RED
runtime tests. Target >= 90% line/function and >= 85% branch coverage on `lib/`.

## Commits and the milestone gate

- Conventional commit subjects (`feat:`, `test:`, `docs:`, `chore:`). One
  coherent purpose per commit; never mix unrelated cleanup into a feature commit.
- **Releases publish only from CI, authenticated by GitHub OIDC** (npm trusted
  publishing, `.github/workflows/release.yml`). Never `npm login`, `npm publish`,
  or otherwise publish from a developer machine, and never add an `NPM_TOKEN` /
  `NODE_AUTH_TOKEN` secret — the release identity is the workflow's own
  short-lived OIDC token. Every user-visible change gets a `CHANGELOG.md` entry,
  and the tag must match `package.json`'s version.
- **Reaching a milestone is a hard stop.** Do not make the milestone's closing
  commit and do not start the next milestone. Present results (tests run and
  outcomes, coverage, staged diff) and wait for explicit human approval. Apply
  requested changes within the same milestone and pass the gate again.
- Before any commit: `git diff --check`, lint, format check, and the relevant
  tests must pass. Inspect the staged diff.

## Completion criteria

Before calling a change complete: unit+coverage, real Node-RED black-box, real
Dapr/Redis and NATS JetStream integration (including unchanged-flow redeploy
without restarting daprd), Playwright e2e with visual inspection of the captured
screenshots, lint + format + `git diff --check` + `npm pack --dry-run`, and
confirmation that `AGENTS.md` is the only durable AI instruction source and
`CLAUDE.md` is exactly `@AGENTS.md`. Audit policy: `npm audit
--omit=dev` must be clean; the full `npm audit` is reviewed and only the specific
advisories explicitly listed in `docs/testing.md` are permitted — any other or
newly-disclosed advisory fails until individually assessed.
