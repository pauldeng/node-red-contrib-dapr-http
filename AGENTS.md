# node-red-contrib-dapr — Engineering Guide

Node-RED nodes that publish and receive Dapr pub/sub messages and invoke and
expose Dapr services through a Dapr sidecar. HTTP over TCP only. Private,
internal package (`"private": true`, `"license": "UNLICENSED"`).

This file is the single, provider-neutral source of durable instructions for
any engineer or coding agent working in this repository. `CLAUDE.md` imports it
verbatim (`@AGENTS.md`) — do not duplicate guidance elsewhere.

## Stack (pinned)

Node.js >= 24 · Node-RED 5.0.1 (`>=5.0.1 <6`) · Dapr runtime 1.18.1 ·
`@dapr/dapr` 3.18.0 (exact, no caret — pinned for reproducible runtime
behavior). Tests use the native `node:test` runner (no Mocha/Jest/Vitest/
Sinon/Supertest and no `node-red-node-test-helper`).

## Layout

- `nodes/*.js` + `nodes/*.html` — thin Node-RED wrappers (runtime + editor/help).
- `lib/*.js` — all behavior, as directly-testable modules with no Node-RED import.
- `test/unit/` — module contract tests. `test/runtime/` — real Node-RED
  child-process black-box tests. `test/integration/` — real daprd + Redis.
  `test/e2e/` — Playwright editor tests. `test/helpers/`, `test/fixtures/`.
- `docs/*.md` — architecture, development, testing, security, deployment,
  subscription runbook.
- `IMPLEMENTATION_PLAN.md` — the authoritative plan and milestone list.

Create a file only in the milestone that first needs it. The layout is a
target, not permission to scaffold empty files.

## Commands

- `npm install` — install dependencies.
- `npm test` — native unit tests (`node --test "test/unit/**/*.test.js"`).
- `npm run lint` / `npm run lint:fix` — ESLint 9 (correctness + security rules).
- `npm run format` / `npm run format:check` — Prettier.

Runtime, integration, and e2e test scripts are added by the milestone that
introduces each tier; keep this list in step with `package.json`.

## Architecture invariants (do not violate)

These are load-bearing. Each traces to a verified constraint recorded in
`IMPLEMENTATION_PLAN.md`.

- **Dedicated app-channel listener.** Each `dapr-connection` owns a small
  `node:http` server for inbound Dapr traffic (subscription discovery, pub/sub
  delivery, service methods). Never attach Dapr routes to `RED.httpAdmin`,
  `RED.httpNode`, or the editor port — doing so would expose the Node-RED Admin
  API to the Dapr mesh (remote flow read/deploy) and let admin routes shadow
  service methods.
- **Loopback by default.** The listener binds `127.0.0.1`. A non-loopback bind
  is allowed only by explicit configuration and must warn in the editor and docs.
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
- **HTTP-only, one keep-alive policy.** The SDK's HTTP agents are process-global;
  keep-alive is centrally owned, not per-node.

## Security boundaries

- Require the app API token (configured credential, else `APP_API_TOKEN`) on
  discovery, delivery, and service routes; compare in constant time. `/healthz`
  stays unauthenticated for app health probes.
- Reject `dapr-caller-app-id` on `/dapr/subscribe` and internal delivery routes
  (a mesh caller must not treat internal endpoints as service methods). Preserve
  it for registered service methods so flows can authorize.
- The app API token authenticates daprd to the app; it does not authorize a
  caller. Caller authorization requires Dapr access-control policies.
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
- **Reaching a milestone is a hard stop.** Do not make the milestone's closing
  commit and do not start the next milestone. Present results (tests run and
  outcomes, coverage, staged diff) and wait for explicit human approval. Apply
  requested changes within the same milestone and pass the gate again.
- Before any commit: `git diff --check`, lint, format check, and the relevant
  tests must pass. Inspect the staged diff.

## Completion criteria

See `IMPLEMENTATION_PLAN.md` §9 (Completion Gate). In short: unit+coverage,
real Node-RED black-box, real Dapr/Redis integration (including unchanged-flow
redeploy without restarting daprd), Playwright e2e with visual inspection,
lint + format + `git diff --check` + `npm audit` + `npm pack --dry-run`, and
confirmation that `AGENTS.md` is the only durable AI instruction source.
