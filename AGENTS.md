# @pauldeng/node-red-contrib-dapr-http

Node-RED nodes that reach a Dapr sidecar over its HTTP API only: pub/sub,
service invocation, state, configuration, output bindings, and secrets.

**Published publicly** to npm and listed in the Node-RED library. The README,
node help, and examples are consumer-facing documentation, and a change to a
node's config fields or the `msg.dapr` contract breaks strangers' flows.

This file is the durable instruction source for any engineer or coding agent
here; `CLAUDE.md` imports it verbatim (`@AGENTS.md`). Keep it short — every
token loads on every request. Detail belongs in `docs/`, linked below.

## Stack

Node.js >= 22.9 · Node-RED 5.0.4 (`>=5.0.1 <6`) · Dapr runtime 1.18.2.
Tests use the native `node:test` runner — no Mocha/Jest/Vitest/Sinon/Supertest,
no `node-red-node-test-helper`.

Runtime dependencies are pinned and deliberately minimal, not zero: the
official OpenTelemetry packages back optional tracing and log export. **Any
other new runtime dependency needs the maintainer's explicit approval — propose
it and wait.**

Some comments cite `dapr/dapr` **source** at an older tag than the pinned
runtime. That is deliberate: it names the tag actually read. Bump the pin and
re-run `npm run test:integration` to re-prove the behaviour rather than editing
those citations.

## Commands

`npm test` (unit) · `npm run test:coverage` · `npm run test:runtime` (real
Node-RED child process) · `npm run test:integration` (real daprd + NATS/Redis,
serialized) · `npm run test:e2e` (Playwright) · `npm run lint` ·
`npm run format`.

`npm run test:integration:memorydb` is optional and self-skips unless
`MEMORYDB_*` are set; never commit those credentials. Keep this list in step
with `package.json`; see `docs/testing.md` for what each tier proves.

## Layout

`lib/*.js` holds all behavior as directly-testable modules with no Node-RED
import. `nodes/*.js` + `*.html` are thin wrappers over it. Coverage is scoped to
`lib/`, so logic left in a wrapper is logic no coverage gate can see.

Create a file when something first needs it. There is deliberately **no
versioned design or plan document** — anything durable belongs here or in
`docs/`. Never cite a file a fresh clone does not contain.

## Invariants — do not violate

Each is load-bearing and states a failure it prevents. **Read
`docs/invariants.md` before changing anything these touch**; the reasoning
there is the justification, so do not relax one because it looks incidental.

- Each connection owns a dedicated app-channel listener. Never attach Dapr
  routes to `RED.httpAdmin`, `RED.httpNode`, or the editor port.
- The listener binds loopback by default. Non-loopback requires an app API
  token and a warning.
- One listener per bind address/port; reject duplicates with a status, never a
  crash.
- Delivery paths derive from persisted node/rule IDs, never array position.
- Subscription changes need a sidecar restart; fingerprint them and surface a
  `restart sidecar` status.
- When the sidecar is down, fail the message via `done(error)`. Never queue.
- Every outbound sidecar call goes through `lib/sidecar-http.js`. No second HTTP
  client. Telemetry export is separate and must fail open.
- Bodies are bounded in both directions; over-size is `RESPONSE_TOO_LARGE`, not
  `SIDECAR_UNAVAILABLE`.
- Sidecar paths are built and percent-encoded, never interpolated.
- Enforce the app API token in constant time; `/healthz` stays open. With no
  token the channel authenticates nobody — never write that it is "required"
  without that qualification.
- Reject `dapr-caller-app-id` on internal routes; preserve it for service
  methods.
- Never return stack traces, tokens, Node-RED configuration, or correlation
  state over HTTP.
- Editor-support endpoints live on `RED.httpAdmin` behind the narrowest
  `RED.auth.needsPermission`, and return only bounded, curated fields.

## Workflow

Red → green → refactor: write a focused failing test, run it and record the
expected failure, implement the smallest passing change, run focused then
affected tests, format and lint. Target >= 90% line/function and >= 85% branch
coverage on `lib/`.

Conventional commit subjects (`feat:`, `test:`, `docs:`, `chore:`), one
coherent purpose each. Before any commit: `git diff --check`, lint, format
check, and the relevant tests pass; inspect the staged diff.

**Reaching a milestone is a hard stop.** Do not make the closing commit and do
not start the next milestone. Present results and wait for explicit approval.

**Releases publish only from CI via GitHub OIDC** (`.github/workflows/release.yml`).
Never `npm login`/`npm publish` locally, never add an `NPM_TOKEN` or
`NODE_AUTH_TOKEN` secret. Every user-visible change gets a `CHANGELOG.md`
entry, and the tag must match `package.json`'s version.

## Done means

Unit+coverage, real Node-RED black-box, real daprd integration (including
unchanged-flow redeploy without restarting daprd), Playwright e2e with the
screenshots actually looked at, lint + format + `git diff --check` +
`npm pack --dry-run`, and `CLAUDE.md` still exactly `@AGENTS.md`.

`npm audit --omit=dev` must be clean. The full `npm audit` is reviewed, and
only the advisories listed in `docs/testing.md` are permitted — any other or
newly-disclosed one fails until individually assessed.

## Where the detail lives

- `docs/invariants.md` — every invariant above, and the failure it prevents.
- `docs/architecture.md` — trust boundaries, listener lifecycle, telemetry.
- `docs/testing.md` — the five test tiers, what each proves, audit policy.
- `docs/security.md` — operator-facing security model.
- `docs/development.md` — setup and commit workflow.
- `docs/deployment.md` · `docs/subscriptions.md` — topology and the restart runbook.
