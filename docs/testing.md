# Testing

Four tiers, each with a distinct job. All use the native `node:test` runner
(plus Playwright for the editor tier). No Mocha/Jest/Vitest/Sinon/Supertest and
no `node-red-node-test-helper`.

## Tiers

| Tier        | Location            | What it proves                                                                                           | Needs                  |
| ----------- | ------------------- | -------------------------------------------------------------------------------------------------------- | ---------------------- |
| Unit        | `test/unit/`        | `lib/` module contracts in isolation.                                                                    | Node only.             |
| Runtime     | `test/runtime/`     | Real Node-RED loads, registers, wires, and runs the nodes; a fake Dapr HTTP sidecar stands in for daprd. | Node + `node-red` CLI. |
| Integration | `test/integration/` | Behavior against real daprd 1.18.1, with Redis or NATS JetStream as the pub/sub broker.                  | Docker.                |
| E2E         | `test/e2e/`         | Editor dialogs, validation, and a full publish/subscribe + invoke/service flow.                          | Playwright + Node-RED. |

Tiers are introduced by the milestone that first needs them; this file grows
with them.

Visual inspection of every node's editor dialog (light/dark, multiple viewports)
is consolidated in the E2E tier (Milestone 10). The `dapr-connection` and
`dapr-publish` dialog checks are explicitly tracked there; the owner deferred
the connection check from Milestone 3. Runtime tests confirm both nodes load.

## Running

```bash
npm test                 # unit
npm run test:runtime     # real Node-RED child-process harness
npm run test:integration # real daprd 1.18.1 via Docker (Redis and NATS JetStream)
```

(The e2e script is added with its milestone.)

`npm run test:integration` first runs `pretest:integration`
(`test/helpers/pull-images.js`), which pre-pulls all four pinned images. A
cold pull can take minutes on a fresh runner — far longer than a single
integration test's own timeout, which includes its setup — so pulling happens
once, up front, outside any individual test's clock, not lazily on whichever
test happens to need an image first.

The runtime harness (`test/helpers/node-red.js`) drives a real `node-red@5.0.1`
process in a throwaway user directory with this package symlinked into its
`node_modules`, deploys flows over the Admin API, and observes behavior over
HTTP. A fake Dapr HTTP sidecar (`test/helpers/fake-dapr.js`) stands in for daprd.

The integration harness spins up a fresh, isolated set of pinned containers
per test file via raw `docker run` (not docker-compose, so files stay
parallel-safe on dynamically allocated ports): `daprio/daprd:1.18.1`,
`redis:7.4-alpine` (`test/helpers/integration.js`), `nats:2.14.3-alpine`
(`test/helpers/nats.js` — JetStream only, started with `-js`; `pubsub.natsstreaming`
is deprecated and out of scope), and — per IMPLEMENTATION_PLAN.md's
integration-tier requirement — Node-RED itself, as the pinned
`nodered/node-red:5.0.1-24` image (`test/helpers/node-red-container.js`,
`ContainerNodeRed`), not the host child process the runtime tier uses
(`NodeRed`, `test/helpers/node-red.js` — unchanged, and still exactly what the
runtime tier runs). All four images are pinned by digest, not just tag; see
`test/helpers/docker.js` (the shared `execFileP`/`ensureImage` plumbing every
container helper uses) for the re-pin procedure.

`startDaprd()` (`test/helpers/integration.js`) is broker-agnostic: `redisPort`
is optional, and a `components` array accepts any pre-rendered Component YAML
a suite needs — the Redis pubsub component and any Resiliency/Configuration/
Subscription fixtures live in `test/integration/fixtures/` and are copied into
a fresh per-run resources directory, while `test/helpers/nats.js`'s
`jetstreamComponentYaml()` renders a NATS one directly, since JetStream needs a
pre-existing stream (`provisionStream()`) that Redis never does.
`docker-compose.yml` at the repo root is a separate, manual-use reference
stack, not what the automated tests run. Startup order matters (see below):
deploy Node-RED first, then start daprd once the app answers `/healthz`,
because daprd fetches `/dapr/subscribe` exactly once at startup.

`ContainerNodeRed` exposes the same public surface as `NodeRed`
(`start`/`deploy`/`waitForHttp`/`stop`/`logText`/`nodeUrl`) so integration test
files use it exactly like the runtime tier uses the host-process version. It
runs the container with `--network host` (same reasoning as daprd — both need
to reach `127.0.0.1:<port>` on the host's own network namespace), bind-mounts
a fresh per-run `userDir` at `/data` (matching the image's `NODE_PATH`, which
includes `/data/node_modules`), and bind-mounts this workspace read-only at
its own absolute host path so a symlink written into that `userDir` — pointing
at the workspace, exactly like the host-process harness's package-discovery
trick — resolves inside the container too.

## Conventions and diagnosis

- Test files are named `*.test.js` (unit/runtime/integration) or `*.spec.js`
  (Playwright).
- Prefer `node:assert/strict`. Use the runner's native mocks and fake timers
  rather than a mocking library.
- Coverage on `lib/`: >= 90% line/function, >= 85% branch, enforced by
  `npm run test:coverage` (Node's native `--test-coverage-*` thresholds, scoped
  to `lib/**`). Node wrappers in `nodes/` run inside the child Node-RED process,
  so they are covered behaviorally by the runtime tier, not by this line-coverage
  gate.
- **Startup ordering (integration):** deploy the Node-RED flow before starting
  daprd, and gate daprd on `GET /healthz` — daprd fetches `/dapr/subscribe` once
  at startup, so a sidecar that starts against an empty app receives nothing.
- Async failures inside flow handlers can surface as timeouts rather than
  assertion errors; assert on observable effects (HTTP calls, node status, sent
  messages), not on internal promises.
- **Post-deploy route race (runtime):** a node-served route becomes live a short
  moment after a deploy, and a full redeploy has a brief teardown gap where it
  404s. Await presence with the harness's `waitForHttp(...)`; make a single
  request (not `waitForHttp`) only when asserting a route is _absent_.
- **Prompt process exit (runtime):** the harness makes HTTP calls over
  non-pooled `node:http` connections (`agent: false`) and clears every timer it
  creates, so a test-file process exits on its own once tests finish. No
  `--test-force-exit` — that flag can hide genuine resource leaks.
- **daprd runs as non-root (integration):** the `daprio/daprd` image runs as
  UID 65532, so the resources directory and every fixture file written into it
  must be explicitly `chmod`-ed open (0755/0644) — the default `mkdtemp` mode
  and normal host file modes are unreadable to it. The `nodered/node-red` image
  runs as its own non-root `node-red` user too (UID 1000, which happens to
  match a common host dev-user UID but must not be relied on); its bind-mounted
  `userDir` is opened fully (0777) for the same reason, and because the
  container also needs to _write_ its own runtime state there.
- **Node-RED containers use `docker stop`, not `SIGINT` (integration):** the
  `nodered/node-red` image's `entrypoint.sh` traps both `SIGINT` and `SIGTERM`
  and always forwards a bare `kill` (`SIGTERM`) to the actual node-red process
  either way, and Node-RED's runtime treats `SIGTERM` the same as `SIGINT` for
  graceful shutdown — so `docker stop --time <n>` reaches the same close
  handlers the host-process tier's explicit `SIGINT` does, then escalates to
  `SIGKILL` automatically past the timeout.
- **Integration test-file concurrency is capped (integration):** `npm run
test:integration` passes `--test-concurrency=4`. Running all files at
  `node:test`'s default (higher) concurrency was observed to intermittently
  time out one test under load — containerizing Node-RED means every
  integration file now runs 2-3 containers at once instead of daprd/Redis
  alone, and this host's 6 cores can't sustain the higher default
  concurrency's peak container count reliably. Re-measure if this becomes a
  bottleneck on faster CI hardware.
- **No `--rm` on daprd containers (integration):** a container that crashes at
  startup (a bad `--config` path, a malformed fixture) would otherwise delete
  itself before `docker logs` can read why. The harness always removes
  containers explicitly on `stop()` instead.
- **One metrics port per host network (integration):** every sidecar in a test
  runs with `--network host`, so more than one daprd in the same test collides
  on the default metrics port 9090. The harness passes `--enable-metrics=false`
  unconditionally; no suite here reads metrics.
- **Dapr ACLs require mTLS, which this package does not stand up (integration):**
  `test/integration/acl.test.js` proves that without mTLS, daprd cannot read a
  caller's identity from a client cert and evaluates every caller as `id: ""` —
  every access-control policy collapses to its `defaultAction`, regardless of
  the caller's real app-id. This is a genuine Dapr constraint, confirmed
  against real daprd 1.18.1 debug logs, not a harness bug. See `AGENTS.md`'s
  security section for the operator-facing consequence; standing up
  Sentry/mTLS to test a real allow/deny split is future work.
- **`rawPayload` is two independent flags, confirmed against real daprd
  (integration):** a raw _subscription_ alone does not strip the CloudEvent
  envelope — if the publisher didn't ALSO mark the publish itself raw
  (`?metadata.rawPayload=true`), the broker message already is a full
  CloudEvent, and a raw subscription just hands that whole envelope over
  verbatim as the delivery body. Only marking both ends raw produces the
  original bytes in `data_base64` that `lib/subscriptions.js`'s
  `extractCloudEvent` comment describes. See
  `test/integration/pubsub-payloads.test.js`.
- **daprd does not validate a JSON-content-typed publish body (integration):**
  publishing a non-JSON string with `content-type: application/json` still
  succeeds (204) — daprd passes the raw text through as a plain CloudEvent
  `data` string rather than rejecting it or coercing it. `parseDelivery` must
  expose that as a plain string, not throw and not treat it as binary.
- **CEL rules must match `event.data.*`, not `event.type`/`event.source`
  (integration):** real daprd fixes a published CloudEvent's `type` to the
  constant `com.dapr.event.sent` and `source` to the publishing app-id,
  regardless of payload content — a rule matching on either never
  differentiates real published messages. `test/integration/cel-routing.test.js`
  confirms real daprd's own CEL evaluator resolves overlapping rules by order
  (first-match-wins) when matching on `event.data.<field>` instead; the
  runtime tier's CEL test delivers directly to each rule's pre-known route and
  so never exercises daprd's actual CEL parser or precedence at all.
- **Bulk redelivery targets only the failed entry, not the whole batch
  (integration):** confirmed against real daprd with a fastRetry Resiliency
  policy (`test/integration/bulk.test.js`) — when one entry in a bulk batch
  is never acked and the rest resolve SUCCESS/DROP, only the unacked entry is
  redelivered; the entries that already resolved are not sent again.
- **`deadLetterTopic` stalls with `pubsub.jetstream` in Dapr 1.18.1
  (integration, Milestone 9):** confirmed twice via real daprd debug logs —
  the runtime logs the original delivery's failure, then logs "Publishing to
  topic \<dlq\>", and logs nothing further within the bounded window each
  test/manual run waited (up to 45+ seconds in one manual run); no
  redelivery, no dead-letter message, within that window. Removing the DLQ's
  own subscriber made no difference, ruling that out as the cause. The exact
  cause inside the component is **not confirmed** — do not treat it as
  established. What IS confirmed by reading both the pinned Dapr runtime and
  this component's source: the runtime creates a 30-second context
  specifically for the dead-letter publish
  (`pkg/runtime/subscription/subscription.go`'s `deadLetterPublishTimeout`)
  and passes it in, but this component's own `Publish(ctx, ...)` accepts
  that context and never uses it when calling `js.jsc.Publish(...)` — so the
  runtime's own safety-net timeout never actually bounds this call. That
  doesn't explain what blocks the underlying NATS publish itself; that would
  need reading the `nats.go` client's own internals, which this project
  doesn't ship or maintain, and is worth reporting upstream as a reproducer
  rather than guessing further here. Not a bug in this package: the
  identical flow shape already works against Redis (Milestone 8's
  `dead-letter.test.js`). `test/integration/nats-dead-letter.test.js` proves
  the actual observed behavior (one delivery attempt, no dead-letter
  message) within a bounded wait, not a claim about what happens after it.

## Security audit

Two-part policy:

- **Package gate:** `npm audit --omit=dev` must report zero vulnerabilities. It
  covers everything the package ships.
- **Full audit review:** `npm audit` is reviewed but need not be empty. Only the
  specific advisories listed below are permitted; any advisory not on the list —
  including a newly-disclosed one reached through `node-red` — fails the review
  until it is individually assessed and added here. `node-red` is pinned
  deliberately and never shipped.

  Permitted advisories (dev-only):
  - [GHSA-86vw-mfpg-wwv9](https://github.com/advisories/GHSA-86vw-mfpg-wwv9) —
    `jsonata` < 2.2.0 resource exhaustion via `$toMillis`, reached only through
    the dev-only `node-red@5.0.1` test dependency.

Do **not** run `npm audit fix --force` — its suggested `node-red` downgrade
breaks the pin. Revisit when a Node-RED 5.x that bumps `jsonata` to >= 2.2.0
ships.
