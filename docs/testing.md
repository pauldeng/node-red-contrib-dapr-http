# Testing

Five tiers, each with a distinct job: four hermetic ones that always run, plus an
optional MemoryDB tier that needs a live AWS cluster and skips itself without
one. All use the native `node:test` runner (plus Playwright for the editor tier).
No Mocha/Jest/Vitest/Sinon/Supertest and no `node-red-node-test-helper`.

## Tiers

| Tier        | Location                                   | What it proves                                                                                           | Needs                                        |
| ----------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| Unit        | `test/unit/`                               | `lib/` module contracts in isolation.                                                                    | Node only.                                   |
| Runtime     | `test/runtime/`                            | Real Node-RED loads, registers, wires, and runs the nodes; a fake Dapr HTTP sidecar stands in for daprd. | Node + `node-red` CLI.                       |
| Integration | `test/integration/`                        | Behavior against real daprd 1.18.1, with Redis or NATS JetStream as the pub/sub broker.                  | Docker.                                      |
| E2E         | `test/e2e/`                                | Editor dialogs, validation, and a full publish/subscribe + invoke/service flow.                          | Playwright + Node-RED.                       |
| MemoryDB    | `test/integration/memorydb-pubsub.test.js` | The same chain against a real AWS MemoryDB cluster: TLS, Redis ACL auth, cluster mode.                   | **Optional** — a live cluster + VPC routing. |

Visual inspection of every node's editor dialog (light/dark, three viewports) is
consolidated in the E2E tier, which captures a screenshot per node per
theme/viewport for a human to review; `dapr-connection` and `dapr-publish` are
covered there like every other node. Runtime tests confirm each node loads.

## Running

```bash
npm test                 # unit
npm run test:runtime     # real Node-RED child-process harness
npm run test:integration # real daprd 1.18.1 via Docker (Redis and NATS JetStream)
npm run test:e2e         # Playwright tests against the real Node-RED editor

npm run test:integration:memorydb  # optional; skips unless credentials are set
```

`npm run test:integration` first runs `pretest:integration`
(`test/helpers/pull-images.js`), which pre-pulls all four pinned images. A
cold pull can take minutes on a fresh runner — far longer than a single
integration test's own timeout, which includes its setup — so pulling happens
once, up front, outside any individual test's clock, not lazily on whichever
test happens to need an image first.

## The optional MemoryDB tier

Every other tier is hermetic: it creates what it needs and destroys it. This one
cannot — AWS MemoryDB has no public endpoint, so it needs a live cluster,
credentials, and network routing into that cluster's VPC. It therefore **skips
itself** unless the environment supplies all of:

| Variable            | Notes                                           |
| ------------------- | ----------------------------------------------- |
| `MEMORYDB_ENDPOINT` | The cluster's configuration endpoint host.      |
| `MEMORYDB_PORT`     | Optional, defaults to `6379`.                   |
| `MEMORYDB_USERNAME` | A Redis ACL user with access to the topic keys. |
| `MEMORYDB_PASSWORD` | That user's password.                           |

`npm run test:integration` includes the file and reports it as skipped, so the
default gate stays green without any of this.

**Credentials never live in the repository.** They are read from the environment
only; `.gitignore` excludes `*.env` so a local credential file cannot be committed
by accident. A password containing shell metacharacters is easiest to pass by
sourcing a file rather than typing it on a command line:

```bash
cat > memorydb.env <<'ENV'
MEMORYDB_ENDPOINT=clustercfg.example.abcdef.memorydb.ap-southeast-2.amazonaws.com
MEMORYDB_USERNAME=your-acl-user
MEMORYDB_PASSWORD='the password, single-quoted so the shell leaves it alone'
ENV
set -a && . ./memorydb.env && set +a
npm run test:integration:memorydb
```

### What it proves, and what it deliberately does not

A managed cluster differs from the local Redis container in three ways, all of
which live between daprd and the broker rather than in this package's code:

- **TLS in transit** (`enableTLS: 'true'`) — MemoryDB requires it; without it the
  component never becomes ready.
- **Redis ACL authentication** — a username _and_ a password, so `redisUsername`
  is required alongside `redisPassword`.
- **Cluster mode** (`redisType: cluster`) — MemoryDB is always a cluster, and its
  `clustercfg` endpoint reports `cluster_state:ok` even with a single shard.

So this tier proves the _component configuration_ an operator needs, plus that
the whole chain (`dapr-publish` → daprd → MemoryDB → daprd → `dapr-subscribe`)
carries a CloudEvent and a raw payload correctly. It does not re-run the
broker-agnostic node matrix already covered against Redis and JetStream.

The password reaches daprd through Dapr's own env secret store
(`secretstores.local.env` plus a `secretKeyRef`), not the component YAML:
`writeResourcesDir` chmods the mounted `/components` files to `0644` so the
container user can read them, which would leave a real credential world-readable
for the life of the test. That is also the pattern an operator should use in
production.

Two things this tier learned the hard way, both encoded in
`test/helpers/memorydb.js`:

- Each run uses a **unique topic**. The cluster is shared and long-lived, so a
  fixed topic inherits an earlier run's stream, consumer group, and backlog.
- Cleanup runs **after daprd stops**, because a live Dapr subscriber recreates a
  deleted stream on its next poll. `node:test` runs `after` hooks in registration
  order, so the cleanup hook is registered last, and it verifies the keys are
  actually gone instead of swallowing the error.

The runtime harness (`test/helpers/node-red.js`) drives a real `node-red@5.0.1`
process in a throwaway user directory with this package symlinked into its
`node_modules`, deploys flows over the Admin API, and observes behavior over
HTTP. A fake Dapr HTTP sidecar (`test/helpers/fake-dapr.js`) stands in for daprd.

The integration harness spins up a fresh, isolated set of pinned containers
per test file via raw `docker run` (not docker-compose, so files stay
parallel-safe on dynamically allocated ports): `daprio/daprd:1.18.1`,
`redis:7.4-alpine` (`test/helpers/integration.js`), `nats:2.14.3-alpine`
(`test/helpers/nats.js` — JetStream only, started with `-js`; `pubsub.natsstreaming`
is deprecated and out of scope), and Node-RED itself, as the pinned
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
  container also needs to _write_ its own runtime state there. Host-side
  removal of that directory is best-effort; cleanup permission mismatches must
  not fail an otherwise completed integration test.
- **Node-RED containers use `docker stop`, not `SIGINT` (integration):** the
  `nodered/node-red` image's `entrypoint.sh` traps both `SIGINT` and `SIGTERM`
  and always forwards a bare `kill` (`SIGTERM`) to the actual node-red process
  either way, and Node-RED's runtime treats `SIGTERM` the same as `SIGINT` for
  graceful shutdown — so `docker stop --time <n>` reaches the same close
  handlers the host-process tier's explicit `SIGINT` does, then escalates to
  `SIGKILL` automatically past the timeout.
- **Integration test-file concurrency is serialized (integration):** `npm run
test:integration` passes `--test-concurrency=1`. Each file starts real Docker
  services (Node-RED plus daprd plus Redis or NATS), and GitHub-hosted runners
  were observed to fail and then hang before printing diagnostics when four
  files ran at once. Re-measure before raising this; local speed is not worth
  losing CI failure output.
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
- **CEL rules can match CloudEvent attributes or `event.data.*`
  (integration):** for ordinary publishes, real daprd fixes the generated
  CloudEvent's `type` to `com.dapr.event.sent` and `source` to the publishing
  app-id, while placing application fields under `event.data`. Differentiate
  ordinary `dapr-publish` messages with `event.data.<field>`. A producer-supplied
  custom CloudEvent can set its own `event.type`, so type-based rules remain
  valid for that case. `test/integration/cel-routing.test.js` confirms real
  daprd's own CEL evaluator resolves overlapping rules by order
  (first-match-wins); the runtime tier's CEL test delivers directly to each
  rule's pre-known route and so never exercises daprd's actual CEL parser or
  precedence at all.
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
  covers everything the package ships — which, since the package has **no runtime
  dependencies at all**, is nothing. This gate can only start failing if a runtime
  dependency is ever added.
- **Full audit review:** `npm audit` is reviewed but need not be empty. Only the
  specific advisories listed below are permitted; any advisory not on the list —
  including a newly-disclosed one reached through `node-red` — fails the review
  until it is individually assessed and added here. `node-red` is pinned
  deliberately and never shipped.

  Every permitted advisory below is reached only through a **dev-only** path
  (`node-red@5.0.1`, `eslint`, or the `npm` CLI bundled inside
  `@node-red/registry`). None of them is in the published package, and none is
  reachable from a flow at runtime: `node-red` here exists to run the runtime and
  e2e tiers, and the bundled `npm` only ever runs when the Node-RED editor
  installs a palette module, which these tests never do.

  Permitted advisories (dev-only), last reviewed 2026-07-29:

  | Advisory                                                                 | Package           | Reached through                                        |
  | ------------------------------------------------------------------------ | ----------------- | ------------------------------------------------------ |
  | [GHSA-86vw-mfpg-wwv9](https://github.com/advisories/GHSA-86vw-mfpg-wwv9) | `jsonata` < 2.2.0 | `node-red` → `@node-red/util`                          |
  | [GHSA-v422-hmwv-36x6](https://github.com/advisories/GHSA-v422-hmwv-36x6) | `body-parser`     | `node-red` → `@node-red/editor-api`, `@node-red/nodes` |
  | [GHSA-v2hh-gcrm-f6hx](https://github.com/advisories/GHSA-v2hh-gcrm-f6hx) | `fast-uri`        | `node-red` → `@node-red/nodes` → `ajv`                 |
  | [GHSA-mh99-v99m-4gvg](https://github.com/advisories/GHSA-mh99-v99m-4gvg) | `brace-expansion` | `npm` (bundled) only — see note below                  |
  | [GHSA-r292-9mhp-454m](https://github.com/advisories/GHSA-r292-9mhp-454m) | `tar`             | `node-red` → `@node-red/registry` → `npm` (bundled)    |
  | axios advisories (10, see below)                                         | `axios` 1.16.0    | `node-red` → `node-red-admin`                          |

  `brace-expansion` reaches the tree twice but is only vulnerable once. ESLint 10
  pulls a patched `5.0.8` through its own `minimatch`; the advisory applies solely
  to the `5.0.7` copy bundled inside `npm` (itself reached via `node-red` →
  `@node-red/registry`), which is why `npm audit` reports a single node under
  `node_modules/npm/`. Under ESLint 9 both copies were vulnerable — recheck this
  row rather than assuming it, if ESLint moves again.

  The `axios` advisories (GHSA-42h9-826w-cgv3, GHSA-xj6q-8x83-jv6g,
  GHSA-pmv8-rq9r-6j72, GHSA-jqh4-m9w3-8hp9, GHSA-mmx7-hfxf-jppx,
  GHSA-f4gw-2p7v-4548, GHSA-gcfj-64vw-6mp9, GHSA-hcpx-6fm6-wx23,
  GHSA-7q8q-rj6j-mhjq, GHSA-mwf2-3pr3-8698) are all reached through
  `node-red-admin`, the CLI shipped alongside `node-red`. Nothing in this
  repository invokes `node-red-admin`, and it is never installed by a consumer of
  this package.

CI runs `npm audit --omit=dev` on every push/PR **and on a weekly schedule**
(`.github/workflows/ci.yml`), so a newly-disclosed advisory surfaces without
waiting for someone to open a pull request. The full-audit review above stays
manual: it is a judgement about reachability, not something a zero-exit check can
express.

Do **not** run `npm audit fix --force` — its suggested `node-red` downgrade
breaks the pin. Revisit the whole list when a Node-RED 5.x with refreshed
transitives ships.
