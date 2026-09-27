# Testing

Five tiers, each with a distinct job: four hermetic ones that always run, plus an
optional MemoryDB tier that needs a live AWS cluster and skips itself without
one. All use the native `node:test` runner (plus Playwright for the editor tier).
No Mocha/Jest/Vitest/Sinon/Supertest and no `node-red-node-test-helper`.

## Tiers

| Tier        | Location                                   | What it proves                                                                                                                       | Needs                                        |
| ----------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------- |
| Unit        | `test/unit/`                               | `lib/` module contracts in isolation.                                                                                                | Node only.                                   |
| Runtime     | `test/runtime/`                            | Real Node-RED loads, registers, wires, and runs the nodes; a fake Dapr HTTP sidecar stands in for daprd.                             | Node + `node-red` CLI.                       |
| Integration | `test/integration/`                        | Behavior against real daprd 1.18.4, with NATS JetStream as the primary pub/sub broker and Redis as a secondary compatibility target. | Docker.                                      |
| E2E         | `test/e2e/`                                | Editor dialogs, validation, and a full publish/subscribe + invoke/service flow.                                                      | Playwright + Node-RED.                       |
| MemoryDB    | `test/integration/memorydb-pubsub.test.js` | The same chain against a real AWS MemoryDB cluster: TLS, Redis ACL auth, cluster mode.                                               | **Optional** — a live cluster + VPC routing. |

Visual inspection of every node's editor dialog (light/dark, three viewports) is
consolidated in the E2E tier, which captures a screenshot per node per
theme/viewport for a human to review; `dapr-connection` and `dapr-publish` are
covered there like every other node. Runtime tests confirm each node loads.

## Running

```bash
npm test                 # unit
npm run test:runtime     # real Node-RED child-process harness
npm run test:integration # the complete serialized gate: NATS, daprd, Redis, then actors
npm run test:e2e         # Playwright tests against the real Node-RED editor

npm run test:integration:nats      # NATS JetStream-backed tests only (test/integration/nats-*.test.js)
npm run test:integration:dapr      # real daprd, no broker at all (service invocation, ACL, shutdown, output bindings, secrets, metadata)
npm run test:integration:redis     # Redis compatibility: pub/sub, retry, dead letter, API-token publish, state management, dynamic configuration
npm run test:integration:actors    # Placement + Scheduler + actor-flagged Redis store: wire probes, the actor-demo example, and reminders end to end
npm run test:integration:memorydb  # optional; skips unless credentials are set
```

`npm run test:integration` chains the four focused scripts above, NATS
first, then a trailing invocation for the two files that belong to neither
bucket — `telemetry.test.js`'s broker-free trace/log correlation test and
`memorydb-pubsub.test.js`'s self-gated optional one. Each `--test-concurrency=1`
sub-invocation stays fully serialized, so the chain as a whole is too; running
a focused script on its own during local iteration is faster and just as
serialized. `npm run test:integration` (only) first runs `pretest:integration`
(`test/helpers/pull-images.js`), which pre-pulls all seven pinned images,
including the Placement and Scheduler services the actor integration tier
needs. A
cold pull can take minutes on a fresh runner — far longer than a single
integration test's own timeout, which includes its setup — so pulling happens
once, up front, outside any individual test's clock, not lazily on whichever
test happens to need an image first. A focused script run standalone still
works correctly without it — each container helper calls `ensureImage()`
itself — it just risks a slow cold pull counting against that test's own
timeout instead. Every image is pinned by tag and digest, so a cached copy is
already the exact pinned content. Never select a Dapr version with `latest`:
a patch on an older release line can move that tag backwards.

`test/integration/state.test.js` lives in the Redis bucket for a different
reason than the pub/sub tests beside it: state management has no
primary/secondary broker split the way pub/sub does (NATS JetStream vs.
Redis) — it needs exactly one backing store, and Redis is the one this
package already has pinned and Docker-tested. It proves `dapr-state`'s five
operations against a real `state.redis` component: a save/get round trip's
real ETag, genuine 409s on stale-etag save/delete, the generic 500 daprd 1.18.2
returns for a transactional ETag conflict, bulk get, and an atomic transaction.

`test/integration/configuration.test.js` lives in the same bucket for the
same reason — dynamic configuration has no primary/secondary broker split
either, and needs Redis's own keyspace notifications enabled
(`startRedis({ notifyKeyspaceEvents: 'KEA' })`) for `configuration.redis`'s
push-based Subscribe to fire at all. It proves a real save-then-get round
trip against Redis, a real push driven by Redis's own keyspace
notifications (not an artifact of this package's own code), and the
initial snapshot for values that existed before daprd started. It waits until
Node-RED actually observes a sidecar outage before restarting daprd, then proves
that a fresh subscription id receives both the current snapshot and later
updates. Finally, it exercises `v1.0` and `v1.0-alpha1` unsubscribe against two
independently live subscriptions and mutates Redis afterward, proving both
prefixes stop delivery rather than merely returning HTTP 200.

`test/integration/dapr-building-blocks.test.js` is the no-broker bucket beside
`acl.test.js`/`shutdown.test.js`: service invocation, output bindings,
secrets, and Test Connection metadata all need no store or broker at all, so
they share one `ContainerNodeRed` + one daprd session and run as sequential
subtests instead of four separate container sessions (merged from the former
`invoke-matrix.test.js`, `binding-out.test.js`, `secret-get.test.js`, and
`metadata.test.js`). Its invoke subtest proves the full invocation matrix —
every supported verb, query strings, headers, a byte-exact binary
request/response, a non-2xx response forwarded verbatim, an outbound
per-message timeout override against a genuinely slow real service, and an
unknown method's real 404. Its binding subtest proves a real
`dapr-binding-out` invoke reaches a plain HTTP target (`bindings.http`'s own
`url` metadata, pointed at this suite's `startCapture()` server) as a genuine
POST (asserted from the capture server's own independently-observed received
body, not this package's code), that the component's real response
round-trips back through `msg.payload`/`msg.dapr`, and that an unconfigured
binding name produces a real `500`/`ERR_INVOKE_OUTPUT_BINDING` from real
daprd 1.18.2 — confirming the source-level finding that daprd gives no
dedicated not-found status for this endpoint (see
`nodes/dapr-binding-out.html`). Its secrets subtest proves the three real,
distinct outcomes daprd 1.18.2 actually produces against a real
`secretstores.local.file` component pointed at a static test fixture
(`test/integration/fixtures/secrets.json`), plus a real Dapr Configuration
resource (`test/integration/fixtures/secret-scopes.yaml`) exercising real
secret-scoping (`spec.secrets.scopes`) -- allowed-and-present (`200`),
allowed-but-absent (`500`/`SECRET_OPERATION_FAILED`), and denied-by-scope
(`403`/`SECRET_ACCESS_DENIED`, which happens before the component is ever
called and so never overlaps with "absent") -- and asserts that daprd's own
real error messages, which embed the requested key by name, never reach the
flow's own HTTP response. That assertion is this node's whole reason to
exist: `nodes/dapr-secret-get.html` documents the same guarantee against a
fake sidecar at the unit and runtime tiers, but only a real daprd response
proves the sanitization holds against the actual wire text, not an assumed
shape. Its metadata subtest verifies that Test Connection reports the real
app id, runtime version, and counts while exposing none of daprd's other
metadata fields; because this daprd registers both the secret store AND the
output-binding component (the other two subtests each need one), the
expected component list is the combined pair, sorted by name for the
assertion since real daprd's own metadata component order is not a
documented contract this package pins a test to.

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

The runtime harness (`test/helpers/node-red.js`) drives the package.json-pinned
`node-red` process in a throwaway user directory with this package symlinked into its
`node_modules`, deploys flows over the Admin API, and observes behavior over
HTTP. A fake Dapr HTTP sidecar (`test/helpers/fake-dapr.js`) stands in for daprd.

The integration harness spins up a fresh, isolated set of pinned containers
per test file via raw `docker run` (not docker-compose, so files stay
parallel-safe on dynamically allocated ports): `daprio/daprd:1.18.4`,
`redis:8.10-alpine` (`test/helpers/integration.js`), `nats:2.15.0-alpine`
(`test/helpers/nats.js` — JetStream only, started with `-js`; `pubsub.natsstreaming`
is deprecated and out of scope), Node-RED itself, as the pinned
`nodered/node-red:5.0.7-24` image (`test/helpers/node-red-container.js`,
`ContainerNodeRed`), not the host child process the runtime tier uses
(`NodeRed`, `test/helpers/node-red.js` — unchanged, and still exactly what the
runtime tier runs), and `otel/opentelemetry-collector-contrib:0.161.0`
(`test/helpers/otel-collector.js`) for the telemetry integration tests —
configured with one OTLP/HTTP receiver feeding two file exporters (traces and
logs, each on its own host-mounted temp file), so those tests assert on the
exported spans and log records directly instead of a tracing backend or
console-log scraping — including one test that checks a real exported log
record's trace/span IDs against the real span it was emitted inside. All five
images are pinned by digest, not just tag; see `test/helpers/docker.js` (the
shared `execFileP`/`ensureImage` plumbing every container helper uses) for the
re-pin procedure.

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

## Actor tests

Actor behavior is split by the cheapest tier that can prove each point:

- **Unit** (`actor-host`, `actor-messages`, `actor-client`, `app-channel-actors`):
  one gate per actor, first reply wins, no write after expiry or pre-commit
  disconnect, commit unknown vs definite failure, close awaiting started
  commits, tombstones, path parsing and the proposal validation matrix; plus
  the milestone 6 observability additions in the same files (span parentage,
  attributes and exactly-once end on every outcome; `watchActive`'s active
  count including a still-committing handler; the `ACTOR_COMMIT_UNKNOWN`/
  `ACTOR_TOMBSTONE_HIT` diagnostics and their fail-open contract),
  `warn-throttle.test.js` (per-code rate limiting and the suppressed count),
  `status-throttle.test.js` (the generic once-per-interval pusher, including
  a dropped-to-zero trailing value), and `connection-status.test.js`'s/
  `metadata-client.test.js`'s actor-readiness warning text and curation.
  `actor-runtime-monitor.test.js` verifies single-flight polling, stale-result
  rejection across registration changes, health failure, and close.
- **Runtime** (`test/runtime/actors.test.js`, fake sidecar with the save held
  on a deferred): no 200 before the save, a full redeploy mid-commit still
  answers the real outcome with exactly one save, method-only and reply-only
  deploys, an unavailable store and its `ACTOR_COMMIT_UNKNOWN` diagnostic
  line, and a bounded shutdown; `test/runtime/telemetry.test.js` proves
  `dapr-actor-call`/`dapr-actor-schedule` inject a real traceparent;
  `test/runtime/connection-metadata.test.js` proves the piggybacked
  `actorRuntime` fetch never reaches the "Test Connection" admin route.
- **Real daprd** (`npm run test:integration:actors`): `actors-probe.test.js`
  pins daprd's actor wire behavior with a raw `node:http` host (see
  `docs/architecture.md`, "Actor wire behavior"); `actors-schedule-probe.test.js`
  pins the reminder/timer wire protocol the same way (envelope shape,
  overwrite/schedule-reset semantics, non-hosted 403, retry cadence, TTL
  expiry, idle-deactivation survival — see "Actor reminders"); `actors.test.js`
  runs the shipped `examples/actor-demo.json` through Placement and an
  actor-flagged Redis store, including an unchanged-flow redeploy without
  restarting daprd, a failing call through the call node with
  `msg.error.cause`, concurrent same-actor updates with no lost update, and a
  Node-RED plus daprd restart that reads the stored state back;
  `actors-reminders.test.js` proves the shipped `dapr-actor-schedule` and
  reminder-trigger `dapr-actor-method` nodes end to end against a real
  Scheduler — set/one-shot/delete, survival across a Node-RED plus daprd
  restart, and a bounded retry count for an always-failing reminder flow;
  `actors-replicas-probe.test.js` runs two replicas of one app and proves a
  reminder forwarded to the replica hosting its actor never carries
  `dapr-caller-app-id`, which the reminder route rejects;
  `actors-telemetry.test.js` proves the actor SERVER span against real daprd:
  a caller-supplied `traceparent` reaches daprd's own actor-invoke API, daprd
  forwards it unchanged to the method callback, and the exported span shares
  that trace ID with the caller's own span as its parent. The Function and
  reply spans retain that ancestry, and read/commit spans remain direct
  children of the actor server span.
- **E2E**: the four actor dialogs, their validators, and importing and
  deploying the example.

Overload beyond the fixed 1,000-handler admission budget is proved at the unit
tier only; a runtime test at that scale would not be representative.

For a small repeatable overhead observation, run
`node test/helpers/actor-host-benchmark.js`. It alternates tracing off/on,
warms each configuration, then executes 4,096 successful read-and-save turns
at concurrency 32 with fake asynchronous storage. Tracing uses an always-on
sampler and a bounded in-memory exporter (three spans per turn). On
2026-09-27 with Node 26.8.1, two measured passes took 51.8–53.0 ms with tracing
off and 133.4–167.6 ms with tracing on. Integration tests were also running on
the host. These are handler overhead observations, not end-to-end latency or
fleet capacity estimates: HTTP, Node-RED, real storage and OTLP export are
excluded. There is no timing assertion or release threshold.

## Test cost and consolidation

CI and release run the complete unit suite once through `test:coverage`;
`npm test` remains the faster local command without coverage instrumentation.
The browser dialog loops check validation while capturing the same light/dark,
three-viewport screenshots, instead of opening every dialog twice. The real
Dapr invocation matrix covers successful outbound calls as well as inbound
verbs, preserving the assertions formerly in a second container fixture.

State, binding, secret-get and configuration-get response tables share one
Node-RED process per file. Each case gets fresh flows and a fresh fake sidecar;
flows are removed before that sidecar closes. Startup/shutdown, redeploy-abort,
credentials and global-telemetry cases still own separate processes. HTTP
callers in abort tests cancel only after observing the sidecar cancellation,
avoiding an unrelated five-second client timeout. Most trace-propagation tests
use a local OTLP receiver; one unreachable-exporter case retains that coverage.
Negative delivery windows, deadline tests, and broker-specific integration
cases remain: similar assertions at those boundaries catch different failures.
The container harness binds its editor to loopback on an OS-assigned port and
reads the bound port from Node-RED's startup log, avoiding the gap between a
free-port probe and container startup. Dapr app-channel ports remain explicit.

On the review host (Node 26.8.1), the serial runtime tier fell from 239.6 s
for 152 cases to 183.5 s for 151; the removed case's assertion remains in
another case. Browser runs with three workers fell from 42 cases / 2.2 min to
40 / 2.0 min. These are single-run observations under concurrent test load,
not performance thresholds. Library coverage remained 98.33% lines, 95.43%
branches and 98.53% functions.

A later consolidation pass removed ten more runtime-tier fake-sidecar cases
(`state.test.js`, `secret-get.test.js`, `binding-out.test.js`,
`config-get.test.js`, `invoke.test.js`) whose happy-path assertions are
already proven, more strongly, against a real daprd container elsewhere
(state/secrets/bindings/configuration's own real-daprd files, and
`invoke-matrix.test.js`'s timeout override) — every "fails fast," in-flight
abort, validation, size-bound, and Admin-API-isolation case stayed. One
candidate, `invoke.test.js`'s outbound binary-body round trip, was
deliberately kept: the real-daprd invocation matrix only exercises binary
content through the INBOUND `dapr-service` direction, never through the
OUTBOUND `dapr-invoke` node, so no real-daprd counterpart actually covers it
yet. The same pass merged four groups of real-daprd integration files down to
one Docker session each, run as sequential subtests, with no assertion
dropped: the Redis pub/sub and dead-letter scenarios into `pubsub.test.js`
(the dead-letter scenario's topics renamed `dlt-orders`/`dlt-orders-dlq` so
the two can never collide); the no-broker bucket's `invoke-matrix.test.js`,
`binding-out.test.js`, `secret-get.test.js`, and `metadata.test.js` into
`dapr-building-blocks.test.js` (metadata's expected component list grew to
the combined secret-store + output-binding pair this shared daprd now
registers); and `nats-publish-client.test.js`'s three already-distinct-topic
scenarios into one session. `nats-redeploy.test.js`'s three scenarios share
one NATS container and one Node-RED container but keep independent daprd
instances — each scenario turns on exactly what daprd's own one-time
`/dapr/subscribe` fetch captured at ITS OWN startup (one of them changes a
subscription after startup and then restarts daprd itself), which a shared
daprd could not represent for all three at once — and each uses its own
disjoint topic name(s) so the shared NATS stream never lets one scenario's
backlog leak into another's.

## Conventions and diagnosis

- Fresh temporary Node-RED profiles log warnings for disabled Projects,
  system-generated credential keys and a missing encrypted-credentials file.
  These describe disposable test profiles, not dependency deprecations; keep
  warnings enabled so unexpected runtime diagnostics remain visible.
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
  against real daprd 1.18.2 debug logs, not a harness bug. See `docs/security.md`
  for the operator-facing consequence; standing up
  Sentry/mTLS to test a real allow/deny split is future work.
- **`rawPayload` is two independent flags, confirmed against real daprd
  (integration):** a raw _subscription_ alone does not strip the CloudEvent
  envelope — if the publisher didn't ALSO mark the publish itself raw
  (`?metadata.rawPayload=true`), the broker message already is a full
  CloudEvent, and a raw subscription just hands that whole envelope over
  verbatim as the delivery body. Only marking both ends raw produces the
  original bytes in `data_base64` that `lib/subscriptions.js`'s
  `extractCloudEvent` comment describes. See
  `test/integration/nats-pubsub.test.js`.
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
  valid for that case. `test/integration/nats-cel-routing.test.js` confirms real
  daprd's own CEL evaluator resolves overlapping rules by order
  (first-match-wins); the runtime tier's CEL test delivers directly to each
  rule's pre-known route and so never exercises daprd's actual CEL parser or
  precedence at all.
- **Bulk redelivery targets only the failed entry, not the whole batch
  (integration):** confirmed against real daprd with a fastRetry Resiliency
  policy (`test/integration/nats-bulk.test.js`) — when one entry in a bulk
  batch is never acked and the rest resolve SUCCESS/DROP, only the unacked
  entry is redelivered; the entries that already resolved are not sent again.
- **Bulk publish (integration):** `test/integration/nats-bulk.test.js` sends
  one real bulk publish through daprd + NATS JetStream and observes all
  entries in one bulk delivery; `test/integration/nats-publish-client.test.js`
  covers mixed JSON, text, and binary entries and the duplicate-`entryId`
  whole-batch rejection, both against daprd + NATS JetStream — the wire
  client itself only talks to daprd's own HTTP API, never the broker
  directly, so which broker backs daprd is not a variable this client-level
  suite needs to hold constant (see "NATS-primary rebalance" below).
- **`deadLetterTopic` stalls with `pubsub.jetstream` in Dapr 1.18.2
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
  dead-letter scenario, now in `pubsub.test.js`).
  `test/integration/nats-dead-letter.test.js` proves
  the actual observed behavior (one delivery attempt, no dead-letter
  message) within a bounded wait, not a claim about what happens after it.

## Security audit

Two-part policy:

- **Package gate:** `npm audit --omit=dev` must report zero vulnerabilities. It
  covers everything the package ships: the official OpenTelemetry packages
  (`@opentelemetry/api`, `api-logs`, `exporter-logs-otlp-http`,
  `exporter-trace-otlp-http`, `resources`, `sdk-logs`, `sdk-trace-node` — see
  `AGENTS.md`'s "Stack" for why runtime dependencies are pinned and
  deliberately minimal here, not zero) and nothing else. Zero vulnerabilities in that tree as of this milestone;
  re-verify whenever those versions move, the same as any other pinned
  dependency.
- **Full audit review:** `npm audit` is reviewed but need not be empty. Only the
  specific advisories listed below are permitted; any advisory not on the list —
  including a newly-disclosed one reached through `node-red` — fails the review
  until it is individually assessed and added here. `node-red` is pinned
  deliberately and never shipped.

  Permitted advisories: **none**, last verified 2026-09-27. Both production
  and full npm audits are clean after the dependency refresh and a scoped
  `express@4.22.2` → `qs@6.16.0` override in the development tree. Express's
  `~6.15.1` constraint otherwise retains vulnerable `qs@6.15.3` despite the
  fix being available in the same major. The upstream advisories are
  [GHSA-x5fp-wj9c-mxmx](https://github.com/advisories/GHSA-x5fp-wj9c-mxmx) and
  [GHSA-4mjr-xmp4-gh2g](https://github.com/advisories/GHSA-4mjr-xmp4-gh2g).
  Runtime HTTP/query tests exercise this override. Remove it when the pinned
  Node-RED/Express tree resolves a patched version without it.

  This override governs the repository's root npm install. It does not patch
  the separately pinned Node-RED container image or consumers' own Node-RED
  installations, and npm does not apply dependency-package overrides to a
  consumer's root dependency tree. A read-only probe of the pinned 5.0.7-24
  image on 2026-09-27 confirmed Express 4.22.2 still resolves `qs` 6.15.3
  there. The clean npm audit is not a clean audit of that upstream image;
  the integration editor is loopback-bound, and the image needs an upstream
  dependency refresh to remove these advisories.

CI runs `npm audit --omit=dev` on every push/PR **and on a weekly schedule**
(`.github/workflows/ci.yml`), so a newly-disclosed advisory surfaces without
waiting for someone to open a pull request. The full-audit review above stays
manual: it is a judgement about reachability, not something a zero-exit check can
express.

Do **not** run `npm audit fix --force` — its suggested `node-red` downgrade
breaks the pin. Recheck the scoped override when Node-RED refreshes Express.
