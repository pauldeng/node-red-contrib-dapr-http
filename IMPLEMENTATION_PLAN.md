# Node-RED Dapr HTTP Nodes Implementation Plan

**Goal:** Build a small, reliable Node-RED node package that publishes and receives Dapr pub/sub messages and invokes and exposes Dapr services through a Dapr sidecar.

**Architecture:** Outbound pub/sub uses `@dapr/dapr`; outbound service invocation uses Node's native HTTP client so status, headers, query strings, and binary bodies are preserved. Each `dapr-connection` owns a dedicated, loopback-by-default app-channel listener managed by a process-wide registry, so the Node-RED Admin API is never exposed to Dapr and unchanged flows can be redeployed without restarting daprd.

**Tech stack:** Node.js 24+, Node-RED 5.0.1, Dapr runtime 1.18.1, `@dapr/dapr` 3.18.0, Node's `node:test`, Docker Compose, Redis, and Playwright.

## 1. Confirmed Scope And Decisions

- Package name and initial version: `node-red-contrib-dapr` `0.1.0`.
- Keep the package private and unlicensed in this phase: `"private": true` and `"license": "UNLICENSED"`.
- Support Dapr's HTTP protocol over TCP only.
- Unix domain sockets and gRPC are permanently outside scope.
- Include pub/sub and service invocation in both directions.
- Use stable HTTP programmatic subscriptions returned by `GET /dapr/subscribe`.
- Do not generate or manage declarative Dapr YAML, Kubernetes resources, or component files.
- Do not use `node-red-node-test-helper`, Mocha, Jest, Vitest, Sinon, or Supertest.
- Use `AGENTS.md` as the single provider-neutral source of repository instructions. `CLAUDE.md` contains exactly `@AGENTS.md`.
- Keep dependencies minimal. Use Node core APIs where they provide the required behavior.
- Use ES6+ syntax, `const` and `let`, and `async`/`await`; prohibit `var`.
- Add comments only around security boundaries, listener ownership, subscription generations, acknowledgement correlation, and bulk aggregation where the code is not self-explanatory.
- Publication to npm, Node-RED Flow Library submission, release automation, bindings, state, secrets, actors, workflows, and custom telemetry are outside scope.

## 2. Runtime Nodes

### `dapr-connection`

A Node-RED configuration node that owns one relationship with one Dapr sidecar.

- Outbound sidecar settings:
  - Use `DAPR_HTTP_ENDPOINT` when no host or port is configured.
  - Otherwise use configured HTTP host and port, defaulting to `127.0.0.1:3500`.
  - Store a configured Dapr API token as a Node-RED password credential; fall back to `DAPR_API_TOKEN`.
- Inbound app-channel settings:
  - Bind address defaults to `127.0.0.1`.
  - Port defaults to `3000` and is configurable.
  - Store a configured app API token as a password credential; fall back to `APP_API_TOKEN`.
  - Default to a 4 MiB body limit, 16 KiB aggregate header limit, 30-second request/acknowledgement timeout, 5-second drain timeout, 2-second redeploy lease grace, and 1,000 pending correlations per connection.
  - Put only the body limit and request/acknowledgement timeout in the advanced editor section. Keep header, drain, redeploy, and pending-correlation limits fixed until a demonstrated deployment need justifies more UI and configuration.
- Omit host and port options entirely in SDK environment-endpoint mode; empty values must not override SDK environment discovery.
- Keep HTTP keep-alive enabled and non-configurable because the Dapr SDK's HTTP agents are process-global.
- Coordinate shared client and agent shutdown centrally. Closing one config node must not break another active config node.
- Health-check `/v1.0/healthz/outbound` with bounded exponential backoff for node status.
- Do not queue messages while the sidecar is unavailable. Fail the current message through `done(error)` and let the flow decide how to retry.
- Reject duplicate app-channel bind address/port combinations with a clear node status and error rather than crashing Node-RED.

### `dapr-publish`

- Publish `msg.payload` through the configured pubsub component and topic.
- Let `msg.dapr` override configured `pubsubName`, `topic`, `contentType`, and publish metadata.
- Infer content type deterministically for Buffer, string, JSON object/array, number, boolean, and null payloads.
- Support raw payload metadata and CloudEvent publishing without silently changing the caller's message.
- On success, send the original message to the output. On failure, call `done(error)`.

### `dapr-subscribe`

- Register a stable programmatic subscription with its connection.
- Use stable delivery paths derived from persisted Node-RED node and rule identifiers, never array position or deployment generation.
- Put event data in `msg.payload`.
- Put the complete CloudEvent or raw-delivery metadata under `msg.dapr`, including pubsub name, topic, route, metadata, delivery ID, bulk entry ID, and acknowledgement correlation ID.
- Decode CloudEvent `data_base64` to a Buffer and preserve the original envelope.
- Support raw payloads, dead-letter topics, bulk settings, and ordered CEL routes.
- Expose one labelled output for each CEL rule plus one default output.
- Apply CEL routes in declared order with first match winning.
- Reject duplicate active pubsub-name/topic pairs within the same connection.
- In bulk mode, emit one Node-RED message per entry and aggregate per-entry Dapr results by entry ID.
- Auto-ack mode returns `SUCCESS` after `send` completes synchronously.
- Explicit-ack mode waits for `dapr-ack`; default timeout is 30 seconds, then returns `RETRY`.
- Show pending acknowledgement count in node status and log a focused warning with pubsub name and topic when an acknowledgement times out.

### `dapr-ack`

- Complete the delivery referenced by `msg.dapr.ackId` with `SUCCESS`, `RETRY`, or `DROP`.
- Let `msg.dapr.status` override the configured status after validation.
- Accept only the first completion for an acknowledgement ID.
- Reject missing, expired, foreign-generation, and already-completed IDs without corrupting another delivery.
- Keep this node separate from invocation responses because the wire contracts and lifecycle semantics differ.

### `dapr-invoke`

- Invoke another Dapr application through the local sidecar using native `node:http` or `node:https`.
- Configure application ID, method path, HTTP verb, content type, headers, and timeout; allow documented `msg.dapr` overrides.
- Support GET, POST, PUT, PATCH, and DELETE.
- Preserve query strings, Buffer bodies, text, JSON, empty bodies, response status, response headers, and response content type.
- Return the parsed or binary response in `msg.payload` and invocation details in `msg.dapr` without discarding the original message fields.
- Remove hop-by-hop headers and calculate `Host` and `Content-Length`; do not allow message overrides to replace Dapr authentication headers.
- Abort timed-out or closing requests and report a stable Node-RED error.

### `dapr-service`

- Register one HTTP verb and method path on the connection's dedicated app-channel listener.
- Reject duplicates, path traversal, reserved `/dapr` paths, internal delivery paths, and unsupported verbs at deploy time.
- Emit a Node-RED message containing the request body in `msg.payload` and method, verb, query, sanitized headers, caller application ID, and response correlation ID in `msg.dapr`.
- Preserve Buffer, text, JSON, and empty request bodies.
- Wait for a matching `dapr-response`; return HTTP 504 after the default 30-second response timeout.
- Return HTTP 503 for pending requests interrupted by shutdown or redeploy.

### `dapr-response`

- Complete the service request referenced by `msg.dapr.responseId`.
- Default to status 200 and allow validated status, headers, content type, and body overrides from `msg.dapr`.
- Support Buffer, text, JSON, null, and empty responses.
- Accept the first response only and reject missing, expired, foreign-generation, or already-completed IDs.
- Keep the node terminal, matching Node-RED's request/response model.

## 3. Dedicated App-Channel Listener

The listener is a small `node:http` server and must expose only these surfaces:

- `GET /healthz`: unauthenticated app health/readiness response.
- `GET /dapr/subscribe`: current programmatic subscription definitions.
- `/node-red-dapr/subscriptions/<stable-id>`: registered pub/sub delivery routes.
- Registered service method paths for `dapr-service` nodes.
- Every other path returns 404.
- A registered service path called with the wrong HTTP verb returns 405 with an `Allow` header.

Security and reliability rules:

- Never attach Dapr routes to `RED.httpAdmin`, `RED.httpNode`, or the Node-RED editor port.
- Require the configured or environment app API token on discovery, delivery, and service routes using constant-time comparison; `/healthz` remains unauthenticated for app health probes.
- Reject requests carrying `dapr-caller-app-id` on `/dapr/subscribe` and internal subscription delivery routes. This prevents a mesh caller from treating internal endpoints as service methods.
- Preserve `dapr-caller-app-id` for registered service requests so flows can make authorization decisions.
- Document that `APP_API_TOKEN` authenticates daprd to the app; it does not authorize a calling application. Dapr access-control policies are required for caller authorization.
- Enforce header count, body size, request duration, and pending request limits before buffering data.
- Track sockets so shutdown first drains requests for a bounded interval and then destroys remaining sockets.
- Do not return stack traces, tokens, Node-RED configuration, or internal correlation state over HTTP.

Deployment topology:

- Kubernetes uses the normal sidecar model: Node-RED and daprd share one pod and loopback network namespace.
- Docker Compose must place daprd in Node-RED's network namespace, for example `network_mode: service:node-red`, so the secure `127.0.0.1` default is reachable by both processes.
- Binding to a non-loopback address is allowed only through explicit configuration and must show a security warning in the editor and documentation.

## 4. Redeploy And Subscription Lifecycle

Dapr HTTP programmatic subscriptions are fetched at application startup and cached by daprd. There is no supported refresh endpoint in Dapr 1.18.1.

Normal redeploy behavior:

1. Delivery paths are stable because they derive from persisted node/rule IDs.
2. A module-scoped listener registry owns the server independently of one transient Node-RED node instance.
3. When a connection closes during deploy, its registry lease enters a short bounded grace period rather than closing the listening socket immediately.
4. The replacement connection reacquires the same listener, cancels idle shutdown, installs a new handler generation, and atomically replaces the old routing table.
5. Old pending pub/sub acknowledgements complete as `RETRY`; old pending service requests complete as 503.
6. Requests arriving between generations receive a retryable 503 rather than being routed to stale flow nodes.
7. If no replacement connection reacquires the lease, the registry drains and closes the server and all sockets.

This means a full or modified flow redeploy with unchanged subscription definitions works without restarting daprd. The sidecar continues delivering to the same stable paths after the new generation is installed.

Subscription-change detection:

- Canonically serialize and hash the subscription fields daprd consumes: pubsub name, topic, metadata, dead-letter topic, bulk settings, CEL matches, route paths, and default path.
- Record the exact fingerprint last served successfully by `GET /dapr/subscribe`.
- Compare it with the current desired fingerprint after every deploy.
- If equal, show normal connected status and do not request a restart.
- If different, keep routes operational where possible but show a yellow `restart sidecar: subscriptions changed` status and a single rate-limited warning.
- Clear the warning only after daprd fetches `/dapr/subscribe` again and receives the current definition.
- On a fresh Node-RED process where no discovery request has been observed, show `waiting for sidecar discovery` rather than claiming that daprd has current subscriptions.

A daprd restart is required only when a subscription definition changes: adding or removing a subscription, changing pubsub name/topic, metadata, dead-letter topic, bulk configuration, CEL rules/order, or route mapping. Changes to publish nodes, invoke nodes, service implementations, downstream flow logic, layout, labels, or wiring that preserve the subscription definition do not require a sidecar restart.

## 5. File And Module Layout

Keep Node-RED wrappers thin and put behavior in directly testable internal modules.

```text
package.json                         package metadata, scripts, Node-RED registration
eslint.config.js                    ES6+, correctness, security-sensitive lint rules
.prettierrc.json                    deterministic formatting
AGENTS.md                           canonical provider-neutral engineering instructions
CLAUDE.md                           exactly @AGENTS.md
README.md                           user quickstart and operational overview
nodes/dapr-connection.js            config-node lifecycle and credentials
nodes/dapr-connection.html          connection editor, validation, and help
nodes/dapr-publish.js               publish Node-RED wrapper
nodes/dapr-publish.html             publish editor and help
nodes/dapr-subscribe.js             subscription Node-RED wrapper
nodes/dapr-subscribe.html           routing/bulk/ack editor and help
nodes/dapr-ack.js                   acknowledgement Node-RED wrapper
nodes/dapr-ack.html                 acknowledgement editor and help
nodes/dapr-invoke.js                outbound invocation wrapper
nodes/dapr-invoke.html              invocation editor and help
nodes/dapr-service.js               inbound service wrapper
nodes/dapr-service.html             service editor and help
nodes/dapr-response.js              invocation response wrapper
nodes/dapr-response.html            response editor and help
lib/options.js                      config/env precedence and validation
lib/messages.js                     content inference and CloudEvent conversion
lib/dapr-client.js                  SDK publish client and shared lifecycle
lib/invoke-client.js                native outbound invocation adapter
lib/app-channel.js                  listener registry, router, auth, limits, sockets
lib/subscriptions.js                definitions, canonical fingerprints, generations
lib/pending.js                      bounded first-wins ack/response correlations
lib/errors.js                       stable internal error types and safe messages
test/unit/*.test.js                 direct module contract tests
test/runtime/*.test.js              real Node-RED child-process black-box tests
test/helpers/node-red.js            temporary userDir, deploy, readiness, cleanup
test/helpers/fake-dapr.js            focused fake Dapr HTTP sidecar
test/integration/*.test.js          real daprd/Redis integration tests
test/e2e/*.spec.js                  Node-RED editor Playwright tests
test/fixtures/flows/*.json          deterministic deployed test flows
test/fixtures/dapr/*                pubsub, resiliency, and access-control resources
docker-compose.yml                  pinned real-stack test environment
examples/*.json                     importable user example flows
docs/architecture.md                trust boundaries and runtime lifecycle
docs/development.md                 setup, TDD, commands, and commit workflow
docs/testing.md                     test tiers and failure diagnosis
docs/security.md                    tokens, ACLs, binding, and limits
docs/deployment.md                  local, Compose, and Kubernetes topology
docs/subscriptions.md               redeploy and sidecar-restart runbook
```

The layout is a target, not permission to add empty scaffolding. Create a file only in the milestone that first needs it.

## 6. Test Strategy

### Unit tests

Use `node:test`, `node:assert/strict`, native mocks, and fake timers.

- Validate config and environment precedence, including omission of empty SDK host/port options.
- Cover serialization and parsing for every supported payload type.
- Cover CloudEvents, `data_base64`, raw payloads, malformed envelopes, and size limits.
- Cover constant-time token comparison without exposing token values in errors.
- Cover route validation, reserved paths, wrong methods, and header filtering.
- Cover subscription canonicalization and stable fingerprints independent of object key order.
- Cover generation replacement, idle grace, re-acquisition, socket cleanup, and multi-connection isolation.
- Cover first-wins correlation, expiry, capacity bounds, shutdown, and foreign generations.
- Cover bulk aggregation, missing entry IDs, mixed statuses, timeout, and duplicate acknowledgements.
- Cover outbound invocation response fidelity for JSON, text, Buffer, null, empty, non-2xx, and timeout responses.
- Enforce at least 90% line/function and 85% branch coverage on `lib/` with Node's native coverage thresholds.

### Real Node-RED runtime tests

Do not recreate Node-RED internals in a helper library.

- Start the exact `node-red@5.0.1` CLI as a child process with a temporary user directory and isolated ports.
- Make this workspace package discoverable from the temporary `node_modules`.
- Wait for readiness and deploy flows through Node-RED's documented `POST /flows` Admin API.
- Observe behavior through built-in HTTP nodes, the fake Dapr server, and the dedicated app-channel port.
- Test registration, credentials, statuses, Catch-node errors, node close handlers, and real message wiring.
- Deploy, modify, and fully redeploy flows while fake daprd remains running.
- Verify an unchanged subscription keeps receiving without another discovery request or sidecar restart.
- Verify a changed subscription produces the restart warning while the last cached route remains deterministic.
- Verify listener reuse, old-route removal, clean rebind, pending RETRY/503 behavior, and multiple connection configs.
- Stop Node-RED with `SIGINT`, wait for a bounded graceful exit, then force-kill only if necessary.

### Real Dapr integration tests

Use pinned `nodered/node-red:5.0.1-24`, `daprio/daprd:1.18.1`, and Redis images.

- Preload the initial Node-RED flow before starting daprd so its first `/dapr/subscribe` fetch is not empty.
- Gate daprd app-channel startup on `GET /healthz`.
- Verify publish and subscribe through real daprd and Redis.
- Verify CloudEvents, raw payloads, binary `data_base64`, metadata, and malformed delivery handling.
- Verify bulk delivery and per-entry mixed results.
- Verify CEL precedence with overlapping rules and assert first-match-wins.
- Test redelivery without a dead-letter topic, or with an explicit Dapr resiliency retry policy.
- Test immediate dead-letter forwarding in a separate fixture so DLT behavior does not invalidate retry expectations.
- Verify explicit SUCCESS, RETRY, DROP, timeout, redeploy interruption, and sidecar recovery.
- Verify outbound and inbound service invocation round trips, all supported verbs, query strings, headers, binary data, non-2xx responses, timeout, and unknown method 404.
- Verify app-token success/failure and Dapr ACL allow/deny behavior separately.
- Assert the app-channel port cannot access Node-RED `/flows`, `/settings`, editor assets, or arbitrary HTTP In endpoints.
- Keep daprd running during an unchanged full flow redeploy and prove delivery resumes without restarting it.
- Then change a subscription, prove the operator warning appears, restart daprd, and prove the new definition becomes active.

### Editor E2E and visual review

- Use Playwright against real Node-RED 5.0.1.
- Verify palette registration, dialogs, typed inputs, credential fields, conditional controls, validation, routing-rule editing, dynamic outputs, and help text.
- Verify all seven nodes can be configured and deployed in a complete publish/subscribe and invoke/service flow.
- Use Node-RED's standard form rows, labels, typed inputs, buttons, icons, spacing, and help structure; do not introduce a separate visual system.
- Capture light and dark screenshots at 1440x900 and 1024x768 and a narrow viewport supported by the editor.
- Manually inspect every dialog for clipped labels, overflow, overlap, unreadable status, excessive whitespace, and inconsistent alignment.

## 7. Documentation Requirements

- `AGENTS.md` is concise and provider-neutral. It documents repository layout, exact commands, architecture invariants, TDD workflow, commit rules, security boundaries, and completion criteria, then links to detailed documents.
- `CLAUDE.md` contains only `@AGENTS.md`; do not duplicate canonical instructions or use a symlink.
- Supporting documents use neutral terms such as engineer, implementer, automation, and coding agent.
- `README.md` includes prerequisites, install/use instructions, a minimal flow, listener topology, tokens, Dapr ACL guidance, and links to detailed docs.
- Node help panels explain configuration, input/output contracts, `msg.dapr` fields, errors, ack/response lifecycle, and Function-node correlation preservation with short examples.
- Explicitly warn that a Function node must mutate and return the original message rather than replace it when an ack or response correlation is pending.
- `docs/subscriptions.md` gives a decision table for whether daprd needs restarting and a safe restart procedure.
- Example flows require no third-party Node-RED nodes and cover:
  - basic publish and subscribe;
  - CEL routing;
  - bulk explicit acknowledgement;
  - outbound invocation;
  - inbound service with `dapr-response`.

## 8. Milestones And Auditable Commits

Every milestone follows red-green-refactor: add a focused failing test, run it and record the expected failure, implement the smallest passing change, run focused and affected tests, format and lint, inspect `git diff --check` and the staged diff, then commit. Never combine unrelated cleanup with a feature commit.

**Milestone approval gate:** reaching a milestone is a hard stop. Do not make the milestone's closing commit and do not start the next milestone. Present the milestone results — tests run and their outcomes, coverage where applicable, and the staged diff — then wait for explicit human approval. Only after approval, make the milestone commit and begin the next milestone. If the reviewer requests changes, apply them within the current milestone and pass through this gate again.

### Milestone 1: Project foundation

- Create package metadata with exact runtime versions and minimal scripts.
- Add ESLint and Prettier configuration, enforcing `no-var` and `prefer-const`.
- Add canonical `AGENTS.md`, the one-line `CLAUDE.md` reference, and the initial development/testing documents.
- Add a registration smoke test using a minimal `RED.nodes.registerType` spy.
- Verify `npm install`, lint, format check, and the smoke test.
- Commit: `chore: establish project and test foundation`.

### Milestone 2: Native runtime harness

- Build the fake Dapr HTTP server and real Node-RED child-process harness using Node core APIs.
- Add deterministic temporary directories, ports, Admin API deployment, readiness, log capture, and bounded cleanup.
- Test an intentionally minimal fixture end to end before adding product behavior.
- Commit: `test: add real Node-RED runtime harness`.

### Milestone 3: Connection and app-channel lifecycle

- Test option precedence, credentials, authentication, route boundaries, listener reuse, generations, limits, health, multi-config isolation, and graceful shutdown.
- Implement `options`, `errors`, `pending`, and `app-channel` modules plus the thin connection node.
- Add the connection editor/help and verify it visually before expanding the UI pattern.
- Commit: `feat: add Dapr connection and app channel`.

### Milestone 4: Publish

- Test payload inference, SDK options, metadata/overrides, success, sidecar-down failure, recovery status, and close behavior.
- Implement message conversion, shared SDK client lifecycle, and the publish wrapper/editor/help.
- Add a real Node-RED publish flow test.
- Commit: `feat: add Dapr publish node`.

### Milestone 5: Subscribe and acknowledgement

- Test discovery JSON, stable paths, fingerprints, CloudEvents/raw/binary input, duplicate detection, auto/explicit ack, timeout, first-wins behavior, and redeploy generation changes.
- Implement subscription registry, discovery/delivery routing, subscribe node, and ack node.
- Add the unchanged-redeploy test with the fake sidecar left running.
- Commit: `feat: add Dapr subscription acknowledgements`.

### Milestone 6: Service invocation

- Test native outbound request fidelity and inbound request/response correlation before implementing nodes.
- Implement the invoke client, invoke node, service node, and separate response node.
- Test 404/405 behavior, reserved routes, header filtering, timeouts, shutdown, and Node-RED Admin API isolation.
- Commit: `feat: add Dapr service invocation nodes`.

### Milestone 7: Bulk subscription and CEL routing

- Test rule ordering, stable rule IDs, dynamic outputs, bulk emission, mixed per-entry acknowledgement, metadata, and dead-letter definitions.
- Implement the smallest routing and bulk additions to the existing subscription modules and editor.
- Add focused example flows and real Node-RED runtime coverage.
- Commit: `feat: add bulk subscriptions and CEL routing`.

### Milestone 8: Real Dapr integration

- Add pinned Compose services and Dapr component, resiliency, ACL, and subscription fixtures.
- Implement pub/sub, invocation, retry, DLT, token, ACL, redeploy, and shutdown integration suites.
- Keep retry and dead-letter scenarios in separate fixtures.
- Commit: `test: add real Dapr integration coverage`.

### Milestone 9: Editor E2E, examples, and documentation

- Complete Node-RED v5 editor help and validation for all nodes.
- Add Playwright interaction tests and visually inspect all required screenshots.
- Finish README, architecture, security, deployment, subscription runbook, and five importable examples.
- Add CI for Node 24 and 26 unit/coverage/lint/format jobs plus one Ubuntu Docker/Playwright job.
- Commit: `docs: complete UI examples and project guidance`.

## 9. Completion Gate

Before declaring the implementation complete:

1. Run the Node 24 native unit and coverage suite.
2. Run real Node-RED black-box tests.
3. Run real Dapr/Redis Docker integration tests, including unchanged flow redeploy without restarting daprd.
4. Run Playwright E2E and inspect all captured screenshots.
5. Run ESLint, Prettier check, and `git diff --check`. For audit: `npm audit --omit=dev` must be clean; the full `npm audit` is reviewed, permitting only the specific advisories explicitly listed in `docs/testing.md` — any other or newly-disclosed advisory fails until individually assessed.
6. Run `npm pack --dry-run` and inspect the package contents even though publication is out of scope.
7. Confirm secrets, tokens, temporary credentials, screenshots with sensitive content, coverage output, and test data are excluded from the package and Git where appropriate.
8. Review every milestone commit for a single coherent purpose and passing state.
9. Confirm `AGENTS.md` is the only durable AI instruction source and `CLAUDE.md` is exactly `@AGENTS.md`.
10. Confirm docs state precisely: routine unchanged flow redeploy needs no daprd restart; subscription-definition changes do.

## 10. Known Constraints

- Programmatic subscription definitions cannot be refreshed in place by Dapr 1.18.1. The fingerprint/status mechanism reports this limitation; it does not pretend to bypass it.
- A Node-RED process restart loses the in-memory record of what the existing sidecar last fetched. Until a new `/dapr/subscribe` request arrives, status must remain `waiting for sidecar discovery` rather than infer synchronization.
- The secure loopback default requires Node-RED and daprd to share a network namespace. Other container layouts must explicitly configure and secure a non-loopback listener.
- Bulk behavior depends on the pubsub component. Redis still validates batching between daprd and the application but does not demonstrate broker-native batching.
- The Dapr SDK's HTTP agents are process-global. The package therefore uses one keep-alive policy and centralized ownership rather than per-node agent settings.

## 11. Primary References

- [Node-RED: Creating Nodes](https://nodered.org/docs/creating-nodes/)
- [Node-RED Admin API: POST /flows](https://nodered.org/docs/api/admin/methods/post/flows/)
- [Node-RED source](https://github.com/node-red/node-red/)
- [Dapr JavaScript SDK client](https://v1-18.docs.dapr.io/developing-applications/sdks/js/js-client/)
- [Dapr JavaScript SDK package](https://www.npmjs.com/package/@dapr/dapr)
- [Dapr programmatic subscriptions](https://docs.dapr.io/developing-applications/building-blocks/pubsub/subscription-methods/)
- [Dapr service invocation](https://docs.dapr.io/developing-applications/building-blocks/service-invocation/howto-invoke-discover-services/)
- [Dapr app API token](https://docs.dapr.io/operations/security/app-api-token/)
- [Dapr access control](https://docs.dapr.io/operations/configuration/invoke-allowlist/)
- [Node.js test runner](https://nodejs.org/docs/latest-v24.x/api/test.html)
- [Claude Code instruction imports](https://code.claude.com/docs/en/best-practices)
- [Codex AGENTS.md guidance](https://learn.chatgpt.com/guides/best-practices)
