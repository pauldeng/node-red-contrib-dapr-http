# Testing

Four tiers, each with a distinct job. All use the native `node:test` runner
(plus Playwright for the editor tier). No Mocha/Jest/Vitest/Sinon/Supertest and
no `node-red-node-test-helper`.

## Tiers

| Tier        | Location            | What it proves                                                                                           | Needs                  |
| ----------- | ------------------- | -------------------------------------------------------------------------------------------------------- | ---------------------- |
| Unit        | `test/unit/`        | `lib/` module contracts in isolation.                                                                    | Node only.             |
| Runtime     | `test/runtime/`     | Real Node-RED loads, registers, wires, and runs the nodes; a fake Dapr HTTP sidecar stands in for daprd. | Node + `node-red` CLI. |
| Integration | `test/integration/` | Behavior against real daprd 1.18.1 + Redis.                                                              | Docker.                |
| E2E         | `test/e2e/`         | Editor dialogs, validation, and a full publish/subscribe + invoke/service flow.                          | Playwright + Node-RED. |

Tiers are introduced by the milestone that first needs them; this file grows
with them.

Visual inspection of every node's editor dialog (light/dark, multiple viewports)
is consolidated in the E2E tier (Milestone 9). The `dapr-connection` and
`dapr-publish` dialog checks are explicitly tracked there; the owner deferred
the connection check from Milestone 3. Runtime tests confirm both nodes load.

## Running

```bash
npm test              # unit
npm run test:runtime  # real Node-RED child-process harness
```

(Integration and e2e scripts are added with their milestones.)

The runtime harness (`test/helpers/node-red.js`) drives a real `node-red@5.0.1`
process in a throwaway user directory with this package symlinked into its
`node_modules`, deploys flows over the Admin API, and observes behavior over
HTTP. A fake Dapr HTTP sidecar (`test/helpers/fake-dapr.js`) stands in for daprd.

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
