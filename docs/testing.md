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

## Running

```bash
npm test          # unit
```

(Runtime, integration, and e2e scripts are added with their milestones.)

## Conventions and diagnosis

- Test files are named `*.test.js` (unit/runtime/integration) or `*.spec.js`
  (Playwright).
- Prefer `node:assert/strict`. Use the runner's native mocks and fake timers
  rather than a mocking library.
- Coverage target on `lib/`: >= 90% line/function, >= 85% branch, enforced with
  Node's native `--test-coverage-*` thresholds (wired once `lib/` exists).
- **Startup ordering (integration):** deploy the Node-RED flow before starting
  daprd, and gate daprd on `GET /healthz` — daprd fetches `/dapr/subscribe` once
  at startup, so a sidecar that starts against an empty app receives nothing.
- Async failures inside flow handlers can surface as timeouts rather than
  assertion errors; assert on observable effects (HTTP calls, node status, sent
  messages), not on internal promises.
