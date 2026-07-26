# Development

Setup, day-to-day commands, and the commit workflow. Architecture invariants
live in `AGENTS.md`; test tiers in `docs/testing.md`.

## Prerequisites

- Node.js >= 24 (supported on Node 24 and 26).
- Docker (only for the integration tier — real daprd + Redis or NATS JetStream).

## Setup

```bash
npm install
```

## Commands

| Command                    | Purpose                                                                     |
| -------------------------- | --------------------------------------------------------------------------- |
| `npm test`                 | Native unit tests over `test/unit/`.                                        |
| `npm run test:coverage`    | Unit tests with native coverage thresholds on `lib/`.                       |
| `npm run test:runtime`     | Real Node-RED child-process harness tests.                                  |
| `npm run test:integration` | Real daprd + Redis/NATS JetStream Docker tests (pulls pinned images first). |
| `npm run test:e2e`         | Playwright tests against the real Node-RED editor.                          |
| `npm run lint`             | ESLint 9 — correctness and security rules.                                  |
| `npm run lint:fix`         | ESLint with autofix.                                                        |
| `npm run format`           | Prettier — write.                                                           |
| `npm run format:check`     | Prettier — verify only (required before commits).                           |

Keep this table and `package.json` in step.

## Test-driven loop

1. Write one focused failing test (`lib/` module tests are the default; wrap
   Node-RED behavior in runtime tests).
2. Run it and confirm it fails for the expected reason.
3. Implement the smallest change that makes it pass.
4. Run focused then affected tests.
5. `npm run format` and `npm run lint`.

Put behavior in `lib/` and keep `nodes/*.js` wrappers thin.

## Commit workflow and the milestone gate

- Conventional commit subjects (`feat:`, `test:`, `docs:`, `chore:`), one
  coherent purpose each. Never fold unrelated cleanup into a feature commit.
- Before committing: `git diff --check`, lint, format check, and the relevant
  tests must pass; inspect the staged diff.
- **Milestone gate:** reaching a milestone is a hard stop. Present results and
  wait for explicit human approval before making the milestone commit and before
  starting the next milestone. See `IMPLEMENTATION_PLAN.md` §8.

## Releasing

Releases are cut from a tag by `.github/workflows/release.yml` and authenticated
by **GitHub OIDC** (npm trusted publishing). Nobody publishes from a laptop:
there is no `NPM_TOKEN` secret, no `NODE_AUTH_TOKEN`, and no `npm login` — the
registry verifies a short-lived token minted for this repository and this
workflow file, and the same identity attests provenance.

1. Add the change under `## Unreleased` in `CHANGELOG.md` as you make it.
2. Bump `version` in `package.json` and move the `Unreleased` entries under the
   new version heading.
3. Tag `v<version>` and push the tag. The workflow re-runs the full gate
   (lint, format, unit, coverage, runtime, audit, pack) and refuses to publish if
   the tag and `package.json` version disagree.

Two one-time prerequisites are the repository owner's call and are deliberately
not pre-decided here: registering this repo + workflow as the package's trusted
publisher, and removing `private: true` (scoping the name to the publishing org,
since an unscoped name on the public registry cannot stay private). Until then
the package is installed from a path — see the README.
