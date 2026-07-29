# Development

Setup, day-to-day commands, and the commit workflow. Architecture invariants
live in `AGENTS.md`; test tiers in `docs/testing.md`.

## Prerequisites

- Node.js >= 22.13 **to develop**, though the package itself supports >= 22.9.
  `engines.node` is the consumer contract and matches Node-RED 5.0.1's own floor;
  ESLint 10 is stricter (`^20.19.0 || ^22.13.0 || >=24`), so `npm run lint` needs
  22.13+. Only contributors on 22.9–22.12 are affected — devDependencies are never
  installed by consumers, and CI's `node-version: '22'` resolves to the latest
  22.x. CI runs the unit, coverage, lint, format, and runtime tiers on Node 22,
  24, and 26.
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
| `npm run validate`         | Node-RED's own packaging checks (`node-red-dev validate`) — advisory.       |
| `npm run lint`             | ESLint 10 over JS + html-validate over the editor HTML.                     |
| `npm run lint:fix`         | ESLint with autofix.                                                        |
| `npm run lint:html`        | html-validate only, for iterating on an editor dialog.                      |
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
  starting the next milestone.

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

The package publishes publicly as `@pauldeng/node-red-contrib-dapr-http` under MIT.

The name is **scoped** because Node-RED's packaging guidance says modules first
published after 2022-01-31 "should use a scoped name". The `-http` suffix names
the **transport**, not the feature set: HTTP-only is a permanent constraint (gRPC
and Unix sockets are out of scope for good), whereas the supported building
blocks could grow. A name like `-pubsub` would become a lie and force a rename,
and renaming a published npm package means republishing, deprecating the old
name, and every user editing their `package.json`. The feature boundary lives in
the README's Scope section and the node help instead, where it can be corrected
without a migration.

`publishConfig.access` is `public` and **load-bearing**: npm defaults scoped
packages to restricted, so without it the first publish would silently produce a
private package.

### How 0.1.0 was first published (history, not a procedure)

npm configures trusted publishing on a **package's own settings page**, so a
package that has never been published has nowhere to register a trusted
publisher — the very first publish of a new package cannot authenticate via OIDC.

`0.1.0` was therefore published from CI using a single-use, scope-granular
Automation token, which was revoked and deleted immediately afterwards along with
the temporary workflow that used it. That route was chosen over publishing from a
laptop so the first release still carried provenance and still passed the full
gate. Trusted publishing has been registered since, so **every release from
`0.1.1` onward uses `release.yml` and OIDC with no token anywhere** — the
invariant in `AGENTS.md` holds without exception.

Nothing here needs repeating. It is recorded because "why does the changelog say
0.1.0 used a token?" is otherwise unanswerable from the repository alone.

**Listing in the Node-RED library is a separate, manual step — it does not happen
on its own.** The flow library stopped auto-indexing npm packages carrying the
`node-red` keyword in April 2020; a submission has to be placed by hand at
<https://flows.nodered.org/add/node> after the package exists on npm. Publishing
alone gets you an npm listing and nothing else.

The package already meets the library's stated requirements: a README describing
what the nodes do and how to use them, a `node-red` section in `package.json`
listing the node files, and `node-red` in `keywords`.

`npm run validate` runs Node-RED's own `node-red-dev validate` against those
requirements. **Run it by hand before a release, not in CI.** It is deliberately
not a workflow step: every finding it still reports is one this package has
assessed and accepted on purpose (the `node-red` range excludes 1.x/2.x/3.x), so
as a `continue-on-error` step it produced no signal while downloading a 618-package
tree containing 16 deprecated packages on every run. It stays available because
its naming-collision, examples-per-node, and compatibility checks are worth having
at a publish decision — a human-gated moment — and `npx` costs nothing when unused.
The metadata it verified once is now asserted permanently by
`test/unit/repository-gates.test.js`.
