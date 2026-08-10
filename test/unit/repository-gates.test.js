'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const read = (name) => fs.readFileSync(path.resolve(__dirname, '../..', name), 'utf8');

test('durable docs list every test tier and CI enforces cheap completion gates', () => {
  for (const name of ['AGENTS.md', 'docs/development.md', 'docs/testing.md']) {
    const contents = read(name);
    assert.match(contents, /npm run test:integration/);
    assert.match(contents, /npm run test:e2e/);
  }

  const workflow = read('.github/workflows/ci.yml');
  const pkg = JSON.parse(read('package.json'));
  assert.equal(
    pkg.scripts['test:integration'],
    'node --test --test-concurrency=1 "test/integration/**/*.test.js"'
  );
  assert.match(workflow, /fetch-depth: 0/);
  assert.match(workflow, /docker:\n[\s\S]*?timeout-minutes: 20/);
  assert.match(workflow, /npm run test:integration\n\s+timeout-minutes: 10/);
  assert.match(workflow, /npm run test:e2e\n\s+timeout-minutes: 6/);
  // Every job is bounded, and the tier most able to hang (real Node-RED child
  // processes) is bounded within its job too — an unbounded job burns the full
  // six-hour GitHub limit on one stuck test.
  assert.match(workflow, /unit:\n[\s\S]*?timeout-minutes: 20/);
  assert.match(workflow, /npm run test:runtime\n\s+timeout-minutes: 10/);
  // A periodic run is what surfaces a newly-disclosed dependency advisory
  // without waiting for someone to open a PR.
  assert.match(workflow, /schedule:\n\s+- cron:/);
  for (const command of [
    'npm audit --omit=dev',
    'npm pack --dry-run',
    'git diff --check "${{ github.event.pull_request.base.sha || github.event.before || \'HEAD^\' }}...HEAD"',
    "printf '@AGENTS.md\\n' | cmp -s - CLAUDE.md",
    'docker compose config --quiet',
  ]) {
    assert.match(workflow, new RegExp(command.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }
});

test('CI actually tests the minimum Node version the package claims to support', () => {
  const pkg = JSON.parse(read('package.json'));
  const workflow = read('.github/workflows/ci.yml');

  const floor = pkg.engines.node.match(/^>=\s*(\d+)/);
  assert.ok(floor, `engines.node must be a ">=<major>" range, got ${pkg.engines.node}`);
  const matrix = workflow.match(/node-version:\s*\[([^\]]+)\]/);
  assert.ok(matrix, 'the unit job must declare a node-version matrix');
  const tested = matrix[1].match(/\d+/g);

  // These two drift apart silently and in both directions: lowering the floor to
  // widen reach without adding a matrix entry advertises a Node version nothing
  // runs, and dropping a matrix entry without raising the floor does the same.
  // npm enforces engines at install time, so the claim is load-bearing for users.
  assert.ok(
    tested.includes(floor[1]),
    `engines.node is >=${floor[1]} but CI tests only Node ${tested.join(', ')} — the declared floor must be in the matrix`
  );
});

test('the lockfile mirrors the manifest it was generated from', () => {
  const pkg = JSON.parse(read('package.json'));
  const root = JSON.parse(read('package-lock.json')).packages[''];

  // package-lock.json duplicates the root manifest's identity, and npm only
  // refreshes it when something triggers a resolve — so editing package.json by
  // hand leaves the copy stale. That drifted three times while making this
  // package publishable (name, license, then engines), each caught by eye rather
  // than by a gate. `npm install --package-lock-only` is the fix.
  for (const field of ['name', 'version', 'license']) {
    assert.equal(root[field], pkg[field], `package-lock.json root ${field} is stale`);
  }
  assert.deepEqual(root.engines, pkg.engines, 'package-lock.json root engines is stale');
});

test('releases publish only from a tag, only via GitHub OIDC, never from a machine login', () => {
  const release = read('.github/workflows/release.yml');

  // OIDC identity only: no publish token may ever appear in an executed line.
  // (Comments name those things precisely to say they are not used.)
  const executed = release
    .split('\n')
    .filter((line) => !line.trim().startsWith('#'))
    .join('\n');
  assert.match(release, /id-token: write/);
  assert.doesNotMatch(executed, /NPM_TOKEN|NODE_AUTH_TOKEN|npm login|secrets\./);
  // The whole job is tag-gated, so workflow_dispatch from a branch cannot publish
  // past the version check — which itself must be unconditional.
  assert.match(release, /if: startsWith\(github\.ref, 'refs\/tags\/v'\)/);
  assert.match(
    release,
    /- name: Check tag matches package version\n\s+run: test "v\$\(node -p/,
    'the tag/version check must not be conditional'
  );
  // Scoped packages default to RESTRICTED. publishConfig.access is the primary
  // guard; --access public is the second, because a silently-private first
  // publish looks like success and is awkward to undo.
  assert.match(release, /npm publish --provenance --access public/);
});

test('both workflows run with least privilege, and CI does not pile up runs', () => {
  const ci = read('.github/workflows/ci.yml');
  const release = read('.github/workflows/release.yml');

  // Neither workflow writes to the repository. Declaring the token scope keeps a
  // future step from inheriting write access it never needed.
  assert.match(ci, /^permissions:\n {2}contents: read$/m);
  // Only the release workflow may mint an OIDC identity, and only for publishing.
  // Matched as a YAML key, not as a word: ci.yml's own comment explains why it
  // has no such permission, and a bare /id-token/ would match that prose — the
  // same trap the NPM_TOKEN assertion above sidesteps by stripping comments.
  assert.doesNotMatch(ci, /^\s*id-token:/m);
  assert.match(release, /^\s*id-token: write/m);
  // Superseded pushes are cancelled rather than paid for twice — this workflow
  // runs Docker and Playwright tiers, so a duplicate run is expensive.
  assert.match(ci, /concurrency:\n {2}group:[^\n]*\n {2}cancel-in-progress: true/);
});

test('the package is publishable to the public registry', () => {
  const pkg = JSON.parse(read('package.json'));

  // `private: true` makes `npm publish` refuse outright, so its absence is the
  // single load-bearing fact here — asserted explicitly rather than left to be
  // discovered by a failed release job.
  assert.equal(pkg.private, undefined, 'private must be unset to publish');
  // Scoped, per Node-RED's packaging guidance for modules first published after
  // 2022-01-31 ("Packages should use a scoped name"). The "-http" suffix names the
  // TRANSPORT, not the feature set: HTTP-only is permanent (gRPC and Unix sockets
  // are permanently out of scope), whereas the supported
  // building blocks could grow — so a "-pubsub" name would eventually be a lie and
  // force a rename that every consumer would have to follow.
  assert.equal(pkg.name, '@pauldeng/node-red-contrib-dapr-http');
  // Load-bearing for a scoped name: npm defaults scoped packages to RESTRICTED,
  // so without this the first publish would silently be a private package.
  assert.equal(pkg.publishConfig.access, 'public');
  assert.equal(pkg.license, 'MIT');
  assert.match(read('LICENSE'), /MIT License/);
  assert.doesNotMatch(read('LICENSE'), /UNLICENSED|Proprietary|All rights reserved/i);

  // node-red-dev validate's P03: a published node needs somewhere to file bugs.
  // Asserted exactly, not by substring: a bare /pauldeng\/node-red-contrib-dapr/
  // matches the renamed repo as a prefix, so stale pre-rename URLs passed silently.
  const REPO = 'https://github.com/pauldeng/node-red-contrib-dapr-http';
  assert.equal(pkg.repository.url, `git+${REPO}.git`);
  assert.equal(pkg.bugs.url, `${REPO}/issues`);
  assert.equal(pkg.homepage, `${REPO}#readme`);
  assert.ok(pkg.author, 'an author is required');
});

test('the package ships only runtime files, and declares itself a Node-RED package', () => {
  const pkg = JSON.parse(read('package.json'));
  const readme = read('README.md');

  // No test suite, plan, review note, or CI/tooling config in the tarball.
  assert.deepEqual(pkg.files, [
    'lib/',
    'nodes/',
    'examples/',
    'docs/',
    'README.md',
    'CHANGELOG.md',
    'LICENSE',
  ]);
  // Node-RED's own packaging guidance requires this keyword.
  assert.ok(pkg.keywords.includes('node-red'));
  // Runtime dependencies are pinned and deliberately minimal, not zero (see
  // AGENTS.md's "Stack (pinned)"): only the official OpenTelemetry packages
  // this package's own optional tracing integration needs. Every call to the
  // sidecar itself still goes through lib/sidecar-http.js with no HTTP
  // client of its own. Asserting the exact set, not just "some dependencies
  // exist", so an unapproved addition fails this test loudly rather than
  // slipping in silently — any new one needs the maintainer's explicit
  // approval before landing here.
  assert.deepEqual(Object.keys(pkg.dependencies).sort(), [
    '@opentelemetry/api',
    '@opentelemetry/api-logs',
    '@opentelemetry/exporter-logs-otlp-http',
    '@opentelemetry/exporter-trace-otlp-http',
    '@opentelemetry/resources',
    '@opentelemetry/sdk-logs',
    '@opentelemetry/sdk-trace-node',
  ]);
  for (const [name, version] of Object.entries(pkg.dependencies)) {
    assert.match(version, /^\d+\.\d+\.\d+$/, `${name} must be pinned exactly`);
  }
  assert.doesNotMatch(readme, /zero runtime dependencies/i);
  assert.match(readme, /OpenTelemetry/);
  for (const name of ['LICENSE', 'CHANGELOG.md']) {
    assert.ok(read(name).length > 0, `${name} must exist and be non-empty`);
  }
});
