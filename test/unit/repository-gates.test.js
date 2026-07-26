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
  assert.match(release, /npm publish --provenance/);
});

test('the package ships only runtime files, and declares itself a Node-RED package', () => {
  const pkg = JSON.parse(read('package.json'));

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
  // Zero runtime dependencies: everything outbound speaks node:http directly.
  assert.equal(pkg.dependencies, undefined);
  for (const name of ['LICENSE', 'CHANGELOG.md']) {
    assert.ok(read(name).length > 0, `${name} must exist and be non-empty`);
  }
});
