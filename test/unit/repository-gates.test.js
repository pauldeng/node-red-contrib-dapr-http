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
  for (const command of [
    'npm audit --omit=dev',
    'npm pack --dry-run',
    'git diff --check "${{ github.event.pull_request.base.sha || github.event.before }}...HEAD"',
    "printf '@AGENTS.md\\n' | cmp -s - CLAUDE.md",
    'docker compose config --quiet',
  ]) {
    assert.match(workflow, new RegExp(command.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }
});
