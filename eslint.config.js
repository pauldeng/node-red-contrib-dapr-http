'use strict';

const js = require('@eslint/js');
const globals = require('globals');

// Flat config (ESLint 10). Correctness- and security-sensitive rules only —
// formatting is owned by Prettier, so no stylistic rules live here.
module.exports = [
  {
    ignores: ['node_modules/', 'coverage/', 'test-results/', 'playwright-report/'],
  },
  js.configs.recommended,
  {
    files: ['**/*.js'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'commonjs',
      globals: { ...globals.node },
    },
    rules: {
      'no-var': 'error',
      'prefer-const': 'error',
      eqeqeq: ['error', 'always'],
      'no-eval': 'error',
      'no-implied-eval': 'error',
      'no-new-func': 'error',
      'no-script-url': 'error',
      'no-throw-literal': 'error',
      'no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
    },
  },
  {
    // Playwright specs run in Node, but page.evaluate()/addInitScript()
    // callback bodies execute in the browser against the Node-RED editor's
    // own global — lint them as such rather than flagging window/RED as
    // undefined. The empty-destructure fixture parameter (`async ({}, use)`)
    // is Playwright's own documented convention for a fixture with no
    // dependency on other fixtures.
    files: ['test/e2e/**/*.js'],
    languageOptions: {
      globals: { ...globals.node, ...globals.browser, RED: 'readonly' },
    },
    rules: {
      'no-empty-pattern': 'off',
    },
  },
];
