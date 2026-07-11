'use strict';

const js = require('@eslint/js');
const globals = require('globals');

// Flat config (ESLint 9). Correctness- and security-sensitive rules only —
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
];
