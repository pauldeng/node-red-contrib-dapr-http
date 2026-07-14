'use strict';

const { defineConfig, devices } = require('@playwright/test');

// Each spec starts its own real Node-RED process (test/helpers/node-red.js)
// against its own free port, so tests are independent and safe to run fully
// in parallel — there is no shared server/baseURL to configure here.
module.exports = defineConfig({
  testDir: './test/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: 'list',
  timeout: 60000,
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
