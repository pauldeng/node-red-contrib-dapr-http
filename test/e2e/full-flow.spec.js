'use strict';

const { test, expect } = require('./helpers/fixtures');
const { gotoEditor, openNodeDialog, closeDialog, fullFlow } = require('./helpers/editor');

test('the complete publish/subscribe and invoke/service flow configures and deploys', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(fullFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  // A real, meaningful edit through the editor UI, not just re-deploying an
  // already-valid pre-seeded flow untouched.
  await openNodeDialog(page, 'e2e-pub');
  await page.fill('#node-input-name', 'publish orders (edited)');
  await closeDialog(page, { save: true });

  await expect(page.locator('#red-ui-header-button-deploy')).not.toHaveClass(/disabled/);
  await page.click('#red-ui-header-button-deploy');

  // A flow with an invalid/unconfigured node shows a confirmation prompt
  // before deploying at all; asserting the deploy button returns to its
  // clean, not-dirty state proves the deploy actually completed rather than
  // stalling behind that prompt.
  await expect(page.locator('#red-ui-header-button-deploy')).toHaveClass(/disabled/, {
    timeout: 10000,
  });

  await expect(page.locator('.red-ui-notification-error')).toHaveCount(0);
});
