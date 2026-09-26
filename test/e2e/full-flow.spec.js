'use strict';

const fs = require('node:fs');
const path = require('node:path');

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

test('the actor-demo example deploys through the editor without invalid actor nodes', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  // Established pattern for this tier (deploy.js posts straight to the admin
  // API's /flows, the same one the editor's own Deploy button hits) rather
  // than driving the editor's clipboard-import dialog.
  const examplePath = path.join(__dirname, '..', '..', 'examples', 'actor-demo.json');
  const flow = JSON.parse(fs.readFileSync(examplePath, 'utf8'));
  const connection = flow.find((node) => node.type === 'dapr-connection');
  connection.daprHost = '127.0.0.1';
  connection.daprPort = String(daprPort);
  connection.appPort = String(appPort);

  await nr.deploy(flow);
  await gotoEditor(page, nr);

  // A real edit through the editor UI, then Deploy, proves the example
  // actually deploys through the editor rather than merely having been
  // pre-seeded valid via the admin API.
  await openNodeDialog(page, 'ex-actor-call');
  await page.fill('#node-input-name', 'actor call (edited)');
  await closeDialog(page, { save: true });

  await expect(page.locator('#red-ui-header-button-deploy')).not.toHaveClass(/disabled/);
  await page.click('#red-ui-header-button-deploy');
  await expect(page.locator('#red-ui-header-button-deploy')).toHaveClass(/disabled/, {
    timeout: 10000,
  });
  await expect(page.locator('.red-ui-notification-error')).toHaveCount(0);

  const actorNodeIds = flow
    .filter((node) => typeof node.type === 'string' && node.type.startsWith('dapr-actor-'))
    .map((node) => node.id);
  expect(actorNodeIds.length).toBeGreaterThan(0);
  const validity = await page.evaluate(
    (ids) => ids.map((id) => RED.nodes.node(id)?.valid),
    actorNodeIds
  );
  expect(validity).toEqual(actorNodeIds.map(() => true));
});
