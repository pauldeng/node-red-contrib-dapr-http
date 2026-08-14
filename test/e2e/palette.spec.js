'use strict';

const { test, expect } = require('./helpers/fixtures');
const { gotoEditor } = require('./helpers/editor');

// Ten of the eleven node types are draggable palette entries; dapr-connection
// is a config node and never appears in the palette itself (confirmed by
// inspecting a real editor's palette DOM) — it's checked separately below via
// the node-type registry, the same thing the editor itself consults to offer
// it in a "add new config node" dropdown.
const PALETTE_NODE_TYPES = [
  'dapr-publish',
  'dapr-subscribe',
  'dapr-ack',
  'dapr-invoke',
  'dapr-service',
  'dapr-response',
  'dapr-state',
  'dapr-config-get',
  'dapr-config-subscribe',
  'dapr-binding-out',
];

test('all eleven node types register with the editor', async ({ page, nr }) => {
  await gotoEditor(page, nr);

  for (const type of PALETTE_NODE_TYPES) {
    await expect(
      page.locator(`#red-ui-palette-network [data-palette-type="${type}"]`)
    ).toBeVisible();
  }

  const connectionRegistered = await page.evaluate(() => !!RED.nodes.getType('dapr-connection'));
  expect(connectionRegistered).toBe(true);
});
