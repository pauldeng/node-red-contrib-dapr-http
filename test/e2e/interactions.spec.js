'use strict';

const { test, expect } = require('./helpers/fixtures');
const {
  gotoEditor,
  openNodeDialog,
  openConnectionDialog,
  closeDialog,
  openHelpFor,
  HELP_LABELS,
} = require('./helpers/editor');

function interactionsFlow({ appPort, daprPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'interactions' },
    {
      id: 'conn',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    {
      id: 'pub',
      type: 'dapr-publish',
      z: 'tab',
      name: 'publish orders',
      connection: 'conn',
      pubsubName: 'pubsub',
      topic: 'orders',
      metadata: '{}',
      x: 200,
      y: 120,
      wires: [[]],
    },
    {
      id: 'sub',
      type: 'dapr-subscribe',
      z: 'tab',
      name: 'orders topic',
      connection: 'conn',
      pubsubName: 'pubsub',
      topic: 'orders',
      ackMode: 'manual',
      rules: [{ id: 'r1', match: 'event.type == "order"' }],
      x: 200,
      y: 200,
      wires: [[]],
    },
  ];
}

test('connection credentials round-trip through a save and reopen', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'sub');
  await openConnectionDialog(page);
  await page.fill('#node-config-input-daprApiToken', 'secret-dapr-token');
  await page.fill('#node-config-input-appApiToken', 'secret-app-token');
  await closeDialog(page, { save: true, config: true });
  await closeDialog(page, { save: false });

  await openNodeDialog(page, 'sub');
  await openConnectionDialog(page);
  await expect(page.locator('#node-config-input-daprApiToken')).toHaveValue('secret-dapr-token');
  await expect(page.locator('#node-config-input-appApiToken')).toHaveValue('secret-app-token');
});

test('connection appPort validates from invalid back to valid', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'sub');
  await openConnectionDialog(page);

  const portInput = page.locator('#node-config-input-appPort');
  await portInput.fill('');
  await portInput.blur();
  await expect(portInput).toHaveClass(/input-error/);

  await portInput.fill(String(appPort));
  await portInput.blur();
  await expect(portInput).not.toHaveClass(/input-error/);
});

test('subscribe bulk fields show and hide with the checkbox', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'sub');
  const bulkFields = page.locator('#node-bulk-fields');
  await expect(bulkFields).toBeHidden();

  await page.check('#node-input-bulkEnabled');
  await expect(bulkFields).toBeVisible();

  await page.uncheck('#node-input-bulkEnabled');
  await expect(bulkFields).toBeHidden();
});

test('publish bulk mode is off for a legacy flow and persists when enabled', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'pub');
  const bulkEnabled = page.locator('#node-input-bulkEnabled');
  await expect(bulkEnabled).not.toBeChecked();
  await bulkEnabled.check();
  await closeDialog(page, { save: true });

  await openNodeDialog(page, 'pub');
  await expect(page.locator('#node-input-bulkEnabled')).toBeChecked();
});

test('CEL routing rules can be added, edited, reordered, and removed', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'sub');
  const items = page.locator('#node-input-rules-container > li');
  await expect(items).toHaveCount(1);

  // Add a second rule and fill its match expression.
  await page.click('.red-ui-editableList-addButton');
  await expect(items).toHaveCount(2);
  await items.nth(1).locator('.node-input-rule-match').fill('event.type == "payment"');

  const valuesBeforeReorder = await items.evaluateAll((els) =>
    els.map((el) => el.querySelector('.node-input-rule-match').value)
  );
  expect(valuesBeforeReorder).toEqual(['event.type == "order"', 'event.type == "payment"']);

  // Drag the second rule's sort handle above the first — Node-RED's
  // editableList uses jQuery UI sortable, which only responds to a real
  // mouse-move sequence, not a synthetic drag event.
  const handles = items.locator('.red-ui-editableList-item-handle');
  const firstBox = await handles.nth(0).boundingBox();
  const secondBox = await handles.nth(1).boundingBox();
  await page.mouse.move(firstBox.x + firstBox.width / 2, firstBox.y + firstBox.height / 2);
  await page.mouse.down();
  await page.mouse.move(secondBox.x + secondBox.width / 2, secondBox.y + secondBox.height + 5, {
    steps: 10,
  });
  await page.waitForTimeout(150);
  await page.mouse.up();

  const valuesAfterReorder = await items.evaluateAll((els) =>
    els.map((el) => el.querySelector('.node-input-rule-match').value)
  );
  expect(valuesAfterReorder).toEqual(['event.type == "payment"', 'event.type == "order"']);

  // Remove what is now the first rule (the reordered "payment" one) — the
  // remove button is hover-revealed, so hover before clicking.
  await items.first().hover();
  await items.first().locator('.red-ui-editableList-item-remove').click({ force: true });
  await expect(items).toHaveCount(1);
  await expect(items.first().locator('.node-input-rule-match')).toHaveValue(
    'event.type == "order"'
  );
});

// Help sidebar coverage (finding: the interaction suite never opened Help,
// so a style-guide or content regression there would be invisible to CI).
// Covers all seven types via the same path, including the config node,
// which has no canvas presence and no direct per-dialog Help button.
for (const type of Object.keys(HELP_LABELS)) {
  test(`Help sidebar shows ${type}'s own documentation`, async ({
    page,
    nr,
    appPort,
    daprPort,
  }) => {
    await nr.deploy(interactionsFlow({ appPort, daprPort }));
    await gotoEditor(page, nr);

    await openHelpFor(page, type);

    await expect(page.locator('.red-ui-help .red-ui-help-title')).toHaveText(HELP_LABELS[type]);
    // Every node's help includes a short worked example — a real gap found
    // while reviewing this tier: dapr-ack, dapr-connection, and dapr-subscribe
    // originally had none.
    await expect(page.locator('h3', { hasText: 'Example' }).first()).toBeVisible();
  });
}

test('the bind-address warning appears only when the value stops being loopback', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'sub');
  await openConnectionDialog(page);

  const warning = page.locator('#dapr-bind-warning');
  // The deployed flow binds loopback, so the dialog opens without a warning.
  await expect(warning).toBeHidden();

  await page.fill('#node-config-input-bindAddress', '0.0.0.0');
  await expect(warning).toBeVisible();
  await expect(warning).toContainText('Not loopback');

  // Blank means "use the 127.0.0.1 default", which is not a warning either.
  await page.fill('#node-config-input-bindAddress', '');
  await expect(warning).toBeHidden();

  await page.fill('#node-config-input-bindAddress', '10.1.2.3');
  await expect(warning).toBeVisible();

  await page.fill('#node-config-input-bindAddress', '127.0.0.1');
  await expect(warning).toBeHidden();

  await closeDialog(page, { save: false, config: true });
  await closeDialog(page, { save: false });
});
