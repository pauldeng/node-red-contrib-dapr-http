'use strict';

const { test, expect } = require('./helpers/fixtures');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');
const { waitFor } = require('../helpers/wait-for');
const {
  gotoEditor,
  openNodeDialog,
  openConnectionDialog,
  closeDialog,
  openHelpFor,
  HELP_LABELS,
  fullFlow,
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
    {
      id: 'secret',
      type: 'dapr-secret-get',
      z: 'tab',
      name: 'get credential',
      connection: 'conn',
      storeName: 'vault',
      key: 'apiKey',
      property: 'payload',
      metadata: '{}',
      x: 200,
      y: 280,
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

test('Test Connection reports a bounded failure and never renders a token', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  // This fixture's daprPort has nothing listening behind it (no real daprd
  // in the e2e tier) -- a genuine, unenhanced SIDECAR_UNAVAILABLE failure,
  // proving the button/AJAX/error-rendering path end to end even though the
  // success path needs a real sidecar (covered at the runtime tier instead).
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'sub');
  await openConnectionDialog(page);
  await page.fill('#node-config-input-daprApiToken', 'secret-dapr-token');

  const result = page.locator('#dapr-test-connection-result');
  await page.click('#dapr-test-connection');
  await expect(result).toBeVisible();
  await expect(result).toHaveClass(/dapr-test-connection-error/);
  await expect(result).toContainText(/could not reach/i);
  await expect(result).not.toContainText('secret-dapr-token');

  const bodyText = await page.locator('.red-ui-tray-content').last().innerText();
  expect(bodyText).not.toContain('secret-dapr-token');
});

test('closing the connection dialog cancels a pending Test Connection request', async ({
  page,
  nr,
  appPort,
}) => {
  const dapr = await createFakeDaprStarted();
  let metadataClosed = false;
  dapr.respond('GET', '/v1.0/healthz/outbound', (_req, res) => res.writeHead(204).end());
  dapr.respond('GET', '/v1.0/metadata', (_req, res) => {
    res.on('close', () => {
      metadataClosed = true;
    });
  });

  try {
    await nr.deploy(interactionsFlow({ appPort, daprPort: dapr.port }));
    await gotoEditor(page, nr);
    await openNodeDialog(page, 'sub');
    await openConnectionDialog(page);
    await page.click('#dapr-test-connection');
    await waitFor(() => dapr.requests.find((request) => request.path === '/v1.0/metadata'));

    await closeDialog(page, { save: false, config: true });
    await waitFor(() => metadataClosed || null, { timeoutMs: 2000 });
  } finally {
    await dapr.stop();
  }
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

test('a legacy ack opens with the safe fixed default and persists message mode', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  const flow = fullFlow({ appPort, daprPort });
  const ack = flow.find((node) => node.id === 'e2e-ack');
  delete ack.ackStatusSource;
  delete ack.ackStatus;
  ack.status = 'RETRY';
  await nr.deploy(flow);
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'e2e-ack');
  await expect(page.locator('#node-input-ackStatusSource')).toHaveValue('fixed');
  await expect(page.locator('#node-input-ackStatus')).toHaveValue('SUCCESS');
  await expect(page.locator('#node-ack-fixed-status')).toBeVisible();

  await page.selectOption('#node-input-ackStatusSource', 'message');
  await expect(page.locator('#node-ack-fixed-status')).toBeHidden();
  await closeDialog(page, { save: true });

  await openNodeDialog(page, 'e2e-ack');
  await expect(page.locator('#node-input-ackStatusSource')).toHaveValue('message');
  await expect(page.locator('#node-ack-fixed-status')).toBeHidden();
});

test('a successful acknowledgement does not send a status update to the editor', async ({
  page,
  nr,
  appPort,
}) => {
  const dapr = await createFakeDaprStarted();
  dapr.respond('GET', '/v1.0/healthz/outbound', (_req, res) => res.writeHead(204).end());
  try {
    await nr.deploy([
      { id: 'tab', type: 'tab', label: 'ack-status' },
      {
        id: 'conn',
        type: 'dapr-connection',
        daprHost: '127.0.0.1',
        daprPort: String(dapr.port),
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
      },
      {
        id: 'sub',
        type: 'dapr-subscribe',
        z: 'tab',
        connection: 'conn',
        pubsubName: 'pubsub',
        topic: 'orders',
        ackMode: 'manual',
        metadata: '{}',
        wires: [['ack']],
      },
      {
        id: 'ack',
        type: 'dapr-ack',
        z: 'tab',
        connection: 'conn',
        ackStatusSource: 'fixed',
        ackStatus: 'SUCCESS',
        wires: [[]],
      },
    ]);
    await gotoEditor(page, nr);
    await waitFor(async () => {
      const response = await httpRequest(`http://127.0.0.1:${appPort}/dapr/subscribe`, {
        timeoutMs: 1000,
      });
      return response.status === 200 && response.text.includes('/node-red-dapr/subscriptions/sub');
    });
    await page.evaluate(() => {
      window.ackStatusUpdates = [];
      RED.comms.subscribe('status/ack', (_topic, status) => window.ackStatusUpdates.push(status));
    });

    const delivery = await httpRequest(
      `http://127.0.0.1:${appPort}/node-red-dapr/subscriptions/sub`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/cloudevents+json' },
        body: JSON.stringify({
          specversion: '1.0',
          id: 'e1',
          source: 'test',
          type: 'order',
          data: { orderId: 7 },
        }),
        timeoutMs: 5000,
      }
    );
    expect(delivery.status).toBe(200);
    expect(JSON.parse(delivery.text)).toEqual({ status: 'SUCCESS' });
    // A short settle is intentional: this is a negative assertion about the
    // browser-side status channel after the delivery itself has completed.
    await page.waitForTimeout(250);
    expect(await page.evaluate(() => window.ackStatusUpdates)).toEqual([]);
    await expect(page.locator('g#ack .red-ui-flow-node-status-group')).toBeHidden();
  } finally {
    await dapr.stop();
  }
});

test('secret output property is a static msg path, not a message-directed destination', async ({
  page,
  nr,
  appPort,
  daprPort,
}) => {
  await nr.deploy(interactionsFlow({ appPort, daprPort }));
  await gotoEditor(page, nr);

  await openNodeDialog(page, 'secret');
  const propertyContainer = page.locator('#node-input-property + .red-ui-typedInput-container');
  const property = propertyContainer.locator('.red-ui-typedInput-input');
  await expect(propertyContainer).toBeVisible();

  await property.fill('payload[msg._msgid]');
  await property.blur();
  await expect(propertyContainer).toHaveClass(/input-error/);

  await property.fill('dapr.secret');
  await property.blur();
  await expect(propertyContainer).toHaveClass(/input-error/);

  await property.fill('__proto__.secret');
  await property.blur();
  await expect(propertyContainer).toHaveClass(/input-error/);

  await property.fill('secret.value');
  await property.blur();
  await expect(propertyContainer).not.toHaveClass(/input-error/);
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
// Covers every type via the same path, including the config node,
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
