'use strict';

const { test, expect } = require('./helpers/fixtures');
const {
  gotoEditor,
  openNodeDialog,
  openConnectionDialog,
  closeDialog,
  fullFlow,
  screenshotPath,
  assertNoHorizontalOverflow,
  VIEWPORTS,
  THEMES,
} = require('./helpers/editor');

// Every non-config node in the complete flow (test/e2e/helpers/editor.js).
// dapr-connection is checked separately below, nested inside dapr-publish's
// dialog — the same path a real user takes via the Connection field's edit
// button.
const NODES = [
  { id: 'e2e-pub', type: 'dapr-publish' },
  { id: 'e2e-sub', type: 'dapr-subscribe' },
  { id: 'e2e-ack', type: 'dapr-ack' },
  { id: 'e2e-invoke', type: 'dapr-invoke' },
  { id: 'e2e-svc', type: 'dapr-service' },
  { id: 'e2e-resp', type: 'dapr-response' },
  { id: 'e2e-state', type: 'dapr-state' },
  { id: 'e2e-config-get', type: 'dapr-config-get' },
  { id: 'e2e-config-sub', type: 'dapr-config-subscribe' },
  { id: 'e2e-binding-out', type: 'dapr-binding-out' },
  { id: 'e2e-secret-get', type: 'dapr-secret-get' },
  { id: 'e2e-actor-method', type: 'dapr-actor-method' },
  { id: 'e2e-actor-reply', type: 'dapr-actor-reply' },
  { id: 'e2e-actor-call', type: 'dapr-actor-call' },
];

// A regression lock for a real bug found while building this tier: a field
// left blank in a hand-authored/imported flow is `undefined` at the node
// level, not '' — Node-RED does not backfill defaults.value for a missing
// key on import. Several optional fields' validate functions only tolerated
// '' and showed a permanent, non-clearing validation error for the exact
// blank state real example flows ship with (see nodes/dapr-connection.html
// and nodes/dapr-response.html). This flow intentionally omits every such
// optional field to lock the fix in place.
for (const theme of THEMES) {
  test(`every node dialog opens with no validation error (${theme} theme)`, async ({
    page,
    nr,
    appPort,
    daprPort,
  }) => {
    await nr.deploy(fullFlow({ appPort, daprPort }));
    await gotoEditor(page, nr, { theme });

    for (const { id } of NODES) {
      await openNodeDialog(page, id);
      await expect(page.locator('.red-ui-tray-content .input-error')).toHaveCount(0);
      await closeDialog(page, { save: false });
    }

    await openNodeDialog(page, 'e2e-pub');
    await openConnectionDialog(page);
    await expect(page.locator('.red-ui-tray-content .input-error')).toHaveCount(0);
    await closeDialog(page, { save: false, config: true }); // connection dialog
    await closeDialog(page, { save: false }); // publish dialog
  });
}

// Screenshots for manual visual inspection (light/dark, three viewports),
// covering every node's dialog including `dapr-connection` and `dapr-publish`. Not a pass/fail assertion
// beyond "the dialog actually opened" — a human inspects the images for
// clipped labels, overflow, overlap, or unreadable status.
for (const theme of THEMES) {
  for (const viewport of VIEWPORTS) {
    test(`screenshot every dialog (${theme} theme, ${viewport.name} viewport)`, async ({
      page,
      nr,
      appPort,
      daprPort,
    }) => {
      await nr.deploy(fullFlow({ appPort, daprPort }));
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await gotoEditor(page, nr, { theme });

      for (const { id, type } of NODES) {
        await openNodeDialog(page, id);
        await expect(page.locator('#node-dialog-ok')).toBeVisible();
        await assertNoHorizontalOverflow(page);
        await page.screenshot({ path: screenshotPath(`${type}-${theme}-${viewport.name}`) });
        await closeDialog(page, { save: false });
      }

      await openNodeDialog(page, 'e2e-pub');
      await openConnectionDialog(page);
      await expect(page.locator('#node-config-dialog-ok')).toBeVisible();
      await assertNoHorizontalOverflow(page);
      await page.screenshot({
        path: screenshotPath(`dapr-connection-${theme}-${viewport.name}`),
      });
      const tracing = page.locator('#node-config-input-tracingEnabled');
      await tracing.scrollIntoViewIfNeeded();
      await expect(tracing).toBeVisible();
      await page.screenshot({
        path: screenshotPath(`dapr-connection-tracing-${theme}-${viewport.name}`),
      });
      await closeDialog(page, { save: false, config: true });
      await closeDialog(page, { save: false });
    });
  }
}
