'use strict';

const path = require('node:path');

const { expect } = require('@playwright/test');

// Three viewports, so a dialog is checked at more than one width: a wide
// desktop size, Node-RED's own commonly-documented minimum-comfortable size,
// and a narrower size still wide enough for the editor's three-pane layout
// (this project's own choice for "narrow", not a Node-RED-documented minimum).
const VIEWPORTS = [
  { name: 'wide', width: 1440, height: 900 },
  { name: 'standard', width: 1024, height: 768 },
  { name: 'narrow', width: 800, height: 600 },
];

const THEMES = ['light', 'dark'];

const SCREENSHOT_DIR = path.join(__dirname, '..', 'screenshots');

function screenshotPath(name) {
  return path.join(SCREENSHOT_DIR, `${name}.png`);
}

// The editor persists theme choice to localStorage under this key (confirmed
// by driving a real editor: Settings > Appearance > Theme writes
// `view-dark-theme`). Setting it before navigation avoids clicking through
// the settings dialog in every single test.
async function gotoEditor(page, nr, { theme = 'light' } = {}) {
  await page.addInitScript((t) => {
    window.localStorage.setItem('view-dark-theme', t);
  }, theme);
  await page.goto(nr.adminUrl('/'));
  await page.waitForSelector('#red-ui-workspace', { state: 'visible' });
  await page.waitForSelector('#red-ui-palette-network [data-palette-type="dapr-publish"]', {
    state: 'visible',
  });
}

// Opens a node's edit dialog by double-clicking its canvas representation.
// Node-RED renders each node as an SVG <g> whose own `id` attribute is the
// flow's node id (confirmed by inspecting a real deployed flow's DOM) — no
// drag-and-drop simulation needed since the flow is already deployed.
// The tray slides in over a CSS transition; "visible" is true well before the
// slide finishes, and clicking a button mid-slide is exactly what Playwright's
// actionability check flags as "not stable" (found by driving this for real).
async function waitForTraySettled(page) {
  await page.waitForFunction(() => {
    const trays = Array.from(document.querySelectorAll('.red-ui-tray')).filter((tray) => {
      const rect = tray.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    });
    return trays.every((tray) =>
      tray
        .getAnimations({ subtree: true })
        .every((animation) => animation.playState === 'finished' || animation.playState === 'idle')
    );
  });
}

// The gesture is retried because the editor redraws the canvas on its own
// schedule — notably right after a config-node save — and a redraw between the
// two clicks replaces the target SVG element, so the browser sees two unrelated
// single clicks: the node ends up merely SELECTED and no dialog opens. Waiting
// longer cannot fix that (nothing is pending), and re-issuing the double-click
// on the redrawn element does. Reproduced at 2/10 with --repeat-each before this
// retry, 0/20 after; the diagnosis came from a retained trace showing the node's
// quick-action toolbar (selection) and no tray in the DOM.
async function openNodeDialog(page, nodeId) {
  const node = page.locator(`g#${nodeId} rect.red-ui-flow-node`).first();
  await expect(async () => {
    await node.dblclick();
    await page.waitForSelector('#node-dialog-ok', { state: 'visible', timeout: 2000 });
  }).toPass({ timeout: 30000 });
  await waitForTraySettled(page);
}

// Opens the referenced dapr-connection config node's own edit dialog, nested
// inside the currently-open node dialog — Node-RED's own typed config-node
// field renders this as a stable-id pencil icon `<a>` (confirmed via a real
// dialog's DOM), not a <button>. A config node's nested tray uses its own
// `node-config-dialog-*` ids for Done/Cancel, distinct from a regular node's
// `node-dialog-*` — the parent's own (still-present-but-covered) `#node-dialog-ok`
// would otherwise make this wait resolve immediately without the nested
// dialog having opened at all (found by driving this for real).
async function openConnectionDialog(page) {
  await page.locator('#node-input-btn-connection-edit').click();
  await page.waitForSelector('#node-config-dialog-ok', { state: 'visible' });
  await waitForTraySettled(page);
}

// `config: true` closes a nested config-node dialog (`node-config-dialog-*`
// ids) opened via openConnectionDialog; otherwise a regular node dialog
// (`node-dialog-*` ids) opened via openNodeDialog.
// Retried for the same reason openNodeDialog is, and against the same editor
// behavior: the canvas/tray redraws on its own schedule, and a click that lands
// mid-redraw is swallowed — the button stays attached and no amount of waiting
// closes it, because nothing is pending. Seen once as three failures on a fully
// parallel local run (all three in this helper, all "waiting for
// #node-dialog-cancel to be detached"), and not reproduced since across 54
// stressed runs, so this is defensive: the gesture openNodeDialog already
// retries is exactly the gesture this one performs. The count() guard keeps a
// retry from clicking a button an earlier attempt already dismissed, which would
// otherwise burn the whole toPass budget waiting for a detached element.
async function closeDialog(page, { save = true, config = false } = {}) {
  const prefix = config ? '#node-config-dialog-' : '#node-dialog-';
  const selector = prefix + (save ? 'ok' : 'cancel');
  await expect(async () => {
    if ((await page.locator(selector).count()) === 0) {
      return;
    }
    await page.locator(selector).click({ timeout: 2000 });
    await page.waitForSelector(selector, { state: 'detached', timeout: 2000 });
  }).toPass({ timeout: 30000 });
  await waitForTraySettled(page);
}

// A complete flow exercising every node type together: publish,
// subscribe with a CEL rule and bulk delivery enabled, manual acknowledgement,
// outbound invocation, an inbound service replying via dapr-response, a
// state get, a configuration get, and a configuration subscribe — the
// "complete publish/subscribe and invoke/service flow" the e2e tier must
// verify configures and deploys.
function fullFlow({ appPort, daprPort }) {
  return [
    { id: 'e2e-tab', type: 'tab', label: 'e2e' },
    {
      id: 'e2e-conn',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    {
      id: 'e2e-pub',
      type: 'dapr-publish',
      z: 'e2e-tab',
      name: 'publish orders',
      connection: 'e2e-conn',
      pubsubName: 'pubsub',
      topic: 'orders',
      metadata: '{}',
      x: 160,
      y: 80,
      wires: [[]],
    },
    {
      id: 'e2e-sub',
      type: 'dapr-subscribe',
      z: 'e2e-tab',
      name: 'orders topic',
      connection: 'e2e-conn',
      pubsubName: 'pubsub',
      topic: 'orders',
      ackMode: 'manual',
      rawPayload: false,
      deadLetterTopic: '',
      metadata: '{}',
      rules: [{ id: 'e2e-rule', match: 'event.type == "order"' }],
      bulkEnabled: true,
      bulkMaxMessagesCount: '10',
      bulkMaxAwaitDurationMs: '1000',
      x: 160,
      y: 160,
      wires: [['e2e-ack']],
    },
    {
      id: 'e2e-ack',
      type: 'dapr-ack',
      z: 'e2e-tab',
      name: '',
      connection: 'e2e-conn',
      ackStatusSource: 'fixed',
      ackStatus: 'SUCCESS',
      x: 380,
      y: 160,
      wires: [[]],
    },
    {
      id: 'e2e-invoke',
      type: 'dapr-invoke',
      z: 'e2e-tab',
      name: 'call order-service',
      connection: 'e2e-conn',
      appId: 'order-service',
      method: 'orders/42',
      verb: 'GET',
      x: 160,
      y: 240,
      wires: [[]],
    },
    {
      id: 'e2e-svc',
      type: 'dapr-service',
      z: 'e2e-tab',
      name: 'orders/echo',
      connection: 'e2e-conn',
      verb: 'POST',
      methodPath: '/orders/echo',
      x: 160,
      y: 320,
      wires: [['e2e-resp']],
    },
    {
      id: 'e2e-resp',
      type: 'dapr-response',
      z: 'e2e-tab',
      name: '',
      connection: 'e2e-conn',
      statusCode: '200',
      x: 380,
      y: 320,
      wires: [],
    },
    {
      id: 'e2e-state',
      type: 'dapr-state',
      z: 'e2e-tab',
      name: 'get order',
      connection: 'e2e-conn',
      operation: 'get',
      storeName: 'statestore',
      key: 'order-1',
      consistency: '',
      concurrency: '',
      ttlSeconds: '',
      metadata: '{}',
      x: 160,
      y: 400,
      wires: [[]],
    },
    {
      id: 'e2e-config-get',
      type: 'dapr-config-get',
      z: 'e2e-tab',
      name: 'get feature flag',
      connection: 'e2e-conn',
      storeName: 'configstore',
      keys: 'featureFlag',
      metadata: '{}',
      x: 160,
      y: 480,
      wires: [[]],
    },
    {
      id: 'e2e-config-sub',
      type: 'dapr-config-subscribe',
      z: 'e2e-tab',
      name: 'watch feature flag',
      connection: 'e2e-conn',
      storeName: 'configstore',
      keys: 'featureFlag',
      metadata: '{}',
      x: 160,
      y: 560,
      wires: [[]],
    },
    {
      id: 'e2e-binding-out',
      type: 'dapr-binding-out',
      z: 'e2e-tab',
      name: 'create order',
      connection: 'e2e-conn',
      bindingName: 'orders-binding',
      operation: 'create',
      metadata: '{}',
      x: 160,
      y: 640,
      wires: [[]],
    },
    {
      id: 'e2e-secret-get',
      type: 'dapr-secret-get',
      z: 'e2e-tab',
      name: 'get credential',
      connection: 'e2e-conn',
      storeName: 'vault',
      key: 'apiKey',
      property: 'payload',
      metadata: '{}',
      x: 160,
      y: 720,
      wires: [[]],
    },
    {
      id: 'e2e-actor-method',
      type: 'dapr-actor-method',
      z: 'e2e-tab',
      name: 'get data',
      connection: 'e2e-conn',
      actorType: 'DemoActor',
      method: 'GetMyData',
      x: 160,
      y: 800,
      wires: [[]],
    },
    {
      id: 'e2e-actor-reply',
      type: 'dapr-actor-reply',
      z: 'e2e-tab',
      name: 'reply complete',
      connection: 'e2e-conn',
      outcome: 'complete',
      x: 380,
      y: 800,
      wires: [],
    },
    {
      id: 'e2e-actor-call',
      type: 'dapr-actor-call',
      z: 'e2e-tab',
      name: 'call actor',
      connection: 'e2e-conn',
      actorType: 'DemoActor',
      actorId: 'demo-1',
      method: 'GetMyData',
      x: 160,
      y: 880,
      wires: [[]],
    },
    {
      id: 'e2e-actor-schedule',
      type: 'dapr-actor-schedule',
      z: 'e2e-tab',
      name: 'schedule reminder',
      connection: 'e2e-conn',
      operation: 'set',
      actorType: 'DemoActor',
      actorId: 'demo-1',
      scheduleName: 'demo_reminder',
      x: 160,
      y: 960,
      wires: [[]],
    },
  ];
}

// Regression lock for a real bug: an open dialog's content div can measure
// wider than the tray's own wrapper (Node-RED sizes `.red-ui-tray-body` to
// the widest UNCONSTRAINED text it finds, then `.red-ui-tray-body-wrapper`
// clips the rest via `overflow: hidden` — no scrollbar, content is simply
// unreachable). Comparing scrollWidth against the wrapper's clientWidth
// catches this directly, regardless of which row causes it.
// Tolerance is deliberately larger than a rounding margin: any node with a
// rule editableList (min-width 450px) — including Node-RED's own core
// Switch node, confirmed by driving it the same way — can still land ~14px
// over its wrapper on a fresh narrow load, a Node-RED editor characteristic
// this package doesn't own or ship. 20px comfortably clears that known,
// measured gap while still catching a real regression: the bug this
// assertion was written for overflowed by 250-500px.
const OVERFLOW_TOLERANCE_PX = 20;

// Text that cannot wrap is a SECOND, independent clipping mechanism, and the
// tray-level measurement above cannot see it: a block with `width: auto` stays
// inside its container, so its overflowing inline text never widens the tray.
// Verified by forcing `white-space: nowrap` onto a tip — the tray-level overflow
// above stayed at exactly 0px while the text was unreadable.
//
// Scoped to the prose-bearing elements this package styles, NOT every `dapr-`
// element: a form container legitimately reports content wider than itself when
// it holds Node-RED's own rule editableList (min-width 450px), which is the same
// known characteristic OVERFLOW_TOLERANCE_PX above accounts for. Measuring
// containers here would re-report that as a text bug.
const TEXT_ELEMENT_SELECTOR = [
  '.dapr-form-tip',
  '.dapr-field-hint',
  '.dapr-unit-label',
  '.dapr-checkbox-copy',
  '.dapr-cel-summary',
  '.dapr-bind-warning',
].join(', ');
const TEXT_CLIP_TOLERANCE_PX = 2; // sub-pixel layout rounding only

async function assertNoHorizontalOverflow(page) {
  const overflow = await page.evaluate(() => {
    const wrapper = document.querySelector('.red-ui-tray-body-wrapper');
    const body = document.querySelector('.red-ui-tray-body');
    return body.scrollWidth - wrapper.clientWidth;
  });
  if (overflow > OVERFLOW_TOLERANCE_PX) {
    throw new Error(
      `dialog content overflows its tray by ${overflow}px — some content is clipped and unreachable`
    );
  }

  const clipped = await page.evaluate(
    ({ selector, tolerance }) =>
      Array.from(document.querySelectorAll(selector))
        .filter(
          (el) =>
            el.getBoundingClientRect().width > 0 &&
            el.clientWidth > 0 &&
            el.scrollWidth - el.clientWidth > tolerance
        )
        .map((el) => `${el.className || el.tagName} (${el.scrollWidth}px in ${el.clientWidth}px)`),
    { selector: TEXT_ELEMENT_SELECTOR, tolerance: TEXT_CLIP_TOLERANCE_PX }
  );
  if (clipped.length > 0) {
    throw new Error(`dialog text is clipped inside its own element: ${clipped.join('; ')}`);
  }
}

// Every node's Help tree label, keyed by type. Palette nodes have an
// explicit `paletteLabel` ('dapr publish', 'dapr subscribe', ...); the
// config node (`dapr-connection`) has none and falls back to its raw,
// hyphenated type — confirmed by driving a real Help search for each.
const HELP_LABELS = {
  'dapr-connection': 'dapr-connection',
  'dapr-publish': 'dapr publish',
  'dapr-subscribe': 'dapr subscribe',
  'dapr-ack': 'dapr ack',
  'dapr-invoke': 'dapr invoke',
  'dapr-service': 'dapr service',
  'dapr-response': 'dapr response',
  'dapr-state': 'dapr state',
  'dapr-config-get': 'dapr config get',
  'dapr-config-subscribe': 'dapr config subscribe',
  'dapr-binding-out': 'dapr binding out',
  'dapr-secret-get': 'dapr secret get',
  'dapr-actor-method': 'actor method',
  'dapr-actor-reply': 'actor reply',
  'dapr-actor-call': 'actor call',
  'dapr-actor-schedule': 'actor schedule',
};

// Opens the Help sidebar and navigates straight to one node type's own
// documentation via its search box — this works uniformly for every node
// types, including the config node (which has no canvas presence to select
// and no direct per-dialog Help button; confirmed by driving a real editor).
// `.last()` on the tree-label match sidesteps an unrelated, currently
// invisible same-text entry the Explorer sidebar can also carry (e.g. a
// collapsed "Global Configuration Nodes > dapr-connection" category row).
async function openHelpFor(page, type) {
  await page.click('#red-ui-header-button-sidemenu');
  await page.waitForSelector('#red-ui-header-button-sidemenu-submenu', { state: 'visible' });
  await page
    .locator('#red-ui-header-button-sidemenu-submenu a', { hasText: 'View' })
    .first()
    .hover();
  await page.waitForSelector('#menu-item-view-menu-help', { state: 'visible' });
  await page.click('#menu-item-view-menu-help');
  await page.waitForSelector('input[placeholder="Search help"]', { state: 'visible' });
  await page.fill('input[placeholder="Search help"]', type);
  await page.press('input[placeholder="Search help"]', 'Enter');
  const helpItem = page.locator('.red-ui-treeList-label', { hasText: HELP_LABELS[type] }).last();
  await helpItem.waitFor({ state: 'visible' });
  await helpItem.click();
  await page.waitForSelector('.red-ui-help .red-ui-help-title', { state: 'visible' });
}

module.exports = {
  VIEWPORTS,
  THEMES,
  screenshotPath,
  gotoEditor,
  openNodeDialog,
  openConnectionDialog,
  closeDialog,
  fullFlow,
  assertNoHorizontalOverflow,
  openHelpFor,
  HELP_LABELS,
};
