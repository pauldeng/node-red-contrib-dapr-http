'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { createConnectionRegistry } = require('../../lib/connection-registry');
const { buildSubscription } = require('../../lib/subscriptions');
const { buildService } = require('../../lib/services');
const { DaprError, ErrorCodes } = require('../../lib/errors');

const handler = () => {};
const subscription = (nodeId, topic = 'orders', pubsubName = 'pubsub') =>
  buildSubscription({ nodeId, pubsubName, topic });

test('an empty registry activates with no routes and a stable fingerprint', () => {
  const a = createConnectionRegistry().activation();
  const b = createConnectionRegistry().activation();
  assert.deepEqual(a.subscriptions, []);
  assert.deepEqual(a.routes, []);
  // Stable across instances: the fingerprint is what decides "daprd needs
  // restarting", so two empty registries must not look different.
  assert.equal(a.fingerprint, b.fingerprint);
});

test('a subscription contributes a discovery entry and one internal route per route', () => {
  const registry = createConnectionRegistry();
  registry.addSubscription(subscription('n1'), handler);
  const { subscriptions, routes } = registry.activation();

  assert.equal(subscriptions.length, 1);
  assert.equal(registry.subscriptionCount, 1);
  assert.ok(routes.length >= 1);
  // Delivery routes are internal: a mesh caller must not be able to address
  // them as if they were service methods.
  assert.ok(routes.every((r) => r.kind === 'internal' && r.method === 'POST'));
});

test('two subscriptions on the same pubsub/topic are rejected', () => {
  const registry = createConnectionRegistry();
  registry.addSubscription(subscription('n1'), handler);
  assert.throws(
    () => registry.addSubscription(subscription('n2'), handler),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('re-registering the same nodeId replaces rather than conflicts with itself', () => {
  // A redeploy re-registers the same node; that must not look like a duplicate.
  const registry = createConnectionRegistry();
  registry.addSubscription(subscription('n1'), handler);
  registry.addSubscription(subscription('n1'), handler);
  assert.equal(registry.subscriptionCount, 1);
});

test('removing a subscription drops its routes and its discovery entry', () => {
  const registry = createConnectionRegistry();
  const remove = registry.addSubscription(subscription('n1'), handler);
  remove();
  const { subscriptions, routes } = registry.activation();
  assert.equal(registry.subscriptionCount, 0);
  assert.deepEqual(subscriptions, []);
  assert.deepEqual(routes, []);
});

test('service and internal routes share one path space for duplicate detection', () => {
  // They are served by the same app channel, so a service method and a
  // config-subscribe callback claiming the same verb+path is a real collision
  // even though they live in different registries.
  const registry = createConnectionRegistry();
  registry.addRoute(
    'service',
    buildService({ nodeId: 's1', verb: 'POST', path: '/shared' }),
    handler
  );
  assert.throws(
    () => registry.addRoute('internal', { nodeId: 'i1', verb: 'POST', path: '/shared' }, handler),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('service routes stay service-kind and are never advertised for discovery', () => {
  const registry = createConnectionRegistry();
  registry.addRoute(
    'service',
    buildService({ nodeId: 's1', verb: 'GET', path: '/orders' }),
    handler
  );
  const { subscriptions, routes } = registry.activation();
  assert.deepEqual(subscriptions, [], 'service methods are not part of the Dapr subscription set');
  assert.equal(routes.length, 1);
  assert.equal(routes[0].kind, 'service');
  assert.equal(routes[0].method, 'GET');
});

test('an unknown route kind is rejected rather than silently ignored', () => {
  const registry = createConnectionRegistry();
  assert.throws(
    () => registry.addRoute('nonsense', { nodeId: 'x', verb: 'GET', path: '/x' }, handler),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('the fingerprint tracks the subscription set only, not routes', () => {
  const registry = createConnectionRegistry();
  registry.addSubscription(subscription('n1'), handler);
  const withSubscription = registry.activation().fingerprint;

  // Adding a service method changes what is served but NOT what daprd
  // fetched, so it must not demand a sidecar restart.
  registry.addRoute(
    'service',
    buildService({ nodeId: 's1', verb: 'GET', path: '/orders' }),
    handler
  );
  assert.equal(registry.activation().fingerprint, withSubscription);

  registry.addSubscription(subscription('n2', 'shipments'), handler);
  assert.notEqual(registry.activation().fingerprint, withSubscription);
});
