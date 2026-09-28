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

// ---- Actor methods ----

test('actorConfig and actorFingerprint are null when no actor method is registered', () => {
  const registry = createConnectionRegistry();
  const { actorConfig, actorFingerprint } = registry.activation();
  assert.equal(actorConfig, null);
  assert.equal(actorFingerprint, null);
});

test('an actor method contributes its type to actorConfig.entities, sorted and deduplicated', () => {
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'm1', actorType: 'Zeta', method: 'A' }, handler);
  registry.addActorMethod({ nodeId: 'm2', actorType: 'Alpha', method: 'B' }, handler);
  registry.addActorMethod({ nodeId: 'm3', actorType: 'Alpha', method: 'C' }, handler);
  const { actorConfig, actorFingerprint } = registry.activation();
  assert.deepEqual(actorConfig.entities, ['Alpha', 'Zeta']);
  assert.deepEqual(actorConfig.reentrancy, { enabled: false });
  assert.equal(actorConfig.drainRebalancedActors, true);
  assert.ok(actorFingerprint);
});

test('drainOngoingCallTimeout reflects the connection request timeout, in seconds', () => {
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'M' }, handler);
  const { actorConfig } = registry.activation({ requestTimeoutMs: 45000 });
  assert.equal(actorConfig.drainOngoingCallTimeout, '45s');
});

test('actorHandlerFor resolves the registered emit function for (type, method)', () => {
  const registry = createConnectionRegistry();
  const emit = () => {};
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'Do' }, emit);
  assert.equal(registry.actorHandlerFor('T', 'Do'), emit);
  assert.equal(registry.actorHandlerFor('T', 'Other'), undefined);
  assert.equal(registry.actorHandlerFor('Other', 'Do'), undefined);
});

test('adding a method to an already-registered type does not change the fingerprint', () => {
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'A' }, handler);
  const before = registry.activation().actorFingerprint;
  registry.addActorMethod({ nodeId: 'm2', actorType: 'T', method: 'B' }, handler);
  assert.equal(registry.activation().actorFingerprint, before);
});

test('a new actor type changes the actor fingerprint', () => {
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'A' }, handler);
  const before = registry.activation().actorFingerprint;
  registry.addActorMethod({ nodeId: 'm2', actorType: 'Other', method: 'A' }, handler);
  assert.notEqual(registry.activation().actorFingerprint, before);
});

test('a duplicate (actorType, method) owned by another node is rejected', () => {
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'Do' }, handler);
  assert.throws(
    () => registry.addActorMethod({ nodeId: 'm2', actorType: 'T', method: 'Do' }, handler),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('re-registering the same nodeId is not a duplicate (redeploy)', () => {
  const registry = createConnectionRegistry();
  const oldHandler = () => 'old';
  const newHandler = () => 'new';
  const removeOld = registry.addActorMethod(
    { nodeId: 'm1', actorType: 'T', method: 'Do' },
    oldHandler
  );
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'Do' }, newHandler);
  assert.equal(registry.activation().actorConfig.entities.length, 1);
  assert.equal(registry.actorHandlerFor('T', 'Do'), newHandler);

  // A delayed close from the old registration must not remove its replacement.
  removeOld();
  assert.equal(registry.actorHandlerFor('T', 'Do'), newHandler);
});

test('overwriting a nodeId with a new pair removes the prior indexed pair', () => {
  const registry = createConnectionRegistry();
  const oldHandler = () => 'old';
  const newHandler = () => 'new';
  const removeOld = registry.addActorMethod(
    { nodeId: 'm1', actorType: 'OldType', method: 'Do' },
    oldHandler
  );
  registry.addActorMethod({ nodeId: 'm1', actorType: 'NewType', method: 'Run' }, newHandler);

  assert.equal(registry.actorHandlerFor('OldType', 'Do'), undefined);
  assert.equal(registry.actorHandlerFor('NewType', 'Run'), newHandler);
  removeOld();
  assert.equal(registry.actorHandlerFor('NewType', 'Run'), newHandler);
});

test('an invalid, empty, or unsafe actorType or method is rejected as INVALID_OPTIONS', () => {
  const registry = createConnectionRegistry();
  for (const bad of [
    { actorType: '', method: 'M' },
    { actorType: 'T', method: '' },
    { actorType: '..', method: 'M' },
    { actorType: 'a/b', method: 'M' },
  ]) {
    assert.throws(
      () => registry.addActorMethod({ nodeId: 'x', ...bad }, handler),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
    );
  }
});

test('removing an actor method drops its type from actorConfig, and clears actorConfig entirely when none remain', () => {
  const registry = createConnectionRegistry();
  const remove = registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'Do' }, handler);
  remove();
  assert.equal(registry.activation().actorConfig, null);
});

test('an actor method collides with an existing service route under /actors (actor registered second)', () => {
  const registry = createConnectionRegistry();
  registry.addRoute(
    'service',
    buildService({ nodeId: 's1', verb: 'GET', path: '/actors/legacy' }),
    handler
  );
  assert.throws(
    () => registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'Do' }, handler),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('a service route under /actors collides with an existing actor method (service registered second)', () => {
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'Do' }, handler);
  assert.throws(
    () =>
      registry.addRoute(
        'service',
        buildService({ nodeId: 's1', verb: 'GET', path: '/actors' }),
        handler
      ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
  );
  assert.throws(
    () =>
      registry.addRoute(
        'service',
        buildService({ nodeId: 's2', verb: 'GET', path: '/actors/x' }),
        handler
      ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('a service route elsewhere is unaffected by actor registration', () => {
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'm1', actorType: 'T', method: 'Do' }, handler);
  registry.addRoute(
    'service',
    buildService({ nodeId: 's1', verb: 'GET', path: '/orders' }),
    handler
  );
  assert.equal(
    registry.activation().routes.some((r) => r.path === '/orders'),
    true
  );
});
