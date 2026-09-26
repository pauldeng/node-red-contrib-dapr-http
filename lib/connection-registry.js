'use strict';

const crypto = require('node:crypto');

const { DaprError, ErrorCodes } = require('./errors');
const { fingerprint, discoveryEntry } = require('./subscriptions');
const { validateActorSegment, REMINDER_METHOD } = require('./actor-messages');

// Paths reserved by actor routing once any actor method is registered on a
// connection (lib/app-channel.js intercepts /actors and /actors/* itself in
// that case) -- checked in both directions so a service route and an actor
// method can never collide, whichever is registered first.
const pathCollidesWithActors = (path) => path === '/actors' || path.startsWith('/actors/');

// What a dapr-connection aggregates on behalf of every node that references
// it: the subscription set, the registered dapr-service methods, and the
// dynamic internal routes (dapr-config-subscribe's callbacks).
//
// Lives here rather than inside nodes/dapr-connection.js because none of it
// needs Node-RED: it is a pure function of what has been registered, so it is
// directly unit-testable — the layout rule this package sets for itself
// ("nodes/*.js are thin wrappers; lib/*.js is all behavior").
function createConnectionRegistry() {
  const subscriptions = new Map(); // nodeId -> { definition, handler }
  const services = new Map(); // nodeId -> { definition, handler }
  const internalRoutes = new Map(); // nodeId -> { definition, handler }
  const actorMethods = new Map(); // nodeId -> { definition: {nodeId, actorType, method}, handler: emit }

  const routeRegistries = { service: services, internal: internalRoutes };

  return {
    get subscriptionCount() {
      return subscriptions.size;
    },

    // Two subscribe nodes on one connection may not claim the same
    // pubsub/topic: Dapr would receive two discovery entries for it and
    // deliver to only one, silently. Rejected at registration instead.
    addSubscription(definition, handler) {
      for (const { definition: existing } of subscriptions.values()) {
        if (
          existing.nodeId !== definition.nodeId &&
          existing.pubsubName === definition.pubsubName &&
          existing.topic === definition.topic
        ) {
          throw new DaprError(
            ErrorCodes.INVALID_OPTIONS,
            `duplicate subscription for ${definition.pubsubName}/${definition.topic}`
          );
        }
      }
      subscriptions.set(definition.nodeId, { definition, handler });
      return () => subscriptions.delete(definition.nodeId);
    },

    // Service methods and internal routes share one app-channel path space, so
    // a duplicate verb+path is checked across BOTH registries, not just the
    // one being added to.
    addRoute(kind, definition, handler) {
      const registry = routeRegistries[kind];
      if (!registry) {
        throw new DaprError(ErrorCodes.INVALID_OPTIONS, `unknown route kind: ${kind}`);
      }
      const label = kind === 'service' ? 'service method' : 'internal route';
      for (const { definition: existing } of [...services.values(), ...internalRoutes.values()]) {
        if (
          existing.nodeId !== definition.nodeId &&
          existing.verb === definition.verb &&
          existing.path === definition.path
        ) {
          throw new DaprError(
            ErrorCodes.INVALID_OPTIONS,
            `duplicate ${label} ${definition.verb} ${definition.path}`
          );
        }
      }
      if (actorMethods.size > 0 && pathCollidesWithActors(definition.path)) {
        throw new DaprError(
          ErrorCodes.INVALID_OPTIONS,
          `${label} ${definition.path} collides with this connection's actor routing`
        );
      }
      registry.set(definition.nodeId, { definition, handler });
      return () => registry.delete(definition.nodeId);
    },

    // Register a dapr-actor-method node's (actorType, method) pair with its
    // emit function -- or, with `reminder: true`, that node's single
    // reminder registration for the actor type (nodes/dapr-actor-method.js's
    // `trigger: 'reminder'`). `method` is validated as an ordinary,
    // user-supplied method name and used verbatim ONLY when `reminder` is
    // NOT set; when it is, `method` is ignored entirely and the key is
    // always the fixed, internal REMINDER_METHOD -- so a config value that
    // happens to equal it (impossible anyway, since validateActorSegment
    // rejects the "/" REMINDER_METHOD contains) can never reach the registry
    // as anything but an ordinary method name, and never as the reserved
    // key. Throws INVALID_OPTIONS on an invalid/empty/unsafe actorType or
    // method, on a duplicate (actorType, method) owned by another node on
    // this connection, or when a service/internal route on this connection
    // already occupies the /actors namespace that actor routing needs
    // exclusively once any actor method exists.
    addActorMethod({ nodeId, actorType, method, reminder = false } = {}, handler) {
      const type = validateActorSegment(actorType, 'actorType', ErrorCodes.INVALID_OPTIONS);
      const name = reminder
        ? REMINDER_METHOD
        : validateActorSegment(method, 'method', ErrorCodes.INVALID_OPTIONS);
      for (const { definition: existing } of actorMethods.values()) {
        if (existing.nodeId !== nodeId && existing.actorType === type && existing.method === name) {
          throw new DaprError(ErrorCodes.INVALID_OPTIONS, `duplicate actor method ${type}/${name}`);
        }
      }
      for (const { definition: existing } of [...services.values(), ...internalRoutes.values()]) {
        if (pathCollidesWithActors(existing.path)) {
          throw new DaprError(
            ErrorCodes.INVALID_OPTIONS,
            `actor method ${type}/${name} collides with route ${existing.path} on this connection`
          );
        }
      }
      const definition = { nodeId, actorType: type, method: name };
      actorMethods.set(nodeId, { definition, handler });
      return () => actorMethods.delete(nodeId);
    },

    // The emit function registered for (actorType, method), or undefined.
    // Used by the app channel to resolve an inbound PUT
    // /actors/{type}/{id}/method/{method} before admitting it.
    actorHandlerFor(actorType, method) {
      for (const { definition, handler } of actorMethods.values()) {
        if (definition.actorType === actorType && definition.method === method) {
          return handler;
        }
      }
      return undefined;
    },

    // The complete app-channel activation payload for what is registered now:
    // what to advertise on /dapr/subscribe, every route to serve, the
    // fingerprint that decides whether daprd needs restarting, and (when any
    // actor method is registered) the actor config to advertise on
    // /dapr/config plus its own separate fingerprint. `requestTimeoutMs`
    // comes from the connection's own resolved limits (lib/options.js);
    // actor methods themselves are never part of the app-channel route list
    // above -- lib/app-channel.js parses /actors/* itself (see
    // actorHandlerFor) so a redeploy that only adds a method never touches
    // daprd's stale-route bookkeeping for ordinary routes.
    activation({ requestTimeoutMs = 30000 } = {}) {
      const defs = [...subscriptions.values()].map((entry) => entry.definition);
      // One route per subscription route (each CEL rule plus the default),
      // all sharing that subscription's single handler — the handler tells
      // which route matched from ctx.path.
      const deliveryRoutes = [...subscriptions.values()].flatMap(({ definition, handler }) =>
        definition.routes.map((route) => ({
          method: 'POST',
          path: route.path,
          kind: 'internal',
          handler,
        }))
      );
      // Service methods are app-channel routes but not part of the Dapr
      // subscription set, so they are neither advertised nor fingerprinted.
      const serviceRoutes = [...services.values()].map(({ definition, handler }) => ({
        method: definition.verb,
        path: definition.path,
        kind: 'service',
        handler,
      }));
      const dynamicInternalRoutes = [...internalRoutes.values()].map(({ definition, handler }) => ({
        method: definition.verb,
        path: definition.path,
        kind: 'internal',
        handler,
      }));
      // Only entities and the fixed drain settings are advertised, so adding a
      // METHOD to an already-registered actor type never changes this: daprd
      // only needs to know the type for placement, and an unrecognized method
      // on a known type answers 404 at the app-channel level, not "restart
      // the sidecar" (see docs/architecture.md's "Actor request ownership"
      // section).
      const actorTypes = [
        ...new Set([...actorMethods.values()].map((e) => e.definition.actorType)),
      ].sort();
      const actorConfig =
        actorTypes.length === 0
          ? null
          : {
              entities: actorTypes,
              reentrancy: { enabled: false },
              drainOngoingCallTimeout: `${Math.round(requestTimeoutMs / 1000)}s`,
              drainRebalancedActors: true,
            };
      const actorFingerprint =
        actorConfig === null
          ? null
          : crypto.createHash('sha256').update(JSON.stringify(actorConfig)).digest('hex');
      // Every currently-registered (actorType, method) pair, so lib/app-channel.js
      // can diff this activation against its own last one and tombstone
      // whatever just dropped out -- a 404 on an actor method PUT is a
      // permanent "not found" to daprd (unlike an ordinary route, it will
      // never retry), so a registration that disappears across a redeploy
      // must not be observable as one.
      const actorMethodPairs = [...actorMethods.values()].map(({ definition }) => ({
        actorType: definition.actorType,
        method: definition.method,
      }));

      return {
        subscriptions: defs.map(discoveryEntry),
        routes: [...deliveryRoutes, ...serviceRoutes, ...dynamicInternalRoutes],
        fingerprint: fingerprint(defs),
        actorConfig,
        actorFingerprint,
        actorMethodPairs,
      };
    },
  };
}

module.exports = { createConnectionRegistry };
