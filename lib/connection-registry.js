'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { fingerprint, discoveryEntry } = require('./subscriptions');

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
      registry.set(definition.nodeId, { definition, handler });
      return () => registry.delete(definition.nodeId);
    },

    // The complete app-channel activation payload for what is registered now:
    // what to advertise on /dapr/subscribe, every route to serve, and the
    // fingerprint that decides whether daprd needs restarting.
    activation() {
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
      return {
        subscriptions: defs.map(discoveryEntry),
        routes: [...deliveryRoutes, ...serviceRoutes, ...dynamicInternalRoutes],
        fingerprint: fingerprint(defs),
      };
    },
  };
}

module.exports = { createConnectionRegistry };
