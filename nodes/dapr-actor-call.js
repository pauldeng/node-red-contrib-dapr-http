'use strict';

const { context, propagation, trace, SpanKind } = require('@opentelemetry/api');

const { invoke } = require('../lib/actor-client');
const { validateActorSegment } = require('../lib/actor-messages');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { requireConnection, openSidecarSession } = require('../lib/sidecar-session');
const { getTracer, endSpan } = require('../lib/telemetry');

// Invokes an actor method through the local sidecar (lib/actor-client.js) and
// replaces msg.payload with the result. Each property PRESENT on
// msg.dapr.actorCall overrides the configured target; a present-but-invalid
// value fails rather than falling back to the configured one. Never reads
// msg.dapr.actor (the inbound method-invocation metadata) for targeting — an
// actor cannot retarget its own storage or invoke itself by editing that.
//
// Shares the same connection lifecycle (health mirror, request timeout,
// abort-on-close) as every other outbound sidecar-calling node in this
// package — see lib/sidecar-session.js — rather than reimplementing it here.
module.exports = function registerDaprActorCall(RED) {
  function DaprActorCallNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);
    if (!requireConnection(node, connection)) {
      return;
    }
    const session = openSidecarSession(node, connection);

    node.on('input', async (msg, send, done) => {
      if (!(await session.isReady())) {
        done(new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'Dapr sidecar is unavailable'));
        return;
      }

      const dapr = msg.dapr !== null && typeof msg.dapr === 'object' ? msg.dapr : {};
      const override =
        dapr.actorCall !== null && typeof dapr.actorCall === 'object' ? dapr.actorCall : {};

      let actorType;
      let actorId;
      let method;
      let body;
      try {
        actorType = validateActorSegment(
          Object.hasOwn(override, 'type') ? override.type : config.actorType,
          'actorType'
        );
        actorId = validateActorSegment(
          Object.hasOwn(override, 'id') ? override.id : config.actorId,
          'actorId'
        );
        method = validateActorSegment(
          Object.hasOwn(override, 'method') ? override.method : config.method,
          'method'
        );
        if (msg.payload !== undefined) {
          // Fails fast on a non-JSON-serializable payload (BigInt, circular,
          // etc.) with a clear INVALID_MESSAGE, before any network attempt —
          // lib/actor-client.js's own JSON.stringify would otherwise throw a
          // bare TypeError from inside an awaited call.
          JSON.stringify(msg.payload);
          body = msg.payload; // explicit null is preserved and sent as JSON null
        }
      } catch (err) {
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.INVALID_MESSAGE, err.message, { cause: err })
        );
        return;
      }

      // Direct self-call guard: only meaningful while this message is a live,
      // not-yet-completed method invocation on this same connection. A
      // different method on the same actor is still a self-call — compare
      // type+id only, never the method name.
      if (typeof dapr.actorRequestId === 'string') {
        const identity = connection.actorIdentity(dapr.actorRequestId);
        if (identity && identity.actorType === actorType && identity.actorId === actorId) {
          done(
            new DaprError(
              ErrorCodes.ACTOR_SELF_CALL,
              `actor call would call itself: ${actorType}/${actorId}`
            )
          );
          return;
        }
      }

      // With tracing disabled the provider returns a no-op span; no branch of
      // its own here, mirroring nodes/dapr-invoke.js's own client span.
      const span = getTracer().startSpan(
        `actor ${actorType}.${method}`,
        {
          kind: SpanKind.CLIENT,
          attributes: { 'rpc.system': 'dapr', 'dapr.actor.type': actorType, 'rpc.method': method },
        },
        context.active()
      );
      const headers = {};
      propagation.inject(trace.setSpan(context.active(), span), headers);

      let result;
      try {
        result = await session.call((transportOptions) =>
          invoke({ ...transportOptions, headers }, { actorType, actorId, method, body })
        );
        span.setAttribute('http.response.status_code', result.status);
        endSpan(span);
      } catch (err) {
        endSpan(span, err);
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, err.message, { cause: err })
        );
        return;
      }

      try {
        if (result.body.length === 0) {
          msg.payload = undefined;
        } else {
          try {
            msg.payload = JSON.parse(result.body.toString('utf8'));
          } catch (err) {
            throw new DaprError(
              ErrorCodes.ACTOR_INVOKE_FAILED,
              'actor method returned a non-JSON response',
              { cause: err }
            );
          }
        }
        send(msg);
        done();
      } catch (err) {
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.INVALID_MESSAGE, err.message, { cause: err })
        );
      }
    });
  }

  RED.nodes.registerType('dapr-actor-call', DaprActorCallNode);
};
