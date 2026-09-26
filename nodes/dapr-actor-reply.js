'use strict';

const { serializeProposal } = require('../lib/actor-messages');
const { DaprError, ErrorCodes } = require('../lib/errors');

// Settles a live actor-method reply proposal (lib/actor-host.js). This node
// never talks to storage itself: serializeProposal fully validates and
// bounds msg.payload / msg.dapr.actor.nextState / msg.dapr.actor.error here,
// before settlement, so a later branch mutating `msg` cannot change what was
// actually proposed. `done()` on success means the proposal was accepted by
// the pending registry, NOT that any state write has committed — the HTTP
// handler in lib/actor-host.js owns the commit and the caller's response.
module.exports = function registerDaprActorReply(RED) {
  function DaprActorReplyNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (!connection?.options || typeof connection.settleActorReply !== 'function') {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      node.on('input', (_msg, _send, done) => {
        done(new DaprError(ErrorCodes.INVALID_OPTIONS, 'Dapr connection is unavailable'));
      });
      return;
    }

    // Undefined/blank (an unedited legacy node, or a hand-authored flow) means
    // the documented default; anything else that is not one of the two known
    // outcomes is a configuration error, not a silent fallback to "complete" —
    // that would turn an imported flow's typo'd outcome into an unintended
    // success reply.
    const outcome =
      config.outcome === undefined || config.outcome === '' ? 'complete' : config.outcome;
    if (outcome !== 'complete' && outcome !== 'fail') {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(`unknown actor reply outcome: ${config.outcome}`);
      node.on('input', (_msg, _send, done) => {
        done(
          new DaprError(
            ErrorCodes.INVALID_OPTIONS,
            `unknown actor reply outcome: ${config.outcome}`
          )
        );
      });
      return;
    }

    node.on('input', (msg, _send, done) => {
      const dapr = msg.dapr !== null && typeof msg.dapr === 'object' ? msg.dapr : {};
      if (typeof dapr.actorRequestId !== 'string' || dapr.actorRequestId === '') {
        done(new DaprError(ErrorCodes.INVALID_MESSAGE, 'msg.dapr.actorRequestId is required'));
        return;
      }

      let proposal;
      try {
        proposal = serializeProposal(msg, outcome, {
          maxBodyBytes: connection.options.limits.bodyLimitBytes,
        });
      } catch (err) {
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.INVALID_MESSAGE, err.message, { cause: err })
        );
        return;
      }

      // First-wins settlement; a missing, foreign, expired, or already-settled
      // id is rejected rather than silently accepted.
      if (connection.settleActorReply(dapr.actorRequestId, proposal)) {
        done();
      } else {
        done(
          new DaprError(
            ErrorCodes.ACTOR_REPLY_EXPIRED,
            `no pending actor request for actorRequestId ${dapr.actorRequestId} (expired, foreign, or already answered)`
          )
        );
      }
    });
  }

  RED.nodes.registerType('dapr-actor-reply', DaprActorReplyNode);
};
