'use strict';

// Registers this node's (actorType, method) pair with the connection's actor
// host (lib/actor-host.js, via lib/connection-registry.js) and emits each
// invocation it is handed. No input: the node exists only to be registered
// and to receive `emit(msg)` calls. All admission, state loading, and commit
// logic lives in lib/actor-host.js — this wrapper owns no behavior of its
// own beyond registration and its own teardown.
//
// `trigger: 'reminder'` passes `reminder: true` to registerActorMethod
// instead of a configured method name -- reusing registerActorMethod/
// addActorMethod as-is (rather than adding a parallel
// registerActorReminder) so the existing duplicate-pair rejection, tombstone,
// and /dapr/config advertisement all apply unchanged: one reminder
// registration per actor type is exactly "one (actorType, REMINDER_METHOD)
// pair" to that registry. `config.method` is never read in that mode --
// lib/connection-registry.js's addActorMethod ignores it entirely rather
// than trusting it might equal the reserved key, so a config value cannot
// forge a reminder registration. The node receives every reminder of this
// actor type, named on each invocation by msg.dapr.actor.trigger.name
// (lib/actor-host.js sets this from the parsed route, never from the
// reminder callback's body).
module.exports = function registerDaprActorMethod(RED) {
  function DaprActorMethodNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (!connection?.options || typeof connection.registerActorMethod !== 'function') {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      return;
    }

    const isReminder = config.trigger === 'reminder';
    const definition = {
      nodeId: node.id,
      actorType: config.actorType,
      method: config.method,
      reminder: isReminder,
    };
    // lib/actor-host.js calls this with the invocation message; see this
    // node's help (data-help-name="dapr-actor-method") for its exact shape.
    const emit = (msg) => node.send(msg);

    let unregister;
    try {
      unregister = connection.registerActorMethod(definition, emit);
    } catch (err) {
      // Invalid/empty actorType or method, a duplicate (actorType, method) on
      // this connection, or a collision with the /actors namespace — see
      // lib/connection-registry.js's addActorMethod.
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(err.message);
      return;
    }
    node.status({ fill: 'green', shape: 'dot', text: 'listening' });

    node.on('close', (_removed, done) => {
      unregister();
      done();
    });
  }

  RED.nodes.registerType('dapr-actor-method', DaprActorMethodNode);
};
