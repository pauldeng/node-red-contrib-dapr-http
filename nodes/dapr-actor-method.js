'use strict';

// Registers this node's (actorType, method) pair with the connection's actor
// host (lib/actor-host.js, via lib/connection-registry.js) and emits each
// invocation it is handed. No input: the node exists only to be registered
// and to receive `emit(msg)` calls. All admission, state loading, and commit
// logic lives in lib/actor-host.js — this wrapper owns no behavior of its
// own beyond registration and its own teardown.
module.exports = function registerDaprActorMethod(RED) {
  function DaprActorMethodNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (!connection?.options || typeof connection.registerActorMethod !== 'function') {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      return;
    }

    const definition = { nodeId: node.id, actorType: config.actorType, method: config.method };
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
