'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

// Milestone 1 foundation smoke test.
//
// It proves two things the rest of the suite depends on:
//   1. the native `node:test` + `node:assert/strict` runner is wired and runs;
//   2. the `RED.nodes.registerType` spy pattern that later milestones use to
//      assert node registration (without loading a full Node-RED runtime).
//
// A Node-RED node module is a function `(RED) => { ... }` that registers one or
// more node types. We exercise that shape against a minimal fake RED so the
// pattern is pinned before any real node exists.

test('native test runner and assert are available', () => {
  assert.equal(1 + 1, 2);
});

test('registerType spy captures a node registration', () => {
  const registered = [];
  const RED = {
    nodes: {
      registerType(type, ctor) {
        registered.push({ type, ctor });
      },
    },
  };

  const exampleNodeModule = (red) => {
    red.nodes.registerType('dapr-example', function ExampleNode() {});
  };

  exampleNodeModule(RED);

  assert.equal(registered.length, 1);
  assert.equal(registered[0].type, 'dapr-example');
  assert.equal(typeof registered[0].ctor, 'function');
});
