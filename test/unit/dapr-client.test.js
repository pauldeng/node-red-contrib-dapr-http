'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { ClientRegistry, toSdkOptions } = require('../../lib/dapr-client');
const { DaprError, ErrorCodes } = require('../../lib/errors');

function options(over = {}) {
  return {
    outbound: {
      mode: 'explicit',
      host: '127.0.0.1',
      port: 3500,
      baseUrl: 'http://127.0.0.1:3500',
    },
    daprApiToken: 'token',
    limits: { bodyLimitBytes: 8 * 1024 * 1024 },
    keepAlive: true,
    ...over,
  };
}

function fakeFactory() {
  const clients = [];
  const factory = (sdkOptions) => {
    const calls = [];
    const client = {
      sdkOptions,
      calls,
      initialized: [],
      stopCalls: 0,
      response: {},
      daprClient: {
        setIsInitialized(value) {
          client.initialized.push(value);
        },
      },
      pubsub: {
        async publish(...args) {
          calls.push(args);
          return client.response;
        },
      },
      async stop() {
        client.stopCalls += 1;
      },
    };
    clients.push(client);
    return client;
  };
  return { factory, clients };
}

test('SDK options omit host and port in environment-endpoint mode', () => {
  const result = toSdkOptions(
    options({ outbound: { mode: 'env', baseUrl: 'http://sidecar:3600' } })
  );
  assert.equal('daprHost' in result, false);
  assert.equal('daprPort' in result, false);
  assert.equal(result.daprApiToken, 'token');
  assert.equal(result.isKeepAlive, true);
  assert.equal(result.maxBodySizeMb, 8);
});

test('SDK options include explicit HTTP endpoint settings', () => {
  const result = toSdkOptions(options());
  assert.equal(result.daprHost, '127.0.0.1');
  assert.equal(result.daprPort, '3500');
  assert.equal(typeof result.communicationProtocol, 'number');
});

test('registry shares a client and does not stop it while another lease is active', async () => {
  const fake = fakeFactory();
  const registry = new ClientRegistry(fake.factory);
  const first = registry.acquire(options());
  const second = registry.acquire(options());

  assert.equal(fake.clients.length, 1);
  assert.deepEqual(fake.clients[0].initialized, [true]);

  await first.release();
  assert.equal(fake.clients[0].stopCalls, 0);
  await second.release();
  assert.equal(fake.clients[0].stopCalls, 1);
});

test('closing one endpoint does not stop global SDK agents used by another endpoint', async () => {
  const fake = fakeFactory();
  const registry = new ClientRegistry(fake.factory);
  const first = registry.acquire(options());
  const second = registry.acquire(
    options({
      outbound: {
        mode: 'explicit',
        host: '127.0.0.1',
        port: 3600,
        baseUrl: 'http://127.0.0.1:3600',
      },
    })
  );

  assert.equal(fake.clients.length, 2);
  await first.release();
  assert.equal(fake.clients[0].stopCalls, 0);
  assert.equal(fake.clients[1].stopCalls, 0);

  await second.release();
  assert.equal(
    fake.clients.reduce((sum, client) => sum + client.stopCalls, 0),
    1
  );
});

test('publish forwards SDK options and treats an SDK error response as failure', async () => {
  const fake = fakeFactory();
  const registry = new ClientRegistry(fake.factory);
  const lease = registry.acquire(options());
  const request = {
    pubsubName: 'pubsub',
    topic: 'orders',
    data: { id: 1 },
    options: { contentType: 'application/json', metadata: { ttlInSeconds: '10' } },
  };

  await lease.publish(request);
  assert.deepEqual(fake.clients[0].calls[0], ['pubsub', 'orders', request.data, request.options]);

  const cause = new Error('sidecar rejected publish');
  fake.clients[0].response = { error: cause };
  await assert.rejects(
    lease.publish(request),
    (err) =>
      err instanceof DaprError && err.code === ErrorCodes.PUBLISH_FAILED && err.cause === cause
  );
  await lease.release();
});

test('falsy JSON values are wrapped so the pinned SDK does not omit their body', async () => {
  const fake = fakeFactory();
  const registry = new ClientRegistry(fake.factory);
  const lease = registry.acquire(options());

  for (const data of [0, false, null]) {
    await lease.publish({
      pubsubName: 'pubsub',
      topic: 'values',
      data,
      options: { contentType: 'application/json', metadata: {} },
    });
    const sdkData = fake.clients[0].calls.at(-1)[2];
    assert.ok(sdkData, `SDK data must be truthy for ${String(data)}`);
    assert.equal(JSON.stringify(sdkData), JSON.stringify(data));
  }

  await lease.publish({
    pubsubName: 'pubsub',
    topic: 'values',
    data: '',
    options: { contentType: 'text/plain', metadata: {} },
  });
  assert.ok(fake.clients[0].calls.at(-1)[2]);
  assert.equal(fake.clients[0].calls.at(-1)[2].toString(), '');
  await lease.release();
});

test('release is idempotent', async () => {
  const fake = fakeFactory();
  const registry = new ClientRegistry(fake.factory);
  const lease = registry.acquire(options());
  await lease.release();
  await lease.release();
  assert.equal(fake.clients[0].stopCalls, 1);
});
