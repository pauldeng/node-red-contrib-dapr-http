'use strict';

const { DaprClient, CommunicationProtocolEnum } = require('@dapr/dapr');

const { DaprError, ErrorCodes } = require('./errors');

const MiB = 1024 * 1024;

function toSdkOptions(options) {
  const result = {
    communicationProtocol: CommunicationProtocolEnum.HTTP,
    daprApiToken: options.daprApiToken,
    isKeepAlive: true,
    maxBodySizeMb: options.limits.bodyLimitBytes / MiB,
  };
  if (options.outbound.mode === 'explicit') {
    result.daprHost = options.outbound.host;
    result.daprPort = String(options.outbound.port);
  }
  return result;
}

function clientKey(options) {
  return JSON.stringify([
    options.outbound.mode,
    options.outbound.baseUrl,
    options.daprApiToken || '',
    options.limits.bodyLimitBytes,
  ]);
}

// The pinned SDK skips serialization when its data argument is falsy. These
// truthy wrappers retain the exact wire representation without changing msg.
function sdkData(data, contentType) {
  if (data) {
    return data;
  }
  if (contentType === 'application/json' || contentType === 'application/cloudevents+json') {
    return { toJSON: () => data };
  }
  return { toString: () => (data === null || data === undefined ? '' : String(data)) };
}

class ClientRegistry {
  constructor(factory = (options) => new DaprClient(options)) {
    this.factory = factory;
    this.entries = new Map();
    this.activeLeases = 0;
  }

  acquire(options) {
    const key = clientKey(options);
    let entry = this.entries.get(key);
    if (!entry) {
      const client = this.factory(toSdkOptions(options));
      // The SDK otherwise performs a hard-coded readiness wait on every first
      // call. Connection health already gates publishes, so direct execution is
      // required for fail-fast Node-RED error handling.
      client.daprClient.setIsInitialized(true);
      entry = { client };
      this.entries.set(key, entry);
    }
    this.activeLeases += 1;

    let released = false;
    return {
      async publish(request) {
        const result = await entry.client.pubsub.publish(
          request.pubsubName,
          request.topic,
          sdkData(request.data, request.options.contentType),
          request.options
        );
        if (result?.error) {
          throw new DaprError(ErrorCodes.PUBLISH_FAILED, 'Dapr publish failed', {
            cause: result.error,
          });
        }
      },
      release: async () => {
        if (released) {
          return;
        }
        released = true;
        this.activeLeases -= 1;
        if (this.activeLeases > 0) {
          return;
        }

        // SDK HTTP agents are process-global. Stop exactly once, only after the
        // final lease closes, then discard all endpoint-specific client shells.
        const client = this.entries.values().next().value?.client;
        this.entries.clear();
        if (client) {
          await client.stop();
        }
      },
    };
  }
}

const sharedClients = new ClientRegistry();

module.exports = { ClientRegistry, sharedClients, toSdkOptions };
