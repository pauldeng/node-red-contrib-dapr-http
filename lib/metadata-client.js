'use strict';

const { DaprError, ErrorCodes } = require('./errors');
const { sidecarRequest } = require('./sidecar-http');

const METADATA_PATH = '/v1.0/metadata';
const MAX_COMPONENTS = 20;
const MAX_TEXT_LENGTH = 256;

const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

const boundedText = (value) =>
  typeof value === 'string' ? value.slice(0, MAX_TEXT_LENGTH) : undefined;

function curateMetadata(data) {
  const allComponents = Array.isArray(data.components) ? data.components : [];
  return {
    id: boundedText(data.id),
    runtimeVersion: boundedText(data.runtimeVersion),
    components: allComponents
      .slice(0, MAX_COMPONENTS)
      .filter((component) => component && typeof component.name === 'string')
      .map((component) => ({
        name: boundedText(component.name),
        type: boundedText(component.type),
      })),
    componentCount: allComponents.length,
    subscriptionCount: Array.isArray(data.subscriptions) ? data.subscriptions.length : 0,
  };
}

// Only these three verified `actorRuntime` fields (see
// docs/architecture.md's actor status section and
// test/integration/actors-probe.test.js's observed real-daprd shape) --
// everything else on that block (activeActors, hostedActors, ...) stays
// discarded, same as before. Returned as a SIBLING of the curated `data`
// object above (getMetadata, below), never merged into it: the
// dapr-connection "Test Connection" admin route spreads only `.data` into its
// response, so actor readiness detail never reaches that endpoint, exactly
// as docs/security.md's bounded-fields promise requires.
function curateActorRuntime(actorRuntime) {
  if (!isPlainObject(actorRuntime)) {
    return undefined;
  }
  return {
    runtimeStatus: boundedText(actorRuntime.runtimeStatus),
    hostReady: typeof actorRuntime.hostReady === 'boolean' ? actorRuntime.hostReady : undefined,
    placement: boundedText(actorRuntime.placement),
  };
}

async function callSidecar(baseUrl, options) {
  try {
    return await sidecarRequest(baseUrl, options);
  } catch (err) {
    if (err instanceof DaprError) {
      throw err;
    }
    throw new DaprError(
      ErrorCodes.SIDECAR_UNAVAILABLE,
      `Dapr metadata get could not reach the sidecar: ${err.message}`,
      { cause: err }
    );
  }
}

// GET /v1.0/metadata: a fixed path, no dynamic segment -- unlike every other
// client in this package, there is nothing here to validate/encode. Uses the
// default keep-alive agent: a one-off admin action, not a recurring probe of
// a possibly-dying sidecar, so the health poll's agent:false reasoning does
// not apply. Resolves { data, status } on 2xx.
async function getMetadata({ baseUrl, token, timeoutMs, signal, maxResponseBytes }) {
  const headers = {};
  if (token) {
    headers['dapr-api-token'] = token;
  }

  const result = await callSidecar(baseUrl, {
    path: METADATA_PATH,
    method: 'GET',
    headers,
    timeoutMs,
    signal,
    maxResponseBytes,
  });

  if (result.status < 200 || result.status > 299) {
    throw new DaprError(
      ErrorCodes.METADATA_OPERATION_FAILED,
      `Dapr metadata get failed with status ${result.status}`
    );
  }
  let data;
  try {
    data = JSON.parse(result.body.toString());
    if (!isPlainObject(data)) {
      throw new TypeError('expected a JSON object');
    }
  } catch {
    throw new DaprError(
      ErrorCodes.METADATA_OPERATION_FAILED,
      'Dapr metadata get returned a malformed response'
    );
  }
  return {
    data: curateMetadata(data),
    actorRuntime: curateActorRuntime(data.actorRuntime),
    status: result.status,
  };
}

module.exports = { getMetadata };
