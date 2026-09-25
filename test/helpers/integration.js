'use strict';

// Spins up real daprd 1.18.4 + Redis via `docker run` for the integration
// tier — one fresh, isolated pair per test file, on dynamically allocated
// ports (so test files stay parallel-safe, matching the runtime tier's
// per-file freePort() convention).
//
// daprd runs with `--network host` (see docker-compose.yml's header comment
// for why): the integration tests spawn Node-RED as a host child process
// (test/helpers/node-red.js), so daprd needs to reach 127.0.0.1:<appPort> on
// the host exactly like production's loopback-only default. `--network host`
// is Linux-only; this tier is documented as needing Docker on Linux CI.
//
// Startup ordering (see docs/testing.md): start and deploy Node-RED FIRST,
// wait for its own /healthz to respond, and only THEN start daprd — daprd
// fetches /dapr/subscribe exactly once at startup, so starting it against an
// app that isn't listening yet means it discovers no subscriptions.

const crypto = require('node:crypto');
const path = require('node:path');
const os = require('node:os');
const fsp = require('node:fs/promises');

const { httpRequest } = require('./http');
const { freePort } = require('./node-red');
const { execFileP, ensureImage } = require('./docker');
const { setTimeout: delay } = require('node:timers/promises');

// Pinned by digest, not just tag — a tag (even a version tag like "1.18.4")
// can be moved to point at different content; only the digest is immutable.
// Re-pin deliberately: `docker image inspect <image>:<tag> --format
// '{{index .RepoDigests 0}}'` after pulling the tag you intend to adopt.
const DAPRD_IMAGE =
  'daprio/daprd:1.18.4@sha256:1e218523a15be5be5f36d64aa33a40cbde8fbe963ba6122d42d9c9de5b24a372';
const REDIS_IMAGE =
  'redis:7.4-alpine@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99';
const PLACEMENT_IMAGE =
  'daprio/placement:1.18.4@sha256:ec614eefbf6dd8153adc8163f67486092debc50c5fe8eedf48cfe2295e9e17e3';
const FIXTURES_DIR = path.resolve(__dirname, '..', 'integration', 'fixtures');

function runId() {
  return crypto.randomBytes(4).toString('hex');
}

async function dockerRun(args) {
  // No --rm: a container that crashes at startup (e.g. a bad --config fixture)
  // must stick around long enough for dockerLogs() to read why. dockerStop()
  // always removes it explicitly instead.
  return execFileP('docker', ['run', '-d', ...args]);
}

async function dockerStop(name) {
  await execFileP('docker', ['stop', '--time', '5', name]).catch(() => {});
  await execFileP('docker', ['rm', '-f', name]).catch(() => {});
}

async function dockerLogs(name) {
  return execFileP('docker', ['logs', name]).catch((err) => err.message);
}

async function waitForHttp(url, { timeoutMs = 20000, intervalMs = 200, headers } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const res = await httpRequest(url, { timeoutMs: 2000, headers });
      if (res.status >= 200 && res.status < 300) {
        return res;
      }
      last = `HTTP ${res.status}`;
    } catch (err) {
      last = err.message;
    }
    await delay(intervalMs);
  }
  throw new Error(`waitForHttp(${url}) timed out: ${last}`);
}

function pubsubComponentYaml(redisPort) {
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.redis
  version: v1
  metadata:
    - name: redisHost
      value: 127.0.0.1:${redisPort}
    - name: redisPassword
      value: ''
`;
}

// A real Dapr Redis state-store component, for the state-management
// integration tier. Distinct from pubsubComponentYaml's `pubsub.redis`
// component -- same backing Redis, a different Dapr component type -- so a
// suite that needs only state (no broker) passes this through startDaprd's
// broker-agnostic `components` array instead of `redisPort` (which would
// also write the pub/sub component this suite has no use for).
function stateComponentYaml(redisPort, { name = 'statestore', actorStateStore = false } = {}) {
  const actorMetadata = actorStateStore
    ? `
    - name: actorStateStore
      value: "true"`
    : '';
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ${name}
spec:
  type: state.redis
  version: v1
  metadata:
    - name: redisHost
      value: 127.0.0.1:${redisPort}
    - name: redisPassword
      value: ''${actorMetadata}
`;
}

// A real Dapr Redis configuration-store component, for the dynamic-
// configuration integration tier. Distinct from stateComponentYaml's
// `state.redis` -- same backing Redis, a different Dapr component type.
// Subscribe relies on Redis keyspace notifications (components-contrib's
// configuration/redis package subscribes to Redis's own key-change pub/sub
// channels, not polling) -- the backing container must be started with
// startRedis({ notifyKeyspaceEvents: 'KEA' }) or Subscribe silently never
// fires, since keyspace notifications are off by default.
function configurationComponentYaml(redisPort, { name = 'configstore' } = {}) {
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ${name}
spec:
  type: configuration.redis
  version: v1
  metadata:
    - name: redisHost
      value: 127.0.0.1:${redisPort}
    - name: redisPassword
      value: ''
`;
}

// A real Dapr HTTP output-binding component, for the output-bindings
// integration tier. Needs no broker or store at all -- `url` points directly
// at an HTTP target this test itself controls (startDaprd runs daprd with
// --network host, so a host-side 127.0.0.1 URL is reachable from inside the
// container unchanged).
function bindingComponentYaml(url) {
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: orders-binding
spec:
  type: bindings.http
  version: v1
  metadata:
    - name: url
      value: '${url}'
`;
}

// A real Dapr local-file secret store, for the secrets integration tier.
// Needs no broker or store at all -- secretsFile points at the static
// test/integration/fixtures/secrets.json fixture, mounted read-only at
// /components alongside every other component YAML this suite writes.
// Component name is fixed at "secretstore" to match
// test/integration/fixtures/secret-scopes.yaml's own scope declaration.
function secretComponentYaml() {
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: secretstore
spec:
  type: secretstores.local.file
  version: v1
  metadata:
    - name: secretsFile
      value: /components/secrets.json
`;
}

// Starts a fresh, isolated Redis container on a dynamically allocated host
// port. Waits for it to accept connections before resolving.
// `notifyKeyspaceEvents`, when set, is passed as redis-server's own
// --notify-keyspace-events flag (e.g. "KEA") -- needed only by the
// configuration.redis component's push-based Subscribe; every existing
// caller that omits it gets the exact same container as before.
async function startRedis({ notifyKeyspaceEvents } = {}) {
  await ensureImage(REDIS_IMAGE);
  const port = await freePort();
  const name = `nrdapr-it-redis-${runId()}`;
  const args = ['--name', name, '-p', `${port}:6379`, REDIS_IMAGE];
  if (notifyKeyspaceEvents) {
    args.push('redis-server', '--notify-keyspace-events', notifyKeyspaceEvents);
  }
  await dockerRun(args);
  const deadline = Date.now() + 15000;
  let ready = false;
  while (Date.now() < deadline && !ready) {
    try {
      await execFileP('docker', ['exec', name, 'redis-cli', 'ping']);
      ready = true;
    } catch {
      await delay(200);
    }
  }
  if (!ready) {
    const logs = await dockerLogs(name);
    await dockerStop(name);
    throw new Error(`redis did not become ready: ${logs}`);
  }
  return {
    port,
    name,
    stop: () => dockerStop(name),
    logs: () => dockerLogs(name),
  };
}

// Starts a fresh Dapr Placement service, needed only by the actor runtime
// (ordinary pub/sub, state, service-invocation, etc. never talk to it).
// Runs the same binary shape as startRedis()/startDaprd(): raw `docker run`,
// no --network host (Placement needs no loopback app-port reachability like
// daprd does), reached instead via an explicit loopback-only -p mapping so a
// --network host daprd can still dial 127.0.0.1:<grpc> without publishing
// Placement beyond loopback. --enable-metrics=false avoids a second container
// claiming the default 9090 metrics port that startDaprd's own --network host
// sidecars already disable for the same reason (see startDaprd's comment on
// --enable-metrics).
async function startPlacement() {
  await ensureImage(PLACEMENT_IMAGE);
  const grpcPort = await freePort();
  const healthzPort = await freePort();
  const name = `nrdapr-it-placement-${runId()}`;
  try {
    await dockerRun([
      '--name',
      name,
      '-p',
      `127.0.0.1:${grpcPort}:${grpcPort}`,
      '-p',
      `127.0.0.1:${healthzPort}:${healthzPort}`,
      PLACEMENT_IMAGE,
      './placement',
      `--port=${grpcPort}`,
      `--healthz-port=${healthzPort}`,
      '--enable-metrics=false',
    ]);
  } catch (err) {
    // docker run can leave a named container behind after a partial failure
    // (e.g. port publication rejected after create). Always gather logs and
    // remove it before rethrowing, matching the readiness-failure path below.
    const logs = await dockerLogs(name);
    await dockerStop(name);
    throw new Error(`${err.message}\n--- placement logs ---\n${logs}`, { cause: err });
  }
  try {
    await waitForHttp(`http://127.0.0.1:${healthzPort}/healthz`);
  } catch (err) {
    const logs = await dockerLogs(name);
    await dockerStop(name);
    throw new Error(`${err.message}\n--- placement logs ---\n${logs}`, { cause: err });
  }
  return {
    name,
    port: grpcPort,
    stop: () => dockerStop(name),
    logs: () => dockerLogs(name),
  };
}

// Writes a fresh per-run resources directory: the Redis pubsub component
// (pointed at the given Redis port, if any), any broker-agnostic pre-rendered
// components a suite supplies directly (e.g. test/helpers/nats.js's
// jetstreamComponentYaml() — this function only ever writes bytes, it does
// not know or care which broker they describe), and any extra static fixtures
// a suite needs (e.g. resiliency-retry.yaml, config-acl.yaml — see
// test/integration/fixtures/). Resiliency/Configuration/Subscription
// resources are all loaded from this same directory (daprd distinguishes them
// by each file's own "kind"), so dropping in extra fixture files is enough
// for Resiliency to take effect; Configuration additionally needs the
// `--config` flag to point at it.
//
// The daprd image runs as a non-root, unrelated container UID (65532), so
// mkdtemp's default 0700 directory (and the host user's normal file modes)
// are unreadable to it — every path here is explicitly opened up.
async function writeResourcesDir({ redisPort, components = [], extraFixtures = [] }) {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'nrdapr-it-'));
  await fsp.chmod(dir, 0o755);
  if (redisPort !== undefined) {
    const pubsubPath = path.join(dir, 'pubsub.yaml');
    await fsp.writeFile(pubsubPath, pubsubComponentYaml(redisPort));
    await fsp.chmod(pubsubPath, 0o644);
  }
  for (const { filename, yaml } of components) {
    const dest = path.join(dir, filename);
    await fsp.writeFile(dest, yaml);
    await fsp.chmod(dest, 0o644);
  }
  for (const fixtureFile of extraFixtures) {
    const src = path.join(FIXTURES_DIR, fixtureFile);
    const dest = path.join(dir, fixtureFile);
    await fsp.copyFile(src, dest);
    await fsp.chmod(dest, 0o644);
  }
  return dir;
}

// Starts a fresh daprd container for one app instance. Call only once the app
// itself (Node-RED) is already listening and its /healthz responds — see the
// startup-ordering note above. Resolves once /v1.0/healthz/outbound is live.
//
// Pass `httpPort` when the caller must know daprd's port BEFORE daprd starts
// (e.g. to configure the dapr-connection node's daprPort up front, avoiding a
// redeploy) — allocate it with freePort() and pass it straight through.
//
// `redisPort` is optional (omit it for a NATS-only or otherwise Redis-free
// daprd); `components` is a broker-agnostic escape hatch for any other
// pre-rendered Component YAML a suite needs (e.g. test/helpers/nats.js's
// jetstreamComponentYaml()) — this function never special-cases what's in it.
//
// `env` sets extra environment variables on the daprd container. Its reason to
// exist is credentials: a component that needs a real secret should reference it
// through Dapr's own env secret store (see test/helpers/memorydb.js) so the
// secret reaches daprd's process environment instead of being written into a
// world-readable component file under /components.
async function startDaprd({
  appId,
  appPort,
  redisPort,
  components = [],
  extraFixtures = [],
  configFixture,
  appApiToken,
  daprApiToken,
  env = {},
  httpPort: presetHttpPort,
  placementAddress,
}) {
  await ensureImage(DAPRD_IMAGE);
  const resourcesDir = await writeResourcesDir({
    redisPort,
    components,
    extraFixtures: configFixture ? [...extraFixtures, configFixture] : extraFixtures,
  });
  const httpPort = presetHttpPort || (await freePort());
  const healthHeaders = daprApiToken ? { 'dapr-api-token': daprApiToken } : undefined;

  // One attempt: bind every port daprd needs explicitly, then wait for the
  // sidecar API. Returns the container name, or throws with daprd's logs.
  const attempt = async () => {
    const name = `nrdapr-it-daprd-${runId()}`;
    const args = ['--name', name, '--network', 'host', '-v', `${resourcesDir}:/components:ro`];
    if (appApiToken) {
      args.push('-e', `APP_API_TOKEN=${appApiToken}`);
    }
    if (daprApiToken) {
      args.push('-e', `DAPR_API_TOKEN=${daprApiToken}`);
    }
    // execFileP runs docker without a shell, so a value containing shell
    // metacharacters (a generated password, say) is passed through verbatim.
    for (const [key, value] of Object.entries(env)) {
      args.push('-e', `${key}=${value}`);
    }
    args.push(
      DAPRD_IMAGE,
      './daprd',
      `--app-id=${appId}`,
      `--app-port=${appPort}`,
      '--app-protocol=http',
      `--dapr-http-port=${httpPort}`,
      '--dapr-grpc-port=0',
      // The INTERNAL gRPC port (sidecar-to-sidecar) must be allocated as
      // deliberately as the HTTP one: --network host puts every sidecar in the
      // suite in one port space, and daprd exits FATALLY if it cannot bind this
      // port ("failed to start internal gRPC server"), failing the test for a
      // reason that has nothing to do with the code under test.
      `--dapr-internal-grpc-port=${await freePort()}`,
      '--resources-path=/components',
      '--log-level=info',
      // --network host means every sidecar in a test binds the same interface;
      // the default metrics port (9090) collides once more than one sidecar
      // runs at a time (see acl.test.js). Nothing here reads metrics.
      '--enable-metrics=false'
    );
    if (configFixture) {
      args.push(`--config=/components/${configFixture}`);
    }
    if (placementAddress) {
      args.push(`--placement-host-address=${placementAddress}`);
    }

    await dockerRun(args);
    try {
      await waitForHttp(`http://127.0.0.1:${httpPort}/v1.0/healthz/outbound`, {
        headers: healthHeaders,
      });
    } catch (err) {
      const logs = await dockerLogs(name);
      await dockerStop(name);
      throw new Error(`${err.message}\n--- daprd logs ---\n${logs}`, { cause: err });
    }
    return name;
  };

  // freePort() is inherently a check-then-bind race: the port is free when
  // asked for and can be taken before daprd binds it. One retry (with freshly
  // allocated ports) turns that rare loss into a slower start rather than a
  // failed suite; a second failure is reported as the real defect it then is.
  let name;
  try {
    name = await attempt();
  } catch (first) {
    if (!/address already in use|could not listen/i.test(first.message)) {
      await fsp.rm(resourcesDir, { recursive: true, force: true });
      throw first;
    }
    try {
      name = await attempt();
    } catch (second) {
      await fsp.rm(resourcesDir, { recursive: true, force: true });
      throw new Error(`daprd failed to start twice (port contention?): ${second.message}`, {
        cause: second,
      });
    }
  }

  return {
    httpPort,
    name,
    baseUrl: `http://127.0.0.1:${httpPort}`,
    stop: async () => {
      await dockerStop(name);
      await fsp.rm(resourcesDir, { recursive: true, force: true });
    },
    logs: () => dockerLogs(name),
  };
}

module.exports = {
  startRedis,
  startPlacement,
  startDaprd,
  waitForHttp,
  stateComponentYaml,
  configurationComponentYaml,
  bindingComponentYaml,
  secretComponentYaml,
  DAPRD_IMAGE,
  REDIS_IMAGE,
  PLACEMENT_IMAGE,
  FIXTURES_DIR,
};
