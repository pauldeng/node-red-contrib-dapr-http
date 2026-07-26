'use strict';

// Support for the OPTIONAL integration tier that runs against a real AWS
// MemoryDB for Redis cluster instead of the local, throwaway Redis container.
//
// Optional on purpose: unlike every other tier, this one needs infrastructure
// this repository cannot create — a live cluster, credentials, and network
// routing into its VPC (MemoryDB has no public endpoint). Tests that use it skip
// themselves unless the environment supplies all four values, so the default
// `npm run test:integration` stays hermetic and green without them.
//
// What it exercises that a local Redis container cannot: TLS in transit, Redis
// ACL authentication with a username, and cluster mode. All three live entirely
// between daprd and the broker — this package's own code never sees them — so
// this tier is about proving the component configuration an operator needs is
// correct, not about re-covering node behavior.

const crypto = require('node:crypto');
const tls = require('node:tls');

// The env var whose value daprd reads through its own env secret store. The
// password is deliberately never written into a component file: writeResourcesDir
// chmods /components to 0644 so the daprd container user can read it, which would
// leave a real credential world-readable for the life of the test.
const PASSWORD_ENV = 'MEMORYDB_PASSWORD';

// Reads the cluster's coordinates from the environment. Returns null when any
// part is missing, which is the signal for a test to skip rather than fail.
function memorydbConfigFromEnv(env = process.env) {
  const endpoint = env.MEMORYDB_ENDPOINT;
  const username = env.MEMORYDB_USERNAME;
  const password = env[PASSWORD_ENV];
  if (!endpoint || !username || !password) {
    return null;
  }
  return { endpoint, port: Number(env.MEMORYDB_PORT || 6379), username, password };
}

// Human-readable reason for node:test's `skip` option, or null when configured.
function memorydbSkipReason(env = process.env) {
  return memorydbConfigFromEnv(env)
    ? null
    : `set MEMORYDB_ENDPOINT, MEMORYDB_USERNAME and ${PASSWORD_ENV} (and optionally MEMORYDB_PORT) to run the optional MemoryDB tier`;
}

// A topic unique to this run. The cluster is shared and long-lived, so a fixed
// topic name would inherit another run's stream, consumer group, and backlog.
function uniqueTopic(prefix) {
  return `nrdapr-it-${prefix}-${crypto.randomBytes(4).toString('hex')}`;
}

// Dapr's env secret store: it exposes daprd's own environment as secrets, so the
// pubsub component below can reference the password without it ever being
// written to disk.
function envSecretStoreYaml(name = 'envvars') {
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ${name}
spec:
  type: secretstores.local.env
  version: v1
`;
}

// A pubsub.redis component pointed at MemoryDB. Three settings differ from the
// local Redis container and are the point of this tier:
//   enableTLS  — MemoryDB requires TLS in transit; without this daprd's
//                handshake fails and the component never becomes ready.
//   redisType  — MemoryDB is always a cluster (its "clustercfg" endpoint reports
//                cluster_state:ok even with a single shard), so the node-mode
//                client would talk to the configuration endpoint as if it were a
//                single server.
//   redisUsername + secretKeyRef — MemoryDB authenticates a Redis ACL user, so
//                both a username and a password are required, and the password
//                comes from the env secret store rather than this file.
function memorydbPubsubYaml({ name = 'pubsub', config, secretStore = 'envvars' }) {
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ${name}
spec:
  type: pubsub.redis
  version: v1
  metadata:
    - name: redisHost
      value: ${config.endpoint}:${config.port}
    - name: redisUsername
      value: ${config.username}
    - name: redisPassword
      secretKeyRef:
        name: ${PASSWORD_ENV}
        key: ${PASSWORD_ENV}
    - name: enableTLS
      value: 'true'
    - name: redisType
      value: cluster
auth:
  secretStore: ${secretStore}
`;
}

// One RESP command over TLS, authenticated as the ACL user. Deliberately tiny:
// this tier needs exactly two things from a client — a readiness check and
// cleanup of the keys Dapr leaves behind — and neither justifies a Redis
// dependency in a package that ships none.
function command(config, args, { timeoutMs = 10000 } = {}) {
  return new Promise((resolve, reject) => {
    const encode = (parts) =>
      `*${parts.length}\r\n` +
      parts.map((p) => `$${Buffer.byteLength(String(p))}\r\n${p}\r\n`).join('');
    const socket = tls.connect({
      host: config.endpoint,
      port: config.port,
      servername: config.endpoint,
    });
    let out = '';
    let settled = false;
    const finish = (fn) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      fn();
    };
    const timer = setTimeout(
      () => finish(() => reject(new Error(`MemoryDB command timed out after ${timeoutMs}ms`))),
      timeoutMs
    );
    socket.on('secureConnect', () => {
      socket.write(encode(['AUTH', config.username, config.password]));
      socket.write(encode(args));
    });
    socket.on('data', (chunk) => {
      out += chunk.toString();
      // The AUTH reply plus the command's own reply: wait for two complete
      // top-level replies rather than guessing at the second one's shape.
      const replies = out.split('\r\n').filter((line) => line !== '');
      if (out.startsWith('-')) {
        finish(() => reject(new Error(`MemoryDB rejected AUTH: ${replies[0]}`)));
        return;
      }
      if (replies.length >= 2) {
        finish(() => resolve(replies.slice(1).join('\n')));
      }
    });
    socket.on('error', (err) => finish(() => reject(err)));
  });
}

// Resolves when the cluster answers an authenticated PING, so a test fails on an
// unreachable cluster or bad credentials with a clear message instead of timing
// out later inside daprd.
async function assertMemorydbReachable(config) {
  const reply = await command(config, ['PING']);
  if (!reply.includes('PONG')) {
    throw new Error(`unexpected MemoryDB PING reply: ${reply}`);
  }
}

// Dapr's Redis pub/sub keeps one stream per topic, named after the topic. Delete
// them so a shared, long-lived cluster does not accumulate this suite's streams
// and consumer groups run after run.
//
// Call this only once daprd has stopped. A running Dapr subscriber recreates the
// stream and its consumer group on its next poll, so deleting first and stopping
// daprd afterwards leaves the keys behind — which is exactly what a tolerant
// `catch {}` hid the first time. Verified rather than assumed, and it throws if a
// key survives: quietly littering a shared cluster is worse than a failed
// teardown telling you why.
async function deleteTopics(config, topics) {
  const remaining = [];
  for (const topic of topics) {
    await command(config, ['DEL', topic]).catch(() => {});
    const type = await command(config, ['TYPE', topic]).catch((err) => `error: ${err.message}`);
    if (!type.includes('none')) {
      remaining.push(`${topic} (${type.trim()})`);
    }
  }
  if (remaining.length > 0) {
    throw new Error(
      `MemoryDB cleanup left keys behind: ${remaining.join(', ')} — is daprd still running?`
    );
  }
}

module.exports = {
  PASSWORD_ENV,
  memorydbConfigFromEnv,
  memorydbSkipReason,
  uniqueTopic,
  envSecretStoreYaml,
  memorydbPubsubYaml,
  command,
  assertMemorydbReachable,
  deleteTopics,
};
