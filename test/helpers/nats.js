'use strict';

// Spins up real NATS JetStream (not NATS Streaming — that component is
// deprecated) via `docker run`, and provisions the one thing daprd's
// pubsub.jetstream component needs that Redis never does: a pre-existing
// stream. Confirmed by reading dapr/components-contrib's
// pubsub/jetstream/jetstream.go: neither Publish() nor Subscribe() ever
// creates a stream, and Subscribe() calls AddConsumer() itself with no
// idempotency check — so this helper provisions the STREAM only, never a
// consumer. durableName/queueGroupName belong in the component YAML's own
// metadata instead (see jetstreamComponentYaml below), where Dapr's own
// Subscribe() call picks them up.

const crypto = require('node:crypto');

const { connect } = require('@nats-io/transport-node');
const { jetstreamManager } = require('@nats-io/jetstream');

const { execFileP, ensureImage } = require('./docker');
const { freePort } = require('./node-red');
const { setTimeout: delay } = require('node:timers/promises');

// Pinned by digest — see test/helpers/integration.js for the re-pin procedure.
const NATS_IMAGE =
  'nats:2.15.0-alpine@sha256:ac8f88a6494bffc2c2a5289a0ca61cb28a9145c11ba5677cf24265d07f46d8d4';

function runId() {
  return crypto.randomBytes(4).toString('hex');
}

async function dockerStop(name) {
  await execFileP('docker', ['stop', '--time', '5', name]).catch(() => {});
  await execFileP('docker', ['rm', '-f', name]).catch(() => {});
}

async function dockerLogs(name) {
  return execFileP('docker', ['logs', name]).catch((err) => err.message);
}

// Starts a fresh, isolated NATS server with JetStream enabled (`-js`), port-
// mapped like startRedis() (not --network host — only daprd needs that, to
// reach the app on the host's loopback interface). Waits for a real client
// connection to succeed before resolving.
async function startNats() {
  await ensureImage(NATS_IMAGE);
  const port = await freePort();
  const name = `nrdapr-it-nats-${runId()}`;
  await execFileP('docker', [
    'run',
    '-d',
    '--name',
    name,
    '-p',
    `127.0.0.1:${port}:4222`,
    NATS_IMAGE,
    '-js',
  ]);

  const deadline = Date.now() + 15000;
  let ready = false;
  let lastErr;
  while (Date.now() < deadline && !ready) {
    try {
      const nc = await connect({ servers: `127.0.0.1:${port}` });
      await nc.close();
      ready = true;
    } catch (err) {
      lastErr = err;
      await delay(200);
    }
  }
  if (!ready) {
    const logs = await dockerLogs(name);
    await dockerStop(name);
    throw new Error(`nats did not become ready: ${lastErr}\n--- nats logs ---\n${logs}`);
  }

  return {
    port,
    name,
    stop: () => dockerStop(name),
    logs: () => dockerLogs(name),
  };
}

// Creates ONLY a JetStream stream bound to the given subjects — never a
// consumer (see the file header). Idempotent-ish: `update: true` lets a
// second call against the same stream name widen its subjects rather than
// erroring, which is convenient when a suite provisions once per topic set
// rather than always up front.
async function provisionStream(port, { streamName, subjects }) {
  const nc = await connect({ servers: `127.0.0.1:${port}` });
  try {
    const jsm = await jetstreamManager(nc);
    try {
      await jsm.streams.add({ name: streamName, subjects });
    } catch (err) {
      if (!/already in use|already exists/i.test(err.message)) {
        throw err;
      }
      await jsm.streams.update(streamName, { subjects });
    }
  } finally {
    await nc.close();
  }
}

// Renders a pubsub.jetstream Component YAML. `durableName`/`queueGroupName`
// are component-wide (parsed once at Init(), applied to every Subscribe()
// call regardless of topic — confirmed in jetstream.go) — omit them on the
// shared baseline component and set them only on a dedicated fixture for the
// competing-consumers suite, or every topic sharing that component will try
// to rebind the same durable consumer to a different subject filter.
function jetstreamComponentYaml({
  name,
  natsPort,
  streamName,
  concurrency,
  durableName,
  queueGroupName,
  ackWait,
  maxDeliver,
  backOff,
}) {
  const metadata = [
    { name: 'natsURL', value: `nats://127.0.0.1:${natsPort}` },
    { name: 'streamName', value: streamName },
  ];
  if (concurrency) {
    metadata.push({ name: 'concurrency', value: concurrency });
  }
  if (durableName) {
    metadata.push({ name: 'durableName', value: durableName });
  }
  if (queueGroupName) {
    metadata.push({ name: 'queueGroupName', value: queueGroupName });
  }
  if (ackWait) {
    metadata.push({ name: 'ackWait', value: ackWait });
  }
  if (maxDeliver !== undefined) {
    metadata.push({ name: 'maxDeliver', value: String(maxDeliver) });
  }
  if (backOff) {
    metadata.push({ name: 'backOff', value: backOff });
  }
  const metadataYaml = metadata
    .map((m) => `    - name: ${m.name}\n      value: '${m.value}'`)
    .join('\n');
  return `apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ${name}
spec:
  type: pubsub.jetstream
  version: v1
  metadata:
${metadataYaml}
`;
}

module.exports = { NATS_IMAGE, startNats, provisionStream, jetstreamComponentYaml };
