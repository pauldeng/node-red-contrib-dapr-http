# Deployment

Local, Compose, and Kubernetes topology. The one constraint every topology
must satisfy: Node-RED and `daprd` need to reach each other over `127.0.0.1`,
because the app-channel listener binds loopback by default (see
`docs/security.md` for why) and the sidecar's outbound API defaults to
`127.0.0.1:3500`.

## Local (no containers)

Run `daprd` directly against a Node-RED process already listening on its app
port:

```bash
daprd --app-id my-flow --app-port 3000 --app-protocol http \
  --dapr-http-port 3500 --resources-path ./components
```

daprd's gRPC API stays on its own default port (`50001`) regardless — passing
`--dapr-grpc-port 0` does not disable it, it only makes daprd bind an
OS-assigned ephemeral port instead (confirmed against real daprd 1.18.1: the
gRPC server still listens and accepts connections). This package only ever
speaks HTTP to the sidecar, so the gRPC port — whichever one it ends up
on — is simply never used; there's no need to configure it either way.
Node-RED's own editor/admin port (default 1880) is unrelated to any of this
and needs no special configuration.

## Docker Compose

**If Node-RED itself is a Compose service**, put `daprd` in its network
namespace so both share `127.0.0.1`:

```yaml
services:
  node-red:
    image: nodered/node-red:5.0.1-24@sha256:6cb1b27fa5a83deec6a662db62eec8bb32e55ac5412d6b7a653e874ce62055d5
    ports:
      - '1880:1880'
  daprd:
    image: daprio/daprd:1.18.1@sha256:4434e34fd782db17094123be93a78c0b18556b5b1fca1f06448e54df24e51cdf
    network_mode: 'service:node-red'
    command:
      - './daprd'
      - '--app-id=my-flow'
      - '--app-port=3000'
      - '--dapr-http-port=3500'
      - '--resources-path=/components'
    volumes:
      - ./components:/components:ro
```

The exact digests above are this project's own pinned versions — see
`test/helpers/node-red-container.js` and `test/helpers/integration.js` for
the re-pin procedure; use whatever's currently pinned there, not these
literal digests, if they've since moved.

`network_mode: service:node-red` makes `daprd` share Node-RED's network
namespace without exposing extra ports on the host — equivalent to two
containers in the same Kubernetes pod.

**This repo's own `docker-compose.yml`** takes a different shape because
Node-RED is _not_ a Compose service there — a human runs it on the host
(`npx node-red`, the devDependency this repo already installs) so they can
iterate on a flow. `daprd` instead uses
`network_mode: host` to reach `127.0.0.1:3000` on the host directly. That's
the right pattern only when Node-RED itself runs on the host; don't copy it
for a topology where Node-RED runs as its own container.

Either way, images are pinned by digest, not just tag — see
`test/helpers/integration.js` for the re-pin procedure this project follows.

## NATS JetStream

The `pubsub.jetstream` component binds to an existing stream. The stream
must exist before the component is used. Two JetStream components ship here,
for two different jobs — check which one you mean:

| File                                                         | Component name | Stream              | For                                                                  |
| ------------------------------------------------------------ | -------------- | ------------------- | -------------------------------------------------------------------- |
| `examples/nats-jetstream-pubsub-component.yaml`              | `pubsub`       | `node-red-examples` | Getting started; `examples/basic-pubsub.json` runs against it as-is. |
| `test/integration/fixtures/components/pubsub-jetstream.yaml` | `pubsub-nats`  | `manual`            | This repository's own manual Compose stack (`docker-compose.yml`).   |

`examples/README.md` is the step-by-step quickstart for the first one. For the
second — the Compose stack, which mounts the whole fixtures directory — provision
its stream first:

```bash
node -e "require('./test/helpers/nats').provisionStream(4222, { streamName: 'manual', subjects: ['manual.>'] }).then(() => console.log('stream ready'))"
```

That component is named `pubsub-nats` (both it and the Redis one live in the same
mounted directory, so only one can be called `pubsub`); use topics under
`manual.*` so they match that stream's subject filter.

With the pinned Dapr 1.18.1 runtime, do not use `deadLetterTopic` with NATS
JetStream. The real integration test observes the original delivery stall
before a dead-letter message arrives. Use Redis or broker-side dead-letter
handling instead, and reverify this limitation before adopting a later Dapr
runtime.

## AWS MemoryDB for Redis

A managed cluster instead of a Redis container. Copy
`examples/memorydb-pubsub-component.yaml` into your resources path, fill in the
endpoint and ACL user, and put the password in daprd's environment as
`MEMORYDB_PASSWORD`. Nothing in a flow changes — the nodes never see the broker,
so `examples/basic-pubsub.json` works against it as-is.

Three settings differ from a local Redis and all three are required:

| Setting              | Why                                                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------------------- |
| `enableTLS: 'true'`  | MemoryDB requires TLS in transit. Without it the component never becomes ready and every publish fails. |
| `redisUsername`      | MemoryDB authenticates a Redis ACL user, so a username is needed as well as a password.                 |
| `redisType: cluster` | MemoryDB is always a cluster; its `clustercfg` endpoint reports `cluster_state:ok` even with one shard. |

Two operational constraints that are easy to miss:

- **The endpoint is VPC-only.** MemoryDB publishes no public endpoint, so daprd
  must run inside the cluster's VPC or reach it over VPN/peering. From outside,
  the hostname does not resolve at all — which is a clearer symptom than it
  sounds, since it fails at DNS rather than at the TLS handshake.
- **The ACL user needs stream permissions on your topics.** Dapr's Redis pub/sub
  keeps one Redis stream per topic, named after the topic, and drives it with
  `XADD`/`XREADGROUP`/`XGROUP`/`XACK`. A user scoped to the wrong key pattern
  authenticates fine and then silently delivers nothing.

The password belongs in daprd's environment rather than the component file: the
component is a file daprd's container user must be able to read, and a real
credential does not belong in one. The example wires this up with Dapr's own
`secretstores.local.env` store and a `secretKeyRef`.

`npm run test:integration:memorydb` exercises this configuration against a real
cluster; see `docs/testing.md` for how that optional tier is gated.

## Kubernetes

Use Dapr's standard sidecar-injection model: annotate the Node-RED pod and
let the Dapr control plane inject `daprd` as a second container in the same
pod.

```yaml
metadata:
  annotations:
    dapr.io/enabled: 'true'
    dapr.io/app-id: 'my-flow'
    dapr.io/app-port: '3000'
    dapr.io/app-protocol: 'http'
```

Containers in one pod already share a network namespace, so `127.0.0.1`
works for both directions with no extra configuration — this is the
loopback-friendly case the whole design assumes.

## Non-default topologies

Only bind the app-channel listener beyond `127.0.0.1`, or run Node-RED and
`daprd` on genuinely separate hosts, if you've read `docs/security.md`'s
non-loopback and ACL sections first — those configurations need an app API
token and network-level access controls that aren't needed in the default,
same-namespace case.
