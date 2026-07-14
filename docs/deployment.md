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
