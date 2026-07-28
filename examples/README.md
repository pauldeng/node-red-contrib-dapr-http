# Examples

Start with `basic-pubsub.json`. It is the smallest complete flow: an Inject node
publishes a message to Dapr pub/sub, and a Subscribe node receives it back on the
same topic.

For pub/sub, `daprd` also needs a broker component. This quickstart uses NATS
JetStream. Redis, AWS MemoryDB, or another Dapr-supported broker can replace it
if the component is still named `pubsub`, matching the node defaults in the
flow.

One caveat about this quickstart's broker, worth knowing before you build on it:
with the pinned Dapr 1.18.1 runtime, do not use `deadLetterTopic` with NATS
JetStream. Delivery stalls before the dead-letter message arrives — a real
integration test observes exactly that. None of the example flows set a
dead-letter topic, so the quickstart is unaffected; if you add one later, use
Redis or broker-side dead-letter handling instead. See
`../docs/subscriptions.md`.

Service invocation examples do not need Redis or NATS. They need `daprd` plus
another Dapr app to call, or another caller that invokes this app.

## Run the basic pub/sub example locally

1. Install this package into your Node-RED user directory, then start Node-RED:

   ```bash
   cd ~/.node-red
   npm install /path/to/node-red-contrib-dapr
   npx node-red
   ```

2. In another terminal, start `nats-server` with JetStream enabled and create a
   memory-backed stream:

   ```bash
   nats-server -js

   nats stream add node-red-examples --server nats://127.0.0.1:4222 \
     --subjects greetings --storage memory --defaults

   mkdir -p components
   cp /path/to/node-red-contrib-dapr/examples/nats-jetstream-pubsub-component.yaml \
     components/pubsub.yaml
   ```

3. Start `daprd`, pointing its app port at the `dapr-connection` default app
   port (`3000`) and its HTTP API at the default sidecar port (`3500`):

   ```bash
   daprd --app-id node-red --app-port 3000 --app-protocol http \
     --dapr-http-port 3500 --resources-path ./components
   ```

4. In the Node-RED editor, import `examples/basic-pubsub.json`.

5. Deploy the flow, then restart `daprd` once. Dapr reads
   `/dapr/subscribe` only at sidecar startup, so the subscribe node is not active
   until the sidecar has fetched the deployed subscription list.

6. Click the Inject node. You should see:

   - `publish result` in the debug sidebar after the publish succeeds.
   - `received` in the debug sidebar when the subscribe node receives the same
     message back.

Routine redeploys of the same flow do not need another sidecar restart. Restart
`daprd` only after changing the subscription definition: pub/sub name, topic,
metadata, dead-letter topic, raw-payload mode, CEL rules, or bulk settings.

## Other flows

- `cel-routing.json` shows CEL rule routes and `msg.dapr.ruleId`.
- `bulk-acknowledgement.json` shows bulk subscribe with per-entry
  acknowledgement.
- `outbound-invocation.json` calls another Dapr app-id. It needs a real target
  app named `order-service` or a sidecar route you change to match your app.
- `inbound-service.json` exposes `POST /orders/echo` and replies through
  `dapr-response`. Invoke it from another Dapr app or with daprd's service
  invocation API.

For `cel-routing.json` or `bulk-acknowledgement.json`, create the NATS stream
with the `orders` subject too:

```bash
nats stream update node-red-examples --server nats://127.0.0.1:4222 \
  --subjects greetings,orders
```

## Running NATS on a target platform

The same rules apply on the target host or platform:

- Run NATS with JetStream enabled.
- Create the memory-backed stream before `daprd` starts.
- Set the Dapr component's `natsURL` to the platform DNS name for NATS.
- Keep the component name `pubsub` if you want the example flows unchanged.

For a VM or bare host, install the NATS server and CLI binaries, then run NATS
under your process manager. A minimal `systemd` service looks like this:

```ini
[Unit]
Description=NATS Server
After=network-online.target

[Service]
ExecStart=/usr/local/bin/nats-server -js
Restart=always
User=nats
Group=nats

[Install]
WantedBy=multi-user.target
```

Then create the stream once:

```bash
nats stream add node-red-examples --server nats://<nats-host>:4222 \
  --subjects greetings,orders --storage memory --defaults
```

For Kubernetes, run NATS with JetStream enabled in the same cluster and point
the component at its service DNS name:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.jetstream
  version: v1
  metadata:
    - name: natsURL
      value: nats://nats.default.svc.cluster.local:4222
    - name: streamName
      value: node-red-examples
```

Create the stream from a one-shot admin job or a shell in a NATS tooling pod
before rolling out Node-RED/daprd:

```bash
nats stream add node-red-examples --server nats://nats.default.svc.cluster.local:4222 \
  --subjects greetings,orders --storage memory --defaults
```

`memorydb-pubsub-component.yaml` is kept for operators who need AWS MemoryDB,
but it is not the beginner path.
