# Dapr sample attribution

`actor-demo.json` adapts the behavior of these Apache-2.0 Dapr samples:

- [Python DemoActor](https://github.com/dapr/python-sdk/blob/c44c28d83f0648052094af6e7dfaa5a362fcf118/examples/demo_actor/demo_actor/demo_actor.py), Copyright 2021 The Dapr Authors.
- [JavaScript DemoActorCounterImpl](https://github.com/dapr/js-sdk/blob/2395a2103cb3e7474e628012189867bfe836ac78/test/actor/DemoActorCounterImpl.ts), Copyright 2022 The Dapr Authors.

The adapted sample is provided under Apache-2.0; see
[the license](LICENSE-DAPR-2.0.txt). The rest of this package retains its MIT license.

Changes: the implementations are Node-RED Function nodes, persist one `record`
through actor replies, and route validation failures through a scoped Catch.
SetMyData adds an ISO UTC timestamp; the counter accepts one JSON argument object,
persists its value and returns the new count. Upstream increment methods return
no result and store the counter in memory. Upstream deletion and timers are
omitted (this package's v1 has no delete, and timers are not supported). The
sample's `register_reminder`/`unregister_reminder` are adapted as a
`dapr-actor-schedule` node (operations set/get/delete), and its
`receive_reminder` is adapted as a `dapr-actor-method` node with Trigger set
to Reminder, which receives every reminder of its actor type by name rather
than one dedicated callback method. This is a teaching example, not an SDK
compatibility layer.
