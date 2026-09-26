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
no result and store the counter in memory. Upstream deletion, timers and reminders
are omitted. This is a teaching example, not an SDK compatibility layer.
