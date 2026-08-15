'use strict';

// Real daprd 1.18.2 + a real bindings.http output-binding component -- output
// bindings need no broker or store at all (mirrors acl/invoke/shutdown's own
// broker-agnostic setup), so this belongs in the test:integration:dapr bucket,
// not test:integration:redis. `url` points directly at this test's own
// capture server (startDaprd runs daprd with --network host, so a host-side
// 127.0.0.1 URL is reachable unchanged from inside the container).
//
// bindings.http's Invoke (components-contrib v1.18.0, bindings/http/http.go):
// operation is upper-cased into an HTTP method ("post"/"create" -> POST,
// sending req.Data as the request body); on a 2xx target response, the
// component's real response body and {statusCode, status, <headers>}
// metadata pass straight through. On a non-2xx target response (with the
// component's own default errorIfNot2XX=true), the component still builds
// that same response+metadata but ALSO returns an error -- daprd's handler
// then discards the real response entirely and answers with the generic
// 500 ERR_INVOKE_OUTPUT_BINDING envelope instead (confirmed against source,
// documented in nodes/dapr-binding-out.html as a real gap, not hidden here).

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd, bindingComponentYaml } = require('../helpers/integration');
const { startCapture } = require('../helpers/capture');
const { waitFor } = require('../helpers/wait-for');

const BINDING = 'orders-binding';
const MISSING_BINDING = 'no-such-binding';

function flow({ appPort, daprHttpPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-binding-out' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    { id: 'in', type: 'http in', z: 'tab', url: '/invoke', method: 'post', wires: [['before']] },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload.dapr; msg.payload = msg.payload.data; return msg;',
      outputs: 1,
      wires: [['out']],
    },
    {
      id: 'out',
      type: 'dapr-binding-out',
      z: 'tab',
      connection: 'c1',
      bindingName: BINDING,
      operation: 'post',
      metadata: '{}',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: `msg.statusCode = 200;
if (Buffer.isBuffer(msg.payload)) { msg.payload = msg.payload.toString(); }
msg.payload = { dapr: msg.dapr, payload: msg.payload };
return msg;`,
      outputs: 1,
      wires: [['res']],
    },
    {
      id: 'errors',
      type: 'catch',
      z: 'tab',
      scope: ['out'],
      uncaught: false,
      wires: [['failure']],
    },
    {
      id: 'failure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab' },
  ];
}

function invoke(nr, { bindingName, data } = {}) {
  return httpRequest(nr.nodeUrl('/invoke'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ dapr: bindingName ? { bindingName } : {}, data }),
    timeoutMs: 5000,
  });
}

test(
  'a real bindings.http component round-trips a POST and reports a missing binding',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    await nr.start({ flows: flow({ appPort, daprHttpPort }) });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const daprd = await startDaprd({
      appId: 'it-binding-out-app',
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'orders-binding.yaml', yaml: bindingComponentYaml(capture.url) }],
    });
    t.after(() => daprd?.stop());

    // startDaprd() resolving only proves daprd's OWN /healthz answers; the
    // dapr-connection node polls independently on its own bounded-backoff
    // schedule and may not have caught up yet.
    await waitFor(() => (nr.logText().includes('Dapr sidecar is available') ? true : null));

    const response = await invoke(nr, { data: { id: 1, note: 'real round trip' } });
    assert.equal(response.status, 200);
    const body = JSON.parse(response.text);
    assert.equal(body.dapr.statusCode, 200);
    assert.equal(body.dapr.bindingName, BINDING);
    assert.equal(body.dapr.operation, 'post');
    // Node's http client lower-cases every incoming header name.
    assert.equal(body.dapr.metadata.statuscode, '200');
    assert.equal(body.payload, 'ok');

    // The POST really reached the capture server -- proof this is a genuine
    // round trip through a real daprd + a real component, not an artifact of
    // this package's own code.
    assert.deepEqual(capture.received.at(-1), { id: 1, note: 'real round trip' });

    const missing = await invoke(nr, { bindingName: MISSING_BINDING, data: {} });
    assert.equal(missing.status, 503);
    const missingBody = JSON.parse(missing.text);
    assert.equal(missingBody.code, 'BINDING_INVOKE_FAILED');
    assert.match(missingBody.message, /ERR_INVOKE_OUTPUT_BINDING/);
  }
);
