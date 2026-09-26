'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const { resolveOptions } = require('../../lib/options');
const { ErrorCodes } = require('../../lib/errors');
const { buildService } = require('../../lib/services');
const { preparePublish } = require('../../lib/messages');
const { prepareStateSave } = require('../../lib/state-messages');
const { prepareBindingRequest } = require('../../lib/binding-messages');
const { prepareSecretGet, resolveSecretProperty } = require('../../lib/secret-messages');
const {
  prepareConfigurationGet,
  prepareConfigurationSubscribe,
} = require('../../lib/configuration-messages');
const { normalisePropertyExpression } = require('@node-red/util').util;
const { buildSubscription } = require('../../lib/subscriptions');
const { parseRequestHeaders } = require('../../lib/http-headers');
const { validateActorSegment } = require('../../lib/actor-messages');

// Editor code and runtime code are separate environments — the browser cannot
// require lib/, and the runtime has no DOM — so a predicate needed on both sides
// is necessarily duplicated. Duplication is acceptable; silent divergence is not.
// This file pins the pairs, one table per predicate.
//
// A regex over the editor's source (does it mention 127.*?) cannot do this: it
// proves the text exists, not that both sides classify the same input the same
// way. So the editor's own function is extracted and EXECUTED against the same
// table the runtime is checked with.

const readNode = (name) => fs.readFileSync(path.resolve(__dirname, '../../nodes', name), 'utf8');

// `vm` rather than `new Function`, which the lint config forbids project-wide.
function loadEditorIsLoopback() {
  const source = readNode('dapr-connection.html').match(
    / {6}const isLoopback = function \(value\) \{[\s\S]*?\n {6}\};/
  );
  assert.ok(
    source,
    'could not find `const isLoopback = function (value)` in nodes/dapr-connection.html — if it was renamed or reshaped, update this extraction rather than deleting the check'
  );
  return vm.runInNewContext(`${source[0]}\nisLoopback;`);
}

// Extract one `validate:` function from a node's `defaults`, along with any
// top-level helpers declared beside it in the same script block (isBlank, etc),
// which the validator may call. `validate:` may be an inline function
// expression OR a bare identifier referencing a top-level helper (e.g.
// `validate: isStaticMessageProperty`) -- both forms resolve correctly
// against the same `helpers` block below.
function loadEditorValidator(file, property) {
  const html = readNode(file);
  const source = html.match(
    new RegExp(
      `${property}: \\{[\\s\\S]*?validate: (function \\(v(?:alue)?\\) \\{[\\s\\S]*?\\n {8}\\}|[A-Za-z_$][A-Za-z0-9_$]*)`
    )
  );
  assert.ok(source, `could not find the ${property} validate function in nodes/${file}`);
  const script = (html.match(/<script type="text\/javascript">([\s\S]*?)<\/script>/) || [
    '',
    '',
  ])[1];
  const helpers = (script.match(/^ {2}function [\s\S]*?^ {2}\}/gm) || []).join('\n');
  return vm.runInNewContext(`${helpers}\n(${source[1]})`);
}

const accepts = (fn) => {
  try {
    fn();
    return true;
  } catch {
    return false;
  }
};

// Two contracts, and the distinction matters more than it looks.
//
//   'exact'     — the pair must agree on every case. Required where the editor's
//                 answer is a claim about what the runtime will do.
//   'no-looser' — the editor may reject what the runtime accepts (friction), but
//                 must never ACCEPT what the runtime rejects. That direction is
//                 the bug: the node looks valid, deploys, then fails at runtime.
//
// A real instance of that bug shipped here: the invoke node's `headers` validator
// checked only JSON shape, so an illegal header name or a CRLF in a value passed
// the editor and then failed every message with INVALID_MESSAGE.
function assertContract({ label, editor, runtime, cases, mode }) {
  for (const value of cases) {
    const e = editor(value);
    const r = accepts(() => runtime(value));
    const shown = JSON.stringify(value);
    assert.ok(
      !(e && !r),
      `${label}: editor ACCEPTS ${shown} but the runtime rejects it — the node would look valid and then fail on deploy`
    );
    if (mode === 'exact') {
      assert.equal(e, r, `${label}: editor and runtime disagree for ${shown}`);
    }
  }
}

// The runtime gate this warning is supposed to predict: a non-loopback bind with
// no app API token is refused outright, because the app channel would then
// authenticate nobody (lib/options.js, lib/app-channel.js).
function runtimeRefusesWithoutToken(bindAddress) {
  try {
    resolveOptions({ config: { bindAddress } });
    return false;
  } catch (err) {
    assert.equal(err.code, ErrorCodes.INVALID_OPTIONS, `unexpected error for ${bindAddress}`);
    return true;
  }
}

// `loopback: true` means "reachable only from this host": the editor shows no
// warning and the runtime starts without a token.
const CASES = [
  // Blank is not a hole in the check — it means "use the 127.0.0.1 default".
  { value: undefined, loopback: true },
  { value: '', loopback: true },
  { value: '   ', loopback: true },
  { value: '127.0.0.1', loopback: true },
  { value: '127.1.2.3', loopback: true },
  { value: '127.255.255.255', loopback: true },
  { value: ' 127.0.0.1 ', loopback: true }, // both sides trim
  { value: 'localhost', loopback: true },
  { value: 'LocalHost', loopback: true }, // both sides lowercase
  { value: '::1', loopback: true },
  { value: '[::1]', loopback: true },
  // 0.0.0.0 binds every interface, including public ones — the case most likely
  // to be mistaken for a safe default.
  { value: '0.0.0.0', loopback: false },
  { value: '10.0.0.5', loopback: false },
  { value: '192.168.1.10', loopback: false },
  { value: 'example.com', loopback: false },
  // Neither side treats the IPv4-mapped IPv6 form as loopback. That is a
  // deliberate fail-closed choice, not an oversight: it warns and demands a token.
  { value: '::ffff:127.0.0.1', loopback: false },
  // Near-misses that must not satisfy the 127.x pattern. The neighbours of 127
  // are here because they are what catches a widened first octet: an earlier
  // version of this table had no 12x address other than 127.x, so loosening the
  // pattern to /^12\d\./ changed no case and the mutation went undetected.
  { value: '126.0.0.1', loopback: false },
  { value: '128.0.0.1', loopback: false },
  { value: '120.0.0.1', loopback: false },
  { value: '12.0.0.1', loopback: false },
  { value: '1270.0.0.1', loopback: false },
  { value: '127x0.0.1', loopback: false },
];

test('the editor bind warning fires exactly when the runtime refuses the bind', () => {
  const editorIsLoopback = loadEditorIsLoopback();

  for (const { value, loopback } of CASES) {
    const label = JSON.stringify(value);

    // Agreement alone is too weak — both sides could drift the same way and stay
    // consistent while being wrong. So the classification itself is pinned first.
    assert.equal(editorIsLoopback(value), loopback, `editor classification changed for ${label}`);
    assert.equal(
      runtimeRefusesWithoutToken(value),
      !loopback,
      `runtime classification changed for ${label}`
    );

    // ...and then the contract that matters to a user: the warning they see
    // before deploying predicts whether the deploy will actually be refused.
    assert.equal(
      !editorIsLoopback(value),
      runtimeRefusesWithoutToken(value),
      `editor warning and runtime gate disagree for ${label}`
    );
  }
});

test('a configured app API token clears the runtime refusal, warning or not', () => {
  // The editor warning says "this needs a token", not "this is forbidden" — with
  // a token the same non-loopback binds are accepted. Pinning this keeps the two
  // meanings from being conflated if either side is reworked.
  for (const { value, loopback } of CASES.filter((c) => !c.loopback)) {
    const opts = resolveOptions({
      config: { bindAddress: value },
      credentials: { appApiToken: 'secret' },
    });
    assert.equal(opts.inbound.bindAddress, String(value).trim());
    assert.equal(loopback, false);
  }
});

// --- the remaining mirrored validators -------------------------------------
// Found by sweeping for literals shared between lib/ and nodes/*.html, then
// enumerating every `validate:` in the editor against its runtime counterpart.
// Reading them was not enough: three disagreed, and one disagreed dangerously.

const PATHS = [
  '/orders',
  'orders',
  '/orders/echo',
  '/orders ',
  ' /orders',
  '  /orders  ', // the runtime trims via requiredString; the editor must too
  '/x~y.z-1_2',
  '/ok/',
  '/dapr', // reserved prefixes, duplicated verbatim on both sides
  '/dapr/x',
  '/healthz',
  '/node-red-dapr/y',
  '/DAPR', // both are case-sensitive, so this is NOT reserved
  '/a..b',
  '/a//b',
  '/a b',
  '',
  '   ',
];

test('service method path: editor validation matches the runtime route rules', () => {
  assertContract({
    label: 'dapr-service methodPath',
    editor: loadEditorValidator('dapr-service.html', 'methodPath'),
    runtime: (path) => buildService({ nodeId: 'n1', verb: 'POST', path }),
    cases: PATHS,
    mode: 'exact',
  });
});

const METADATA = [
  '',
  '{}',
  '{"a":"b"}',
  '{"a":1}',
  '{"a":true}',
  '{"a":null}', // null is not a scalar Dapr metadata value
  '{"a":{}}',
  '{"a":[]}',
  '{" ":1}', // blank key after trimming
  '{"":1}',
  '[]',
  'null',
  '0',
  '"s"',
  'not json',
  '{"a":1,',
  ' {"a":1} ',
];

test('pub/sub metadata: both editors match their runtime normalizer', () => {
  assertContract({
    label: 'dapr-publish metadata',
    editor: loadEditorValidator('dapr-publish.html', 'metadata'),
    runtime: (metadata) =>
      preparePublish({ pubsubName: 'p', topic: 't', contentType: '', metadata }, { payload: 'x' }),
    cases: METADATA,
    mode: 'exact',
  });
  assertContract({
    label: 'dapr-state metadata',
    editor: loadEditorValidator('dapr-state.html', 'metadata'),
    runtime: (metadata) =>
      prepareStateSave(
        { storeName: 's', key: 'k', consistency: '', concurrency: '', ttlSeconds: '', metadata },
        { payload: 'x' }
      ),
    cases: METADATA,
    mode: 'exact',
  });
  assertContract({
    label: 'dapr-subscribe metadata',
    editor: loadEditorValidator('dapr-subscribe.html', 'metadata'),
    runtime: (metadata) =>
      buildSubscription({ nodeId: 'n1', pubsubName: 'p', topic: 't', metadata }),
    cases: METADATA,
    mode: 'exact',
  });
  assertContract({
    label: 'dapr-binding-out metadata',
    editor: loadEditorValidator('dapr-binding-out.html', 'metadata'),
    runtime: (metadata) =>
      prepareBindingRequest({ bindingName: 'b', operation: 'create', metadata }, { payload: 'x' }),
    cases: METADATA,
    mode: 'exact',
  });
  assertContract({
    label: 'dapr-secret-get metadata',
    editor: loadEditorValidator('dapr-secret-get.html', 'metadata'),
    runtime: (metadata) => prepareSecretGet({ storeName: 's', key: 'k', metadata }, {}),
    cases: METADATA,
    mode: 'exact',
  });
  assertContract({
    label: 'dapr-config-get metadata',
    editor: loadEditorValidator('dapr-config-get.html', 'metadata'),
    runtime: (metadata) => prepareConfigurationGet({ storeName: 's', keys: '', metadata }, {}),
    cases: METADATA,
    mode: 'exact',
  });
  assertContract({
    label: 'dapr-config-subscribe metadata',
    editor: loadEditorValidator('dapr-config-subscribe.html', 'metadata'),
    runtime: (metadata) => prepareConfigurationSubscribe({ storeName: 's', keys: 'k', metadata }),
    cases: METADATA,
    mode: 'exact',
  });
});

// Node-RED editor scripts cannot require() a shared module, and the documented
// alternatives (a cross-file global, a plugin-served script) trade this
// duplication for load-order coupling between node files. So the seven copies
// stay — but they may never silently drift apart. Each is already pinned
// against the runtime by assertContract above; this pins them to each other,
// so editing one and forgetting the rest fails here rather than in a dialog.
test('the metadata validator is identical in every node that has one', () => {
  const extract = (file) => {
    const source = readNode(file).match(/ {6}metadata: \{[\s\S]*?\n {6}\},/);
    assert.ok(source, `could not find the metadata validator in nodes/${file}`);
    return source[0];
  };
  const files = fs
    .readdirSync(path.resolve(__dirname, '../../nodes'))
    .filter((name) => name.endsWith('.html'))
    .filter((name) => readNode(name).includes('      metadata: {'));

  assert.ok(files.length >= 7, `expected every metadata-bearing node, found ${files.length}`);
  const [reference, ...rest] = files;
  for (const file of rest) {
    assert.equal(
      extract(file),
      extract(reference),
      `nodes/${file}'s metadata validator has drifted from nodes/${reference}'s — ` +
        `update every copy together, or none`
    );
  }
});

test('dapr-secret-get property: the editor never accepts a path the runtime rejects', () => {
  // 'no-looser', not 'exact': the runtime defaults a blank value to "payload"
  // (defensive handling for hand-authored/legacy flow JSON), while the
  // editor's own `required: true` already independently blocks saving a
  // blank value -- that one divergence is the safe direction (editor
  // stricter), not the dangerous one this contract exists to catch.
  assertContract({
    label: 'dapr-secret-get property',
    editor: loadEditorValidator('dapr-secret-get.html', 'property'),
    runtime: (property) => resolveSecretProperty(property, normalisePropertyExpression),
    cases: [
      '',
      'payload',
      'msg.payload',
      'secret',
      'secret.nested',
      'dapr',
      'msg.dapr',
      'dapr.secret',
      '__proto__',
      'secret.__proto__.polluted',
      'secret[msg.topic]',
      '..',
      'secret.',
      '   ',
    ],
    mode: 'no-looser',
  });
});

test('invoke headers: the editor rejects the header names and values the runtime does', () => {
  // The regression that motivated this file's `no-looser` rule. Header grammar
  // cannot be shared: the editor has no access to Node's http.validateHeaderName,
  // so it restates RFC 9110's token rule and Node's accepted value range.
  assertContract({
    label: 'dapr-invoke headers',
    editor: loadEditorValidator('dapr-invoke.html', 'headers'),
    runtime: (headers) => parseRequestHeaders(headers, 'headers'),
    cases: [
      '',
      '{}',
      '{"a":"b"}',
      '{"a":1}',
      '{"a":{}}', // coerced to "[object Object]" by both, and legal
      '{"traceparent":"00-abc-def-01"}',
      '{"X-Custom_Header.1":"v"}',
      '{"bad name":"x"}', // space is not a token character
      '{"a:b":"x"}',
      '{"a(paren)":"x"}',
      '{"a@b":"x"}',
      '{"a,b":"x"}',
      '{"a/b":"x"}',
      '{"":"x"}',
      JSON.stringify({ x: 'a\r\nb' }), // CRLF injection
      JSON.stringify({ x: 'a\nb' }),
      JSON.stringify({ x: 'a\tb' }), // tab IS legal in a value
      JSON.stringify({ a: 'café' }), // latin-1 high range IS legal
      '[]',
      'null',
      '0',
      'not json',
    ],
    mode: 'exact',
  });
});

const NUMBERS = [
  '',
  '0',
  '1',
  '-1',
  '0.5',
  '1.5',
  'abc',
  '1e3',
  '  5  ',
  '64',
  '65',
  '300',
  '301',
  '1000',
  '1001',
  '60000',
  '60001',
  '65535',
  '65536',
  'Infinity',
  'NaN',
];

test('numeric editor fields never accept a value their runtime bound rejects', () => {
  // 'no-looser', not 'exact': these editors require plain digits, while the
  // runtime parses with Number(), so the runtime also accepts "1e3". Keeping the
  // editor stricter is deliberate — a port field should not take exponent
  // notation — and stricter is the safe direction.
  const pairs = [
    ['daprPort', 'dapr-connection.html', (v) => resolveOptions({ config: { daprPort: v } })],
    ['appPort', 'dapr-connection.html', (v) => resolveOptions({ config: { appPort: v } })],
    ['bodyLimitMb', 'dapr-connection.html', (v) => resolveOptions({ config: { bodyLimitMb: v } })],
    [
      'requestTimeoutSec',
      'dapr-connection.html',
      (v) => resolveOptions({ config: { requestTimeoutSec: v } }),
    ],
    [
      'bulkMaxMessagesCount',
      'dapr-subscribe.html',
      (v) =>
        buildSubscription({
          nodeId: 'n',
          pubsubName: 'p',
          topic: 't',
          bulkSubscribe: { enabled: true, maxMessagesCount: v },
        }),
    ],
    [
      'bulkMaxAwaitDurationMs',
      'dapr-subscribe.html',
      (v) =>
        buildSubscription({
          nodeId: 'n',
          pubsubName: 'p',
          topic: 't',
          bulkSubscribe: { enabled: true, maxAwaitDurationMs: v },
        }),
    ],
    [
      'ttlSeconds',
      'dapr-state.html',
      (v) =>
        prepareStateSave(
          {
            storeName: 's',
            key: 'k',
            consistency: '',
            concurrency: '',
            ttlSeconds: v,
            metadata: '',
          },
          { payload: 'x' }
        ),
    ],
  ];
  for (const [property, file, runtime] of pairs) {
    assertContract({
      label: `${file} ${property}`,
      editor: loadEditorValidator(file, property),
      runtime,
      cases: NUMBERS,
      mode: 'no-looser',
    });
  }
});

const ACTOR_SEGMENTS = [
  '',
  '   ',
  'DemoActor',
  'demo-1',
  'a/b',
  '.',
  '..',
  'a.b',
  'a..b',
  'x'.repeat(256),
  'x'.repeat(257),
  'a\u0000b',
  'a\u007fb',
  'a\tb',
  'café',
  '/leading',
  'trailing/',
];

test('actor type/id/method: editor validation matches lib/actor-messages.js validateActorSegment', () => {
  const pairs = [
    ['actorType', 'dapr-actor-method.html'],
    ['method', 'dapr-actor-method.html'],
    ['actorType', 'dapr-actor-call.html'],
    ['actorId', 'dapr-actor-call.html'],
    ['method', 'dapr-actor-call.html'],
  ];
  for (const [property, file] of pairs) {
    assertContract({
      label: `${file} ${property}`,
      editor: loadEditorValidator(file, property),
      runtime: (value) => validateActorSegment(value, property),
      cases: ACTOR_SEGMENTS,
      mode: 'exact',
    });
  }
});
