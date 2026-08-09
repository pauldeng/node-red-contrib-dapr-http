'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { resolveOptions } = require('../../lib/options');
const { DaprError, ErrorCodes } = require('../../lib/errors');

const MiB = 1024 * 1024;

test('outbound: env mode when neither host nor port is configured', () => {
  const opts = resolveOptions({ config: {}, credentials: {}, env: {} });
  assert.equal(opts.outbound.mode, 'env');
  assert.equal(opts.outbound.baseUrl, 'http://127.0.0.1:3500'); // Dapr HTTP default
});

test('outbound: env mode adopts DAPR_HTTP_ENDPOINT as the base URL', () => {
  const opts = resolveOptions({ env: { DAPR_HTTP_ENDPOINT: 'http://sidecar:3510/' } });
  assert.equal(opts.outbound.mode, 'env');
  assert.equal(opts.outbound.baseUrl, 'http://sidecar:3510'); // trailing slash normalized
});

test('outbound: explicit mode fills defaults and derives a base URL', () => {
  const onlyPort = resolveOptions({ config: { daprPort: '3600' } });
  assert.equal(onlyPort.outbound.mode, 'explicit');
  assert.equal(onlyPort.outbound.host, '127.0.0.1');
  assert.equal(onlyPort.outbound.port, 3600);
  assert.equal(onlyPort.outbound.baseUrl, 'http://127.0.0.1:3600');

  const onlyHost = resolveOptions({ config: { daprHost: 'sidecar' } });
  assert.equal(onlyHost.outbound.baseUrl, 'http://sidecar:3500');
});

test('validation: a malformed DAPR_HTTP_ENDPOINT throws INVALID_OPTIONS', () => {
  assert.throws(
    () => resolveOptions({ env: { DAPR_HTTP_ENDPOINT: 'not-a-url' } }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('validation: an explicit host that yields an invalid URL throws INVALID_OPTIONS', () => {
  assert.throws(
    () => resolveOptions({ config: { daprHost: 'bad host', daprPort: '3500' } }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS
  );
});

test('validation: non-http or non-origin endpoints are rejected (scope is http over tcp)', () => {
  const bad = [
    'https://sidecar:3500',
    'file:///tmp/dapr.sock',
    'http://sidecar:3500/path',
    'http://sidecar:3500/?x=1',
    'http://sidecar:3500/#frag',
  ];
  for (const endpoint of bad) {
    assert.throws(
      () => resolveOptions({ env: { DAPR_HTTP_ENDPOINT: endpoint } }),
      (e) => e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS,
      `expected ${endpoint} to be rejected`
    );
  }
});

test('outbound: blank values do not trigger explicit mode (env discovery preserved)', () => {
  const opts = resolveOptions({ config: { daprHost: '  ', daprPort: '' } });
  assert.equal(opts.outbound.mode, 'env');
});

test('dapr API token precedence: credential, then env, then undefined', () => {
  assert.equal(
    resolveOptions({ credentials: { daprApiToken: 'cred' }, env: { DAPR_API_TOKEN: 'env' } })
      .daprApiToken,
    'cred'
  );
  assert.equal(resolveOptions({ env: { DAPR_API_TOKEN: 'env' } }).daprApiToken, 'env');
  assert.equal(resolveOptions({}).daprApiToken, undefined);
});

test('inbound: defaults to loopback bind and port 3000', () => {
  const opts = resolveOptions({});
  assert.equal(opts.inbound.bindAddress, '127.0.0.1');
  assert.equal(opts.inbound.port, 3000);
});

test('inbound: honors configured bind address and app port', () => {
  // A non-loopback bind additionally requires an app API token — see the
  // dedicated test below.
  const opts = resolveOptions({
    config: { bindAddress: '0.0.0.0', appPort: '3005' },
    credentials: { appApiToken: 'secret' },
  });
  assert.equal(opts.inbound.bindAddress, '0.0.0.0');
  assert.equal(opts.inbound.port, 3005);
});

test('app API token precedence: credential, then env, then undefined', () => {
  assert.equal(
    resolveOptions({ credentials: { appApiToken: 'cred' }, env: { APP_API_TOKEN: 'env' } }).inbound
      .appApiToken,
    'cred'
  );
  assert.equal(resolveOptions({ env: { APP_API_TOKEN: 'env' } }).inbound.appApiToken, 'env');
  assert.equal(resolveOptions({}).inbound.appApiToken, undefined);
});

test('limits: canonical defaults with unit conversion', () => {
  const { limits } = resolveOptions({});
  assert.equal(limits.bodyLimitBytes, 4 * MiB);
  assert.equal(limits.requestTimeoutMs, 30000);
  assert.equal(limits.headerLimitBytes, 16 * 1024);
  assert.equal(limits.drainTimeoutMs, 5000);
  assert.equal(limits.leaseGraceMs, 2000);
  assert.equal(limits.maxPending, 1000);
});

test('limits: body size and request timeout are configurable (MB, seconds)', () => {
  const { limits } = resolveOptions({ config: { bodyLimitMb: '8', requestTimeoutSec: '10' } });
  assert.equal(limits.bodyLimitBytes, 8 * MiB);
  assert.equal(limits.requestTimeoutMs, 10000);
});

test('limits: header, drain, lease and pending caps are fixed and not overridable', () => {
  const { limits } = resolveOptions({
    config: { headerLimitBytes: 1, drainTimeoutMs: 1, leaseGraceMs: 1, maxPending: 1 },
  });
  assert.equal(limits.headerLimitBytes, 16 * 1024);
  assert.equal(limits.drainTimeoutMs, 5000);
  assert.equal(limits.leaseGraceMs, 2000);
  assert.equal(limits.maxPending, 1000);
});

test('validation: non-numeric or out-of-range ports throw INVALID_OPTIONS', () => {
  for (const bad of ['abc', '0', '70000', '-1', '80.5']) {
    assert.throws(
      () => resolveOptions({ config: { appPort: bad } }),
      (e) => {
        return e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS;
      }
    );
    assert.throws(
      () => resolveOptions({ config: { daprPort: bad } }),
      (e) => {
        return e instanceof DaprError && e.code === ErrorCodes.INVALID_OPTIONS;
      }
    );
  }
});

test('validation: non-positive body size or timeout throw INVALID_OPTIONS', () => {
  assert.throws(() => resolveOptions({ config: { bodyLimitMb: '0' } }), DaprError);
  assert.throws(() => resolveOptions({ config: { requestTimeoutSec: '-5' } }), DaprError);
});

test('limits: body size is bounded to 1..64 MiB', () => {
  assert.throws(() => resolveOptions({ config: { bodyLimitMb: '0.5' } }), DaprError);
  assert.throws(() => resolveOptions({ config: { bodyLimitMb: '65' } }), DaprError);
  assert.equal(resolveOptions({ config: { bodyLimitMb: '1' } }).limits.bodyLimitBytes, 1 * MiB);
  assert.equal(resolveOptions({ config: { bodyLimitMb: '64' } }).limits.bodyLimitBytes, 64 * MiB);
});

test('limits: request timeout is bounded to 1..300 seconds', () => {
  assert.throws(() => resolveOptions({ config: { requestTimeoutSec: '0' } }), DaprError);
  assert.throws(() => resolveOptions({ config: { requestTimeoutSec: '301' } }), DaprError);
  assert.equal(
    resolveOptions({ config: { requestTimeoutSec: '300' } }).limits.requestTimeoutMs,
    300000
  );
});

test('a non-loopback bind requires an app API token, and loopback variants do not', () => {
  // Fail closed: exposing the app channel beyond this host without a token would
  // let anything that can reach the interface post deliveries into flows.
  assert.throws(
    () => resolveOptions({ config: { bindAddress: '0.0.0.0', appPort: '3000' } }),
    (err) => err.code === ErrorCodes.INVALID_OPTIONS && /app API token/i.test(err.message)
  );
  assert.throws(
    () => resolveOptions({ config: { bindAddress: '10.1.2.3' } }),
    (err) => err.code === ErrorCodes.INVALID_OPTIONS
  );

  // With a token, a non-loopback bind is allowed (explicit operator choice).
  assert.equal(
    resolveOptions({
      config: { bindAddress: '0.0.0.0' },
      credentials: { appApiToken: 'secret' },
    }).inbound.bindAddress,
    '0.0.0.0'
  );
  // ...including a token supplied via the environment.
  assert.equal(
    resolveOptions({ config: { bindAddress: '0.0.0.0' }, env: { APP_API_TOKEN: 'secret' } }).inbound
      .appApiToken,
    'secret'
  );

  // Every loopback form stays zero-config.
  for (const bindAddress of ['127.0.0.1', '127.0.0.5', 'localhost', '::1', '[::1]']) {
    assert.equal(resolveOptions({ config: { bindAddress } }).inbound.bindAddress, bindAddress);
  }
});
