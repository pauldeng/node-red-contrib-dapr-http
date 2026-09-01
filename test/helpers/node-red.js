'use strict';

const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const fsp = require('node:fs/promises');
const { spawn } = require('node:child_process');
const { EventEmitter, once } = require('node:events');

const { httpRequest } = require('./http');

const WORKSPACE = path.resolve(__dirname, '..', '..');
const PKG = require(path.join(WORKSPACE, 'package.json'));
const { setTimeout: delay } = require('node:timers/promises');
const NODE_RED_BIN = path.join(WORKSPACE, 'node_modules', 'node-red', 'red.js');

async function waitForLogEvent(logEvents, initialText, pattern, timeoutMs) {
  const matches = (text) =>
    typeof pattern === 'string' ? text.includes(pattern) : text.search(pattern) !== -1;
  let text = initialText;
  if (matches(text)) {
    return text;
  }
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    while (!matches(text)) {
      [text] = await once(logEvents, 'data', { signal });
    }
    return text;
  } catch (err) {
    if (err.name !== 'AbortError') {
      throw err;
    }
    throw new Error(
      `waitForLog(${pattern}) timed out after ${timeoutMs}ms\n--- logs ---\n${text}`,
      { cause: err }
    );
  }
}

// A free loopback port for the child Node-RED's admin API, and for each flow's
// Dapr app channel.
//
// Deliberately NOT listen(0). That returns a port from the OS ephemeral range —
// the same range the OS hands out spontaneously to outbound sockets — so in the
// window between this probe closing and the real listener binding, a busy CI
// runner loses the port to something else. The resulting EADDRINUSE does not
// stop Node-RED, because the app channel binds inside a deployed flow: the
// connection node logs the error, no health poll ever runs, and the test hangs
// until some unrelated waiter times out with a message that never names the
// port. Reproduced by squatting the port — the failure is byte-identical to the
// CI one ("waitForRequest timed out ... (saw: no requests)"), which is what
// makes it worth removing rather than waiting longer for.
//
// Picking below the ephemeral floor closes that race: nothing is assigned there
// spontaneously, so the only possible claimant is another caller here — covered
// by the handed-out set and the retry.
const PORT_FLOOR = 20000;
const HANDED_OUT = new Set();

async function ephemeralFloor() {
  try {
    const range = await fsp.readFile('/proc/sys/net/ipv4/ip_local_port_range', 'utf8');
    const low = Number(range.trim().split(/\s+/)[0]);
    if (Number.isInteger(low) && low > PORT_FLOOR) {
      return low;
    }
  } catch {
    // Not Linux, or the knob is unreadable: fall back to the usual default.
  }
  return 32768;
}

function bindable(port) {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.once('error', () => resolve(false));
    srv.listen(port, '127.0.0.1', () => srv.close(() => resolve(true)));
  });
}

async function freePort(attempts = 50) {
  const ceiling = Math.min(await ephemeralFloor(), 32768);
  for (let i = 0; i < attempts; i += 1) {
    const port = PORT_FLOOR + Math.floor(Math.random() * (ceiling - PORT_FLOOR));
    if (HANDED_OUT.has(port)) {
      continue;
    }
    // Serial by design: each probe must settle before the next candidate.
    if (await bindable(port)) {
      HANDED_OUT.add(port);
      return port;
    }
  }
  throw new Error(
    `freePort: no free loopback port in [${PORT_FLOOR}, ${ceiling}) in ${attempts} tries`
  );
}

function settingsSource(uiPort, { loggingExtra = '', startupFlowLoadDelayMs = 0 } = {}) {
  // Minimal, deterministic settings: fixed port, default admin/node roots,
  // no editor auth, projects/tours off, info logging for readiness detection.
  // telemetry.enabled=false skips the first-run "Enable Update Notifications"
  // consent modal entirely — undocumented in a fresh userDir, it otherwise
  // blocks every editor interaction behind a full-screen shade (found via a
  // real browser drive while building the e2e tier). loggingExtra is a raw
  // source fragment (not a serializable value: it needs its own require()
  // call) spliced into the `logging` object for tests exercising the
  // settings.js-installed OTel logging bridge; the default preserves every
  // other test's exact prior settings.js output.
  const storageLine = startupFlowLoadDelayMs
    ? `  storageModule: require(${JSON.stringify(path.join(__dirname, 'delayed-storage'))}).delayedStorage(${startupFlowLoadDelayMs}),\n`
    : '';
  return `module.exports = {
  uiPort: ${uiPort},
  httpAdminRoot: '/',
  httpNodeRoot: '/',
  flowFile: 'flows.json',
${storageLine}  logging: { console: { level: 'info', metrics: false, audit: false }${loggingExtra} },
  editorTheme: { projects: { enabled: false }, tours: false },
  telemetry: { enabled: false },
  functionGlobalContext: {},
};
`;
}

// Drives the package.json-pinned `node-red` process in an isolated temporary user
// directory, with this workspace package made discoverable so its nodes load
// exactly as an installed package would. Black-box: tests observe behavior over
// HTTP, never by importing Node-RED internals.
class NodeRed {
  constructor() {
    this.userDir = null;
    this.proc = null;
    this.port = null;
    this.logs = [];
    this._logEvents = new EventEmitter();
    this._logEvents.setMaxListeners(0);
    this._exited = null;
    this._linkPath = null;
  }

  // Called for every stdout/stderr chunk so waitForLog can resolve off the
  // stream's own 'data' event rather than a poll of logText().
  _appendLog(chunk) {
    this.logs.push(chunk);
    if (this._logEvents.listenerCount('data') > 0) {
      this._logEvents.emit('data', this.logText());
    }
  }

  // Resolve as soon as the child's own output matches. `pattern` is a
  // substring or a RegExp; String#search is used rather than RegExp#test so a
  // /g/ pattern's lastIndex cannot make repeat checks flap.
  //
  // Output already seen is checked first: the line frequently lands before the
  // test gets around to awaiting it, and an event-only API would hang there.
  async waitForLog(pattern, { timeoutMs = 10000 } = {}) {
    return waitForLogEvent(this._logEvents, this.logText(), pattern, timeoutMs);
  }

  adminUrl(p = '/') {
    return `http://127.0.0.1:${this.port}${p}`;
  }

  // httpNodeRoot is '/', so node-served routes share the admin origin.
  nodeUrl(p = '/') {
    return `http://127.0.0.1:${this.port}${p}`;
  }

  logText() {
    return this.logs.join('');
  }

  async start({
    flows = [],
    readyTimeoutMs = 30000,
    env = {},
    loggingExtra = '',
    startupFlowLoadDelayMs = 0,
  } = {}) {
    this.port = await freePort();
    // Transactional: if any step fails (including readiness), tear down the
    // process and temp directory before rethrowing so nothing leaks.
    try {
      this.userDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'nrdapr-'));

      // Make the workspace package discoverable via userDir/node_modules, the
      // way an installed contrib package is found. A symlink (not a copy) keeps
      // its requires resolving against the workspace's own node_modules.
      const nodeModules = path.join(this.userDir, 'node_modules');
      this._linkPath = path.join(nodeModules, PKG.name);
      // mkdir the link's PARENT, not just node_modules: a scoped package name
      // ("@scope/pkg") puts the link one level deeper, and symlink() does not
      // create intermediate directories — it fails ENOENT without the scope dir.
      await fsp.mkdir(path.dirname(this._linkPath), { recursive: true });
      await fsp.symlink(WORKSPACE, this._linkPath, 'dir');

      await fsp.writeFile(
        path.join(this.userDir, 'settings.js'),
        settingsSource(this.port, { loggingExtra, startupFlowLoadDelayMs })
      );
      await fsp.writeFile(path.join(this.userDir, 'flows.json'), JSON.stringify(flows));

      // Run red.js with our own Node binary so the runtime uses the same version
      // the suite runs under, regardless of PATH.
      this.proc = spawn(
        process.execPath,
        [
          NODE_RED_BIN,
          '--userDir',
          this.userDir,
          '--settings',
          path.join(this.userDir, 'settings.js'),
        ],
        { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, ...env } }
      );
      this.proc.stdout.on('data', (d) => this._appendLog(d.toString()));
      this.proc.stderr.on('data', (d) => this._appendLog(d.toString()));
      this._exited = new Promise((resolve) =>
        this.proc.once('exit', (code, signal) => resolve({ code, signal }))
      );

      await this._waitReady(readyTimeoutMs);
    } catch (err) {
      await this.stop().catch(() => {});
      throw err;
    }
    return this;
  }

  // Ready means the runtime has STARTED its own flows, not merely that the
  // admin port is listening. red.js calls server.listen() when RED.start()
  // resolves, but runtime start() resolves as soon as the context plugin
  // loads and leaves loadFlows()/startFlows() running detached (see
  // @node-red/runtime/lib/index.js) — so /settings answers while the startup
  // flow load is still in flight.
  //
  // A deploy posted into that window is silently lost: setFlows() sees its own
  // `started` flag false, so it saves the flows and returns 200 WITHOUT
  // starting them, and the in-flight startup load then overwrites
  // activeFlowConfig with the config it read from disk before that POST. No
  // node is ever created, so every node-served route 404s for the rest of the
  // process's life and the test fails as a route-never-appeared timeout that
  // names nothing. The harness regression reproduces this deterministically
  // with a storage adapter that pauses after snapshotting the startup flow.
  //
  // Node-RED writes "Started flows" only after startFlows() completes. The log
  // stream's data event wakes waitForLog directly, so readiness neither polls
  // an endpoint nor mistakes an open admin socket for a running flow runtime.
  async _waitReady(timeoutMs) {
    try {
      await this.waitForLog('Started flows', { timeoutMs });
    } catch (err) {
      if (this.proc.exitCode !== null || this.proc.signalCode !== null) {
        throw new Error(`Node-RED exited before becoming ready.\n--- logs ---\n${this.logText()}`, {
          cause: err,
        });
      }
      throw new Error(
        `Node-RED did not become ready within ${timeoutMs}ms.\n--- logs ---\n${this.logText()}`,
        { cause: err }
      );
    }
  }

  // Full-deploy a flow set via the documented Admin API (v2). Resolves once the
  // runtime has applied and started the flows.
  async deploy(flows, { timeoutMs = 30000, deploymentType = 'full' } = {}) {
    const res = await httpRequest(this.adminUrl('/flows'), {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'Node-RED-API-Version': 'v2',
        'Node-RED-Deployment-Type': deploymentType,
      },
      body: JSON.stringify({ flows }),
      timeoutMs,
    });
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`POST /flows failed: ${res.status} ${res.text}`);
    }
    return JSON.parse(res.text);
  }

  // Poll a node-served route until `until` is satisfied. A route becomes live a
  // short, non-deterministic moment after a deploy (and during a full redeploy
  // there is a teardown gap where it 404s), so presence must be awaited, not
  // assumed. The body is read once and exposed as { status, headers, text,
  // json() } so `until` can inspect it without consuming the response. Tests
  // asserting a route is ABSENT should make a single request, not use this.
  async waitForHttp(
    pathname,
    { method = 'GET', headers, body, until, timeoutMs = 10000, intervalMs = 100 } = {}
  ) {
    const check = until || ((r) => r.status >= 200 && r.status < 300);
    const deadline = Date.now() + timeoutMs;
    let last;
    while (Date.now() < deadline) {
      try {
        const attemptMs = Math.min(deadline - Date.now(), 2000);
        const res = await httpRequest(this.nodeUrl(pathname), {
          method,
          headers,
          body,
          timeoutMs: attemptMs,
        });
        const result = {
          status: res.status,
          headers: res.headers,
          text: res.text,
          json: () => JSON.parse(res.text),
        };
        last = result;
        if (check(result)) {
          return result;
        }
      } catch (err) {
        last = err;
      }
      await delay(intervalMs);
    }
    const detail = last instanceof Error ? last.message : `last status ${last && last.status}`;
    throw new Error(
      `waitForHttp(${method} ${pathname}) not satisfied within ${timeoutMs}ms (${detail})`
    );
  }

  // SIGINT for a graceful stop, escalating to SIGKILL only after a bounded wait,
  // then remove the temporary directory (unlinking the package symlink first so
  // the workspace it points to is never touched).
  async stop({ timeoutMs = 10000 } = {}) {
    if (this.proc && this._exited) {
      this.proc.kill('SIGINT');
      // Race the exit against a deadline, then CLEAR the timer: Promise.race
      // does not cancel the loser, so an abandoned setTimeout would keep the
      // event loop alive for the full timeout after a fast exit.
      let killTimer;
      const deadline = new Promise((resolve) => {
        killTimer = setTimeout(() => resolve('timeout'), timeoutMs);
      });
      const outcome = await Promise.race([this._exited.then(() => 'exited'), deadline]);
      clearTimeout(killTimer);
      if (outcome === 'timeout') {
        this.proc.kill('SIGKILL');
        await this._exited;
      }
    }
    if (this._linkPath) {
      await fsp.unlink(this._linkPath).catch(() => {});
      this._linkPath = null;
    }
    if (this.userDir) {
      await fsp.rm(this.userDir, { recursive: true, force: true });
      this.userDir = null;
    }
  }
}

module.exports = { NodeRed, freePort, waitForLogEvent };
