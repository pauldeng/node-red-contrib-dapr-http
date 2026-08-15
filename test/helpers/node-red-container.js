'use strict';

// Same public surface as NodeRed (test/helpers/node-red.js) — start/deploy/
// waitForHttp/stop/logText/nodeUrl — but drives the pinned real
// nodered/node-red container image instead of a host child process, so the
// integration tier runs Node-RED from a pinned image rather than the host's
// installed copy. The runtime tier is untouched and keeps using the
// host-process NodeRed class.
//
// Networking: runs with --network host, exactly like daprd in
// test/helpers/integration.js — both need to reach 127.0.0.1:<port> on the
// host's own network namespace (the app-channel's loopback-only default),
// so sharing the host network is the simplest way to keep that reachable
// without container-to-container DNS/bridge networking.
//
// Package discovery: the image's NODE_PATH includes /data/node_modules
// (where /data is this run's bind-mounted userDir). A symlink there, pointing
// at this workspace's absolute host path, resolves inside the container only
// because the workspace itself is ALSO bind-mounted at that same absolute
// path — the same symlink-based discovery the host-process harness uses,
// just resolved across a container boundary instead of a plain filesystem.
//
// Signal handling: `docker stop` sends SIGTERM. The image's entrypoint.sh
// traps both SIGINT and SIGTERM and always forwards a bare `kill` (SIGTERM)
// to the node-red child either way, and Node-RED's runtime treats SIGTERM the
// same as SIGINT for graceful shutdown — so `docker stop --time <n>` reaches
// the app's own close handlers, then escalates to SIGKILL automatically after
// the timeout, mirroring NodeRed.stop()'s manual SIGINT-then-SIGKILL logic.

const crypto = require('node:crypto');
const path = require('node:path');
const os = require('node:os');
const fsp = require('node:fs/promises');
const { spawn } = require('node:child_process');

const { httpRequest } = require('./http');
const { freePort } = require('./node-red');
const { execFileP, ensureImage } = require('./docker');
const { setTimeout: delay } = require('node:timers/promises');

const WORKSPACE = path.resolve(__dirname, '..', '..');
const PKG = require(path.join(WORKSPACE, 'package.json'));

// Pinned by digest — see test/helpers/integration.js for the re-pin procedure.
const NODE_RED_IMAGE =
  'nodered/node-red:5.0.1-24@sha256:6cb1b27fa5a83deec6a662db62eec8bb32e55ac5412d6b7a653e874ce62055d5';

function runId() {
  return crypto.randomBytes(4).toString('hex');
}

function settingsSource(uiPort, { loggingExtra = '' } = {}) {
  // loggingExtra: see test/helpers/node-red.js's own settingsSource for why
  // this is a raw source fragment rather than a serializable value.
  return `module.exports = {
  uiPort: ${uiPort},
  httpAdminRoot: '/',
  httpNodeRoot: '/',
  flowFile: 'flows.json',
  logging: { console: { level: 'info', metrics: false, audit: false }${loggingExtra} },
  editorTheme: { projects: { enabled: false }, tours: false },
  functionGlobalContext: {},
};
`;
}

class ContainerNodeRed {
  constructor() {
    this.userDir = null;
    this.name = null;
    this.port = null;
    this._logProc = null;
    this._logs = [];
  }

  adminUrl(p = '/') {
    return `http://127.0.0.1:${this.port}${p}`;
  }

  // httpNodeRoot is '/', so node-served routes share the admin origin.
  nodeUrl(p = '/') {
    return `http://127.0.0.1:${this.port}${p}`;
  }

  logText() {
    return this._logs.join('');
  }

  async start({ flows = [], readyTimeoutMs = 30000, env = {}, loggingExtra = '' } = {}) {
    await ensureImage(NODE_RED_IMAGE);
    this.port = await freePort();
    this.name = `nrdapr-it-nodered-${runId()}`;
    try {
      this.userDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'nrdapr-container-'));
      // The container's node-red user is a different uid than whatever
      // created this directory; open it fully so the container can write its
      // own runtime state into it. Cleanup stays best-effort because a
      // GitHub runner's host uid may still differ from files the image wrote.
      await fsp.chmod(this.userDir, 0o777);

      const nodeModules = path.join(this.userDir, 'node_modules');
      const linkPath = path.join(nodeModules, PKG.name);
      // mkdir the link's PARENT, not just node_modules: a scoped package name
      // ("@scope/pkg") puts the link one level deeper, and symlink() does not
      // create intermediate directories — it fails ENOENT without the scope dir.
      await fsp.mkdir(path.dirname(linkPath), { recursive: true });
      await fsp.symlink(WORKSPACE, linkPath, 'dir');

      await fsp.writeFile(
        path.join(this.userDir, 'settings.js'),
        settingsSource(this.port, { loggingExtra })
      );
      await fsp.writeFile(path.join(this.userDir, 'flows.json'), JSON.stringify(flows));
      await fsp.chmod(path.join(this.userDir, 'settings.js'), 0o666);
      await fsp.chmod(path.join(this.userDir, 'flows.json'), 0o666);

      const envArgs = Object.entries(env).flatMap(([k, v]) => ['-e', `${k}=${v}`]);
      await execFileP('docker', [
        'run',
        '-d',
        '--name',
        this.name,
        '--network',
        'host',
        '-v',
        `${this.userDir}:/data`,
        '-v',
        `${WORKSPACE}:${WORKSPACE}:ro`,
        ...envArgs,
        NODE_RED_IMAGE,
      ]);

      this._startLogStream();
      await this._waitReady(readyTimeoutMs);
    } catch (err) {
      await this.stop().catch(() => {});
      throw err;
    }
    return this;
  }

  _startLogStream() {
    this._logProc = spawn('docker', ['logs', '-f', this.name], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    this._logProc.stdout.on('data', (d) => this._logs.push(d.toString()));
    this._logProc.stderr.on('data', (d) => this._logs.push(d.toString()));
  }

  async _containerStatus() {
    return execFileP('docker', ['inspect', '--format', '{{.State.Status}}', this.name]);
  }

  async _waitReady(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const status = await this._containerStatus().catch(() => null);
      if (status && status !== 'running') {
        throw new Error(
          `Node-RED container exited before becoming ready (state: ${status}).\n--- logs ---\n${this.logText()}`
        );
      }
      try {
        const res = await httpRequest(this.adminUrl('/settings'), {
          timeoutMs: Math.min(deadline - Date.now(), 2000),
        });
        if (res.status >= 200 && res.status < 300) {
          return;
        }
      } catch {
        // not listening yet
      }
      await delay(200);
    }
    throw new Error(
      `Node-RED container did not become ready within ${timeoutMs}ms.\n--- logs ---\n${this.logText()}`
    );
  }

  // Full-deploy a flow set via the documented Admin API (v2) — identical to
  // NodeRed.deploy(); it's a plain HTTP call regardless of what's on the
  // other end.
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

  async stop({ timeoutMs = 10000 } = {}) {
    if (this.name) {
      await execFileP(
        'docker',
        ['stop', '--time', String(Math.ceil(timeoutMs / 1000)), this.name],
        { timeout: timeoutMs + 10000 }
      ).catch(() => {});
      await execFileP('docker', ['rm', '-f', this.name]).catch(() => {});
      this.name = null;
    }
    if (this._logProc) {
      this._logProc.kill();
      this._logProc = null;
    }
    if (this.userDir) {
      await fsp.rm(this.userDir, { recursive: true, force: true }).catch(() => {});
      this.userDir = null;
    }
  }
}

module.exports = { ContainerNodeRed, NODE_RED_IMAGE };
