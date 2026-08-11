'use strict';

// Spins up the pinned OpenTelemetry Collector contrib image for the
// integration tier: an OTLP/HTTP receiver plus a file exporter writing to a
// host-mounted temp directory, so a test can assert on exported spans by
// reading and parsing that file directly — no tracing backend, no console
// log scraping.

const crypto = require('node:crypto');
const path = require('node:path');
const os = require('node:os');
const fsp = require('node:fs/promises');
const { execFile } = require('node:child_process');

const { httpRequest } = require('./http');
const { freePort } = require('./node-red');
const { execFileP, ensureImage } = require('./docker');

// Pinned by digest — see test/helpers/integration.js for the re-pin procedure.
const OTEL_COLLECTOR_IMAGE =
  'otel/opentelemetry-collector-contrib:0.158.0@sha256:c5918f78992ee73b0d6f0e599423ac5ec52dd5d9726733114d6eca53d5a32ed5';

function runId() {
  return crypto.randomBytes(4).toString('hex');
}

async function dockerStop(name) {
  try {
    await execFileP('docker', ['stop', '--time', '5', name]);
  } catch {
    // Best effort cleanup.
  }
  try {
    await execFileP('docker', ['rm', '-f', name]);
  } catch {
    // Best effort cleanup.
  }
}

// Not execFileP: this collector's own logger (like many Go/zap-based CLIs)
// writes to stderr, and execFileP's resolved value is stdout only (correct
// for its other callers here, e.g. a container id on `docker run`, where
// merging stderr would pollute that value) — so a diagnostic-only combined
// read is its own small function rather than a shared-helper behavior change.
function dockerLogs(name) {
  return new Promise((resolve) => {
    execFile('docker', ['logs', name], { timeout: 15000 }, (err, stdout, stderr) => {
      resolve(err ? `${err.message}\n${stderr}` : `${stdout}${stderr}`);
    });
  });
}

function collectorConfigYaml() {
  return `receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
processors:
  batch:
exporters:
  file/traces:
    path: /output/spans.jsonl
    rotation:
      max_megabytes: 1
      max_backups: 1
  file/logs:
    path: /output/logs.jsonl
    rotation:
      max_megabytes: 1
      max_backups: 1
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/traces]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/logs]
`;
}

// Starts a fresh collector container on a dynamically allocated host port
// (or the given fixed one — see below), with its file exporter's output
// directory bind-mounted so the caller can read it directly. Resolves once
// the collector's own health check answers.
//
// `port`, when given, is reused as-is instead of allocating a new one: a
// test proving telemetry export resumes after a collector restart needs the
// SAME already-running Node-RED process (whose OTEL_EXPORTER_OTLP_ENDPOINT
// env var was fixed at its own startup) to reach the replacement collector
// without itself being restarted.
async function startOtelCollector({ port: fixedPort } = {}) {
  await ensureImage(OTEL_COLLECTOR_IMAGE);
  const port = fixedPort ?? (await freePort());
  const name = `nrdapr-it-otelcol-${runId()}`;
  const configDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'nrdapr-otelcol-config-'));
  const outputDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'nrdapr-otelcol-output-'));
  // The collector image runs as a non-root, unrelated container UID; open
  // both mounts up so it can read its config and write its output.
  await fsp.chmod(configDir, 0o755);
  await fsp.chmod(outputDir, 0o777);
  const configPath = path.join(configDir, 'config.yaml');
  await fsp.writeFile(configPath, collectorConfigYaml());
  await fsp.chmod(configPath, 0o644);

  await execFileP('docker', [
    'run',
    '-d',
    '--name',
    name,
    '-p',
    `${port}:4318`,
    '-v',
    `${configPath}:/etc/otelcol-contrib/config.yaml:ro`,
    '-v',
    `${outputDir}:/output`,
    OTEL_COLLECTOR_IMAGE,
  ]);

  const outputPath = path.join(outputDir, 'spans.jsonl');
  const logsOutputPath = path.join(outputDir, 'logs.jsonl');
  const deadline = Date.now() + 20000;
  let ready = false;
  while (Date.now() < deadline && !ready) {
    try {
      // The OTLP/HTTP receiver answers 405 on a bare GET (POST-only), which
      // is enough to prove the collector is up and listening.
      const res = await httpRequest(`http://127.0.0.1:${port}/v1/traces`, { timeoutMs: 1000 });
      ready = res.status === 405 || res.status === 400;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }
  if (!ready) {
    const logs = await dockerLogs(name);
    await dockerStop(name);
    await fsp.rm(configDir, { recursive: true, force: true });
    await fsp.rm(outputDir, { recursive: true, force: true });
    throw new Error(`otel collector did not become ready: ${logs}`);
  }

  // Parses one file exporter's newline-delimited OTLP JSON output (one JSON
  // object per export batch) into a flat list of the leaf records `extract`
  // pulls from each resource/scope wrapper. A batch mid-write at read time is
  // read on the NEXT poll instead, so a partial-line parse failure here is
  // swallowed rather than thrown.
  async function readJsonlRecords(filePath, extract) {
    let text;
    try {
      text = await fsp.readFile(filePath, 'utf8');
    } catch {
      return [];
    }
    const records = [];
    for (const line of text.split('\n')) {
      if (!line.trim()) {
        continue;
      }
      let doc;
      try {
        doc = JSON.parse(line);
      } catch {
        continue;
      }
      records.push(...extract(doc));
    }
    return records;
  }

  return {
    port,
    name,
    // Every span the file exporter has flushed so far, parsed from its OTLP
    // JSON representation.
    readSpans: () =>
      readJsonlRecords(outputPath, (doc) =>
        (doc.resourceSpans || []).flatMap((rs) =>
          (rs.scopeSpans || []).flatMap((ss) => ss.spans || [])
        )
      ),
    // Every log record the file exporter has flushed so far.
    readLogRecords: () =>
      readJsonlRecords(logsOutputPath, (doc) =>
        (doc.resourceLogs || []).flatMap((rl) =>
          (rl.scopeLogs || []).flatMap((sl) => sl.logRecords || [])
        )
      ),
    dockerLogs: () => dockerLogs(name),
    stop: async () => {
      await dockerStop(name);
      await fsp.rm(configDir, { recursive: true, force: true });
      await fsp.rm(outputDir, { recursive: true, force: true });
    },
  };
}

module.exports = { startOtelCollector, OTEL_COLLECTOR_IMAGE };
