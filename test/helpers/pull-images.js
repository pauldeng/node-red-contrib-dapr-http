'use strict';

// Pre-pulls every pinned image the integration tier uses. A cold pull can
// take minutes (test/helpers/docker.js's own PULL_TIMEOUT_MS budgets 5), but
// almost every integration test has a much shorter overall timeout that
// includes its own setup — if the first test to need an image pays for its
// pull, it can time out before the pull even finishes. Run once, up front,
// outside any single test's clock. ensureImage() is idempotent: a second run
// with images already cached just inspects and returns.

const { ensureImage } = require('./docker');
const { DAPRD_IMAGE, REDIS_IMAGE, PLACEMENT_IMAGE } = require('./integration');
const { NODE_RED_IMAGE } = require('./node-red-container');
const { NATS_IMAGE } = require('./nats');
const { OTEL_COLLECTOR_IMAGE } = require('./otel-collector');

async function main() {
  for (const image of [
    DAPRD_IMAGE,
    REDIS_IMAGE,
    PLACEMENT_IMAGE,
    NODE_RED_IMAGE,
    NATS_IMAGE,
    OTEL_COLLECTOR_IMAGE,
  ]) {
    process.stdout.write(`ensuring ${image}...\n`);
    await ensureImage(image);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
