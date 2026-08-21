'use strict';

const ACK_STATUSES = new Set(['SUCCESS', 'RETRY', 'DROP']);

function resolveAckStatus(msg, config = {}) {
  let requested;
  let field;

  if (config.ackStatusSource === 'message') {
    requested = msg && msg.ackStatus;
    field = 'msg.ackStatus';
  } else if (config.ackStatusSource === 'fixed') {
    requested = config.ackStatus;
    field = "the node's Ack status";
  } else if (config.ackStatusSource === undefined) {
    // 0.2 compatibility: keep old flows running until the node is edited into
    // one of the explicit modes. A status badge object is not configuration.
    requested =
      msg && msg.dapr && msg.dapr.status !== undefined
        ? msg.dapr.status
        : typeof config.ackStatus === 'string'
          ? config.ackStatus
          : typeof config.status === 'string'
            ? config.status
            : 'SUCCESS';
    field = 'legacy ack status';
  } else {
    return { error: `invalid ack status source: ${config.ackStatusSource}` };
  }

  if (requested === undefined || requested === null || requested === '') {
    return { error: `${field} is required` };
  }
  if (typeof requested !== 'string') {
    return { error: `ack status must be a string, got ${typeof requested}` };
  }
  const status = requested.toUpperCase();
  if (!ACK_STATUSES.has(status)) {
    return { error: `invalid ack status: ${requested}` };
  }
  return { status };
}

module.exports = { resolveAckStatus };
