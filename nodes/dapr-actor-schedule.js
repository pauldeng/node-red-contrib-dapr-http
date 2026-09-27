'use strict';

const { context, propagation, trace, SpanKind } = require('@opentelemetry/api');

const { setReminder, getReminder, deleteReminder } = require('../lib/actor-client');
const {
  validateActorSegment,
  validateScheduleTime,
  validateOverwrite,
  prepareReminderData,
} = require('../lib/actor-messages');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { requireConnection, openSidecarSession } = require('../lib/sidecar-session');
const { getTracer, endSpan } = require('../lib/telemetry');

const OPERATIONS = new Set(['set', 'get', 'delete']);

// Sets, gets, or deletes one actor reminder through the local sidecar
// (lib/actor-client.js's setReminder/getReminder/deleteReminder). Timers are
// not supported by this package; this node only ever talks to the reminders
// endpoint. Mirrors dapr-actor-call.js's per-message override pattern
// (msg.dapr.actorSchedule, each property PRESENT overriding the configured
// value; a present-but-invalid value fails rather than falling back) and
// dapr-state.js's operation-selector shape.
module.exports = function registerDaprActorSchedule(RED) {
  function DaprActorScheduleNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);
    if (!requireConnection(node, connection)) {
      return;
    }
    const session = openSidecarSession(node, connection);

    node.on('input', async (msg, send, done) => {
      if (!(await session.isReady())) {
        done(new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'Dapr sidecar is unavailable'));
        return;
      }

      const dapr = msg.dapr !== null && typeof msg.dapr === 'object' ? msg.dapr : {};
      const override =
        dapr.actorSchedule !== null && typeof dapr.actorSchedule === 'object'
          ? dapr.actorSchedule
          : {};
      const pick = (key, configValue) =>
        Object.hasOwn(override, key) ? override[key] : configValue;

      let operation;
      let request;
      try {
        operation = pick('operation', config.operation);
        if (typeof operation !== 'string' || !OPERATIONS.has(operation)) {
          throw new DaprError(
            ErrorCodes.INVALID_MESSAGE,
            `operation must be one of ${[...OPERATIONS].join(', ')}`
          );
        }
        const actorType = validateActorSegment(pick('actorType', config.actorType), 'actorType');
        const actorId = validateActorSegment(pick('actorId', config.actorId), 'actorId');
        const name = validateActorSegment(
          pick('scheduleName', config.scheduleName),
          'scheduleName'
        );

        if (operation === 'set') {
          const dueTime = validateScheduleTime(pick('dueTime', config.dueTime), 'dueTime');
          const period = validateScheduleTime(pick('period', config.period), 'period');
          const ttl = validateScheduleTime(pick('ttl', config.ttl), 'ttl');
          const overwrite = validateOverwrite(pick('overwrite', config.overwrite));
          const hasData = msg.payload !== undefined;
          const { data } = prepareReminderData(
            msg.payload,
            hasData,
            connection.options.limits.bodyLimitBytes
          );
          request = { actorType, actorId, name, dueTime, period, ttl, overwrite, hasData, data };
        } else {
          request = { actorType, actorId, name };
        }
      } catch (err) {
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.INVALID_MESSAGE, err.message, { cause: err })
        );
        return;
      }

      // With tracing disabled the provider returns a no-op span; no branch of
      // its own here, mirroring nodes/dapr-invoke.js's own client span. One
      // span per invocation regardless of which operation ran.
      const span = getTracer().startSpan(
        `actor ${request.actorType}.${operation}`,
        {
          kind: SpanKind.CLIENT,
          attributes: {
            'rpc.system': 'dapr',
            'dapr.actor.type': request.actorType,
            'dapr.actor.operation': operation,
          },
        },
        context.active()
      );
      const headers = {};
      propagation.inject(trace.setSpan(context.active(), span), headers);

      try {
        if (operation === 'set') {
          await session.call((transportOptions) =>
            setReminder({ ...transportOptions, headers }, request)
          );
          endSpan(span);
          node.status({ fill: 'green', shape: 'dot', text: 'set' });
          send(msg);
          done();
        } else if (operation === 'get') {
          const result = await session.call((transportOptions) =>
            getReminder({ ...transportOptions, headers }, request)
          );
          endSpan(span);
          msg.payload = result.found ? result.reminder : null;
          node.status({ fill: 'green', shape: 'dot', text: 'get' });
          send(msg);
          done();
        } else {
          await session.call((transportOptions) =>
            deleteReminder({ ...transportOptions, headers }, request)
          );
          endSpan(span);
          node.status({ fill: 'green', shape: 'dot', text: 'delete' });
          send(msg);
          done();
        }
      } catch (err) {
        endSpan(span, err);
        node.status({ fill: 'red', shape: 'ring', text: 'failed' });
        done(
          err instanceof DaprError
            ? err
            : new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, err.message, { cause: err })
        );
      }
    });
  }

  RED.nodes.registerType('dapr-actor-schedule', DaprActorScheduleNode);
};
