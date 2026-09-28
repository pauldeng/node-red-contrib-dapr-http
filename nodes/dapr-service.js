'use strict';

const { setImmediate: nextTurn } = require('node:timers/promises');

const crypto = require('node:crypto');

const { context, propagation, trace, SpanKind } = require('@opentelemetry/api');

const { buildService } = require('../lib/services');
const { decodeBody } = require('../lib/invoke-client');
const { HOP_BY_HOP } = require('../lib/http-headers');
const { getTracer, endSpan } = require('../lib/telemetry');

// Transport and sensitive headers not exposed to flows as request metadata: the
// full hop-by-hop/framing set (so proxy-authorization, te, trailer, upgrade,
// etc. never leak into a flow) plus the sidecar API token.
const EXCLUDED_HEADERS = new Set([...HOP_BY_HOP, 'dapr-api-token']);

module.exports = function registerDaprService(RED) {
  function DaprServiceNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const connection = RED.nodes.getNode(config.connection);

    if (!connection?.options || typeof connection.registerService !== 'function') {
      node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
      return;
    }

    let definition;
    try {
      definition = buildService({ nodeId: node.id, verb: config.verb, path: config.methodPath });
    } catch (err) {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(err.message);
      return;
    }

    // Resolve just before the app channel's request deadline so a slow flow
    // yields a clean 504 rather than the 503 backstop.
    const responseTimeoutMs = Math.max(1, connection.options.limits.requestTimeoutMs - 250);

    let pending = 0;
    const activeIds = new Set(); // response ids this node still owns
    const showStatus = () => {
      node.status(
        pending > 0
          ? { fill: 'blue', shape: 'dot', text: `pending ${pending}` }
          : { fill: 'green', shape: 'dot', text: 'listening' }
      );
    };

    const handler = async (ctx) => {
      const responseId = crypto.randomUUID();
      let responseResult;
      try {
        responseResult = connection.addPendingResponse(responseId, {
          timeoutMs: responseTimeoutMs,
          onTimeout: () => ({ status: 504, headers: {}, body: 'service response timed out' }),
        });
      } catch {
        return { status: 503, body: 'service overloaded' };
      }

      const headers = {};
      for (const [key, value] of Object.entries(ctx.headers)) {
        if (typeof value === 'string' && !EXCLUDED_HEADERS.has(key)) {
          headers[key] = value;
        }
      }

      pending += 1;
      activeIds.add(responseId);
      showStatus();

      // If the caller disconnects before the flow replies, the app channel
      // aborts ctx.signal. Settle the correlation immediately so it does not sit
      // pending until the response timeout — repeated disconnects would otherwise
      // exhaust the pending-request budget and 503 valid callers. The discarded
      // response is never written (the app channel already finished the request).
      const onAbort = () => {
        connection.settleResponse(responseId, { status: 0, headers: {}, body: undefined });
      };
      if (ctx.signal) {
        if (ctx.signal.aborted) {
          onAbort();
        } else {
          ctx.signal.addEventListener('abort', onAbort, { once: true });
        }
      }

      // A no-op extract/span (tracing disabled) costs nothing and needs no
      // branch of its own here.
      const parentContext = propagation.extract(context.active(), ctx.headers);
      const span = getTracer().startSpan(
        `${definition.verb} ${definition.path}`,
        {
          kind: SpanKind.SERVER,
          attributes: {
            'rpc.system': 'dapr',
            'rpc.method': definition.path,
            'http.request.method': definition.verb,
            ...(ctx.callerAppId ? { 'dapr.caller_app_id': ctx.callerAppId } : {}),
          },
        },
        parentContext
      );

      context.with(trace.setSpan(parentContext, span), () =>
        node.send({
          _msgid: RED.util.generateId(),
          payload: decodeBody(ctx.body, ctx.headers['content-type']),
          dapr: {
            method: definition.path,
            verb: definition.verb,
            query: ctx.query,
            headers,
            callerAppId: ctx.callerAppId,
            responseId,
          },
        })
      );

      const response = await responseResult;
      if (ctx.signal) {
        ctx.signal.removeEventListener('abort', onAbort);
      }
      // A 5xx is this service's own failure to answer the request; 4xx is a
      // valid RPC outcome (the caller's request was rejected, not mishandled)
      // and does not mark the span an error, per common RPC/HTTP conventions.
      span.setAttribute('http.response.status_code', response.status);
      endSpan(
        span,
        response.status >= 500
          ? new Error(`service responded with status ${response.status}`)
          : undefined
      );
      activeIds.delete(responseId);
      pending -= 1;
      showStatus();
      return response;
    };

    let unregister;
    try {
      unregister = connection.registerService(definition, handler);
    } catch (err) {
      node.status({ fill: 'red', shape: 'ring', text: 'duplicate method' });
      node.error(err.message);
      return;
    }
    showStatus();

    node.on('close', async (_removed, done) => {
      // On a modified-node ("nodes") redeploy the connection stays up, so it does
      // not drain our correlations. Settle them as 503 ourselves — otherwise each
      // in-flight caller would hang until the response timeout (a stale 504).
      unregister();
      for (const id of activeIds) {
        connection.settleResponse(id, {
          status: 503,
          headers: {},
          body: 'service restarting',
        });
      }
      activeIds.clear();
      if (pending > 0) {
        // Let the settled handlers write their 503 before the node tears down.
        await nextTurn();
      }
      done();
    });
  }

  RED.nodes.registerType('dapr-service', DaprServiceNode);
};
