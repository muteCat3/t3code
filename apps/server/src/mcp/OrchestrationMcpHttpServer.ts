import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Types from "effect/Types";
import { McpProtocol, McpServer } from "effect/unstable/ai";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import packageJson from "../../package.json" with { type: "json" };
import * as OrchestrationMcpInvocationContext from "./OrchestrationMcpInvocationContext.ts";
import * as OrchestrationMcpSessionRegistry from "./OrchestrationMcpSessionRegistry.ts";

const unauthorized = HttpServerResponse.jsonUnsafe(
  {
    error: "invalid_orchestration_mcp_credential",
    message: "A valid orchestration-scoped MCP bearer credential is required.",
  },
  {
    status: 401,
    headers: {
      "cache-control": "no-store",
      "www-authenticate": "Bearer",
    },
  },
);

type AuthenticatedHttpEffect = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  Types.unhandled,
  OrchestrationMcpInvocationContext.OrchestrationMcpInvocationContext
>;

type OrchestrationMcpAuthMiddleware = (
  httpEffect: AuthenticatedHttpEffect,
) => Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  Types.unhandled,
  HttpServerRequest.HttpServerRequest
>;

const makeAuthMiddleware = OrchestrationMcpSessionRegistry.OrchestrationMcpSessionRegistry.pipe(
  Effect.map(
    (registry): OrchestrationMcpAuthMiddleware =>
      Effect.fn("OrchestrationMcpHttpServer.authenticateRequest")(function* (httpEffect) {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const authorization = request.headers.authorization;
        const token =
          authorization?.startsWith("Bearer ") === true
            ? authorization.slice("Bearer ".length).trim()
            : "";
        const invocation = yield* registry.resolve(token);
        if (!invocation) {
          yield* Effect.logWarning("rejected orchestration MCP request", {
            reason:
              token.length === 0 ? "missing_bearer_token" : "unknown_revoked_or_ineligible_token",
          });
          return unauthorized;
        }
        return yield* httpEffect.pipe(
          Effect.provideService(
            OrchestrationMcpInvocationContext.OrchestrationMcpInvocationContext,
            invocation,
          ),
          Effect.map((response) => {
            const bodyIsEmpty =
              response.body._tag === "Empty" ||
              (response.body._tag === "Uint8Array" && response.body.contentLength === 0) ||
              (response.body._tag === "Raw" && response.body.contentLength === 0);
            return response.status === 200 && bodyIsEmpty
              ? HttpServerResponse.setStatus(response, 202)
              : response;
          }),
        );
      }),
  ),
  Effect.withSpan("OrchestrationMcpHttpServer.makeAuthMiddleware"),
);

export const OrchestrationMcpAuthMiddlewareLive = HttpRouter.middleware<{
  provides: OrchestrationMcpInvocationContext.OrchestrationMcpInvocationContext;
}>()(makeAuthMiddleware).layer;

/**
 * Transport-only layer. Agent orchestration owns its toolkit registration and
 * combines that registration with this layer just as preview combines its
 * toolkit with the independent `/mcp` transport.
 */
export const OrchestrationMcpTransportLive = McpServer.layerHttp({
  name: "t3-orchestration",
  version: packageJson.version,
  path: "/mcp/orchestration",
  protocols: [McpProtocol.v2025_06_18],
}).pipe(Layer.provide(OrchestrationMcpAuthMiddlewareLive));
