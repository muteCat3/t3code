import { expect, it } from "@effect/vitest";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, PreviewTabId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { McpProtocol, McpSchema, McpServer } from "effect/unstable/ai";
import { HttpBody, HttpClient, HttpRouter, HttpServerResponse } from "effect/unstable/http";

import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import * as OrchestrationMcpInvocationContext from "./OrchestrationMcpInvocationContext.ts";
import * as OrchestrationMcpHttpServer from "./OrchestrationMcpHttpServer.ts";
import * as OrchestrationMcpSessionRegistry from "./OrchestrationMcpSessionRegistry.ts";
import { AgentOrchestrationService } from "../agentOrchestration/AgentOrchestrationService.ts";
import { AgentToolkit } from "./toolkits/agents/tools.ts";
import { AgentToolkitHandlersLive } from "./toolkits/agents/handlers.ts";
import * as PreviewAutomationBroker from "./PreviewAutomationBroker.ts";

const environmentId = EnvironmentId.make("environment-mcp-test");
const threadId = ThreadId.make("thread-mcp-test");
const tabId = PreviewTabId.make("tab-mcp-test");
const alternateTabId = PreviewTabId.make("tab-mcp-alternate");
const invocation = {
  environmentId,
  threadId,
  providerSessionId: "provider-session-mcp-test",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["preview"] as const),
  issuedAt: 1,
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "mcp-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});
const TestLayer = McpHttpServer.PreviewToolkitRegistrationLive.pipe(
  Layer.provideMerge(McpServer.McpServer.layer),
  Layer.provideMerge(PreviewAutomationBroker.layer.pipe(Layer.provide(NodeServices.layer))),
);

it("normalizes empty successful notification responses to accepted", () => {
  const notificationResponse = McpHttpServer.normalizeMcpHttpResponse(
    HttpServerResponse.text("", { status: 200, contentType: "application/json" }),
  );
  expect(notificationResponse.status).toBe(202);

  const resultResponse = McpHttpServer.normalizeMcpHttpResponse(
    HttpServerResponse.jsonUnsafe({ jsonrpc: "2.0", id: 1, result: {} }),
  );
  expect(resultResponse.status).toBe(200);
});

it.effect("returns bounded structural preview snapshot failures", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
      const events = yield* broker.connect({
        clientId: "mcp-failure-client",
        environmentId,
      });
      yield* Stream.runForEach(events, (event) =>
        event.type === "connected"
          ? Effect.void
          : broker.respond({
              clientId: "mcp-failure-client",
              connectionId: event.connectionId,
              requestId: event.request.requestId,
              ok: false,
              error: {
                _tag: "PreviewAutomationExecutionError",
                message: "sensitive renderer failure",
                detail: { consoleOutput: "sensitive browser output" },
              },
            }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const snapshot = yield* server
        .callTool({ name: "preview_snapshot", arguments: {} })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provideService(McpSchema.McpServerClient, client),
        );

      expect(snapshot.isError).toBe(true);
      expect(snapshot.content).toEqual([{ type: "text", text: "Preview snapshot failed." }]);
      expect(snapshot.structuredContent).toEqual({
        error: {
          _tag: "PreviewAutomationExecutionError",
          operation: "snapshot",
          failureCount: 1,
        },
      });
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect("terminates HTTP MCP sessions with DELETE", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const serverLayer = McpServer.layerHttp({
        name: "MCP termination test",
        version: "1.0.0",
        path: "/mcp",
        protocols: [McpProtocol.v2025_06_18],
      });
      yield* HttpRouter.serve(serverLayer, {
        disableListenLog: true,
        disableLogger: true,
      }).pipe(Layer.build);
      const httpClient = yield* HttpClient.HttpClient;

      const initializeResponse = yield* httpClient.post("/mcp", {
        headers: { accept: "application/json, text/event-stream" },
        body: HttpBody.text(
          `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"mcp-test","version":"1.0.0"}}}`,
          "application/json",
        ),
      });
      const sessionId = initializeResponse.headers["mcp-session-id"];
      expect(initializeResponse.status).toBe(200);
      expect(sessionId).not.toBeNull();

      const missingSessionResponse = yield* httpClient.del("/mcp");
      expect(missingSessionResponse.status).toBe(400);

      const unknownSessionResponse = yield* httpClient.del("/mcp", {
        headers: { "mcp-session-id": "unknown-session" },
      });
      expect(unknownSessionResponse.status).toBe(404);

      const terminateResponse = yield* httpClient.del("/mcp", {
        headers: { "mcp-session-id": sessionId! },
      });
      expect(terminateResponse.status).toBe(204);

      const reusedSessionResponse = yield* httpClient.post("/mcp", {
        headers: {
          accept: "application/json, text/event-stream",
          "mcp-session-id": sessionId!,
        },
        body: HttpBody.text(
          `{"jsonrpc":"2.0","id":2,"method":"ping","params":{}}`,
          "application/json",
        ),
      });
      expect(reusedSessionResponse.status).toBe(404);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("keeps preview and orchestration HTTP tools/list registries isolated", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const previewRegistry = {
        resolve: (token: string) =>
          Effect.succeed(token === "preview-token" ? invocation : undefined),
      } as McpSessionRegistry.McpSessionRegistryShape;
      const orchestrationInvocation = {
        environmentId,
        threadId,
        providerSessionId: "provider-session-orchestration-test",
        providerInstanceId: ProviderInstanceId.make("codex"),
        issuedAt: 1,
      } satisfies OrchestrationMcpInvocationContext.OrchestrationMcpInvocationScope;
      const orchestrationRegistry = {
        resolve: (token: string) =>
          Effect.succeed(token === "orchestration-token" ? orchestrationInvocation : undefined),
      } as OrchestrationMcpSessionRegistry.OrchestrationMcpSessionRegistryShape;
      const previewLayer = McpHttpServer.layer.pipe(
        Layer.provide(Layer.succeed(McpSessionRegistry.McpSessionRegistry, previewRegistry)),
        Layer.provide(PreviewAutomationBroker.layer),
        Layer.provide(NodeServices.layer),
      );
      const orchestrationLayer = Layer.effectDiscard(McpServer.registerToolkit(AgentToolkit)).pipe(
        Layer.provide(AgentToolkitHandlersLive),
        Layer.provideMerge(Layer.fresh(OrchestrationMcpHttpServer.OrchestrationMcpTransportLive)),
        Layer.provide(
          Layer.succeed(
            OrchestrationMcpSessionRegistry.OrchestrationMcpSessionRegistry,
            orchestrationRegistry,
          ),
        ),
        Layer.provide(Layer.succeed(AgentOrchestrationService, {} as never)),
      );
      const routes = Layer.merge(previewLayer, orchestrationLayer);
      yield* HttpRouter.serve(routes, { disableListenLog: true, disableLogger: true }).pipe(
        Layer.build,
      );
      const httpClient = yield* HttpClient.HttpClient;
      const initialize = (path: string, token: string, id: number) =>
        httpClient.post(path, {
          headers: {
            accept: "application/json, text/event-stream",
            authorization: `Bearer ${token}`,
          },
          body: HttpBody.text(
            JSON.stringify({
              jsonrpc: "2.0",
              id,
              method: "initialize",
              params: {
                protocolVersion: "2025-06-18",
                capabilities: {},
                clientInfo: { name: "mcp-isolation-test", version: "1.0.0" },
              },
            }),
            "application/json",
          ),
        });
      const previewInitialize = yield* initialize("/mcp", "preview-token", 1);
      const orchestrationInitialize = yield* initialize(
        "/mcp/orchestration",
        "orchestration-token",
        2,
      );
      expect(previewInitialize.status).toBe(200);
      expect(orchestrationInitialize.status).toBe(200);
      const previewSession = previewInitialize.headers["mcp-session-id"];
      const orchestrationSession = orchestrationInitialize.headers["mcp-session-id"];
      expect(previewSession).toBeTruthy();
      expect(orchestrationSession).toBeTruthy();
      expect((yield* initialize("/mcp", "orchestration-token", 5)).status).toBe(401);
      expect((yield* initialize("/mcp/orchestration", "preview-token", 6)).status).toBe(401);
      const markInitialized = (path: string, token: string, session: string) =>
        httpClient.post(path, {
          headers: {
            accept: "application/json, text/event-stream",
            authorization: `Bearer ${token}`,
            "mcp-session-id": session,
            "mcp-protocol-version": "2025-06-18",
          },
          body: HttpBody.text(
            JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }),
            "application/json",
          ),
        });
      const previewInitialized = yield* markInitialized("/mcp", "preview-token", previewSession!);
      const orchestrationInitialized = yield* markInitialized(
        "/mcp/orchestration",
        "orchestration-token",
        orchestrationSession!,
      );
      expect(previewInitialized.status).toBe(202);
      expect(orchestrationInitialized.status).toBe(202);
      const toolsList = (path: string, token: string, session: string, id: number) =>
        httpClient.post(path, {
          headers: {
            accept: "application/json, text/event-stream",
            authorization: `Bearer ${token}`,
            "mcp-session-id": session,
            "mcp-protocol-version": "2025-06-18",
          },
          body: HttpBody.text(
            JSON.stringify({ jsonrpc: "2.0", id, method: "tools/list", params: {} }),
            "application/json",
          ),
        });
      const previewToolsResponse = yield* toolsList("/mcp", "preview-token", previewSession!, 3);
      const orchestrationToolsResponse = yield* toolsList(
        "/mcp/orchestration",
        "orchestration-token",
        orchestrationSession!,
        4,
      );
      expect(previewToolsResponse.status).toBe(200);
      expect(orchestrationToolsResponse.status).toBe(200);
      const readSseJson = (body: string) => {
        const data = body
          .split("\n")
          .find((line) => line.startsWith("data: "))
          ?.slice("data: ".length);
        return JSON.parse(data ?? body) as { result: { tools: Array<{ name: string }> } };
      };
      const previewTools = readSseJson(yield* previewToolsResponse.text);
      const orchestrationTools = readSseJson(yield* orchestrationToolsResponse.text);
      const previewNames = previewTools.result.tools.map(({ name }) => name);
      const orchestrationNames = orchestrationTools.result.tools.map(({ name }) => name);
      expect(previewNames.length).toBeGreaterThan(0);
      expect(previewNames).toContain("preview_status");
      expect(previewNames.every((name) => name.startsWith("preview_"))).toBe(true);
      expect(previewNames).not.toContain("agent_spawn");
      expect(orchestrationNames).toContain("agent_spawn");
      expect(orchestrationNames.every((name) => name.startsWith("agent_"))).toBe(true);
      expect(orchestrationNames).not.toContain("preview_status");
      expect(orchestrationTools.result.tools.length).toBeGreaterThan(0);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("registers annotated tools and preserves authenticated request context", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
      const routedRequests: Array<{
        readonly operation: string;
        readonly tabId?: string | undefined;
      }> = [];
      const events = yield* broker.connect({
        clientId: "mcp-test-client",
        environmentId,
      });
      yield* Stream.runForEach(events, (event) => {
        if (event.type === "connected") return Effect.void;
        routedRequests.push(event.request);
        return broker.respond({
          clientId: "mcp-test-client",
          connectionId: event.connectionId,
          requestId: event.request.requestId,
          ok: true,
          result:
            event.request.operation === "snapshot"
              ? {
                  url: "http://example.test/",
                  title: "Example",
                  loading: false,
                  visibleText: "Example",
                  interactiveElements: [],
                  accessibilityTree: {},
                  consoleEntries: [],
                  networkEntries: [],
                  actionTimeline: [],
                  screenshot: {
                    mimeType: "image/png",
                    data: Buffer.from("png").toString("base64"),
                    width: 10,
                    height: 5,
                  },
                }
              : event.request.operation === "press"
                ? undefined
                : {
                    available: true,
                    visible: true,
                    tabId,
                    url: "http://example.test/",
                    title: "Example",
                    loading: false,
                  },
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const statusTool = server.tools.find(({ tool }) => tool.name === "preview_status");
      expect(statusTool?.tool.annotations?.readOnlyHint).toBe(true);
      expect(statusTool?.tool.annotations?.idempotentHint).toBe(true);
      expect(statusTool?.tool.annotations?.destructiveHint).toBe(false);

      const snapshotTool = server.tools.find(({ tool }) => tool.name === "preview_snapshot");
      expect(snapshotTool?.tool.annotations?.readOnlyHint).toBe(true);
      expect(snapshotTool?.tool.annotations?.idempotentHint).toBe(true);
      expect(snapshotTool?.tool.annotations?.openWorldHint).toBe(true);

      const clickTool = server.tools.find(({ tool }) => tool.name === "preview_click");
      expect(clickTool?.tool.annotations?.readOnlyHint).toBe(false);
      expect(clickTool?.tool.annotations?.destructiveHint).toBe(true);
      expect(clickTool?.tool.annotations?.openWorldHint).toBe(true);
      expect(clickTool?.tool.outputSchema).toEqual({
        type: "object",
        additionalProperties: false,
        description: "The preview action completed successfully.",
      });

      const navigateTool = server.tools.find(({ tool }) => tool.name === "preview_navigate");
      expect(navigateTool?.tool.annotations?.destructiveHint).toBe(false);
      expect(navigateTool?.tool.annotations?.openWorldHint).toBe(true);

      const status = yield* server
        .callTool({ name: "preview_status", arguments: {} })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
      expect(status.isError).toBe(false);
      expect(status.structuredContent).toMatchObject({
        available: true,
        tabId,
      });

      const malformed = yield* server
        .callTool({ name: "preview_click", arguments: { selector: "" } })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provideService(McpSchema.McpServerClient, client),
          Effect.flip,
        );
      expect(malformed._tag).toBe("InvalidParams");

      const snapshot = yield* server
        .callTool({ name: "preview_snapshot", arguments: { tabId: alternateTabId } })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
      expect(snapshot.isError).toBe(false);
      expect(snapshot.content.some((content) => content.type === "image")).toBe(true);
      expect(snapshot.structuredContent).toMatchObject({
        screenshot: { mimeType: "image/png", width: 10, height: 5 },
      });
      expect(routedRequests.find(({ operation }) => operation === "snapshot")?.tabId).toBe(
        alternateTabId,
      );

      const actionRequests = [
        { name: "preview_click", arguments: { x: 10, y: 10 } },
        { name: "preview_type", arguments: { text: "Hello" } },
        { name: "preview_press", arguments: { key: "Enter" } },
        { name: "preview_scroll", arguments: { deltaY: 100 } },
        { name: "preview_wait_for", arguments: { text: "Example" } },
      ];
      for (const request of actionRequests) {
        const result = yield* server
          .callTool(request)
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );
        expect(result.isError).toBe(false);
        expect(result.structuredContent).toEqual({});
        expect(result.content).toEqual([{ type: "text", text: "{}" }]);
      }
    }),
  ).pipe(Effect.provide(TestLayer)),
);
