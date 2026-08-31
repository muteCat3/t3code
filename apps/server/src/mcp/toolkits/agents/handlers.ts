import { AgentToolkit } from "./tools.ts";
import { AgentOrchestrationService } from "../../../agentOrchestration/AgentOrchestrationService.ts";
import {
  OrchestrationMcpInvocationContext,
  type OrchestrationMcpInvocationScope,
} from "../../OrchestrationMcpInvocationContext.ts";
import * as Effect from "effect/Effect";

const callerFrom = (scope: OrchestrationMcpInvocationScope) => ({
  threadId: scope.threadId,
  providerInstanceId: scope.providerInstanceId,
});

const handlers = {
  agent_spawn: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      const service = yield* AgentOrchestrationService;
      return { child: yield* service.spawn(callerFrom(scope), input) };
    }),
  agent_send: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      const service = yield* AgentOrchestrationService;
      return { child: yield* service.send(callerFrom(scope), input.threadId, input.brief) };
    }),
  agent_wait: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      return yield* (yield* AgentOrchestrationService).wait(callerFrom(scope), input);
    }),
  agent_inspect: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      return {
        child: yield* (yield* AgentOrchestrationService).inspect(callerFrom(scope), input.threadId),
      };
    }),
  agent_respond: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      return {
        child: yield* (yield* AgentOrchestrationService).respond(callerFrom(scope), input),
      };
    }),
  agent_interrupt: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      return {
        child: yield* (yield* AgentOrchestrationService).interrupt(
          callerFrom(scope),
          input.threadId,
        ),
      };
    }),
  agent_archive: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      return {
        child: yield* (yield* AgentOrchestrationService).archive(callerFrom(scope), input.threadId),
      };
    }),
  agent_unarchive: (input) =>
    Effect.gen(function* () {
      const scope = yield* OrchestrationMcpInvocationContext;
      return {
        child: yield* (yield* AgentOrchestrationService).unarchive(
          callerFrom(scope),
          input.threadId,
        ),
      };
    }),
} satisfies Parameters<typeof AgentToolkit.toLayer>[0];

export const AgentToolkitHandlersLive = AgentToolkit.toLayer(handlers);
