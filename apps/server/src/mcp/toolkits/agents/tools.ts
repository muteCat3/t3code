import {
  AgentArchiveResult,
  AgentChildInput,
  AgentInspectResult,
  AgentInterruptResult,
  AgentOrchestrationError,
  AgentRespondInput,
  AgentRespondResult,
  AgentSendInput,
  AgentSendResult,
  AgentSpawnInput,
  AgentSpawnResult,
  AgentUnarchiveResult,
  AgentWaitInput,
  AgentWaitResult,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/unstable/ai";
import { AgentOrchestrationService } from "../../../agentOrchestration/AgentOrchestrationService.ts";
import { OrchestrationMcpInvocationContext } from "../../OrchestrationMcpInvocationContext.ts";

const dependencies = [AgentOrchestrationService, OrchestrationMcpInvocationContext];
export const AgentSpawnTool = Tool.make("agent_spawn", {
  description:
    "Start a durable direct T3 child thread on an exact live provider model. Driver targets must resolve to exactly one configured instance.",
  parameters: AgentSpawnInput,
  success: AgentSpawnResult,
  failure: AgentOrchestrationError,
  dependencies,
});
export const AgentSendTool = Tool.make("agent_send", {
  description: "Start another turn on a direct child after its previous turn settles.",
  parameters: AgentSendInput,
  success: AgentSendResult,
  failure: AgentOrchestrationError,
  dependencies,
});
export const AgentWaitTool = Tool.make("agent_wait", {
  description:
    "Wait for a direct child to change after an opaque cursor. Timeout reports current state without cancellation.",
  parameters: AgentWaitInput,
  success: AgentWaitResult,
  failure: AgentOrchestrationError,
  dependencies,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
export const AgentInspectTool = Tool.make("agent_inspect", {
  description: "Inspect a direct child and its pending requests.",
  parameters: AgentChildInput,
  success: AgentInspectResult,
  failure: AgentOrchestrationError,
  dependencies,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);
export const AgentRespondTool = Tool.make("agent_respond", {
  description: "Answer a child approval or structured user-input request.",
  parameters: AgentRespondInput,
  success: AgentRespondResult,
  failure: AgentOrchestrationError,
  dependencies,
});
export const AgentInterruptTool = Tool.make("agent_interrupt", {
  description: "Interrupt a direct child's active turn.",
  parameters: AgentChildInput,
  success: AgentInterruptResult,
  failure: AgentOrchestrationError,
  dependencies,
});
export const AgentArchiveTool = Tool.make("agent_archive", {
  description: "Drain and archive a direct child.",
  parameters: AgentChildInput,
  success: AgentArchiveResult,
  failure: AgentOrchestrationError,
  dependencies,
});
export const AgentUnarchiveTool = Tool.make("agent_unarchive", {
  description: "Unarchive a direct child without resuming old work.",
  parameters: AgentChildInput,
  success: AgentUnarchiveResult,
  failure: AgentOrchestrationError,
  dependencies,
});

export const AgentToolkit = Toolkit.make(
  AgentSpawnTool,
  AgentSendTool,
  AgentWaitTool,
  AgentInspectTool,
  AgentRespondTool,
  AgentInterruptTool,
  AgentArchiveTool,
  AgentUnarchiveTool,
);
