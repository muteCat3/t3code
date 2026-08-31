import type { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";

export interface OrchestrationMcpInvocationScope {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly issuedAt: number;
}

/** Invocation identity for the isolated `/mcp/orchestration` boundary. */
export class OrchestrationMcpInvocationContext extends Context.Service<
  OrchestrationMcpInvocationContext,
  OrchestrationMcpInvocationScope
>()("t3/mcp/OrchestrationMcpInvocationContext") {}
