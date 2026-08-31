import type { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";

export interface OrchestrationMcpProviderSessionConfig {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly endpoint: string;
  readonly authorizationHeader: string;
}

const sessionsByThread = new Map<ThreadId, OrchestrationMcpProviderSessionConfig>();

export function setOrchestrationMcpProviderSession(
  config: OrchestrationMcpProviderSessionConfig,
): void {
  sessionsByThread.set(config.threadId, config);
}

export function readOrchestrationMcpProviderSession(
  threadId: ThreadId,
): OrchestrationMcpProviderSessionConfig | undefined {
  return sessionsByThread.get(threadId);
}

export function clearOrchestrationMcpProviderSession(threadId: ThreadId): void {
  sessionsByThread.delete(threadId);
}

export function clearAllOrchestrationMcpProviderSessions(): void {
  sessionsByThread.clear();
}
