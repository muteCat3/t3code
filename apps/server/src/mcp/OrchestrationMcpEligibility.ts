import type { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

export interface OrchestrationMcpEligibilityRequest {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
}

export type OrchestrationMcpEligibilityResolver = (
  request: OrchestrationMcpEligibilityRequest,
) => Effect.Effect<boolean>;

let activeResolver: OrchestrationMcpEligibilityResolver | undefined;

/**
 * Installs the live projection-backed eligibility check owned by the agent
 * orchestration service. The registry calls it both when issuing a credential
 * and on every authenticated request, so trust/archive/lineage changes revoke
 * access without waiting for a provider restart.
 *
 * Returning the disposer keeps the service lifecycle explicit and prevents a
 * stopped server runtime from leaving a stale projection reader behind.
 */
export function installActiveOrchestrationMcpEligibilityResolver(
  resolver: OrchestrationMcpEligibilityResolver,
): () => void {
  activeResolver = resolver;
  return () => {
    if (activeResolver === resolver) activeResolver = undefined;
  };
}

export const checkActiveOrchestrationMcpEligibility = (
  request: OrchestrationMcpEligibilityRequest,
): Effect.Effect<boolean> => activeResolver?.(request) ?? Effect.succeed(false);
