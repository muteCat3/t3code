import type {
  ModelSelection,
  OrchestrationCommand,
  OrchestrationProject,
  OrchestrationThread,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderInteractionMode,
  RuntimeMode,
  ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import { ProviderDriverKind as ProviderDriverKindSchema } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

export interface AgentChildObservation {
  /** Captured after the live subscription is installed. */
  readonly initial: OrchestrationThread;
  readonly changes: Stream.Stream<OrchestrationThread, AgentOrchestrationBackendError>;
}

export interface StartChildTurnRequest {
  readonly parentThreadId: ThreadId;
  readonly project: OrchestrationProject;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly brief: string;
  readonly isolation: "shared" | "managed-worktree";
}

/**
 * Transport/bootstrap seam owned by the server integration.
 *
 * `startChildTurn` must use the same normalized `thread.turn.start` bootstrap as
 * websocket clients. It generates the child id/title and, for managed worktrees,
 * the `t3code/<hex>` ref/path from committed parent HEAD. Callers never supply a
 * filesystem path, branch, project, runtime mode, or title. The created thread
 * must be stamped with `agentParentThreadId = parentThreadId` before the turn is
 * allowed to start.
 *
 * `observeChild` has a stronger contract than a query followed by a subscription:
 * it installs the event subscription first and only then captures `initial`.
 */
export interface AgentOrchestrationBackendShape {
  readonly getThread: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationThread | null, AgentOrchestrationBackendError>;
  readonly getProject: (
    projectId: OrchestrationProject["id"],
  ) => Effect.Effect<OrchestrationProject | null, AgentOrchestrationBackendError>;
  readonly listDirectChildren: (
    parentThreadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<OrchestrationThread>, AgentOrchestrationBackendError>;
  /** Bounded to active top-level threads that currently have direct children. */
  readonly listAgentRoots: Effect.Effect<
    ReadonlyArray<OrchestrationThread>,
    AgentOrchestrationBackendError
  >;
  readonly getProviders: Effect.Effect<
    ReadonlyArray<ServerProvider>,
    AgentOrchestrationBackendError
  >;
  readonly startChildTurn: (request: StartChildTurnRequest) => Effect.Effect<
    {
      readonly child: OrchestrationThread;
      /** Subscription installed before the bootstrap command is dispatched. */
      readonly observation: AgentChildObservation;
    },
    AgentOrchestrationBackendError
  >;
  readonly observeChild: (
    threadId: ThreadId,
  ) => Effect.Effect<AgentChildObservation, AgentOrchestrationBackendError>;
  readonly dispatch: (
    command: OrchestrationCommand,
  ) => Effect.Effect<void, AgentOrchestrationBackendError>;
  // Successful dispatch is projection-synchronous: a following getThread
  // observes the command's projected state. Domain-event hub delivery may lag.
}

export class AgentOrchestrationBackendError extends Error {
  readonly _tag = "AgentOrchestrationBackendError";
  override readonly cause: unknown;
  constructor(message: string, cause?: unknown) {
    super(message);
    this.cause = cause;
  }
}

export class AgentOrchestrationBackend extends Context.Service<
  AgentOrchestrationBackend,
  AgentOrchestrationBackendShape
>()("t3/agentOrchestration/AgentOrchestrationBackend") {}

export const ALLOWED_AGENT_DRIVERS = new Set<ProviderDriverKind>([
  ProviderDriverKindSchema.make("codex"),
  ProviderDriverKindSchema.make("claudeAgent"),
  ProviderDriverKindSchema.make("grok"),
]);

export const isSameInstance = (left: ProviderInstanceId, right: ProviderInstanceId) =>
  left === right;
