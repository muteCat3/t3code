import {
  CommandId,
  IsoDateTime,
  MessageId,
  ProviderDriverKind,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationThread,
  type VcsCreateWorktreeInput,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as OrchestrationMcpEligibility from "../mcp/OrchestrationMcpEligibility.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import { ThreadDeletionReactor } from "../orchestration/Services/ThreadDeletionReactor.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import {
  AgentOrchestrationBackend,
  AgentOrchestrationBackendError,
  type AgentChildObservation,
  type AgentOrchestrationBackendShape,
} from "./AgentOrchestrationBackend.ts";
import {
  AgentOrchestrationService,
  type AgentOrchestrationServiceShape,
} from "./AgentOrchestrationService.ts";

const backendError = (operation: string) => (cause: unknown) =>
  new AgentOrchestrationBackendError(`${operation} failed.`, cause);

const makeTimestamp = DateTime.now.pipe(
  Effect.map((value) => IsoDateTime.make(DateTime.formatIso(value))),
);

const childTitle = (brief: string): string => {
  const firstLine = brief.trim().split(/\r?\n/, 1)[0]?.trim() ?? "";
  const title = firstLine.length > 0 ? firstLine : "Delegated task";
  return title.length <= 96 ? title : `${title.slice(0, 93).trimEnd()}...`;
};

export const makeChildObservationHub = () => PubSub.unbounded<OrchestrationThread>();

export const subscribeChildChanges = (hub: PubSub.PubSub<OrchestrationThread>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const subscription = yield* PubSub.subscribe(hub).pipe(
      Effect.provideService(Scope.Scope, scope),
    );
    const close = Scope.close(scope, Exit.void);
    return {
      changes: Stream.fromSubscription(subscription).pipe(Stream.ensuring(close)),
      close,
    };
  });

export const makeManagedWorktreeInput = (input: {
  readonly projectWorkspaceRoot: string;
  readonly parentHeadCommit: string;
  readonly branch: string;
}): VcsCreateWorktreeInput => ({
  cwd: input.projectWorkspaceRoot,
  refName: input.parentHeadCommit,
  newRefName: input.branch,
  path: null,
});

export const isOrchestrationMcpEligible = (input: {
  readonly request: OrchestrationMcpEligibility.OrchestrationMcpEligibilityRequest;
  readonly root: OrchestrationThread | null;
  readonly project: {
    readonly deletedAt?: string | null;
    readonly agentOrchestrationTrusted?: boolean;
  } | null;
  readonly providers: ReadonlyArray<{
    readonly instanceId: string;
    readonly driver: string;
    readonly status?: string;
    readonly enabled: boolean;
    readonly installed: boolean;
    readonly availability?: string;
  }>;
}) => {
  const { root, project, request } = input;
  if (
    root === null ||
    root.archivedAt !== null ||
    root.agentParentThreadId != null ||
    root.modelSelection.instanceId !== request.providerInstanceId ||
    project === null ||
    project.deletedAt !== null ||
    project.agentOrchestrationTrusted !== true
  ) {
    return false;
  }
  const provider = input.providers.find(
    (candidate) => candidate.instanceId === request.providerInstanceId,
  );
  return (
    provider?.driver === ProviderDriverKind.make("codex") &&
    provider.status !== "disabled" &&
    provider.enabled &&
    provider.installed &&
    provider.availability !== "unavailable"
  );
};

type BootstrapTurnStartCommand = Extract<
  OrchestrationCommand,
  { readonly type: "thread.turn.start" }
>;

export class BootstrapTurnStartFailure extends Error {
  readonly _tag = "BootstrapTurnStartFailure";
  readonly bootstrapCause: unknown;
  readonly cleanupDisposition: "not-created" | "deleted" | "cleanup-failed";
  readonly cleanupCause: unknown | undefined;

  constructor(
    bootstrapCause: unknown,
    cleanupDisposition: "not-created" | "deleted" | "cleanup-failed",
    cleanupCause?: unknown,
  ) {
    super(
      bootstrapCause instanceof Error
        ? bootstrapCause.message
        : "Failed to bootstrap thread turn start.",
      { cause: bootstrapCause },
    );
    this.bootstrapCause = bootstrapCause;
    this.cleanupDisposition = cleanupDisposition;
    this.cleanupCause = cleanupCause;
  }
}

/**
 * The single bootstrap sequence used by both websocket-created threads and
 * orchestration children. Callers provide the environment-specific worktree
 * and setup hooks, while creation fences and failure cleanup stay identical.
 */
export const dispatchBootstrapTurnStartCore = <E, R>(input: {
  readonly command: BootstrapTurnStartCommand;
  readonly makeCommandId: (tag: string) => Effect.Effect<CommandId, E, R>;
  readonly dispatch: (
    command: OrchestrationCommand,
  ) => Effect.Effect<{ readonly sequence: number }, E, R>;
  readonly drainThreadDeletionThrough: (sequence: number) => Effect.Effect<void, E, R>;
  readonly prepareWorktree: (
    input: NonNullable<NonNullable<BootstrapTurnStartCommand["bootstrap"]>["prepareWorktree"]>,
  ) => Effect.Effect<{ readonly branch: string; readonly worktreePath: string }, E, R>;
  readonly afterWorktreePrepared: (worktreePath: string) => Effect.Effect<void, E, R>;
  readonly runSetupScript: (input: {
    readonly projectId:
      | NonNullable<
          NonNullable<BootstrapTurnStartCommand["bootstrap"]>["createThread"]
        >["projectId"]
      | undefined;
    readonly projectCwd: string | undefined;
    readonly worktreePath: string;
  }) => Effect.Effect<void, E, R>;
}): Effect.Effect<{ readonly sequence: number }, BootstrapTurnStartFailure, R> =>
  Effect.gen(function* () {
    const bootstrap = input.command.bootstrap;
    const { bootstrap: _bootstrap, ...finalTurnStartCommand } = input.command;
    let createdThread = false;
    let targetWorktreePath = bootstrap?.createThread?.worktreePath ?? null;

    const program = Effect.gen(function* () {
      if (bootstrap?.createThread) {
        const created = yield* input.dispatch({
          type: "thread.create",
          commandId: yield* input.makeCommandId("bootstrap-thread-create"),
          threadId: input.command.threadId,
          projectId: bootstrap.createThread.projectId,
          agentParentThreadId: bootstrap.createThread.agentParentThreadId,
          title: bootstrap.createThread.title,
          modelSelection: bootstrap.createThread.modelSelection,
          runtimeMode: bootstrap.createThread.runtimeMode,
          interactionMode: bootstrap.createThread.interactionMode,
          branch: bootstrap.createThread.branch,
          worktreePath: bootstrap.createThread.worktreePath,
          createdAt: bootstrap.createThread.createdAt,
        });
        createdThread = true;
        yield* input.drainThreadDeletionThrough(created.sequence);
      }

      if (bootstrap?.prepareWorktree) {
        const prepared = yield* input.prepareWorktree(bootstrap.prepareWorktree);
        targetWorktreePath = prepared.worktreePath;
        yield* input.dispatch({
          type: "thread.meta.update",
          commandId: yield* input.makeCommandId("bootstrap-thread-meta-update"),
          threadId: input.command.threadId,
          branch: prepared.branch,
          worktreePath: prepared.worktreePath,
        });
        yield* input.afterWorktreePrepared(prepared.worktreePath);
      }

      if (bootstrap?.runSetupScript && targetWorktreePath) {
        yield* input.runSetupScript({
          projectId: bootstrap.createThread?.projectId,
          projectCwd: bootstrap.prepareWorktree?.projectCwd,
          worktreePath: targetWorktreePath,
        });
      }

      return yield* input.dispatch(finalTurnStartCommand);
    });

    return yield* program.pipe(
      Effect.catchCause((bootstrapCause) => {
        if (Cause.hasInterruptsOnly(bootstrapCause)) return Effect.interrupt;
        if (!createdThread) {
          return Effect.fail(
            new BootstrapTurnStartFailure(Cause.squash(bootstrapCause), "not-created"),
          );
        }
        return Effect.uninterruptible(
          input.makeCommandId("bootstrap-thread-delete").pipe(
            Effect.flatMap((commandId) =>
              input.dispatch({
                type: "thread.delete",
                commandId,
                threadId: input.command.threadId,
              }),
            ),
          ),
        ).pipe(
          Effect.matchCauseEffect({
            onFailure: (cleanupCause) =>
              Effect.fail(
                new BootstrapTurnStartFailure(
                  Cause.squash(bootstrapCause),
                  "cleanup-failed",
                  Cause.squash(cleanupCause),
                ),
              ),
            onSuccess: () =>
              Effect.fail(new BootstrapTurnStartFailure(Cause.squash(bootstrapCause), "deleted")),
          }),
        );
      }),
    );
  });

const makeBackend = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  const git = yield* GitWorkflowService.GitWorkflowService;
  const gitDriver = yield* GitVcsDriver.GitVcsDriver;
  const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
  const threadDeletionReactor = yield* ThreadDeletionReactor;
  const setupScriptRunner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const childHubs = new Map<ThreadId, PubSub.PubSub<OrchestrationThread>>();
  const childHubMutex = yield* Semaphore.make(1);

  const getThread: AgentOrchestrationBackendShape["getThread"] = Effect.fn(
    "AgentOrchestrationBackend.getThread",
  )(
    function* (threadId) {
      const thread = yield* snapshots.getThreadDetailById(threadId);
      if (Option.isSome(thread)) {
        return { ...thread.value, agentParentThreadId: thread.value.agentParentThreadId ?? null };
      }
      // Root admission only needs shell state. Detail remains authoritative
      // for children, but shell fallback also supports lightweight readers.
      const shell = yield* snapshots.getThreadShellById(threadId);
      return Option.match(shell, {
        onNone: () => null,
        onSome: (value) => ({
          ...value,
          agentParentThreadId: value.agentParentThreadId ?? null,
          messages: [],
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          deletedAt: null,
        }),
      });
    },
    Effect.mapError(backendError("Reading the child thread projection")),
  );

  const getProject: AgentOrchestrationBackendShape["getProject"] = Effect.fn(
    "AgentOrchestrationBackend.getProject",
  )(
    function* (projectId) {
      const snapshot = yield* snapshots.getCommandReadModel();
      return snapshot.projects.find((project) => project.id === projectId) ?? null;
    },
    Effect.mapError(backendError("Reading the project projection")),
  );

  const listDirectChildren: AgentOrchestrationBackendShape["listDirectChildren"] = Effect.fn(
    "AgentOrchestrationBackend.listDirectChildren",
  )(
    function* (parentThreadId) {
      const snapshot = yield* snapshots.getCommandReadModel();
      return snapshot.threads
        .filter((thread) => thread.agentParentThreadId === parentThreadId)
        .map((thread) => ({ ...thread, agentParentThreadId: thread.agentParentThreadId ?? null }));
    },
    Effect.mapError(backendError("Reading direct child projections")),
  );

  const listAgentRoots: AgentOrchestrationBackendShape["listAgentRoots"] = snapshots
    .getCommandReadModel()
    .pipe(
      Effect.map((snapshot) => {
        const parentIds = new Set(
          snapshot.threads.flatMap((thread) =>
            thread.agentParentThreadId == null ? [] : [thread.agentParentThreadId],
          ),
        );
        return snapshot.threads
          .filter(
            (thread) =>
              thread.agentParentThreadId == null &&
              thread.archivedAt === null &&
              parentIds.has(thread.id),
          )
          .map((thread) => ({ ...thread, agentParentThreadId: null }));
      }),
      Effect.mapError(backendError("Reading active agent Root projections")),
    );

  const getProviders: AgentOrchestrationBackendShape["getProviders"] = providers.getProviders.pipe(
    Effect.mapError(backendError("Reading live provider snapshots")),
  );

  const getChildHub = (threadId: ThreadId) =>
    childHubMutex.withPermits(1)(
      Effect.gen(function* () {
        const existing = childHubs.get(threadId);
        if (existing !== undefined) return existing;
        // Consumers subscribe eagerly before their snapshot/dispatch. The hub
        // intentionally has no replay buffer: `initial` is the baseline, and
        // every stream item must be a later projected state.
        const hub = yield* makeChildObservationHub();
        childHubs.set(threadId, hub);
        return hub;
      }),
    );

  yield* engine.streamDomainEvents.pipe(
    Stream.filter((event) => event.aggregateKind === "thread"),
    Stream.runForEach((event) => {
      const threadId = ThreadId.make(event.aggregateId);
      const hub = childHubs.get(threadId);
      if (hub === undefined) return Effect.void;
      if (event.type === "thread.deleted") {
        childHubs.delete(threadId);
        return PubSub.shutdown(hub);
      }
      return getThread(threadId).pipe(
        Effect.flatMap((thread) => (thread === null ? Effect.void : PubSub.publish(hub, thread))),
        Effect.ignoreCause({ log: true }),
      );
    }),
    Effect.forkScoped,
  );

  const observeChild: AgentOrchestrationBackendShape["observeChild"] = Effect.fn(
    "AgentOrchestrationBackend.observeChild",
  )(function* (threadId) {
    const hub = yield* getChildHub(threadId);
    const observation = yield* subscribeChildChanges(hub);
    const initial = yield* getThread(threadId);
    if (initial === null) {
      yield* observation.close;
      return yield* Effect.fail(
        new AgentOrchestrationBackendError(`Child thread ${threadId} does not exist.`),
      );
    }
    return {
      initial,
      changes: observation.changes,
    } satisfies AgentChildObservation;
  });

  const dispatch: AgentOrchestrationBackendShape["dispatch"] = Effect.fn(
    "AgentOrchestrationBackend.dispatch",
  )(
    function* (command) {
      yield* startup.enqueueCommand(engine.dispatch(command));
    },
    Effect.mapError(backendError("Dispatching the orchestration command")),
  );

  const startChildTurn: AgentOrchestrationBackendShape["startChildTurn"] = Effect.fn(
    "AgentOrchestrationBackend.startChildTurn",
  )(
    function* (request) {
      const parent = yield* getThread(request.parentThreadId);
      if (parent === null || parent.projectId !== request.project.id) {
        return yield* Effect.fail(
          new AgentOrchestrationBackendError("The Root thread no longer belongs to this project."),
        );
      }

      const childId = ThreadId.make(yield* crypto.randomUUIDv4);
      const branchSuffix = (yield* crypto.randomUUIDv4).replaceAll("-", "").slice(0, 12);
      const branch = `t3code/${branchSuffix}`;
      const childHub = yield* getChildHub(childId);
      const childObservation = yield* subscribeChildChanges(childHub);
      const createdAt = yield* makeTimestamp;

      yield* startup.enqueueCommand(
        dispatchBootstrapTurnStartCore({
          command: {
            type: "thread.turn.start",
            commandId: CommandId.make(`agent-turn:${yield* crypto.randomUUIDv4}`),
            threadId: childId,
            message: {
              messageId: MessageId.make(`agent-message:${yield* crypto.randomUUIDv4}`),
              role: "user",
              text: request.brief,
              attachments: [],
            },
            modelSelection: request.modelSelection,
            runtimeMode: request.runtimeMode,
            interactionMode: request.interactionMode,
            createdAt,
            bootstrap: {
              createThread: {
                projectId: request.project.id,
                agentParentThreadId: request.parentThreadId,
                title: childTitle(request.brief),
                modelSelection: request.modelSelection,
                runtimeMode: request.runtimeMode,
                interactionMode: request.interactionMode,
                branch: parent.branch,
                // null is the canonical shared-workspace representation. The
                // provider cwd falls back to project.workspaceRoot; stamping
                // that root as a worktree would make cleanup treat it as owned.
                worktreePath: null,
                createdAt,
              },
              ...(request.isolation === "managed-worktree"
                ? {
                    prepareWorktree: {
                      projectCwd: request.project.workspaceRoot,
                      baseBranch: parent.branch ?? "HEAD",
                      branch,
                    },
                    runSetupScript: true,
                  }
                : {}),
            },
          },
          makeCommandId: (tag) =>
            crypto.randomUUIDv4.pipe(
              Effect.map((uuid) => CommandId.make(`agent:${tag}:${uuid}`)),
              Effect.mapError(backendError("Generating an agent bootstrap command id")),
            ),
          dispatch: (command) =>
            engine
              .dispatch(command)
              .pipe(Effect.mapError(backendError("Dispatching an agent bootstrap command"))),
          drainThreadDeletionThrough: (sequence) =>
            threadDeletionReactor
              .drainThrough(sequence)
              .pipe(Effect.mapError(backendError("Draining prior child deletion"))),
          prepareWorktree: () =>
            gitDriver
              .resolveCommit({
                cwd: parent.worktreePath ?? request.project.workspaceRoot,
                revision: "HEAD",
              })
              .pipe(
                Effect.flatMap(({ commitSha }) =>
                  git.createWorktree(
                    makeManagedWorktreeInput({
                      projectWorkspaceRoot: request.project.workspaceRoot,
                      parentHeadCommit: commitSha,
                      branch,
                    }),
                  ),
                ),
                Effect.map((worktree) => ({
                  branch: worktree.worktree.refName,
                  worktreePath: worktree.worktree.path,
                })),
                Effect.mapError(backendError("Preparing the managed child worktree")),
              ),
          afterWorktreePrepared: () => Effect.void,
          runSetupScript: ({ projectId, projectCwd, worktreePath }) =>
            setupScriptRunner
              .runForThread({
                threadId: childId,
                ...(projectId ? { projectId } : {}),
                ...(projectCwd ? { projectCwd } : {}),
                worktreePath,
              })
              .pipe(
                Effect.asVoid,
                Effect.catch(() => Effect.void),
              ),
        }),
      );

      const child = yield* getThread(childId);
      if (child === null || child.agentParentThreadId !== request.parentThreadId) {
        yield* childObservation.close;
        return yield* Effect.fail(
          new AgentOrchestrationBackendError(
            `Child ${childId} was not projected with the requested immutable lineage.`,
          ),
        );
      }
      return {
        child,
        observation: {
          initial: child,
          changes: childObservation.changes,
        },
      };
    },
    Effect.mapError(backendError("Bootstrapping the durable child turn")),
  );

  return AgentOrchestrationBackend.of({
    getThread,
    getProject,
    listDirectChildren,
    listAgentRoots,
    getProviders,
    startChildTurn,
    observeChild,
    dispatch,
  });
});

export const AgentOrchestrationBackendLive = Layer.effect(AgentOrchestrationBackend, makeBackend);

export const OrchestrationMcpEligibilityLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const backend = yield* AgentOrchestrationBackend;
    const resolveEligibility = Effect.fn("AgentOrchestrationMcpEligibility.resolve")(
      function* (request: OrchestrationMcpEligibility.OrchestrationMcpEligibilityRequest) {
        const root = yield* backend.getThread(request.threadId);
        if (root === null) return false;
        const project = yield* backend.getProject(root.projectId);
        const liveProviders = yield* backend.getProviders;
        return isOrchestrationMcpEligible({
          request,
          root,
          project:
            project === null
              ? null
              : {
                  deletedAt: project.deletedAt ?? null,
                  ...(project.agentOrchestrationTrusted !== undefined
                    ? { agentOrchestrationTrusted: project.agentOrchestrationTrusted }
                    : {}),
                },
          providers: liveProviders.map((provider) => ({
            instanceId: provider.instanceId,
            driver: provider.driver,
            status: provider.status,
            enabled: provider.enabled,
            installed: provider.installed,
            ...(provider.availability !== undefined ? { availability: provider.availability } : {}),
          })),
        });
      },
      Effect.orElseSucceed(() => false),
    );
    const dispose =
      OrchestrationMcpEligibility.installActiveOrchestrationMcpEligibilityResolver(
        resolveEligibility,
      );
    yield* Effect.addFinalizer(() => Effect.sync(dispose));
  }),
).pipe(Layer.provide(AgentOrchestrationBackendLive));

let activeAgentOrchestration: AgentOrchestrationServiceShape | undefined;
const rootSessionsPendingOrchestrationRefresh = new Set<ThreadId>();

export const markRootSessionPendingOrchestrationRefresh = (threadId: ThreadId) =>
  Effect.sync(() => rootSessionsPendingOrchestrationRefresh.add(threadId)).pipe(Effect.asVoid);

export const isRootSessionPendingOrchestrationRefresh = (threadId: ThreadId) =>
  Effect.sync(() => rootSessionsPendingOrchestrationRefresh.has(threadId));

export const clearRootSessionPendingOrchestrationRefresh = (threadId: ThreadId) =>
  Effect.sync(() => rootSessionsPendingOrchestrationRefresh.delete(threadId)).pipe(Effect.asVoid);

export const AgentOrchestrationRuntimeBridgeLive = Layer.effectDiscard(
  Effect.acquireRelease(
    AgentOrchestrationService.pipe(
      Effect.tap((service) =>
        Effect.sync(() => {
          activeAgentOrchestration = service;
        }),
      ),
    ),
    (service) =>
      service.drainLifecycle.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (activeAgentOrchestration === service) activeAgentOrchestration = undefined;
          }),
        ),
      ),
  ),
);

export const prepareActiveParentArchive = (threadId: ThreadId) =>
  activeAgentOrchestration?.prepareParentArchive(threadId) ??
  Effect.die(new Error("Agent orchestration runtime is not active."));

export const acquireActiveRootMutation = (threadId: ThreadId) =>
  activeAgentOrchestration?.acquireRootMutation(threadId) ??
  Effect.die(new Error("Agent orchestration runtime is not active."));

export const acquireActiveParentArchive = (threadId: ThreadId) =>
  activeAgentOrchestration?.acquireParentArchive(threadId) ??
  Effect.die(new Error("Agent orchestration runtime is not active."));

export const cleanupActiveDisabledProject = (threadId: ThreadId) =>
  activeAgentOrchestration?.cleanupDisabledProject(threadId) ??
  Effect.die(new Error("Agent orchestration runtime is not active."));
