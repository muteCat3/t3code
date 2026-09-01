import {
  AgentOrchestrationError,
  ApprovalRequestId,
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  type AgentChildSnapshot,
  type AgentRespondInput,
  type AgentSpawnInput,
  type AgentWaitInput,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationThread,
  ProviderDriverKind,
  type ProviderOptionSelection,
  type ServerProvider,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";

import {
  AgentOrchestrationBackend,
  AgentOrchestrationBackendError,
  ALLOWED_AGENT_DRIVERS,
} from "./AgentOrchestrationBackend.ts";
import { providerReportedIdentityMatches } from "../provider/ProviderModelIdentity.ts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";

const timestamp = DateTime.now.pipe(
  Effect.map((value) => IsoDateTime.make(DateTime.formatIso(value))),
);
const fail = (
  code: ConstructorParameters<typeof AgentOrchestrationError>[0]["code"],
  message: string,
  extras?: Pick<
    ConstructorParameters<typeof AgentOrchestrationError>[0],
    "childThreadId" | "candidates"
  >,
) => new AgentOrchestrationError({ code, message, ...extras });

const mapBackendError = (error: AgentOrchestrationBackendError) =>
  fail("unavailable", error.message || "Agent orchestration backend is unavailable.");

export const isTopLevelAgentRoot = (thread: Pick<OrchestrationThread, "agentParentThreadId">) =>
  (thread.agentParentThreadId ?? null) === null;

function cursorFor(thread: OrchestrationThread): string {
  const lastActivity = thread.activities.at(-1);
  const value = `${thread.updatedAt}:${lastActivity?.sequence ?? 0}:${lastActivity?.id ?? "none"}`;
  return Buffer.from(value, "utf8").toString("base64url");
}

function payloadRequestId(payload: unknown): ApprovalRequestId | undefined {
  if (!Predicate.isObject(payload) || !Predicate.isString(payload.requestId)) return undefined;
  const value = payload.requestId.trim();
  return value.length > 0 ? ApprovalRequestId.make(value) : undefined;
}

function pendingRequests(thread: OrchestrationThread): AgentChildSnapshot["pendingRequests"] {
  const open = new Map<string, AgentChildSnapshot["pendingRequests"][number]>();
  for (const activity of thread.activities) {
    const requestId = payloadRequestId(activity.payload);
    if (requestId === undefined) continue;
    if (activity.kind === "approval.requested") {
      open.set(requestId, { kind: "approval", requestId, payload: activity.payload });
    } else if (activity.kind === "user-input.requested") {
      open.set(requestId, { kind: "user-input", requestId, payload: activity.payload });
    } else if (activity.kind === "approval.resolved" || activity.kind === "user-input.resolved") {
      open.delete(requestId);
    }
  }
  return [...open.values()];
}

export function childSnapshot(thread: OrchestrationThread): AgentChildSnapshot {
  const pending = pendingRequests(thread);
  const identityFailureActivity = thread.activities.findLast(
    (activity) => activity.kind === "agent.identity.failed",
  );
  const identityFailureDetail =
    Predicate.isObject(identityFailureActivity?.payload) &&
    Predicate.isString(identityFailureActivity.payload.detail)
      ? identityFailureActivity.payload.detail
      : undefined;
  const latestAssistantText = thread.messages.findLast(
    (message) => message.role === "assistant",
  )?.text;
  const state = thread.latestTurn?.state;
  const sessionStatus = thread.session?.status;
  const status: AgentChildSnapshot["status"] =
    identityFailureActivity !== undefined
      ? "failed"
      : pending.length > 0
        ? "waiting"
        : state === "error" || sessionStatus === "error"
          ? "failed"
          : state === "interrupted"
            ? "interrupted"
            : state === "running" || sessionStatus === "starting" || sessionStatus === "running"
              ? "running"
              : thread.archivedAt !== null
                ? "completed"
                : "idle";
  return {
    threadId: thread.id,
    title: thread.title,
    status,
    cursor: cursorFor(thread),
    modelSelection: thread.modelSelection,
    pendingRequests: pending,
    ...(latestAssistantText === undefined ? {} : { latestAssistantText }),
    ...(identityFailureDetail !== undefined
      ? { error: identityFailureDetail }
      : thread.session?.lastError
        ? { error: thread.session.lastError }
        : {}),
  };
}

function validateOptions(
  provider: ServerProvider,
  model: ServerProvider["models"][number],
  selections: ReadonlyArray<ProviderOptionSelection> | undefined,
): void {
  const descriptors = model.capabilities?.optionDescriptors ?? [];
  const byId = new Map(descriptors.map((descriptor) => [descriptor.id, descriptor]));
  const seen = new Set<string>();
  for (const selection of selections ?? []) {
    const descriptor = byId.get(selection.id);
    if (seen.has(selection.id))
      throw fail("invalid_options", `Option ${selection.id} was supplied more than once.`);
    seen.add(selection.id);
    if (descriptor === undefined)
      throw fail(
        "invalid_options",
        `Unknown option ${selection.id} for ${provider.instanceId}/${model.slug}.`,
      );
    if (descriptor.type === "boolean" && typeof selection.value !== "boolean") {
      throw fail("invalid_options", `Option ${selection.id} requires a boolean value.`);
    }
    if (
      descriptor.type === "select" &&
      (typeof selection.value !== "string" ||
        !descriptor.options.some((choice) => choice.id === selection.value))
    ) {
      throw fail(
        "invalid_options",
        `Option ${selection.id} is not one of the live catalog choices.`,
      );
    }
  }
}

export function resolveProvider(
  providers: ReadonlyArray<ServerProvider>,
  input: AgentSpawnInput,
): ServerProvider {
  const target = input.target;
  const candidates =
    "instanceId" in target
      ? providers.filter((provider) => provider.instanceId === target.instanceId)
      : providers.filter((provider) => provider.driver === target.driver);
  if (candidates.length === 0)
    throw fail("invalid_target", "No configured provider instance matches the requested target.");
  if (candidates.length > 1) {
    throw fail(
      "ambiguous_target",
      "More than one configured provider instance matches this driver.",
      {
        candidates: candidates.map(({ instanceId, driver, displayName }) => ({
          instanceId,
          driver,
          ...(displayName === undefined ? {} : { displayName }),
        })),
      },
    );
  }
  const provider = candidates[0]!;
  if (!ALLOWED_AGENT_DRIVERS.has(provider.driver))
    throw fail("invalid_target", `Provider driver ${provider.driver} cannot run T3 child agents.`);
  if (
    !provider.enabled ||
    !provider.installed ||
    provider.status === "disabled" ||
    provider.availability === "unavailable"
  ) {
    throw fail(
      "invalid_target",
      `Provider instance ${provider.instanceId} is not enabled and available.`,
    );
  }
  const model = provider.models.find((candidate) => candidate.slug === input.model);
  if (model === undefined)
    throw fail(
      "invalid_model",
      `Model ${input.model} is not in the live catalog for ${provider.instanceId}.`,
    );
  validateOptions(provider, model, input.options);
  return provider;
}

const sameSelection = (left: ModelSelection | undefined, right: ModelSelection): boolean =>
  left !== undefined &&
  left.instanceId === right.instanceId &&
  left.model === right.model &&
  JSON.stringify(left.options ?? []) === JSON.stringify(right.options ?? []);

/** Provider-aware comparison of runtime-reported identity with the applied catalog slug. */
function identityFailure(
  thread: OrchestrationThread,
  provider: ServerProvider,
  requested: ModelSelection,
): string | undefined {
  const session = thread.session;
  if (session === null) return undefined;
  if (
    session.requestedModelSelection !== undefined &&
    !sameSelection(session.requestedModelSelection, requested)
  ) {
    return "Provider session requested selection differs from the validated child selection.";
  }
  if (
    session.appliedModelSelection !== undefined &&
    !sameSelection(session.appliedModelSelection, requested)
  ) {
    return "Provider applied a different model selection than requested.";
  }
  if (
    session.providerReportedModelId !== undefined &&
    !providerReportedIdentityMatches({
      driver: provider.driver,
      appliedModelSelection: session.appliedModelSelection,
      providerReportedModelId: session.providerReportedModelId,
    })
  ) {
    return `Provider reported model ${session.providerReportedModelId}, expected ${requested.model}.`;
  }
  return undefined;
}

const identityReady = (
  thread: OrchestrationThread,
  provider: ServerProvider,
  requested: ModelSelection,
) => {
  const session = thread.session;
  return (
    session !== null &&
    sameSelection(session.requestedModelSelection, requested) &&
    sameSelection(session.appliedModelSelection, requested) &&
    session.providerReportedModelId !== undefined &&
    providerReportedIdentityMatches({
      driver: provider.driver,
      appliedModelSelection: session.appliedModelSelection,
      providerReportedModelId: session.providerReportedModelId,
    })
  );
};

export function isStartupIdentityBarrierState(
  state: OrchestrationThread,
  provider: ServerProvider,
  requested: ModelSelection,
): boolean {
  const mismatch = identityFailure(state, provider, requested);
  const terminal =
    state.latestTurn?.state === "error" ||
    state.latestTurn?.state === "interrupted" ||
    state.latestTurn?.state === "completed" ||
    state.session?.status === "error" ||
    state.session?.status === "stopped";
  const providerReadyRequiresIdentity = provider.driver !== ProviderDriverKind.make("claudeAgent");
  const postHandshakeWithoutIdentity =
    providerReadyRequiresIdentity &&
    (state.session?.status === "running" || state.session?.status === "ready") &&
    state.session.providerReportedModelId === undefined;
  return (
    mismatch !== undefined ||
    identityReady(state, provider, requested) ||
    terminal ||
    postHandshakeWithoutIdentity
  );
}

export interface AgentOrchestrationCaller {
  readonly threadId: ThreadId;
  /** Exact provider instance authenticated by the orchestration MCP bearer. */
  readonly providerInstanceId?: ServerProvider["instanceId"];
}

type AgentOrchestrationCallerInput = ThreadId | AgentOrchestrationCaller;

const callerThreadId = (caller: AgentOrchestrationCallerInput): ThreadId =>
  typeof caller === "string" ? caller : caller.threadId;

export interface AgentOrchestrationServiceShape {
  readonly spawn: (
    caller: AgentOrchestrationCallerInput,
    input: AgentSpawnInput,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  readonly send: (
    caller: AgentOrchestrationCallerInput,
    threadId: ThreadId,
    brief: string,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  readonly inspect: (
    caller: AgentOrchestrationCallerInput,
    threadId: ThreadId,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  readonly wait: (
    caller: AgentOrchestrationCallerInput,
    input: AgentWaitInput,
  ) => Effect.Effect<{ child: AgentChildSnapshot; timedOut: boolean }, AgentOrchestrationError>;
  readonly respond: (
    caller: AgentOrchestrationCallerInput,
    input: AgentRespondInput,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  readonly interrupt: (
    caller: AgentOrchestrationCallerInput,
    threadId: ThreadId,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  readonly archive: (
    caller: AgentOrchestrationCallerInput,
    threadId: ThreadId,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  readonly unarchive: (
    caller: AgentOrchestrationCallerInput,
    threadId: ThreadId,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  readonly interruptAndDrain: (
    callerThreadId: ThreadId,
    threadId: ThreadId,
    deadlineMs?: number,
  ) => Effect.Effect<AgentChildSnapshot, AgentOrchestrationError>;
  /** Serializes ordinary Root turn admission with parent archive and agent mutations. */
  readonly acquireRootMutation: (
    callerThreadId: ThreadId,
  ) => Effect.Effect<{ readonly release: Effect.Effect<void> }>;
  /** Drain and archive direct children. The caller may commit Root archive only after this succeeds. */
  readonly prepareParentArchive: (
    callerThreadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<AgentChildSnapshot>, AgentOrchestrationError>;
  /** Holds Root mutation admission until the caller commits or cancels parent archive. */
  readonly acquireParentArchive: (callerThreadId: ThreadId) => Effect.Effect<
    {
      readonly children: ReadonlyArray<AgentChildSnapshot>;
      readonly release: Effect.Effect<void>;
    },
    AgentOrchestrationError
  >;
  /** Called only after trust=false is committed and orchestration credentials are revoked. */
  readonly cleanupDisabledProject: (
    callerThreadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<AgentChildSnapshot>, AgentOrchestrationError>;
  readonly drainLifecycle: Effect.Effect<void>;
}

export class AgentOrchestrationService extends Context.Service<
  AgentOrchestrationService,
  AgentOrchestrationServiceShape
>()("t3/agentOrchestration/AgentOrchestrationService") {
  static readonly layer = Layer.effect(
    AgentOrchestrationService,
    Effect.gen(function* () {
      const backend = yield* AgentOrchestrationBackend;
      const crypto = yield* Crypto.Crypto;
      const workerScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(workerScope, Exit.void));
      const uuid = crypto.randomUUIDv4.pipe(Effect.orDie);
      const commandId = uuid.pipe(Effect.map((value) => CommandId.make(`agent-${value}`)));
      const eventId = uuid.pipe(Effect.map((value) => EventId.make(`agent-${value}`)));

      const getRoot = Effect.fn("AgentOrchestration.getRoot")(function* (
        caller: AgentOrchestrationCallerInput,
      ) {
        const threadId = callerThreadId(caller);
        const root = yield* backend.getThread(threadId).pipe(Effect.mapError(mapBackendError));
        if (root === null)
          return yield* fail("not_found", `Caller thread ${threadId} does not exist.`);
        if (!isTopLevelAgentRoot(root))
          return yield* fail(
            "forbidden",
            "Only top-level Root threads may orchestrate child agents.",
          );
        if (root.archivedAt !== null)
          return yield* fail("forbidden", "Archived Root threads cannot orchestrate child agents.");
        const project = yield* backend
          .getProject(root.projectId)
          .pipe(Effect.mapError(mapBackendError));
        if (project === null || project.deletedAt !== null)
          return yield* fail("not_found", `Project ${root.projectId} is unavailable.`);
        if (!project.agentOrchestrationTrusted)
          return yield* fail("forbidden", "Agent orchestration is disabled for this project.");
        const providers = yield* backend.getProviders.pipe(Effect.mapError(mapBackendError));
        const rootProvider = providers.find(
          (provider) => provider.instanceId === root.modelSelection.instanceId,
        );
        if (rootProvider?.driver !== ProviderDriverKind.make("codex"))
          return yield* fail("forbidden", "Only a Codex Root may orchestrate child agents.");
        if (
          typeof caller !== "string" &&
          caller.providerInstanceId !== undefined &&
          caller.providerInstanceId !== rootProvider.instanceId
        ) {
          return yield* fail(
            "forbidden",
            "The orchestration credential is not bound to the Root's active provider instance.",
          );
        }
        if (
          !rootProvider.enabled ||
          !rootProvider.installed ||
          rootProvider.status === "disabled" ||
          rootProvider.availability === "unavailable"
        ) {
          return yield* fail(
            "forbidden",
            "The Root's provider instance is no longer enabled and available.",
          );
        }
        return { root, project, providers };
      });

      const getChild = Effect.fn("AgentOrchestration.getChild")(function* (
        caller: AgentOrchestrationCallerInput,
        threadId: ThreadId,
      ) {
        const context = yield* getRoot(caller);
        const child = yield* backend.getThread(threadId).pipe(Effect.mapError(mapBackendError));
        if (child === null)
          return yield* fail("not_found", `Child thread ${threadId} does not exist.`);
        if (
          child.agentParentThreadId !== context.root.id ||
          child.projectId !== context.root.projectId
        ) {
          return yield* fail("forbidden", `Thread ${threadId} is not a direct child of this Root.`);
        }
        return { ...context, child };
      });

      const emitParentTask = Effect.fn("AgentOrchestration.emitParentTask")(function* (
        parentThreadId: ThreadId,
        child: OrchestrationThread,
        status: AgentChildSnapshot["status"],
        started = false,
      ) {
        const createdAt = yield* timestamp;
        const terminal = status === "failed" || status === "interrupted" || status === "completed";
        const kind = started ? "task.started" : terminal ? "task.completed" : "task.progress";
        const payload = {
          taskId: child.id,
          agentKind: "agent",
          timelineBypass: true,
          title: child.title,
          model: child.modelSelection.model,
          ...(kind === "task.started"
            ? { description: child.title }
            : kind === "task.completed"
              ? {
                  status:
                    status === "failed"
                      ? "failed"
                      : status === "interrupted"
                        ? "stopped"
                        : "completed",
                }
              : { description: child.title, status }),
        };
        yield* backend
          .dispatch({
            type: "thread.activity.append",
            commandId: yield* commandId,
            threadId: parentThreadId,
            activity: {
              id: yield* eventId,
              tone: status === "failed" ? "error" : "info",
              kind,
              summary: `${child.title}: ${status}`,
              payload,
              turnId: null,
              createdAt,
            },
            createdAt,
          })
          .pipe(Effect.mapError(mapBackendError));
        if (
          status === "idle" &&
          child.latestTurn?.state === "completed" &&
          child.settledAt === null
        ) {
          yield* backend
            .dispatch({ type: "thread.settle", commandId: yield* commandId, threadId: child.id })
            .pipe(Effect.mapError(mapBackendError));
        }
      });

      const recordIdentityFailure = Effect.fn("AgentOrchestration.recordIdentityFailure")(
        function* (rootThreadId: ThreadId, child: OrchestrationThread, detail: string) {
          const createdAt = yield* timestamp;
          yield* backend
            .dispatch({
              type: "thread.activity.append",
              commandId: yield* commandId,
              threadId: child.id,
              activity: {
                id: yield* eventId,
                tone: "error",
                kind: "agent.identity.failed",
                summary: "Provider model identity verification failed",
                payload: { detail },
                turnId: child.latestTurn?.turnId ?? null,
                createdAt,
              },
              createdAt,
            })
            .pipe(Effect.mapError(mapBackendError));
          yield* emitParentTask(rootThreadId, child, "failed");
        },
      );

      interface LifecycleItem {
        readonly root: OrchestrationThread;
        readonly child: OrchestrationThread;
        readonly provider: ServerProvider;
      }
      const lifecycleState = new Map<
        ThreadId,
        {
          lastStatus: AgentChildSnapshot["status"] | undefined;
          identityFailed: boolean;
        }
      >();
      const monitoredChildIds = new Set<ThreadId>();
      const rootMutationGates = new Map<ThreadId, Semaphore.Semaphore>();
      const getRootMutationGate = (rootId: ThreadId) => {
        let gate = rootMutationGates.get(rootId);
        if (gate === undefined) {
          gate = Semaphore.makeUnsafe(1);
          rootMutationGates.set(rootId, gate);
        }
        return gate;
      };
      const withRootMutationGate = <A, E, R>(rootId: ThreadId, effect: Effect.Effect<A, E, R>) =>
        getRootMutationGate(rootId).withPermits(1)(effect);
      const acquireRootMutation: AgentOrchestrationServiceShape["acquireRootMutation"] = Effect.fn(
        "AgentOrchestration.acquireRootMutation",
      )((rootId) => {
        const gate = getRootMutationGate(rootId);
        return Effect.uninterruptibleMask((restore) =>
          restore(gate.take(1)).pipe(Effect.as({ release: gate.release(1) })),
        );
      });
      const lifecycleWorker = yield* makeDrainableWorker<LifecycleItem, never, never>(
        ({ root, child, provider }) =>
          Effect.gen(function* () {
            const durablyIdentityFailed = child.activities.some(
              (activity) => activity.kind === "agent.identity.failed",
            );
            const state = lifecycleState.get(child.id) ?? {
              lastStatus: durablyIdentityFailed ? ("failed" as const) : undefined,
              identityFailed: durablyIdentityFailed,
            };
            if (!lifecycleState.has(child.id)) lifecycleState.set(child.id, state);
            const mismatch = identityFailure(child, provider, child.modelSelection);
            const missingPostHandshakeIdentity =
              ((provider.driver !== ProviderDriverKind.make("claudeAgent") &&
                (child.session?.status === "running" || child.session?.status === "ready")) ||
                child.latestTurn?.state === "completed" ||
                child.latestTurn?.state === "error" ||
                child.session?.status === "stopped" ||
                child.session?.status === "error") &&
              !identityReady(child, provider, child.modelSelection);
            if ((mismatch !== undefined || missingPostHandshakeIdentity) && !state.identityFailed) {
              lifecycleState.set(child.id, { lastStatus: "failed", identityFailed: true });
              const createdAt = yield* timestamp;
              yield* backend
                .dispatch({
                  type: "thread.turn.interrupt",
                  commandId: yield* commandId,
                  threadId: child.id,
                  createdAt,
                })
                .pipe(Effect.ignore);
              yield* backend
                .dispatch({
                  type: "thread.session.stop",
                  commandId: yield* commandId,
                  threadId: child.id,
                  createdAt,
                })
                .pipe(Effect.ignore);
              yield* backend
                .dispatch({
                  type: "thread.settle",
                  commandId: yield* commandId,
                  threadId: child.id,
                })
                .pipe(Effect.ignore);
              yield* recordIdentityFailure(
                root.id,
                child,
                mismatch ?? "Provider did not report a matching model identity.",
              ).pipe(Effect.ignore);
              return;
            }
            if (state.identityFailed) return;
            const status = childSnapshot(child).status;
            if (state.lastStatus === status) return;
            lifecycleState.set(child.id, { lastStatus: status, identityFailed: false });
            yield* emitParentTask(root.id, child, status).pipe(Effect.ignore);
          }),
      ).pipe(Effect.provideService(Scope.Scope, workerScope));

      const observeThenDispatch = Effect.fn("AgentOrchestration.observeThenDispatch")(function* (
        child: OrchestrationThread,
        command: OrchestrationCommand,
      ) {
        yield* backend.dispatch(command).pipe(Effect.mapError(mapBackendError));
        const projected = yield* backend.getThread(child.id).pipe(Effect.mapError(mapBackendError));
        if (projected === null)
          return yield* fail(
            "not_found",
            `Child thread ${child.id} disappeared after ${command.type}.`,
          );
        return childSnapshot(projected);
      });

      const monitorChild = Effect.fn("AgentOrchestration.monitorChild")(function* (
        root: OrchestrationThread,
        child: OrchestrationThread,
        provider: ServerProvider,
      ) {
        if (monitoredChildIds.has(child.id)) return;
        monitoredChildIds.add(child.id);
        const observation = yield* backend.observeChild(child.id).pipe(
          Effect.mapError(mapBackendError),
          Effect.tapError(() => Effect.sync(() => monitoredChildIds.delete(child.id))),
        );
        yield* Effect.gen(function* () {
          yield* lifecycleWorker.enqueue({ root, child: observation.initial, provider });
          yield* observation.changes.pipe(
            Stream.mapError(mapBackendError),
            Stream.runForEach((state) => lifecycleWorker.enqueue({ root, child: state, provider })),
          );
        }).pipe(
          Effect.ensuring(observation.close),
          Effect.ensuring(Effect.sync(() => monitoredChildIds.delete(child.id))),
          Effect.ignore,
          Effect.forkIn(workerScope),
          Effect.orDie,
        );
      });

      const spawn: AgentOrchestrationServiceShape["spawn"] = Effect.fn("AgentOrchestration.spawn")(
        function* (callerThreadId, input) {
          const { root, project, providers } = yield* getRoot(callerThreadId);
          const provider = yield* Effect.try({
            try: () => resolveProvider(providers, input),
            catch: (error) =>
              Schema.is(AgentOrchestrationError)(error)
                ? error
                : fail(
                    "invalid_target",
                    error instanceof Error ? error.message : "Invalid provider target.",
                  ),
          });
          const modelSelection: ModelSelection = {
            instanceId: provider.instanceId,
            model: input.model,
            ...(input.options === undefined ? {} : { options: input.options }),
          };
          const started = yield* backend
            .startChildTurn({
              parentThreadId: root.id,
              project,
              modelSelection,
              runtimeMode: root.runtimeMode,
              interactionMode: input.interactionMode,
              brief: input.brief,
              isolation: input.isolation,
            })
            .pipe(Effect.mapError((error) => fail("bootstrap_failed", error.message)));
          const child = started.child;
          const startup = Stream.make(started.observation.initial).pipe(
            Stream.concat(started.observation.changes),
          );
          const barrier = yield* Stream.runHead(
            startup.pipe(
              Stream.filter((state) =>
                isStartupIdentityBarrierState(state, provider, modelSelection),
              ),
            ),
          ).pipe(Effect.mapError(mapBackendError), Effect.ensuring(started.observation.close));
          const barrierThread = Option.getOrElse(barrier, () => child);
          const mismatch = identityFailure(barrierThread, provider, modelSelection);
          if (mismatch !== undefined || !identityReady(barrierThread, provider, modelSelection)) {
            const createdAt = yield* timestamp;
            const detail =
              mismatch ?? "Provider exited before reporting the selected model identity.";
            yield* recordIdentityFailure(root.id, barrierThread, detail);
            yield* backend
              .dispatch({
                type: "thread.turn.interrupt",
                commandId: yield* commandId,
                threadId: child.id,
                createdAt,
              })
              .pipe(Effect.mapError(mapBackendError));
            yield* backend
              .dispatch({
                type: "thread.session.stop",
                commandId: yield* commandId,
                threadId: child.id,
                createdAt,
              })
              .pipe(Effect.mapError(mapBackendError));
            yield* backend
              .dispatch({ type: "thread.settle", commandId: yield* commandId, threadId: child.id })
              .pipe(Effect.mapError(mapBackendError), Effect.ignore);
            return yield* fail("bootstrap_failed", detail, { childThreadId: child.id });
          }
          // Re-read immutable lineage rather than trusting the bootstrap return.
          const verified = yield* getChild(callerThreadId, child.id);
          const initialStatus = childSnapshot(verified.child).status;
          yield* emitParentTask(root.id, verified.child, initialStatus, true);
          // task.started folds as running. Seed that observed state so a child
          // which already completed during the startup barrier emits idle next.
          lifecycleState.set(verified.child.id, { lastStatus: "running", identityFailed: false });
          yield* monitorChild(root, verified.child, provider);
          return childSnapshot(verified.child);
        },
      );

      const inspect: AgentOrchestrationServiceShape["inspect"] = Effect.fn(
        "AgentOrchestration.inspect",
      )(function* (callerThreadId, threadId) {
        return childSnapshot((yield* getChild(callerThreadId, threadId)).child);
      });

      const send: AgentOrchestrationServiceShape["send"] = Effect.fn("AgentOrchestration.send")(
        function* (callerThreadId, threadId, brief) {
          const { root, providers, child } = yield* getChild(callerThreadId, threadId);
          if (child.archivedAt !== null)
            return yield* fail(
              "invalid_state",
              "Archived children must be unarchived before sending work.",
              { childThreadId: threadId },
            );
          if (child.activities.some((activity) => activity.kind === "agent.identity.failed")) {
            return yield* fail(
              "invalid_state",
              "A child with failed provider identity cannot be reused; start a new child.",
              { childThreadId: threadId },
            );
          }
          if (child.latestTurn?.state === "running")
            return yield* fail("invalid_state", "The child already has a running turn.", {
              childThreadId: threadId,
            });
          const provider = yield* Effect.try({
            try: () =>
              resolveProvider(providers, {
                target: { instanceId: child.modelSelection.instanceId },
                model: child.modelSelection.model,
                ...(child.modelSelection.options === undefined
                  ? {}
                  : { options: child.modelSelection.options }),
                brief,
                isolation: "shared",
                interactionMode: child.interactionMode,
              }),
            catch: (error) =>
              Schema.is(AgentOrchestrationError)(error)
                ? error
                : fail(
                    "invalid_target",
                    error instanceof Error ? error.message : "Invalid provider target.",
                  ),
          });
          const createdAt = yield* timestamp;
          const result = yield* observeThenDispatch(child, {
            type: "thread.turn.start",
            commandId: yield* commandId,
            threadId,
            message: {
              messageId: MessageId.make(`agent-${yield* uuid}`),
              role: "user",
              text: brief,
              attachments: [],
            },
            modelSelection: child.modelSelection,
            runtimeMode: child.runtimeMode,
            interactionMode: child.interactionMode,
            createdAt,
          });
          const projected = yield* backend
            .getThread(threadId)
            .pipe(Effect.mapError(mapBackendError));
          if (projected === null)
            return yield* fail(
              "not_found",
              `Child thread ${threadId} disappeared after turn start.`,
            );
          yield* monitorChild(root, projected, provider);
          return result;
        },
      );

      const wait: AgentOrchestrationServiceShape["wait"] = Effect.fn("AgentOrchestration.wait")(
        function* (callerThreadId, input) {
          const { child } = yield* getChild(callerThreadId, input.threadId);
          const observation = yield* backend
            .observeChild(child.id)
            .pipe(Effect.mapError(mapBackendError));
          return yield* Effect.gen(function* () {
            const initial = childSnapshot(observation.initial);
            if (input.cursor === undefined || input.cursor !== initial.cursor)
              return { child: initial, timedOut: false };
            const next = Stream.runHead(
              observation.changes.pipe(
                Stream.mapError(mapBackendError),
                Stream.filter((value) => cursorFor(value) !== input.cursor),
              ),
            );
            if (input.timeoutMs === undefined) {
              const value = yield* next;
              return {
                child: childSnapshot(Option.getOrElse(value, () => observation.initial)),
                timedOut: false,
              };
            }
            const value = yield* next.pipe(
              Effect.timeoutOption(Duration.millis(input.timeoutMs)),
              Effect.map(Option.flatten),
            );
            if (Option.isSome(value)) return { child: childSnapshot(value.value), timedOut: false };
            const current = yield* backend
              .getThread(child.id)
              .pipe(Effect.mapError(mapBackendError));
            return { child: childSnapshot(current ?? observation.initial), timedOut: true };
          }).pipe(Effect.ensuring(observation.close));
        },
      );

      const respond: AgentOrchestrationServiceShape["respond"] = Effect.fn(
        "AgentOrchestration.respond",
      )(function* (callerThreadId, input) {
        const { child } = yield* getChild(callerThreadId, input.threadId);
        // The provider contract supports persistent approvals, but agent
        // orchestration does not: those decisions belong in Ben's Root chat.
        // Keep this guard even though AgentRespondInput's schema excludes the
        // values, because service callers are not necessarily schema-decoded.
        if (
          input.kind === "approval" &&
          (input.decision as string) !== "accept" &&
          (input.decision as string) !== "decline" &&
          (input.decision as string) !== "cancel"
        ) {
          return yield* fail(
            "forbidden",
            "Persistent approval decisions must be relayed to Ben in the Root chat.",
            { childThreadId: child.id },
          );
        }
        const createdAt = yield* timestamp;
        const command: OrchestrationCommand =
          input.kind === "approval"
            ? {
                type: "thread.approval.respond",
                commandId: yield* commandId,
                threadId: input.threadId,
                requestId: input.requestId,
                decision: input.decision,
                createdAt,
              }
            : {
                type: "thread.user-input.respond",
                commandId: yield* commandId,
                threadId: input.threadId,
                requestId: input.requestId,
                answers: input.answers,
                createdAt,
              };
        return yield* observeThenDispatch(child, command);
      });

      const simpleCommand = (
        type: "thread.turn.interrupt" | "thread.archive" | "thread.unarchive",
      ) =>
        Effect.fn(`AgentOrchestration.${type}`)(function* (
          callerThreadId: AgentOrchestrationCallerInput,
          threadId: ThreadId,
        ) {
          const { child } = yield* getChild(callerThreadId, threadId);
          if (type === "thread.unarchive" && child.archivedAt === null) return childSnapshot(child);
          if (
            type === "thread.turn.interrupt" &&
            childSnapshot(child).status !== "running" &&
            childSnapshot(child).status !== "waiting"
          ) {
            return childSnapshot(child);
          }
          const createdAt = yield* timestamp;
          const command =
            type === "thread.turn.interrupt"
              ? ({
                  type,
                  commandId: yield* commandId,
                  threadId,
                  ...(child.latestTurn?.turnId ? { turnId: child.latestTurn.turnId } : {}),
                  createdAt,
                } as OrchestrationCommand)
              : ({ type, commandId: yield* commandId, threadId } as OrchestrationCommand);
          return yield* observeThenDispatch(child, command);
        });
      const interrupt = simpleCommand("thread.turn.interrupt");
      const unarchive = simpleCommand("thread.unarchive");

      const drainKnownChild = Effect.fn("AgentOrchestration.drainKnownChild")(function* (
        child: OrchestrationThread,
        deadlineMs = 30_000,
      ) {
        const current = childSnapshot(child);
        if (current.status !== "running" && current.status !== "waiting") return current;
        const first = yield* backend.observeChild(child.id).pipe(Effect.mapError(mapBackendError));
        return yield* Effect.gen(function* () {
          const createdAt = yield* timestamp;
          yield* backend
            .dispatch({
              type: "thread.turn.interrupt",
              commandId: yield* commandId,
              threadId: child.id,
              ...(child.latestTurn?.turnId ? { turnId: child.latestTurn.turnId } : {}),
              createdAt,
            })
            .pipe(Effect.mapError(mapBackendError));
          const drained = yield* Stream.runHead(
            first.changes.pipe(
              Stream.mapError(mapBackendError),
              Stream.filter(
                (state) => !["running", "waiting"].includes(childSnapshot(state).status),
              ),
            ),
          ).pipe(Effect.timeoutOption(Duration.millis(deadlineMs)), Effect.map(Option.flatten));
          if (Option.isSome(drained)) return childSnapshot(drained.value);

          const second = yield* backend
            .observeChild(child.id)
            .pipe(Effect.mapError(mapBackendError));
          return yield* Effect.gen(function* () {
            const secondInitial = childSnapshot(second.initial);
            if (secondInitial.status !== "running" && secondInitial.status !== "waiting") {
              return secondInitial;
            }
            const stopAt = yield* timestamp;
            yield* backend
              .dispatch({
                type: "thread.session.stop",
                commandId: yield* commandId,
                threadId: child.id,
                createdAt: stopAt,
              })
              .pipe(Effect.mapError(mapBackendError));
            const stopped = yield* Stream.runHead(
              second.changes.pipe(
                Stream.mapError(mapBackendError),
                Stream.filter(
                  (state) => !["running", "waiting"].includes(childSnapshot(state).status),
                ),
              ),
            ).pipe(Effect.timeoutOption(Duration.millis(deadlineMs)), Effect.map(Option.flatten));
            if (Option.isNone(stopped))
              return yield* fail(
                "drain_timeout",
                `Child ${child.id} did not drain after interrupt and session stop.`,
                { childThreadId: child.id },
              );
            return childSnapshot(stopped.value);
          }).pipe(Effect.ensuring(second.close));
        }).pipe(Effect.ensuring(first.close));
      });

      const archiveKnownChild = Effect.fn("AgentOrchestration.archiveKnownChild")(function* (
        child: OrchestrationThread,
      ) {
        const latest = yield* backend.getThread(child.id).pipe(Effect.mapError(mapBackendError));
        const target = latest ?? child;
        if (target.archivedAt !== null) return childSnapshot(target);
        return yield* observeThenDispatch(target, {
          type: "thread.archive",
          commandId: yield* commandId,
          threadId: target.id,
        });
      });

      const archive: AgentOrchestrationServiceShape["archive"] = Effect.fn(
        "AgentOrchestration.archive",
      )(function* (callerThreadId, threadId) {
        const { child } = yield* getChild(callerThreadId, threadId);
        yield* drainKnownChild(child);
        return yield* archiveKnownChild(child);
      });

      const interruptAndDrain: AgentOrchestrationServiceShape["interruptAndDrain"] = Effect.fn(
        "AgentOrchestration.interruptAndDrain",
      )(function* (callerThreadId, threadId, deadlineMs = 30_000) {
        const { child } = yield* getChild(callerThreadId, threadId);
        return yield* drainKnownChild(child, deadlineMs);
      });

      const revalidateDirectChild = Effect.fn("AgentOrchestration.revalidateDirectChild")(
        function* (parent: OrchestrationThread, threadId: ThreadId) {
          const child = yield* backend.getThread(threadId).pipe(Effect.mapError(mapBackendError));
          if (child === null)
            return yield* fail("not_found", `Child thread ${threadId} no longer exists.`);
          if (child.agentParentThreadId !== parent.id || child.projectId !== parent.projectId) {
            return yield* fail(
              "forbidden",
              `Thread ${threadId} is no longer a direct child of this Root.`,
            );
          }
          return child;
        },
      );

      const drainAndArchiveChildren = Effect.fn("AgentOrchestration.drainAndArchiveChildren")(
        function* (callerThreadId: ThreadId) {
          // getRoot deliberately re-reads trust/archive/lineage immediately before
          // cleanup. Trust-off integration uses cleanupDisabledProject below,
          // where trust=false is the required precondition instead.
          const root = yield* backend
            .getThread(callerThreadId)
            .pipe(Effect.mapError(mapBackendError));
          if (
            root === null ||
            (root.agentParentThreadId ?? null) !== null ||
            root.archivedAt !== null
          )
            return yield* fail(
              "forbidden",
              "Cleanup requires an unarchived top-level Root thread.",
            );
          const project = yield* backend
            .getProject(root.projectId)
            .pipe(Effect.mapError(mapBackendError));
          if (project === null || project.deletedAt !== null)
            return yield* fail("not_found", `Project ${root.projectId} is unavailable.`);
          const children = yield* backend
            .listDirectChildren(callerThreadId)
            .pipe(Effect.mapError(mapBackendError));
          const results: Array<AgentChildSnapshot> = [];
          for (const listedChild of children) {
            const child = yield* revalidateDirectChild(root, listedChild.id);
            if (child.archivedAt !== null) {
              results.push(childSnapshot(child));
              continue;
            }
            const state = childSnapshot(child);
            if (state.status === "running" || state.status === "waiting") {
              yield* drainKnownChild(child);
            }
            results.push(yield* archiveKnownChild(child));
          }
          return results;
        },
      );

      const prepareParentArchive: AgentOrchestrationServiceShape["prepareParentArchive"] =
        Effect.fn("AgentOrchestration.prepareParentArchive")(function* (callerThreadId) {
          const root = yield* backend
            .getThread(callerThreadId)
            .pipe(Effect.mapError(mapBackendError));
          if (
            root === null ||
            (root.agentParentThreadId ?? null) !== null ||
            root.archivedAt !== null
          ) {
            return yield* fail(
              "forbidden",
              "Parent archive preparation requires an unarchived top-level Root.",
            );
          }
          const project = yield* backend
            .getProject(root.projectId)
            .pipe(Effect.mapError(mapBackendError));
          if (project === null || project.deletedAt !== null)
            return yield* fail("not_found", `Project ${root.projectId} is unavailable.`);
          return yield* drainAndArchiveChildren(callerThreadId);
        });

      const cleanupDisabledProject: AgentOrchestrationServiceShape["cleanupDisabledProject"] =
        Effect.fn("AgentOrchestration.cleanupDisabledProject")(function* (callerThreadId) {
          const root = yield* backend
            .getThread(callerThreadId)
            .pipe(Effect.mapError(mapBackendError));
          if (
            root === null ||
            (root.agentParentThreadId ?? null) !== null ||
            root.archivedAt !== null
          )
            return yield* fail(
              "forbidden",
              "Trust-off cleanup requires an unarchived top-level Root thread.",
            );
          const project = yield* backend
            .getProject(root.projectId)
            .pipe(Effect.mapError(mapBackendError));
          if (project === null || project.deletedAt !== null)
            return yield* fail("not_found", `Project ${root.projectId} is unavailable.`);
          if (project.agentOrchestrationTrusted)
            return yield* fail(
              "invalid_state",
              "Trust must be disabled before child cleanup begins.",
            );
          // This service cannot revoke MCP credentials; the caller MUST do so before
          // invoking this method. Trust-off keeps the old fleet visible and settled.
          const children = yield* backend
            .listDirectChildren(callerThreadId)
            .pipe(Effect.mapError(mapBackendError));
          const results: Array<AgentChildSnapshot> = [];
          for (const listedChild of children) {
            const child = yield* revalidateDirectChild(root, listedChild.id);
            if (child.archivedAt !== null) {
              results.push(childSnapshot(child));
              continue;
            }
            yield* drainKnownChild(child);
            const target = yield* revalidateDirectChild(root, child.id);
            if (target.settledAt === null) {
              results.push(
                yield* observeThenDispatch(target, {
                  type: "thread.settle",
                  commandId: yield* commandId,
                  threadId: target.id,
                }),
              );
            } else results.push(childSnapshot(target));
          }
          return results;
        });

      const acquireParentArchive: AgentOrchestrationServiceShape["acquireParentArchive"] =
        Effect.fn("AgentOrchestration.acquireParentArchive")(function* (callerThreadId) {
          const lease = yield* acquireRootMutation(callerThreadId);
          return yield* prepareParentArchive(callerThreadId).pipe(
            Effect.onExit((exit) => (Exit.isFailure(exit) ? lease.release : Effect.void)),
            Effect.map((children) => ({ children, release: lease.release })),
          );
        });

      const roots = yield* backend.listAgentRoots.pipe(Effect.mapError(mapBackendError));
      const providersAtStartup = yield* backend.getProviders.pipe(Effect.mapError(mapBackendError));
      for (const root of roots) {
        if (root.archivedAt !== null) continue;
        const project = yield* backend
          .getProject(root.projectId)
          .pipe(Effect.mapError(mapBackendError));
        if (project?.agentOrchestrationTrusted !== true) continue;
        const children = yield* backend
          .listDirectChildren(root.id)
          .pipe(Effect.mapError(mapBackendError));
        for (const child of children) {
          if (child.archivedAt !== null) continue;
          const provider = providersAtStartup.find(
            (candidate) => candidate.instanceId === child.modelSelection.instanceId,
          );
          if (provider !== undefined && ALLOWED_AGENT_DRIVERS.has(provider.driver))
            yield* monitorChild(root, child, provider);
        }
      }
      const drainLifecycle = lifecycleWorker.drain.pipe(
        Effect.andThen(Scope.close(workerScope, Exit.void)),
        Effect.orDie,
      );
      return AgentOrchestrationService.of({
        spawn: (caller, input) =>
          withRootMutationGate(callerThreadId(caller), spawn(caller, input)),
        send: (caller, threadId, brief) =>
          withRootMutationGate(callerThreadId(caller), send(caller, threadId, brief)),
        inspect,
        wait,
        respond: (caller, input) =>
          withRootMutationGate(callerThreadId(caller), respond(caller, input)),
        interrupt: (caller, threadId) =>
          withRootMutationGate(callerThreadId(caller), interrupt(caller, threadId)),
        archive: (caller, threadId) =>
          withRootMutationGate(callerThreadId(caller), archive(caller, threadId)),
        unarchive: (caller, threadId) =>
          withRootMutationGate(callerThreadId(caller), unarchive(caller, threadId)),
        interruptAndDrain: (rootId, threadId, deadlineMs) =>
          withRootMutationGate(rootId, interruptAndDrain(rootId, threadId, deadlineMs)),
        acquireRootMutation,
        prepareParentArchive: (rootId) =>
          withRootMutationGate(rootId, prepareParentArchive(rootId)),
        acquireParentArchive,
        cleanupDisabledProject: (rootId) =>
          withRootMutationGate(rootId, cleanupDisabledProject(rootId)),
        drainLifecycle,
      });
    }),
  );
}
