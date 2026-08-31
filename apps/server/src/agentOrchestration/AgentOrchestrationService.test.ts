import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApprovalRequestId,
  EventId,
  IsoDateTime,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationThread,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";

import {
  AgentOrchestrationService,
  childSnapshot,
  isStartupIdentityBarrierState,
  isTopLevelAgentRoot,
  resolveProvider,
} from "./AgentOrchestrationService.ts";
import { AgentOrchestrationBackend } from "./AgentOrchestrationBackend.ts";
import { AgentOrchestrationError } from "@t3tools/contracts";

const time = IsoDateTime.make("2026-08-31T12:00:00.000Z");
const provider = (driver: string, instanceId = driver): ServerProvider => ({
  instanceId: ProviderInstanceId.make(instanceId),
  driver: ProviderDriverKind.make(driver),
  enabled: true,
  installed: true,
  version: "1",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: time,
  models: [
    {
      slug: "model-1",
      name: "Model 1",
      isCustom: false,
      capabilities: {
        optionDescriptors: [
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: [
              { id: "low", label: "Low" },
              { id: "high", label: "High" },
            ],
          },
        ],
      },
    },
  ],
  slashCommands: [],
  skills: [],
});

const baseThread = (): OrchestrationThread => ({
  id: ThreadId.make("child"),
  projectId: ProjectId.make("project"),
  agentParentThreadId: ThreadId.make("root"),
  title: "Child",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "model-1" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: {
    turnId: "turn" as never,
    state: "running",
    requestedAt: time,
    startedAt: time,
    completedAt: null,
    assistantMessageId: null,
  },
  createdAt: time,
  updatedAt: time,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  deletedAt: null,
  messages: [],
  proposedPlans: [],
  activities: [],
  checkpoints: [],
  session: null,
});

describe("AgentOrchestrationService", () => {
  it("rejects ambiguous driver custody and returns exact candidates", () => {
    expect(() =>
      resolveProvider([provider("codex", "personal"), provider("codex", "work")], {
        target: { driver: ProviderDriverKind.make("codex") },
        model: "model-1",
        brief: "work",
        isolation: "shared",
        interactionMode: "default",
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "ambiguous_target",
        candidates: expect.arrayContaining([
          expect.objectContaining({ instanceId: "personal" }),
          expect.objectContaining({ instanceId: "work" }),
        ]),
      }),
    );
  });

  it("validates options against the selected model live catalog", () => {
    expect(() =>
      resolveProvider([provider("codex")], {
        target: { instanceId: ProviderInstanceId.make("codex") },
        model: "model-1",
        options: [{ id: "effort", value: "ultra" }],
        brief: "work",
        isolation: "shared",
        interactionMode: "default",
      }),
    ).toThrowError(expect.objectContaining({ code: "invalid_options" }));
  });

  it("rejects unsupported Cursor and OpenCode targets before bootstrap", () => {
    for (const driver of ["cursor", "opencode"]) {
      expect(() =>
        resolveProvider([provider(driver)], {
          target: { instanceId: ProviderInstanceId.make(driver) },
          model: "model-1",
          brief: "work",
          isolation: "shared",
          interactionMode: "default",
        }),
      ).toThrowError(expect.objectContaining({ code: "invalid_target" }));
    }
  });

  it("projects pending requests as waiting and closes them by request id", () => {
    const thread: OrchestrationThread = {
      ...baseThread(),
      activities: [
        {
          id: EventId.make("a1"),
          tone: "approval",
          kind: "approval.requested",
          summary: "Approve",
          payload: { requestId: "req-1" },
          turnId: null,
          createdAt: time,
        },
        {
          id: EventId.make("a2"),
          tone: "info",
          kind: "approval.resolved",
          summary: "Resolved",
          payload: { requestId: "req-1" },
          turnId: null,
          createdAt: time,
        },
        {
          id: EventId.make("a3"),
          tone: "approval",
          kind: "user-input.requested",
          summary: "Question",
          payload: { requestId: "req-2" },
          turnId: null,
          createdAt: time,
        },
      ],
    };
    const result = childSnapshot(thread);
    expect(result.status).toBe("waiting");
    expect(result.pendingRequests).toEqual([
      {
        kind: "user-input",
        requestId: ApprovalRequestId.make("req-2"),
        payload: { requestId: "req-2" },
      },
    ]);
  });

  it("returns schema-backed orchestration failures", () => {
    const error = new AgentOrchestrationError({ code: "forbidden", message: "no" });
    expect(Schema.is(AgentOrchestrationError)(error)).toBe(true);
  });

  it("treats absent legacy lineage as a top-level root", () => {
    expect(isTopLevelAgentRoot({} as Pick<OrchestrationThread, "agentParentThreadId">)).toBe(true);
    expect(isTopLevelAgentRoot({ agentParentThreadId: ThreadId.make("parent") })).toBe(false);
  });

  it.effect("binds MCP admission to the exact live Root provider instance", () => {
    const root: OrchestrationThread = {
      ...baseThread(),
      id: ThreadId.make("root"),
      agentParentThreadId: null,
    };
    const child = baseThread();
    let rootProvider = provider("codex");
    const backend = AgentOrchestrationBackend.of({
      getThread: (id) => Effect.succeed(id === root.id ? root : id === child.id ? child : null),
      getProject: () =>
        Effect.succeed({
          id: root.projectId,
          title: "Project",
          workspaceRoot: "C:/project",
          defaultModelSelection: root.modelSelection,
          agentOrchestrationTrusted: true,
          scripts: [],
          createdAt: time,
          updatedAt: time,
          deletedAt: null,
        }),
      listDirectChildren: () => Effect.succeed([child]),
      listAgentRoots: Effect.succeed([]),
      getProviders: Effect.sync(() => [rootProvider]),
      startChildTurn: () => Effect.die("unused"),
      observeChild: () => Effect.die("unused"),
      dispatch: () => Effect.die("unused"),
    });
    const layer = AgentOrchestrationService.layer.pipe(
      Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
      Layer.provide(NodeServices.layer),
    );
    return Effect.gen(function* () {
      const service = yield* AgentOrchestrationService;
      const wrongInstance = yield* service
        .inspect(
          {
            threadId: root.id,
            providerInstanceId: ProviderInstanceId.make("different-codex-account"),
          },
          child.id,
        )
        .pipe(Effect.flip);
      expect(wrongInstance.code).toBe("forbidden");

      rootProvider = { ...rootProvider, status: "disabled" };
      const disabled = yield* service
        .inspect(
          { threadId: root.id, providerInstanceId: root.modelSelection.instanceId },
          child.id,
        )
        .pipe(Effect.flip);
      expect(disabled.code).toBe("forbidden");
    }).pipe(Effect.provide(layer));
  });

  it("terminates the startup barrier when a running provider omits reported identity", () => {
    const thread: OrchestrationThread = {
      ...baseThread(),
      session: {
        threadId: ThreadId.make("child"),
        status: "running",
        providerName: "codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: "full-access",
        requestedModelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "model-1" },
        appliedModelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "model-1" },
        activeTurnId: null,
        lastError: null,
        updatedAt: time,
      },
    };
    expect(isStartupIdentityBarrierState(thread, provider("codex"), thread.modelSelection)).toBe(
      true,
    );
  });

  it("does not fail Claude on synthetic running before authoritative init identity", () => {
    const selection = { instanceId: ProviderInstanceId.make("claudeAgent"), model: "model-1" };
    const thread: OrchestrationThread = {
      ...baseThread(),
      modelSelection: selection,
      session: {
        threadId: ThreadId.make("child"),
        status: "running",
        providerName: "claudeAgent",
        providerInstanceId: selection.instanceId,
        runtimeMode: "full-access",
        requestedModelSelection: selection,
        appliedModelSelection: selection,
        activeTurnId: null,
        lastError: null,
        updatedAt: time,
      },
    };
    expect(isStartupIdentityBarrierState(thread, provider("claudeAgent"), selection)).toBe(false);
  });

  it.effect("rehydrates existing child monitors and drains their owned scope", () => {
    let observations = 0;
    let dispatches = 0;
    const root: OrchestrationThread = {
      ...baseThread(),
      id: ThreadId.make("root"),
      agentParentThreadId: null,
    };
    const child = baseThread();
    const codex = provider("codex");
    const backend = AgentOrchestrationBackend.of({
      getThread: (id) => Effect.succeed(id === root.id ? root : id === child.id ? child : null),
      getProject: () =>
        Effect.succeed({
          id: root.projectId,
          title: "Project",
          workspaceRoot: "C:/project",
          defaultModelSelection: root.modelSelection,
          agentOrchestrationTrusted: true,
          scripts: [],
          createdAt: time,
          updatedAt: time,
          deletedAt: null,
        }),
      listDirectChildren: () => Effect.succeed([child]),
      listAgentRoots: Effect.succeed([root]),
      getProviders: Effect.succeed([codex]),
      startChildTurn: () => Effect.die("unused"),
      observeChild: () => {
        observations += 1;
        return Effect.succeed({ initial: child, changes: Stream.make(child, child) });
      },
      dispatch: () =>
        Effect.sync(() => {
          dispatches += 1;
        }),
    });
    const layer = AgentOrchestrationService.layer.pipe(
      Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
      Layer.provide(NodeServices.layer),
    );
    return Effect.gen(function* () {
      const service = yield* AgentOrchestrationService;
      expect(observations).toBe(1);
      yield* Effect.yieldNow;
      yield* service.drainLifecycle;
      expect(dispatches).toBe(1);
    }).pipe(Effect.provide(layer));
  });

  it.effect(
    "handles a rehydrated identity mismatch once despite recursive unchanged observations",
    () => {
      const root: OrchestrationThread = {
        ...baseThread(),
        id: ThreadId.make("root"),
        agentParentThreadId: null,
      };
      const child: OrchestrationThread = {
        ...baseThread(),
        session: {
          threadId: ThreadId.make("child"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "full-access",
          requestedModelSelection: baseThread().modelSelection,
          appliedModelSelection: baseThread().modelSelection,
          providerReportedModelId: "wrong-model",
          activeTurnId: null,
          lastError: null,
          updatedAt: time,
        },
      };
      let dispatches = 0;
      const backend = AgentOrchestrationBackend.of({
        getThread: (id) => Effect.succeed(id === root.id ? root : child),
        getProject: () =>
          Effect.succeed({
            id: root.projectId,
            title: "Project",
            workspaceRoot: "C:/project",
            defaultModelSelection: root.modelSelection,
            agentOrchestrationTrusted: true,
            scripts: [],
            createdAt: time,
            updatedAt: time,
            deletedAt: null,
          }),
        listDirectChildren: () => Effect.succeed([child]),
        listAgentRoots: Effect.succeed([root]),
        getProviders: Effect.succeed([provider("codex")]),
        startChildTurn: () => Effect.die("unused"),
        observeChild: () => Effect.succeed({ initial: child, changes: Stream.make(child, child) }),
        dispatch: () =>
          Effect.sync(() => {
            dispatches += 1;
          }),
      });
      const layer = AgentOrchestrationService.layer.pipe(
        Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
        Layer.provide(NodeServices.layer),
      );
      return Effect.gen(function* () {
        const service = yield* AgentOrchestrationService;
        yield* Effect.yieldNow;
        yield* service.drainLifecycle;
        // interrupt + stop + settle + durable child marker + one parent task.failed
        expect(dispatches).toBe(5);
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect(
    "does not replay a durable identity failure after restart and rejects agent_send",
    () => {
      const root: OrchestrationThread = {
        ...baseThread(),
        id: ThreadId.make("root"),
        agentParentThreadId: null,
      };
      const child: OrchestrationThread = {
        ...baseThread(),
        activities: [
          {
            id: EventId.make("identity-failed"),
            tone: "error",
            kind: "agent.identity.failed",
            summary: "Provider model identity verification failed",
            payload: { detail: "wrong model" },
            turnId: null,
            createdAt: time,
          },
        ],
      };
      let dispatches = 0;
      const backend = AgentOrchestrationBackend.of({
        getThread: (id) => Effect.succeed(id === root.id ? root : child),
        getProject: () =>
          Effect.succeed({
            id: root.projectId,
            title: "Project",
            workspaceRoot: "C:/project",
            defaultModelSelection: root.modelSelection,
            agentOrchestrationTrusted: true,
            scripts: [],
            createdAt: time,
            updatedAt: time,
            deletedAt: null,
          }),
        listDirectChildren: () => Effect.succeed([child]),
        listAgentRoots: Effect.succeed([root]),
        getProviders: Effect.succeed([provider("codex")]),
        startChildTurn: () => Effect.die("unused"),
        observeChild: () => Effect.succeed({ initial: child, changes: Stream.make(child) }),
        dispatch: () =>
          Effect.sync(() => {
            dispatches += 1;
          }),
      });
      const layer = AgentOrchestrationService.layer.pipe(
        Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
        Layer.provide(NodeServices.layer),
      );
      return Effect.gen(function* () {
        const service = yield* AgentOrchestrationService;
        yield* Effect.yieldNow;
        yield* service.drainLifecycle;
        expect(dispatches).toBe(0);
        const error = yield* service.send(root.id, child.id, "retry").pipe(Effect.flip);
        expect(error.code).toBe("invalid_state");
        expect(error.message).toContain("start a new child");
        expect(dispatches).toBe(0);
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect(
    "projects idle after task.started when the initial child turn already completed",
    () => {
      const root: OrchestrationThread = {
        ...baseThread(),
        id: ThreadId.make("root"),
        agentParentThreadId: null,
      };
      const selection = baseThread().modelSelection;
      const child: OrchestrationThread = {
        ...baseThread(),
        latestTurn: { ...baseThread().latestTurn!, state: "completed", completedAt: time },
        session: {
          threadId: ThreadId.make("child"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: selection.instanceId,
          runtimeMode: "full-access",
          requestedModelSelection: selection,
          appliedModelSelection: selection,
          providerReportedModelId: selection.model,
          activeTurnId: null,
          lastError: null,
          updatedAt: time,
        },
      };
      const commands: Array<unknown> = [];
      const backend = AgentOrchestrationBackend.of({
        getThread: (id) => Effect.succeed(id === root.id ? root : child),
        getProject: () =>
          Effect.succeed({
            id: root.projectId,
            title: "Project",
            workspaceRoot: "C:/project",
            defaultModelSelection: root.modelSelection,
            agentOrchestrationTrusted: true,
            scripts: [],
            createdAt: time,
            updatedAt: time,
            deletedAt: null,
          }),
        listDirectChildren: () => Effect.succeed([]),
        listAgentRoots: Effect.succeed([]),
        getProviders: Effect.succeed([provider("codex")]),
        startChildTurn: () =>
          Effect.succeed({ child, observation: { initial: child, changes: Stream.empty } }),
        observeChild: () => Effect.succeed({ initial: child, changes: Stream.empty }),
        dispatch: (command) =>
          Effect.sync(() => {
            commands.push(command);
          }),
      });
      const layer = AgentOrchestrationService.layer.pipe(
        Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
        Layer.provide(NodeServices.layer),
      );
      return Effect.gen(function* () {
        const service = yield* AgentOrchestrationService;
        yield* service.spawn(root.id, {
          target: { instanceId: ProviderInstanceId.make("codex") },
          model: "model-1",
          brief: "fast work",
          isolation: "shared",
          interactionMode: "default",
        });
        yield* Effect.yieldNow;
        yield* service.drainLifecycle;
        const activityKinds = commands.flatMap((command) => {
          if (
            typeof command !== "object" ||
            command === null ||
            !("type" in command) ||
            command.type !== "thread.activity.append"
          )
            return [];
          return [String((command as unknown as { activity: { kind: string } }).activity.kind)];
        });
        expect(activityKinds).toEqual(["task.started", "task.progress"]);
        expect(
          commands.some(
            (command) =>
              typeof command === "object" &&
              command !== null &&
              "type" in command &&
              command.type === "thread.settle",
          ),
        ).toBe(true);
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect(
    "prepares a Root archive when trust is already false by archiving children first",
    () => {
      const root: OrchestrationThread = {
        ...baseThread(),
        id: ThreadId.make("root"),
        agentParentThreadId: null,
      };
      let child: OrchestrationThread = {
        ...baseThread(),
        latestTurn: { ...baseThread().latestTurn!, state: "completed", completedAt: time },
      };
      const trace: Array<string> = [];
      const backend = AgentOrchestrationBackend.of({
        getThread: (id) => Effect.succeed(id === root.id ? root : child),
        getProject: () =>
          Effect.succeed({
            id: root.projectId,
            title: "Project",
            workspaceRoot: "C:/project",
            defaultModelSelection: root.modelSelection,
            agentOrchestrationTrusted: false,
            scripts: [],
            createdAt: time,
            updatedAt: time,
            deletedAt: null,
          }),
        listDirectChildren: () => Effect.succeed([child]),
        listAgentRoots: Effect.succeed([]),
        getProviders: Effect.succeed([provider("codex")]),
        startChildTurn: () => Effect.die("unused"),
        observeChild: () =>
          Effect.succeed({ initial: child, changes: Stream.make({ ...child, archivedAt: time }) }),
        dispatch: (command) =>
          Effect.sync(() => {
            trace.push(command.type);
            if (command.type === "thread.archive") child = { ...child, archivedAt: time };
          }),
      });
      const layer = AgentOrchestrationService.layer.pipe(
        Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
        Layer.provide(NodeServices.layer),
      );
      return Effect.gen(function* () {
        const result = yield* (yield* AgentOrchestrationService).prepareParentArchive(root.id);
        expect(result[0]?.status).toBe("completed");
        expect(trace).toEqual(["thread.archive"]);
      }).pipe(Effect.provide(layer));
    },
  );

  it.live(
    "reports drain_timeout after interrupt and normal session stop both fail to drain",
    () => {
      const root: OrchestrationThread = {
        ...baseThread(),
        id: ThreadId.make("root"),
        agentParentThreadId: null,
      };
      const child = baseThread();
      const backend = AgentOrchestrationBackend.of({
        getThread: (id) => Effect.succeed(id === root.id ? root : child),
        getProject: () =>
          Effect.succeed({
            id: root.projectId,
            title: "Project",
            workspaceRoot: "C:/project",
            defaultModelSelection: root.modelSelection,
            agentOrchestrationTrusted: true,
            scripts: [],
            createdAt: time,
            updatedAt: time,
            deletedAt: null,
          }),
        listDirectChildren: () => Effect.succeed([child]),
        listAgentRoots: Effect.succeed([]),
        getProviders: Effect.succeed([provider("codex")]),
        startChildTurn: () => Effect.die("unused"),
        observeChild: () => Effect.succeed({ initial: child, changes: Stream.never }),
        dispatch: () => Effect.void,
      });
      const layer = AgentOrchestrationService.layer.pipe(
        Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
        Layer.provide(NodeServices.layer),
      );
      return Effect.gen(function* () {
        const error = yield* (yield* AgentOrchestrationService)
          .interruptAndDrain(root.id, child.id, 1)
          .pipe(Effect.flip);
        expect(error.code).toBe("drain_timeout");
      }).pipe(Effect.provide(layer));
    },
  );

  it.live("does not stop a child that drained between interrupt timeout and resubscribe", () => {
    const root: OrchestrationThread = {
      ...baseThread(),
      id: ThreadId.make("root"),
      agentParentThreadId: null,
    };
    const running = baseThread();
    const idle: OrchestrationThread = {
      ...running,
      latestTurn: { ...running.latestTurn!, state: "completed", completedAt: time },
    };
    let observations = 0;
    const commands: Array<string> = [];
    const backend = AgentOrchestrationBackend.of({
      getThread: (id) => Effect.succeed(id === root.id ? root : running),
      getProject: () =>
        Effect.succeed({
          id: root.projectId,
          title: "Project",
          workspaceRoot: "C:/project",
          defaultModelSelection: root.modelSelection,
          agentOrchestrationTrusted: true,
          scripts: [],
          createdAt: time,
          updatedAt: time,
          deletedAt: null,
        }),
      listDirectChildren: () => Effect.succeed([running]),
      listAgentRoots: Effect.succeed([]),
      getProviders: Effect.succeed([provider("codex")]),
      startChildTurn: () => Effect.die("unused"),
      observeChild: () => {
        observations += 1;
        return Effect.succeed({
          initial: observations === 1 ? running : idle,
          changes: Stream.never,
        });
      },
      dispatch: (command) =>
        Effect.sync(() => {
          commands.push(command.type);
        }),
    });
    const layer = AgentOrchestrationService.layer.pipe(
      Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
      Layer.provide(NodeServices.layer),
    );
    return Effect.gen(function* () {
      const result = yield* (yield* AgentOrchestrationService).interruptAndDrain(
        root.id,
        running.id,
        1,
      );
      expect(result.status).toBe("idle");
      expect(commands).toEqual(["thread.turn.interrupt"]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("revalidates direct lineage before parent cleanup mutates a listed child", () => {
    const root: OrchestrationThread = {
      ...baseThread(),
      id: ThreadId.make("root"),
      agentParentThreadId: null,
    };
    const listed = baseThread();
    const foreign: OrchestrationThread = {
      ...listed,
      agentParentThreadId: ThreadId.make("other-root"),
    };
    let childReads = 0;
    let dispatches = 0;
    const backend = AgentOrchestrationBackend.of({
      getThread: (id) => {
        if (id === root.id) return Effect.succeed(root);
        childReads += 1;
        return Effect.succeed(foreign);
      },
      getProject: () =>
        Effect.succeed({
          id: root.projectId,
          title: "Project",
          workspaceRoot: "C:/project",
          defaultModelSelection: root.modelSelection,
          agentOrchestrationTrusted: false,
          scripts: [],
          createdAt: time,
          updatedAt: time,
          deletedAt: null,
        }),
      listDirectChildren: () => Effect.succeed([listed]),
      listAgentRoots: Effect.succeed([]),
      getProviders: Effect.succeed([provider("codex")]),
      startChildTurn: () => Effect.die("unused"),
      observeChild: () => Effect.die("must not observe foreign child"),
      dispatch: () =>
        Effect.sync(() => {
          dispatches += 1;
        }),
    });
    const layer = AgentOrchestrationService.layer.pipe(
      Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
      Layer.provide(NodeServices.layer),
    );
    return Effect.gen(function* () {
      const error = yield* (yield* AgentOrchestrationService)
        .prepareParentArchive(root.id)
        .pipe(Effect.flip);
      expect(error.code).toBe("forbidden");
      expect(childReads).toBe(1);
      expect(dispatches).toBe(0);
    }).pipe(Effect.provide(layer));
  });

  it.effect("shares parent archive admission with ordinary Root turns and agent mutations", () => {
    const root: OrchestrationThread = {
      ...baseThread(),
      id: ThreadId.make("root"),
      agentParentThreadId: null,
    };
    const child: OrchestrationThread = {
      ...baseThread(),
      latestTurn: { ...baseThread().latestTurn!, state: "completed", completedAt: time },
    };
    const backend = AgentOrchestrationBackend.of({
      getThread: (id) => Effect.succeed(id === root.id ? root : child),
      getProject: () =>
        Effect.succeed({
          id: root.projectId,
          title: "Project",
          workspaceRoot: "C:/project",
          defaultModelSelection: root.modelSelection,
          agentOrchestrationTrusted: true,
          scripts: [],
          createdAt: time,
          updatedAt: time,
          deletedAt: null,
        }),
      listDirectChildren: () => Effect.succeed([]),
      listAgentRoots: Effect.succeed([]),
      getProviders: Effect.succeed([provider("codex")]),
      startChildTurn: () => Effect.die("unused"),
      observeChild: () => Effect.die("idle interrupt must not observe"),
      dispatch: () => Effect.void,
    });
    const layer = AgentOrchestrationService.layer.pipe(
      Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
      Layer.provide(NodeServices.layer),
    );
    return Effect.gen(function* () {
      const service = yield* AgentOrchestrationService;
      const lease = yield* service.acquireParentArchive(root.id);
      const rootTurn = yield* service.acquireRootMutation(root.id).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(rootTurn.pollUnsafe()).toBeUndefined();
      yield* lease.release;
      const rootTurnLease = yield* Fiber.join(rootTurn);
      const mutation = yield* service.interrupt(root.id, child.id).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(mutation.pollUnsafe()).toBeUndefined();
      yield* rootTurnLease.release;
      expect((yield* Fiber.join(mutation)).threadId).toBe(child.id);
    }).pipe(Effect.provide(layer));
  });

  it.effect(
    "reads authoritative projection after dispatch instead of a stale first hub event",
    () => {
      const root: OrchestrationThread = {
        ...baseThread(),
        id: ThreadId.make("root"),
        agentParentThreadId: null,
      };
      let current: OrchestrationThread = {
        ...baseThread(),
        latestTurn: { ...baseThread().latestTurn!, state: "completed", completedAt: time },
      };
      let observations = 0;
      const backend = AgentOrchestrationBackend.of({
        getThread: (id) => Effect.succeed(id === root.id ? root : current),
        getProject: () =>
          Effect.succeed({
            id: root.projectId,
            title: "Project",
            workspaceRoot: "C:/project",
            defaultModelSelection: root.modelSelection,
            agentOrchestrationTrusted: true,
            scripts: [],
            createdAt: time,
            updatedAt: time,
            deletedAt: null,
          }),
        listDirectChildren: () => Effect.succeed([]),
        listAgentRoots: Effect.succeed([]),
        getProviders: Effect.succeed([provider("codex")]),
        startChildTurn: () => Effect.die("unused"),
        observeChild: () => {
          observations += 1;
          return Effect.succeed({ initial: current, changes: Stream.make(current) });
        },
        dispatch: (command) =>
          Effect.sync(() => {
            if (command.type === "thread.archive") current = { ...current, archivedAt: time };
          }),
      });
      const layer = AgentOrchestrationService.layer.pipe(
        Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
        Layer.provide(NodeServices.layer),
      );
      return Effect.gen(function* () {
        const result = yield* (yield* AgentOrchestrationService).archive(root.id, current.id);
        expect(result.status).toBe("completed");
        expect(observations).toBe(0);
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect(
    "attaches exactly one monitor when an old trust-off child is explicitly reactivated",
    () => {
      const root: OrchestrationThread = {
        ...baseThread(),
        id: ThreadId.make("root"),
        agentParentThreadId: null,
      };
      let trusted = false;
      let current: OrchestrationThread = {
        ...baseThread(),
        latestTurn: { ...baseThread().latestTurn!, state: "completed", completedAt: time },
      };
      let observations = 0;
      const backend = AgentOrchestrationBackend.of({
        getThread: (id) => Effect.succeed(id === root.id ? root : current),
        getProject: () =>
          Effect.succeed({
            id: root.projectId,
            title: "Project",
            workspaceRoot: "C:/project",
            defaultModelSelection: root.modelSelection,
            agentOrchestrationTrusted: trusted,
            scripts: [],
            createdAt: time,
            updatedAt: time,
            deletedAt: null,
          }),
        listDirectChildren: () => Effect.succeed([current]),
        listAgentRoots: Effect.succeed([root]),
        getProviders: Effect.succeed([provider("codex")]),
        startChildTurn: () => Effect.die("unused"),
        observeChild: () => {
          observations += 1;
          return Effect.succeed({ initial: current, changes: Stream.never });
        },
        dispatch: (command) =>
          Effect.sync(() => {
            if (command.type === "thread.turn.start") {
              current = {
                ...current,
                latestTurn: { ...current.latestTurn!, state: "running", completedAt: null },
              };
            }
          }),
      });
      const layer = AgentOrchestrationService.layer.pipe(
        Layer.provide(Layer.succeed(AgentOrchestrationBackend, backend)),
        Layer.provide(NodeServices.layer),
      );
      return Effect.gen(function* () {
        const service = yield* AgentOrchestrationService;
        expect(observations).toBe(0);
        trusted = true;
        yield* service.send(root.id, current.id, "first explicit reactivation");
        expect(observations).toBe(1);
        current = {
          ...current,
          latestTurn: { ...current.latestTurn!, state: "completed", completedAt: time },
        };
        yield* service.send(root.id, current.id, "second explicit turn");
        expect(observations).toBe(1);
        yield* service.drainLifecycle;
      }).pipe(Effect.provide(layer));
    },
  );
});
