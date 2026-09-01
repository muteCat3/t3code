import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  IsoDateTime,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationThread,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { resolveThreadWorkspaceCwd } from "../checkpointing/Utils.ts";

import {
  dispatchBootstrapTurnStartCore,
  clearRootSessionPendingOrchestrationRefresh,
  isOrchestrationMcpEligible,
  makeChildObservationHub,
  makeManagedWorktreeInput,
  isRootSessionPendingOrchestrationRefresh,
  markRootSessionPendingOrchestrationRefresh,
  subscribeChildChanges,
} from "./AgentOrchestrationBackendLive.ts";

const time = IsoDateTime.make("2026-08-31T12:00:00.000Z");
const child: OrchestrationThread = {
  id: ThreadId.make("child"),
  projectId: ProjectId.make("project"),
  agentParentThreadId: ThreadId.make("root"),
  title: "Child",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-test" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: null,
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
};

describe("AgentOrchestrationBackendLive", () => {
  it.effect("captures state published after subscription but before stream consumption", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const hub = yield* makeChildObservationHub();
        const subscription = yield* PubSub.subscribe(hub);
        const snapshot = child;
        const terminal = { ...child, settledAt: time };
        yield* PubSub.publish(hub, terminal);

        const observed = yield* Stream.fromSubscription(subscription).pipe(Stream.runHead);

        expect(snapshot.settledAt).toBeNull();
        expect(observed).toEqual(Option.some(terminal));
      }),
    ),
  );

  it.effect("releases every eager observation subscription after runHead", () =>
    Effect.gen(function* () {
      const hub = yield* makeChildObservationHub();
      for (let index = 0; index < 3; index += 1) {
        const observation = yield* subscribeChildChanges(hub);
        const head = yield* Stream.runHead(observation.changes).pipe(Effect.forkChild);
        yield* PubSub.publish(hub, child);
        yield* Fiber.join(head);
        yield* observation.close;
        expect(hub.subscribers.size).toBe(0);
      }
    }),
  );

  it("creates a managed worktree from the resolved parent commit at the project root", () => {
    expect(
      makeManagedWorktreeInput({
        projectWorkspaceRoot: "F:/workspace/project",
        parentHeadCommit: "0123456789abcdef",
        branch: "t3code/abc123",
      }),
    ).toEqual({
      cwd: "F:/workspace/project",
      refName: "0123456789abcdef",
      newRefName: "t3code/abc123",
      path: null,
    });
  });

  it("keeps shared children canonical while resolving their provider cwd to the dirty workspace", () => {
    expect(
      resolveThreadWorkspaceCwd({
        thread: { projectId: child.projectId, worktreePath: null },
        projects: [{ id: child.projectId, workspaceRoot: "F:/workspace/project" }],
      }),
    ).toBe("F:/workspace/project");
  });
  it("revokes orchestration eligibility from live trust and provider status", () => {
    const root = { ...child, id: ThreadId.make("root"), agentParentThreadId: null };
    const request = {
      threadId: root.id,
      providerInstanceId: root.modelSelection.instanceId,
    };
    const provider = {
      instanceId: root.modelSelection.instanceId,
      driver: "codex",
      status: "ready",
      enabled: true,
      installed: true,
      availability: "available",
    };

    expect(
      isOrchestrationMcpEligible({
        request,
        root,
        project: { deletedAt: null, agentOrchestrationTrusted: true },
        providers: [provider],
      }),
    ).toBe(true);
    expect(
      isOrchestrationMcpEligible({
        request,
        root,
        project: { deletedAt: null, agentOrchestrationTrusted: false },
        providers: [provider],
      }),
    ).toBe(false);
    expect(
      isOrchestrationMcpEligible({
        request,
        root: { ...root, archivedAt: time },
        project: { deletedAt: null, agentOrchestrationTrusted: true },
        providers: [provider],
      }),
    ).toBe(false);
    expect(
      isOrchestrationMcpEligible({
        request,
        root: { ...root, agentParentThreadId: ThreadId.make("another-root") },
        project: { deletedAt: null, agentOrchestrationTrusted: true },
        providers: [provider],
      }),
    ).toBe(false);
    expect(
      isOrchestrationMcpEligible({
        request,
        root,
        project: { deletedAt: time, agentOrchestrationTrusted: true },
        providers: [provider],
      }),
    ).toBe(false);
    expect(
      isOrchestrationMcpEligible({
        request,
        root,
        project: { deletedAt: null, agentOrchestrationTrusted: true },
        providers: [{ ...provider, status: "disabled" }],
      }),
    ).toBe(false);
  });

  it.effect("marks a Root for orchestration refresh at its next turn boundary", () =>
    Effect.gen(function* () {
      const rootId = ThreadId.make("active-root");
      yield* markRootSessionPendingOrchestrationRefresh(rootId);
      expect(yield* isRootSessionPendingOrchestrationRefresh(rootId)).toBe(true);
      yield* clearRootSessionPendingOrchestrationRefresh(rootId);
      expect(yield* isRootSessionPendingOrchestrationRefresh(rootId)).toBe(false);
    }),
  );

  it.effect("shares create-fence-worktree-setup-turn ordering and stamps immutable lineage", () =>
    Effect.gen(function* () {
      const commands: Array<OrchestrationCommand> = [];
      const order: Array<string> = [];
      let sequence = 0;
      const rootId = ThreadId.make("root");
      const childId = ThreadId.make("child-bootstrap");

      yield* dispatchBootstrapTurnStartCore({
        command: {
          type: "thread.turn.start",
          commandId: CommandId.make("turn"),
          threadId: childId,
          message: {
            messageId: MessageId.make("message"),
            role: "user",
            text: "Do the work",
            attachments: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          createdAt: time,
          bootstrap: {
            createThread: {
              projectId: ProjectId.make("project"),
              agentParentThreadId: rootId,
              title: "Child",
              modelSelection: child.modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: "main",
              worktreePath: null,
              createdAt: time,
            },
            prepareWorktree: {
              projectCwd: "F:/workspace/project",
              baseBranch: "main",
              branch: "t3code/abc123",
            },
            runSetupScript: true,
          },
        },
        makeCommandId: (tag) => Effect.succeed(CommandId.make(tag)),
        dispatch: (command) =>
          Effect.sync(() => {
            commands.push(command);
            order.push(command.type);
            sequence += 1;
            return { sequence };
          }),
        drainThreadDeletionThrough: () =>
          Effect.sync(() => {
            order.push("deletion-drained");
          }),
        prepareWorktree: () =>
          Effect.sync(() => {
            order.push("worktree-prepared");
            return { branch: "t3code/abc123", worktreePath: "F:/worktrees/abc123" };
          }),
        afterWorktreePrepared: () => Effect.void,
        runSetupScript: () =>
          Effect.sync(() => {
            order.push("setup-run");
          }),
      });

      expect(order).toEqual([
        "thread.create",
        "deletion-drained",
        "worktree-prepared",
        "thread.meta.update",
        "setup-run",
        "thread.turn.start",
      ]);
      expect(commands[0]).toMatchObject({
        type: "thread.create",
        agentParentThreadId: rootId,
      });
    }),
  );

  it.effect("deletes a created child when the deletion fence fails", () =>
    Effect.gen(function* () {
      const dispatchedTypes: Array<OrchestrationCommand["type"]> = [];
      const exit = yield* dispatchBootstrapTurnStartCore({
        command: {
          type: "thread.turn.start",
          commandId: CommandId.make("turn-fence-failure"),
          threadId: ThreadId.make("child-fence-failure"),
          message: {
            messageId: MessageId.make("message-fence-failure"),
            role: "user",
            text: "Do the work",
            attachments: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          createdAt: time,
          bootstrap: {
            createThread: {
              projectId: child.projectId,
              agentParentThreadId: ThreadId.make("root"),
              title: "Child",
              modelSelection: child.modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: "main",
              worktreePath: null,
              createdAt: time,
            },
          },
        },
        makeCommandId: (tag) => Effect.succeed(CommandId.make(tag)),
        dispatch: (command) =>
          Effect.sync(() => {
            dispatchedTypes.push(command.type);
            return { sequence: dispatchedTypes.length };
          }),
        drainThreadDeletionThrough: () => Effect.fail("fence-failed" as const),
        prepareWorktree: () => Effect.die("not called"),
        afterWorktreePrepared: () => Effect.die("not called"),
        runSetupScript: () => Effect.die("not called"),
      }).pipe(Effect.exit);

      expect(dispatchedTypes).toEqual(["thread.create", "thread.delete"]);
      expect(exit._tag).toBe("Failure");
    }),
  );
});
