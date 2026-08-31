import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { describe, expect, it } from "vite-plus/test";

import { archiveThreadConfirmationMessage } from "./agentOrchestrationUi";

function shell(
  input: Pick<EnvironmentThreadShell, "id" | "title" | "agentParentThreadId"> &
    Partial<Pick<EnvironmentThreadShell, "environmentId" | "archivedAt">>,
): EnvironmentThreadShell {
  return {
    environmentId: input.environmentId ?? ("env-1" as EnvironmentThreadShell["environmentId"]),
    archivedAt: input.archivedAt ?? null,
    ...input,
  } as EnvironmentThreadShell;
}

describe("archiveThreadConfirmationMessage", () => {
  it("mentions visible direct children when archiving a Root thread", () => {
    const root = shell({ id: "root" as never, title: "Root", agentParentThreadId: null });
    const child = shell({
      id: "child" as never,
      title: "Child",
      agentParentThreadId: root.id,
    });

    expect(archiveThreadConfirmationMessage(root, [root, child])).toContain(
      "1 direct child thread will be interrupted, drained, and archived first:",
    );
    expect(archiveThreadConfirmationMessage(root, [root, child])).toContain("- Child");
  });

  it("ignores archived and foreign-environment children", () => {
    const root = shell({ id: "root" as never, title: "Root", agentParentThreadId: null });
    const archived = shell({
      id: "archived" as never,
      title: "Archived",
      agentParentThreadId: root.id,
      archivedAt: "2026-08-31T00:00:00.000Z",
    });
    const foreign = shell({
      id: "foreign" as never,
      title: "Foreign",
      agentParentThreadId: root.id,
      environmentId: "env-2" as EnvironmentThreadShell["environmentId"],
    });

    expect(archiveThreadConfirmationMessage(root, [root, archived, foreign])).toBe(
      'Archive thread "Root"?',
    );
  });
});
