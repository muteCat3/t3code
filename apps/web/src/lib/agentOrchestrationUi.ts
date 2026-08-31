import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";

export function visibleDirectChildren(
  thread: EnvironmentThreadShell,
  threads: ReadonlyArray<EnvironmentThreadShell>,
): ReadonlyArray<EnvironmentThreadShell> {
  return threads.filter(
    (candidate) =>
      candidate.environmentId === thread.environmentId &&
      candidate.agentParentThreadId === thread.id &&
      candidate.archivedAt === null,
  );
}

export function visibleDirectChildCount(
  thread: EnvironmentThreadShell,
  threads: ReadonlyArray<EnvironmentThreadShell>,
): number {
  return visibleDirectChildren(thread, threads).length;
}

/** Confirmation copy for Root archive, enriched when its direct children are in the shell snapshot. */
export function archiveThreadConfirmationMessage(
  thread: EnvironmentThreadShell,
  threads: ReadonlyArray<EnvironmentThreadShell>,
): string {
  const directChildren = visibleDirectChildren(thread, threads);
  if (directChildren.length === 0) return `Archive thread "${thread.title}"?`;

  return [
    `Archive thread "${thread.title}"?`,
    `Its ${directChildren.length} direct child ${directChildren.length === 1 ? "thread" : "threads"} will be interrupted, drained, and archived first:`,
    ...directChildren.map((child) => `- ${child.title}`),
  ].join("\n");
}
