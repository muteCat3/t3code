# Delegate work to child agents

Agent orchestration lets one Codex thread coordinate durable child threads from other configured
providers. It is a private-fork feature and is disabled for every project until you trust that project
in **Settings > Projects**.

Only an unarchived, top-level Codex thread can be Root. Root can delegate to configured Codex, Grok,
and Claude instances. Cursor and OpenCode are not supported. If more than one account matches a
provider, Root shows the candidates and asks you to choose; it never guesses.

Each child appears as a normal thread and in Root's Agents activity. Shared children work in the
project workspace, including dirty files. Managed-worktree children start from the committed parent
HEAD in an isolated worktree. Child status remains visible while it is running, waiting for an answer,
idle, interrupted, completed, or failed.

Root can handle routine one-time questions. It brings destructive, persistent, costly, or ambiguous
requests back to the Root chat for your decision.

Turning trust off immediately removes Root's orchestration access, then stops and settles its direct
children. Archiving Root stops and archives direct children before archiving Root. A child that cannot
drain remains visible with its failure instead of being silently discarded. Turning trust back on or
unarchiving Root does not resume an old child fleet.
