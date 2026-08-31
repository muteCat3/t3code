# muteCat T3 Code policy

This private fork adds provider-mixed agent orchestration without changing the upstream preview MCP
contract. The feature is opt-in per project and disabled by default.

## Product routing

- A trusted, unarchived, top-level Codex thread is the Root; product routing uses the live-catalog
  Sol selection for it.
- Root may create durable T3 child threads backed by Codex, Grok, or Claude. Cursor and OpenCode are
  not orchestration targets.
- Explorer work uses the live-catalog Luna selection in `plan` interaction mode. Worker work uses
  Grok in `default` mode. Important review uses Claude so the review comes from a different provider
  family. Exact models and provider options must always be selected from the target instance's live
  catalog.
- Children are direct children of Root. They cannot create grandchildren, inherit orchestration
  credentials, or silently fall back to a different provider or model.

## Approval custody

Root may answer routine, one-time approvals or structured questions when the user's instruction
already supplies the answer. Root must relay persistent, destructive, costly, or ambiguous requests
to Ben in the Root chat. A child never broadens its own authority.

Turning project trust off revokes orchestration access before child cleanup. Archiving Root drains
and archives its direct children first. Failed cleanup remains visible; it is never hidden by killing
a provider process by name or pattern.

## Runtime truth

Provider-reported model identity is part of child startup. A missing identity, a mismatch with the
applied selection, or a later reroute fails and settles the child. Root starts a new child explicitly
if a different selection is desired.

Native in-process Codex collaboration is unsupported for this feature. Native collaboration inherits
Root's MCP configuration and cannot provide the per-child credential boundary required here. Use the
`agent_*` tools exposed by the orchestration MCP endpoint; hard prevention of native collaboration
requires upstream per-child MCP identity support or disabling the native facility.
