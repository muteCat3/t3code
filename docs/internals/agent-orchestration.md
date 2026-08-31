# Agent orchestration

> For maintainers. Using the feature? See [Delegate work to child agents](../user/agent-orchestration.md).

The private fork models delegation as durable T3 threads. A child has an immutable
`agentParentThreadId`; Root activity is projected from normal `task.*` events whose stable task ID is
the child thread ID. Native Codex collaboration is not used.

## Boundary and admission

Orchestration has its own `/mcp/orchestration` endpoint, registry, session store, invocation context,
toolkit, and `T3_ORCHESTRATION_MCP_BEARER_TOKEN`. The preview `/mcp` endpoint, `t3-code` server name,
and `T3_MCP_BEARER_TOKEN` remain unchanged. Tokens are not accepted across endpoints.

Credentials are attached only to a trusted, unarchived, top-level Codex Root. Every tool invocation
re-reads project trust, archive state, caller and target lineage, and project identity. Children may
receive preview MCP access, but never orchestration credentials. All child-control tools enforce a
direct-parent relationship.

Spawning accepts only provider instance or unambiguous supported driver, exact live-catalog model and
options, brief, isolation, and interaction mode. Project, workspace, branch, runtime, permission, and
title are derived server-side. Cursor, OpenCode, disabled providers, ambiguous drivers, and unknown
catalog values fail before thread creation.

## Lifecycle and identity

Async flows subscribe before taking their initial snapshot. A completed turn settles the child as
idle; a later send unsets it and starts a new turn. Waiting approvals and questions are reflected in
Root activity. Wait timeouts return the current snapshot and never cancel work.

Startup does not pass until provider-reported identity matches the applied model selection. Codex
uses the opened thread model and reroute events, Claude uses its real initialization payload, and Grok
requires ACP `currentModelId`. Missing, mismatched, or rerouted identity fails and settles the child;
there is no automatic fallback.

Trust-off revokes credentials before interrupting children. Parent archive drains and archives direct
children before committing the parent archive. Cleanup waits up to 30 seconds for an event-driven
interrupt, requests a normal session stop, then gives the worker one final 30-second drain. It never
kills provider processes by pattern. A drain timeout stays visible, trust remains off, and a parent
archive is not committed.

Native in-process Codex collaboration currently inherits Root's MCP configuration. Tests document
that limitation; they must not claim the credential boundary is technically closed until upstream
supports per-child MCP identity or native collaboration is disabled.
