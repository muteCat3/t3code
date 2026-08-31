import {
  type ModelSelection,
  ProviderDriverKind,
  type ProviderDriverKind as ProviderDriverKindType,
} from "@t3tools/contracts";

import { resolveClaudeApiModelId } from "./Layers/ClaudeProvider.ts";
import { resolveGrokAcpBaseModelId } from "./acp/GrokAcpSupport.ts";

/**
 * Compares the adapter-applied catalog selection with identity reported by the
 * provider protocol. Picker labels and legacy session.model are not evidence.
 */
export function providerReportedIdentityMatches(input: {
  readonly driver: ProviderDriverKindType;
  readonly appliedModelSelection: ModelSelection | undefined;
  readonly providerReportedModelId: string | undefined;
}): boolean {
  const reported = input.providerReportedModelId?.trim();
  const applied = input.appliedModelSelection;
  if (!reported || !applied) {
    return false;
  }
  switch (input.driver) {
    case ProviderDriverKind.make("codex"):
      return reported === applied.model;
    case ProviderDriverKind.make("claudeAgent"):
      return reported === resolveClaudeApiModelId(applied);
    case ProviderDriverKind.make("grok"):
      return resolveGrokAcpBaseModelId(reported) === resolveGrokAcpBaseModelId(applied.model);
    default:
      return false;
  }
}
