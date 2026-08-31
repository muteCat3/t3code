import { describe, expect, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";

import { providerReportedIdentityMatches } from "./ProviderModelIdentity.ts";

describe("providerReportedIdentityMatches", () => {
  it("requires both applied and provider-reported identity", () => {
    expect(
      providerReportedIdentityMatches({
        driver: ProviderDriverKind.make("grok"),
        appliedModelSelection: {
          instanceId: ProviderInstanceId.make("grok"),
          model: "grok-4.6",
        },
        providerReportedModelId: undefined,
      }),
    ).toBe(false);
  });

  it("matches Codex only against the exact opened model", () => {
    const appliedModelSelection = {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.6-sol",
    };
    expect(
      providerReportedIdentityMatches({
        driver: ProviderDriverKind.make("codex"),
        appliedModelSelection,
        providerReportedModelId: "gpt-5.6-sol",
      }),
    ).toBe(true);
    expect(
      providerReportedIdentityMatches({
        driver: ProviderDriverKind.make("codex"),
        appliedModelSelection,
        providerReportedModelId: "gpt-5.6-terra",
      }),
    ).toBe(false);
  });

  it("matches Claude against the SDK/API model rather than the picker label", () => {
    const appliedModelSelection = {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-opus-4-6",
    };
    expect(
      providerReportedIdentityMatches({
        driver: ProviderDriverKind.make("claudeAgent"),
        appliedModelSelection,
        providerReportedModelId: "claude-opus-4-6[1m]",
      }),
    ).toBe(true);
    expect(
      providerReportedIdentityMatches({
        driver: ProviderDriverKind.make("claudeAgent"),
        appliedModelSelection,
        providerReportedModelId: "claude-sonnet-4-6",
      }),
    ).toBe(false);
  });

  it("normalizes Grok ACP identity without accepting the implicit fallback for missing data", () => {
    const appliedModelSelection = {
      instanceId: ProviderInstanceId.make("grok"),
      model: "grok-4.6",
    };
    expect(
      providerReportedIdentityMatches({
        driver: ProviderDriverKind.make("grok"),
        appliedModelSelection,
        providerReportedModelId: " grok-4.6 ",
      }),
    ).toBe(true);
    expect(
      providerReportedIdentityMatches({
        driver: ProviderDriverKind.make("grok"),
        appliedModelSelection,
        providerReportedModelId: "grok-build",
      }),
    ).toBe(false);
  });
});
