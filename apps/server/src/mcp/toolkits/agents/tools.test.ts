import { describe, expect, it } from "@effect/vitest";
import { Tool } from "effect/unstable/ai";

import { PreviewToolkit } from "../preview/tools.ts";
import { AgentRespondTool, AgentToolkit } from "./tools.ts";

describe("AgentToolkit", () => {
  it("exposes only the isolated orchestration tools", () => {
    expect(Object.keys(AgentToolkit.tools).sort()).toEqual([
      "agent_archive",
      "agent_inspect",
      "agent_interrupt",
      "agent_respond",
      "agent_send",
      "agent_spawn",
      "agent_unarchive",
      "agent_wait",
    ]);
  });

  it("shares no discovery names with the preview toolkit", () => {
    const previewNames = new Set(Object.keys(PreviewToolkit.tools));
    expect(Object.keys(AgentToolkit.tools).filter((name) => previewNames.has(name))).toEqual([]);
  });

  it("publishes provider-compatible top-level object parameters", () => {
    for (const tool of Object.values(AgentToolkit.tools)) {
      const schema = Tool.getJsonSchema(tool) as Record<string, unknown>;
      expect(schema.type, tool.name).toBe("object");
      expect(schema.anyOf, tool.name).toBeUndefined();
      expect(schema.oneOf, tool.name).toBeUndefined();
    }
  });

  it("does not expose persistent approval choices to agent_respond", () => {
    const schema = Tool.getJsonSchema(AgentRespondTool);
    expect(JSON.stringify(schema)).not.toContain("acceptForSession");
    expect(JSON.stringify(schema)).not.toContain("acceptAlways");
  });
});
