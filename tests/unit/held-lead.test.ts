import { describe, it, expect } from "vitest";

import { classifyHeldRow } from "../../src/lib/held-lead";

const T = new Date("2026-09-11T00:17:05Z");

describe("classifyHeldRow — queued with us vs lost", () => {
  it("an active sequence with nothing sent is QUEUED and awaiting its first email", () => {
    expect(
      classifyHeldRow(
        { instantlyCampaignId: "self:x", status: "active", createdAt: T, provisionedSteps: 3, sentSteps: 0 },
        "reserving:",
      ),
    ).toEqual({ state: "queued", awaitingFirstEmail: true, queuedSince: T.toISOString(), remainingSteps: 3 });
  });

  it("mid-sequence is queued, not awaiting a first email", () => {
    expect(
      classifyHeldRow(
        { instantlyCampaignId: "self:x", status: "active", createdAt: T, provisionedSteps: 1, sentSteps: 2 },
        "reserving:",
      ),
    ).toMatchObject({ state: "queued", awaitingFirstEmail: false, remainingSteps: 1 });
  });

  it("nothing provisioned or a stopped row is finished", () => {
    for (const row of [
      { status: "active", provisionedSteps: 0 },
      { status: "completed", provisionedSteps: 2 },
    ]) {
      expect(
        classifyHeldRow(
          { instantlyCampaignId: "self:x", createdAt: T, sentSteps: 1, ...row },
          "reserving:",
        ),
      ).toEqual({ state: "finished", awaitingFirstEmail: false, queuedSince: null, remainingSteps: 0 });
    }
  });

  it("a reservation sentinel is in flight", () => {
    expect(
      classifyHeldRow(
        { instantlyCampaignId: "reserving:abc", status: "active", createdAt: T, provisionedSteps: 0, sentSteps: 0 },
        "reserving:",
      ).state,
    ).toBe("in_flight");
  });
});
