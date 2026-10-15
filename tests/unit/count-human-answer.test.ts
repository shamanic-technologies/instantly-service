import { describe, it, expect, vi, beforeEach } from "vitest";

const mockFindLead = vi.fn();
const mockReadState = vi.fn();
const mockActed = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
  readFollowupState: (...a: unknown[]) => mockReadState(...a),
  recordFollowupActed: (...a: unknown[]) => mockActed(...a),
}));

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import {
  FOLLOWUP_LADDER_DAYS,
  countHumanAnswerAsOurs,
  nextDueAfterHumanAnswer,
} from "../../src/lib/count-human-answer";

const NOW = Date.parse("2026-10-15T10:00:00.000Z");

// Staff test org only (never a real client or prospect).
const INPUT = {
  orgId: "staff-test-org",
  campaignId: "camp-1",
  leadEmail: "k.lourd+noah-replay@distribute.you",
  instantlyCampaignId: "seq-1",
  answeredAt: "2026-10-15T09:00:00.000Z",
  nowMs: NOW,
};

function state(over: Record<string, unknown> = {}) {
  return {
    id: "row-1",
    leadId: "lead-1",
    campaignId: "camp-1",
    dueAt: "2026-10-15T08:00:00.000Z",
    claimedAt: "2026-10-15T09:59:00.000Z",
    followupCount: 0,
    lastActionAt: null,
    stoppedReason: null,
    ...over,
  };
}

describe("nextDueAfterHumanAnswer — the usual ladder, from the moment the person answered", () => {
  it("is the responder's own ladder", () => {
    expect([...FOLLOWUP_LADDER_DAYS]).toEqual([3, 7, 21, 60, 180]);
  });

  it("counts the rung from the answer time, by follow-ups already sent", () => {
    expect(nextDueAfterHumanAnswer("2026-10-15T09:00:00.000Z", 0, NOW)).toBe("2026-10-18T09:00:00.000Z");
    expect(nextDueAfterHumanAnswer("2026-10-15T09:00:00.000Z", 1, NOW)).toBe("2026-10-22T09:00:00.000Z");
    expect(nextDueAfterHumanAnswer("2026-10-15T09:00:00.000Z", 9, NOW)).toBe("2027-04-13T09:00:00.000Z");
  });

  it("is never in the past: an old answer makes the follow-up due now", () => {
    expect(nextDueAfterHumanAnswer("2026-09-01T09:00:00.000Z", 0, NOW)).toBe("2026-10-15T10:01:00.000Z");
  });
});

describe("countHumanAnswerAsOurs — the founder answered by hand, the prospect stays followed up", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockFindLead.mockResolvedValue({ id: "row-1" });
    mockReadState.mockResolvedValue(state());
    mockActed.mockResolvedValue(state({ followupCount: 1 }));
    mockDbExecute.mockResolvedValue({ rows: [], rowCount: 1 });
  });

  it("states `acted` with the next ladder date and stamps the thread", async () => {
    expect(await countHumanAnswerAsOurs(INPUT)).toBe("counted");

    expect(mockActed).toHaveBeenCalledWith({
      orgId: "staff-test-org",
      leadRowId: "row-1",
      nextDueAt: "2026-10-18T09:00:00.000Z",
    });
    expect(mockDbExecute).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(mockDbExecute.mock.calls[0][0])).toContain("human_answer_counted_at");
  });

  it("leaves a row with no live debt alone (opt-out, decline, booked, handed stay ended)", async () => {
    mockReadState.mockResolvedValue(state({ dueAt: null, stoppedReason: "lead_opted_out" }));

    expect(await countHumanAnswerAsOurs(INPUT)).toBe("no_live_debt");
    expect(mockActed).not.toHaveBeenCalled();
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("does not stamp the thread when lead-service refused the statement", async () => {
    mockActed.mockRejectedValue(new Error("lead-service 502"));

    expect(await countHumanAnswerAsOurs(INPUT)).toBe("failed");
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("no lead row = nothing counted", async () => {
    mockFindLead.mockResolvedValue(null);
    expect(await countHumanAnswerAsOurs(INPUT)).toBe("no_lead_row");
    expect(await countHumanAnswerAsOurs({ ...INPUT, orgId: null })).toBe("no_lead_row");
    expect(mockActed).not.toHaveBeenCalled();
  });
});
