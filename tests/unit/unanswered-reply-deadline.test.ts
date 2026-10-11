import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockAsk = vi.fn();

vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));
vi.mock("../../src/lib/ask-client-to-answer", () => ({
  maybeAskClientToAnswer: (...a: unknown[]) => mockAsk(...a),
}));

import {
  ANSWERABLE_REPLY_KINDS,
  askClientsAboutUnansweredReplies,
  UNANSWERED_REPLY_DEADLINE_MS,
} from "../../src/lib/unanswered-reply-deadline";

const row = {
  instantly_campaign_id: "ic-1",
  lead_email: "steve@kaizen.com",
  campaign_id: "c-1",
  org_id: "org-1",
  user_id: "u-1",
  run_id: "r-1",
  brand_ids: ["b-1"],
};

/** Every literal string and bound value of a drizzle sql`` object, flattened. */
function sqlText(query: unknown): string {
  return JSON.stringify(query);
}

describe("a reply a responder took but never answered → client asked after 24 h", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockAsk.mockResolvedValue({ sent: true });
  });

  it("deadline is a working day; covers interested, questions and meeting requests", () => {
    expect(UNANSWERED_REPLY_DEADLINE_MS).toBe(24 * 60 * 60 * 1000);
    expect([...ANSWERABLE_REPLY_KINDS].sort()).toEqual(
      ["lead_info_requested", "lead_interested", "lead_meeting_requested"].sort(),
    );
  });

  it("asks the client (staff in Cc is the ask's own job) with because=overdue", async () => {
    mockExecute.mockResolvedValue({ rows: [row] });
    const out = await askClientsAboutUnansweredReplies(new Date("2026-10-11T12:00:00Z"));
    expect(out).toEqual({ unanswered: 1, asked: 1 });
    const [campaign, lead, options] = mockAsk.mock.calls[0];
    expect(campaign).toMatchObject({ instantlyCampaignId: "ic-1", orgId: "org-1", runId: "r-1", brandIds: ["b-1"] });
    expect(lead).toBe("steve@kaizen.com");
    expect(options).toEqual({ because: "overdue" });
  });

  it("the read keys on the OUTCOME and skips threads already asked, escalated or left with staff", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    await askClientsAboutUnansweredReplies(new Date("2026-10-11T12:00:00Z"));
    const q = sqlText(mockExecute.mock.calls[0][0]);
    expect(q).toContain("smtp_dispatch_raw");
    expect(q).toContain("d.step = 0");
    expect(q).toContain("ue_type' = '3'");
    expect(q).toContain("client_answer_requested_at IS NULL");
    expect(q).toContain("escalated_at IS NULL");
    expect(q).toContain("infoRequestUnansweredAlert");
    expect(mockAsk).not.toHaveBeenCalled();
  });

  it("a failed read throws (the worker logs it loud), nobody is asked", async () => {
    mockExecute.mockRejectedValue(new Error("pool timeout"));
    await expect(askClientsAboutUnansweredReplies()).rejects.toThrow("pool timeout");
    expect(mockAsk).not.toHaveBeenCalled();
  });

  it("concurrent calls join one sweep", async () => {
    mockExecute.mockResolvedValue({ rows: [row] });
    const [a, b] = await Promise.all([askClientsAboutUnansweredReplies(), askClientsAboutUnansweredReplies()]);
    expect(a).toBe(b);
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });
});
