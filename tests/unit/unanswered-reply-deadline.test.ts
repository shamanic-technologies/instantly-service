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
  reply_kind: "lead_interested",
  asked_at: new Date("2026-09-18T12:17:12Z"),
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
    expect(options).toEqual({ because: "overdue", replyKind: "lead_interested", repliedAt: new Date("2026-09-18T12:17:12Z") });
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

  it("'answered since' is dated from when THEY wrote (Gallego: wrote Aug 2, labeled Oct 8, our bump Sep 4)", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    await askClientsAboutUnansweredReplies(new Date("2026-10-11T12:00:00Z"));
    const q = sqlText(mockExecute.mock.calls[0][0]);
    expect(q).toContain("ue_type' = '2'");
    expect(q).toContain("lower(c.lead_email)");
    expect(q).toContain("d.dispatched_at >= a.asked_at");
    expect(q).toContain(">= a.asked_at AT TIME ZONE 'UTC'");
    expect(q).not.toContain(">= q.known_at");
  });

  it("dated from her NEWEST message wherever it was ingested (Gallego: Sep 14 reply only in our IMAP copy, after our Sep 4 bump)", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    await askClientsAboutUnansweredReplies(new Date("2026-10-12T12:00:00Z"));
    const q = sqlText(mockExecute.mock.calls[0][0]);
    // newest of the Instantly mirror and our IMAP copy, the label time only when neither has one
    expect(q).toContain("GREATEST(");
    expect(q).toContain("FROM imap_messages_raw i");
    expect(q).toContain("i.kind = 'reply'");
    expect(q).not.toContain("LEAST(");
    // a staff answer our IMAP poller filed after her message counts as answered
    expect(q).toContain("i.kind = 'staff_reply'");
    expect(q).toContain("COALESCE(i.received_at, i.polled_at) >= a.asked_at");
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

  it("NO lookback: a positive reply from weeks ago is still chased (Robert, Sep 18; the claim makes the backlog one pass)", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    await askClientsAboutUnansweredReplies(new Date("2026-10-13T12:00:00Z"));
    const q = sqlText(mockExecute.mock.calls[0][0]);
    expect(q).not.toContain("::interval");
    expect(q).not.toContain("14 days");
    expect(q).toContain("g.reply_kind, a.asked_at");
  });
});
