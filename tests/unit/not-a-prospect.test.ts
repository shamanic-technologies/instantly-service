import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

const mockReplyToLead = vi.fn();
vi.mock("../../src/lib/reply-to-lead", () => ({
  replyToLead: (...a: unknown[]) => mockReplyToLead(...a),
}));

const mockLoadHistory = vi.fn();
vi.mock("../../src/lib/prospect-history", () => ({
  loadHistoryWithLatestReply: (...a: unknown[]) => mockLoadHistory(...a),
  REPLY_WAIT_BACKGROUND_MS: [],
}));

const mockComplete = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({ orgComplete: (...a: unknown[]) => mockComplete(...a) }));

const mockStopLeadSequence = vi.fn();
vi.mock("../../src/lib/stop-lead-sequence", () => ({
  stopLeadSequence: (...a: unknown[]) => mockStopLeadSequence(...a),
}));

const mockClaim = vi.fn();
const mockRelease = vi.fn();
const mockRecord = vi.fn();
vi.mock("../../src/lib/escalate-reply", async () => {
  const actual = await vi.importActual<typeof import("../../src/lib/escalate-reply")>(
    "../../src/lib/escalate-reply",
  );
  return {
    handoffTextToHtml: actual.handoffTextToHtml,
    renderQuotedHistory: actual.renderQuotedHistory,
    claimEscalation: (...a: unknown[]) => mockClaim(...a),
    releaseEscalation: (...a: unknown[]) => mockRelease(...a),
    recordHandedTo: (...a: unknown[]) => mockRecord(...a),
  };
});

const mockRecordCustomer = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  recordExistingCustomerByEmail: (...a: unknown[]) => mockRecordCustomer(...a),
}));

vi.mock("../../src/lib/celebrate-positive-reply", async () => {
  const actual = await vi.importActual<typeof import("../../src/lib/celebrate-positive-reply")>(
    "../../src/lib/celebrate-positive-reply",
  );
  return { escapeHtml: actual.escapeHtml, brandContextOrNull: vi.fn() };
});

import {
  assertReassuranceDraft,
  findNotAProspect,
  maybeHandleNotAProspect,
  NOT_A_PROSPECT_REFUSAL_CODE,
  notAProspectRefusal,
  REASSURANCE_SYSTEM_PROMPT,
} from "../../src/lib/not-a-prospect";

// ─── Fixtures ────────────────────────────────────────────────────────────────

const ANDREW_REPLY = {
  direction: "inbound" as const,
  from: '"Andrew Kakishita" <dr.k@kineticchiropracticutah.com>',
  to: "kevin.l@maildistribute.com",
  date: "2026-09-30T18:25:00.000Z",
  subject: "Re: Kinetic Chiropractic shockwave",
  bodyText:
    "Hey Kevin,\n\nThanks for reaching out. I actually am a Shockwave Centers of America clinic. I have the OTG unit. Is this email meant for those who don't have shockwave units?\n\nAndrew",
};

const CAMPAIGN = {
  instantlyCampaignId: "self:abc",
  campaignId: "camp-1",
  orgId: "org-1",
  userId: "user-1",
  runId: "run-1",
  brandIds: ["brand-1"],
};

const pg = (rows: unknown[]) => ({ command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows });

beforeEach(() => {
  vi.clearAllMocks();
  mockExecute.mockResolvedValue(pg([]));
  mockClaim.mockResolvedValue(true);
  mockRelease.mockResolvedValue(undefined);
  mockRecord.mockResolvedValue(undefined);
  mockStopLeadSequence.mockResolvedValue(true);
  mockLoadHistory.mockResolvedValue({
    history: { items: [], messages: [ANDREW_REPLY], notes: [] },
    latestReply: ANDREW_REPLY,
  });
  mockReplyToLead.mockResolvedValue({ status: "sent", reply: {} });
  mockRecordCustomer.mockResolvedValue({ status: "recorded" });
});

// ─── The draft's hard rule ───────────────────────────────────────────────────

describe("the reassurance draft never states a fact about the prospect", () => {
  it("carries the hard rule in the system prompt", () => {
    expect(REASSURANCE_SYSTEM_PROMPT).toContain("Never state or imply any fact about them");
    expect(REASSURANCE_SYSTEM_PROMPT).toContain("Do not repeat or confirm what they told us about themselves");
    expect(REASSURANCE_SYSTEM_PROMPT).not.toContain("—");
  });

  it("refuses a draft that restates what Andrew told us about himself", () => {
    expect(() =>
      assertReassuranceDraft(
        "Hi Andrew,\n\nSorry about that, since you already have the OTG unit this was not meant for you.\n\nBest,",
      ),
    ).toThrow(/states a fact/);
    expect(() =>
      assertReassuranceDraft("Hi Andrew,\n\nYou are already part of the Shockwave Centers network, my mistake.\n\nBest,"),
    ).toThrow(/states a fact/);
    expect(() =>
      assertReassuranceDraft("Hi Andrew,\n\nAs an existing customer you should not have received this.\n\nBest,"),
    ).toThrow(/states a fact/);
  });

  it("accepts a plain apology that claims nothing", () => {
    const ok =
      "Hi Andrew,\n\nSorry about that, and thanks for flagging it. This email was not meant for you, and I have taken you off the list so you will not get any more of these.\n\nBest,";
    expect(assertReassuranceDraft(ok)).toBe(ok);
  });
});

// ─── The side effect ─────────────────────────────────────────────────────────

describe("maybeHandleNotAProspect", () => {
  it("does nothing on any other kind", async () => {
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_interested", "self_send");
    expect(mockExecute).not.toHaveBeenCalled();
    expect(mockReplyToLead).not.toHaveBeenCalled();
  });

  it("stops every other live sequence of the brand to the person", async () => {
    mockExecute.mockResolvedValueOnce(pg([{ id: "self:other" }, { id: "inst-2" }]));
    mockComplete.mockResolvedValue({ content: "Hi Andrew,\n\nSorry, this was not meant for you. I have taken you off the list.\n\nBest," });
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "self_send");
    expect(mockStopLeadSequence).toHaveBeenCalledTimes(2);
    expect(mockStopLeadSequence.mock.calls.map((c) => (c[0] as { instantlyCampaignId: string }).instantlyCampaignId)).toEqual([
      "self:other",
      "inst-2",
    ]);
  });

  it("answers in the thread, Kevin in Bcc, nobody in Cc, drafted on opus, exactly once", async () => {
    mockComplete.mockResolvedValue({ content: "Hi Andrew,\n\nSorry, this was not meant for you. I have taken you off the list.\n\nBest," });
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "self_send");
    expect(mockClaim).toHaveBeenCalledWith("self:abc");
    expect(mockComplete.mock.calls[0][0]).toMatchObject({ provider: "anthropic", model: "opus" });
    expect(mockComplete.mock.calls[0][1]).toEqual({ orgId: "org-1", userId: "user-1", runId: "run-1" });
    const sent = mockReplyToLead.mock.calls[0][0];
    expect(sent).toMatchObject({
      campaignId: "camp-1",
      leadEmail: "dr.k@x.com",
      sentBy: "automation",
      handoff: true,
      copy: { cc: [], bcc: ["kevin@distribute.you"] },
    });
    expect(sent.bodyHtml).toContain("not meant for you");
    expect(mockRecord).toHaveBeenCalledWith("self:abc", "reassured");
  });

  it("NEVER answers a qualification a person stated (they handle the conversation)", async () => {
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "manual");
    expect(mockClaim).not.toHaveBeenCalled();
    expect(mockComplete).not.toHaveBeenCalled();
    expect(mockReplyToLead).not.toHaveBeenCalled();
  });

  it("records an existing customer as a won sale that is not ours, on every source", async () => {
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "manual");
    expect(mockRecordCustomer).toHaveBeenCalledWith({ orgId: "org-1", campaignId: "camp-1", email: "dr.k@x.com" });
  });

  it("records no sale for the client's own team", async () => {
    mockComplete.mockResolvedValue({ content: "Hi,\n\nSorry, this was not meant for you.\n\nBest," });
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_is_client", "self_send");
    expect(mockRecordCustomer).not.toHaveBeenCalled();
  });

  it("still reassures when lead-service refuses the sale", async () => {
    mockRecordCustomer.mockRejectedValue(new Error("404 lead_not_found"));
    mockComplete.mockResolvedValue({ content: "Hi,\n\nSorry, this was not meant for you.\n\nBest," });
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "self_send");
    expect(mockReplyToLead).toHaveBeenCalledTimes(1);
  });

  it("sends nothing when the thread was already escalated or answered", async () => {
    mockClaim.mockResolvedValue(false);
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_is_client", "self_send");
    expect(mockReplyToLead).not.toHaveBeenCalled();
  });

  it("retries once on a draft that states a fact, then sends the clean one", async () => {
    mockComplete
      .mockResolvedValueOnce({ content: "Hi Andrew,\n\nSince you already have the unit, sorry.\n\nBest," })
      .mockResolvedValueOnce({ content: "Hi Andrew,\n\nSorry, this was not meant for you.\n\nBest," });
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "self_send");
    expect(mockComplete).toHaveBeenCalledTimes(2);
    expect(mockReplyToLead).toHaveBeenCalledTimes(1);
  });

  it("releases the claim and sends nothing when both drafts state a fact", async () => {
    mockComplete.mockResolvedValue({ content: "Hi Andrew,\n\nYou already have the unit.\n\nBest," });
    await maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "self_send");
    expect(mockReplyToLead).not.toHaveBeenCalled();
    expect(mockRelease).toHaveBeenCalledWith("self:abc");
  });

  it("never throws", async () => {
    mockExecute.mockRejectedValue(new Error("db down"));
    await expect(
      maybeHandleNotAProspect(CAMPAIGN, "dr.k@x.com", "lead_already_customer", "self_send"),
    ).resolves.toBeUndefined();
  });
});

// ─── The send gate ───────────────────────────────────────────────────────────

describe("findNotAProspect", () => {
  it("is null for a brand-less send, without a query", async () => {
    expect(await findNotAProspect("a@b.com", [])).toBeNull();
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("reads the standing reply kind off gold, case-insensitively, for the send's brands", async () => {
    mockExecute.mockResolvedValueOnce(pg([{ brand_id: "brand-1", reply_kind: "lead_already_customer" }]));
    const found = await findNotAProspect("Dr.K@X.com", ["brand-1"]);
    expect(found).toEqual({ brandId: "brand-1", replyKind: "lead_already_customer" });
    const query = JSON.stringify(mockExecute.mock.calls[0][0]);
    expect(query).toContain("instantly_lead_status_current");
    expect(query).toContain("lower(g.lead_email)");
    expect(query).toContain("dr.k@x.com");
  });

  it("refuses with its own code", () => {
    const body = notAProspectRefusal("a@b.com", { brandId: "brand-1", replyKind: "lead_is_client" });
    expect(body.code).toBe(NOT_A_PROSPECT_REFUSAL_CODE);
    expect(body.code).not.toBe("recent_brand_contact");
  });
});
