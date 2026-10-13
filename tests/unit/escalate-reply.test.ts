import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

const { FakeReplyToLeadError } = vi.hoisted(() => ({
  FakeReplyToLeadError: class FakeReplyToLeadError extends Error {
    constructor(public readonly code: string, public readonly status: number, message: string) {
      super(message);
    }
  },
}));
const mockLoadCampaign = vi.fn();
const mockReplyToLead = vi.fn();
vi.mock("../../src/lib/reply-to-lead", () => ({
  loadCampaign: (...a: unknown[]) => mockLoadCampaign(...a),
  replyToLead: (...a: unknown[]) => mockReplyToLead(...a),
  ReplyToLeadError: FakeReplyToLeadError,
}));

vi.mock("../../src/lib/forward-positive-reply", () => ({
  formatThreadDate: (iso: string) => iso,
}));

const mockListMembers = vi.fn();
const mockExternalOrgId = vi.fn();
vi.mock("../../src/lib/client-org-client", () => ({
  listOrgMembers: (...a: unknown[]) => mockListMembers(...a),
  getExternalOrgId: (...a: unknown[]) => mockExternalOrgId(...a),
}));

const mockLoadHistory = vi.fn();
vi.mock("../../src/lib/prospect-history", () => ({
  loadHistoryWithLatestReply: (...a: unknown[]) => mockLoadHistory(...a),
  renderProspectHistory: () => "From: amy@send.com\n\ncold 1\n\nFrom: michael@clinic.com\n\n1. Results? <b>",
  REPLY_WAIT_SHORT_MS: [],
}));

const mockBrand = vi.fn();
const mockCelebrate = vi.fn();
vi.mock("../../src/lib/celebrate-positive-reply", async () => {
  const actual = await vi.importActual<typeof import("../../src/lib/celebrate-positive-reply")>(
    "../../src/lib/celebrate-positive-reply",
  );
  return {
    ...actual,
    brandContextOrNull: (...a: unknown[]) => mockBrand(...a),
    celebrateOnce: (...a: unknown[]) => mockCelebrate(...a),
  };
});

const mockComplete = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({ orgComplete: (...a: unknown[]) => mockComplete(...a) }));

const mockSendEmail = vi.fn();
vi.mock("../../src/lib/email-client", () => ({ sendEmail: (...a: unknown[]) => mockSendEmail(...a) }));

const mockFindLead = vi.fn();
const mockHandFollowups = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
  handFollowupsToHuman: (...a: unknown[]) => mockHandFollowups(...a),
}));

import {
  colleagueLine,
  escalateReply,
  EscalateReplyError,
  handThreadToHuman,
  handoffTextToHtml,
  HANDOFF_SYSTEM_PROMPT,
  renderQuotedHistory,
} from "../../src/lib/escalate-reply";

// ─── Fixtures ────────────────────────────────────────────────────────────────

const CAMPAIGN = {
  campaignId: "camp-1",
  instantlyCampaignId: "ic-1",
  leadEmail: "michael@clinic.com",
  accountEmail: "hanna@send.com",
  sendTransport: "instantly",
  createdAt: null,
  timezone: null,
  brandId: "brand-1",
};

const INPUT = {
  orgId: "org-1",
  userId: "user-1",
  runId: "run-1",
  campaignId: "camp-1",
  leadEmail: "Michael@Clinic.com",
  question: "Comparable results, guarantee terms, patient privacy...",
};

const REPLY = {
  direction: "inbound" as const,
  from: "michael@clinic.com",
  to: "hanna@send.com",
  date: "2026-09-29T15:00:00.000Z",
  subject: "Re: Doc Dinners for your clinic",
  bodyText: "1. Comparable clinic results?\n2. Guarantee terms?\n\nDr. Karlfeldt",
};
const HISTORY = {
  items: [],
  messages: [
    { direction: "outbound", from: "hanna@send.com", to: "michael@clinic.com", date: "2026-09-20T10:00:00.000Z", subject: "Doc Dinners for your clinic", bodyText: "cold 1" },
    REPLY,
  ],
  notes: [],
};

let claimWon = true;

beforeEach(() => {
  vi.resetAllMocks();
  claimWon = true;
  mockExecute.mockImplementation(async (q: unknown) => {
    const text = JSON.stringify(q);
    if (text.includes("escalated_at IS NULL")) return { rows: claimWon ? [{ id: "row" }] : [] };
    if (text.includes("SELECT escalation_handed_to")) return { rows: [{ handedTo: "rep@docdinners.com" }] };
    return { rows: [] };
  });
  mockLoadCampaign.mockResolvedValue(CAMPAIGN);
  mockLoadHistory.mockResolvedValue({ history: HISTORY, latestReply: REPLY });
  mockBrand.mockResolvedValue({
    name: "Doc Dinners",
    rep: { email: "rep@docdinners.com", firstName: null, role: null },
  });
  mockCelebrate.mockResolvedValue(true);
  mockComplete.mockResolvedValue({
    content:
      "Hi Dr. Karlfeldt,\n\nThanks for being clear about what you want to see before a call — results, guarantee terms and privacy. I've copied the Doc Dinners team, who will answer your questions directly.\n\nBest,",
  });
  mockReplyToLead.mockResolvedValue({ status: "sent", reply: {} });
  mockSendEmail.mockResolvedValue(undefined);
  mockFindLead.mockResolvedValue({ id: "lead-row-1", email: "michael@clinic.com" });
  mockHandFollowups.mockResolvedValue(undefined);
  mockListMembers.mockResolvedValue([
    { email: "owner@docdinners.com", firstName: "Ana" },
    { email: "sales@docdinners.com", firstName: null },
  ]);
  mockExternalOrgId.mockResolvedValue("org_2abc");
});

// ─── The hand-over to the client's rep ───────────────────────────────────────

describe("escalating a reply when the brand names a rep", () => {
  it("replies in the lead's own thread, rep in Cc, agency in Bcc, history quoted", async () => {
    const result = await escalateReply(INPUT);

    expect(mockReplyToLead).toHaveBeenCalledTimes(1);
    const [reply] = mockReplyToLead.mock.calls[0];
    expect(reply).toMatchObject({
      campaignId: "camp-1",
      leadEmail: "michael@clinic.com",
      sentBy: "automation",
      handoff: true,
      copy: { cc: ["rep@docdinners.com"], bcc: ["kevin@distribute.you"] },
    });
    // the history is quoted under it, the reply included, verbatim + escaped
    expect(reply.quotedHtml).toContain("1. Comparable clinic results?");
    expect(reply.quotedHtml).toContain("cold 1");
    // no em-dash survives the model
    expect(reply.bodyHtml).not.toContain("—");
    expect(reply.bodyHtml).toContain("<p>Hi Dr. Karlfeldt,</p>");

    expect(result).toMatchObject({ handoff: "rep", handedTo: "rep@docdinners.com", followupsStopped: true, replyRead: true });
  });

  it("sends the agency NO second email and celebrates exactly through the shared claim", async () => {
    await escalateReply(INPUT);
    expect(mockSendEmail).not.toHaveBeenCalled();
    expect(mockCelebrate).toHaveBeenCalledTimes(1);
    const [thread] = mockCelebrate.mock.calls[0];
    expect(thread).toMatchObject({ runId: "run-1", campaignId: null, conversationCampaignId: "camp-1" });
  });

  it("drafts on opus, org-billed on the caller's run, from the reply's own words", async () => {
    await escalateReply(INPUT);
    const [params, identity] = mockComplete.mock.calls[0];
    expect(params).toMatchObject({ provider: "anthropic", model: "opus", systemPrompt: HANDOFF_SYSTEM_PROMPT });
    expect(params.message).toContain("1. Comparable clinic results?");
    expect(identity).toEqual({ orgId: "org-1", userId: "user-1", runId: "run-1" });
  });

  it("names the team, never an invented person, when the rep has no name", () => {
    expect(colleagueLine({ name: "Doc Dinners", rep: { email: "r@d.com", firstName: null, role: null } })).toContain(
      "the Doc Dinners team",
    );
    expect(colleagueLine({ name: "Doc Dinners", rep: { email: "r@d.com", firstName: null, role: null } })).toContain(
      "Never invent",
    );
    expect(
      colleagueLine({ name: "Doc Dinners", rep: { email: "r@d.com", firstName: "Marie", role: "Head of Partnerships" } }),
    ).toContain("Marie, Head of Partnerships at Doc Dinners");
  });

  it("records ONE hand-over (permanent, not a stop), naming who took the thread", async () => {
    await escalateReply(INPUT);
    expect(mockHandFollowups).toHaveBeenCalledTimes(1);
    expect(mockHandFollowups.mock.calls[0][0].reason).toBe(
      "Handed to rep@docdinners.com: the automated responder could not answer this reply.",
    );
  });
});

// ─── Exactly once ────────────────────────────────────────────────────────────

describe("a repeated escalation of the same thread", () => {
  it("sends nothing and stops nothing", async () => {
    claimWon = false;
    const result = await escalateReply(INPUT);
    expect(mockReplyToLead).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
    expect(mockCelebrate).not.toHaveBeenCalled();
    expect(mockHandFollowups).not.toHaveBeenCalled();
    expect(result).toMatchObject({ handoff: "already_escalated", handedTo: "rep@docdinners.com" });
  });
});

// ─── No rep: the brand's own team is told (REPLY-NOTIFS-1013) ──────────────────

const UNIBOX = "https://dashboard.distribute.you/v2/orgs/org_2abc/brands/brand-1/unibox";

describe("escalating when the brand names no rep", () => {
  beforeEach(() => {
    mockBrand.mockResolvedValue({ name: "Doc Dinners", rep: { email: null, firstName: null, role: null } });
  });

  it("emails EVERY member of the brand's org, never staff, with their words and the Unibox link", async () => {
    const result = await escalateReply(INPUT);
    expect(mockReplyToLead).not.toHaveBeenCalled();
    expect(mockComplete).not.toHaveBeenCalled();
    expect(mockListMembers).toHaveBeenCalledWith("org-1");
    expect(mockSendEmail).toHaveBeenCalledTimes(2);
    const recipients = mockSendEmail.mock.calls.map(([p]) => p.recipientEmail);
    expect(recipients).toEqual(["owner@docdinners.com", "sales@docdinners.com"]);
    expect(recipients).not.toContain("kevin@distribute.you");
    const [params, identity] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("reply-escalation");
    expect(params.ccEmails).toBeUndefined();
    expect(params.bccEmails).toBeUndefined();
    expect(params.metadata.subject).toBe("michael@clinic.com asked your Doc Dinners team a question");
    expect(params.metadata.text).toContain("Hi Ana,");
    expect(params.metadata.text).toContain("1. Comparable clinic results?");
    expect(params.metadata.text).toContain(UNIBOX);
    expect(params.metadata.html).toContain(UNIBOX);
    // escaped: the template interpolates raw
    expect(params.metadata.html).toContain("1. Results? &lt;b&gt;");
    // the model's paraphrase is shown to nobody
    expect(JSON.stringify(params.metadata)).not.toContain("patient privacy...");
    expect(mockSendEmail.mock.calls[1][0].metadata.text).toContain("Hi,\n");
    expect(identity).toMatchObject({ orgId: "org-1", userId: "user-1", runId: "run-1" });
    expect(result).toMatchObject({
      handoff: "team",
      handedTo: "owner@docdinners.com, sales@docdinners.com",
      followupsStopped: true,
    });
    expect(mockHandFollowups.mock.calls[0][0].reason).toContain("Handed to the brand's team");
  });

  it("releases the claim and fails loud when no member could be emailed", async () => {
    mockSendEmail.mockRejectedValue(new Error("postmark down"));
    await expect(escalateReply(INPUT)).rejects.toThrow("no member of org org-1 could be emailed");
    const released = mockExecute.mock.calls.some(([q]) => JSON.stringify(q).includes("escalated_at = NULL"));
    expect(released).toBe(true);
    expect(mockHandFollowups).not.toHaveBeenCalled();
  });

  it("fails loud, never falls back to staff, when the org has no member at all", async () => {
    mockListMembers.mockResolvedValue([]);
    await expect(escalateReply(INPUT)).rejects.toThrow("no member with an email address");
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("emails only the org's OWN people: Clerk's whole membership (staff included) is not read", async () => {
    await escalateReply(INPUT);
    expect(mockListMembers).toHaveBeenCalledTimes(1);
    expect(mockListMembers).toHaveBeenCalledWith("org-1");
  });

  // ORG-MEMBERS-1013: our own distribute.you org has no user of its own (Kevin's
  // user row points at another org); its Clerk members are its people.
  it("reads Clerk's whole membership only when the org has no people of its own", async () => {
    mockListMembers.mockImplementation(async (_org: string, opts?: { allMembers?: boolean }) =>
      opts?.allMembers ? [{ email: "kevin.lourd@gmail.com", firstName: "Kevin" }] : [],
    );
    const result = await escalateReply(INPUT);
    expect(mockListMembers).toHaveBeenLastCalledWith("org-1", { allMembers: true });
    expect(mockSendEmail.mock.calls.map(([p]) => p.recipientEmail)).toEqual(["kevin.lourd@gmail.com"]);
    expect(result).toMatchObject({ handoff: "team", handedTo: "kevin.lourd@gmail.com" });
  });

  it("links the dashboard home when the org's dashboard id is unknown", async () => {
    mockExternalOrgId.mockRejectedValue(new Error("client-service down"));
    await escalateReply(INPUT);
    expect(mockSendEmail.mock.calls[0][0].metadata.text).toContain("https://dashboard.distribute.you/v2\n");
  });
});

describe("a hand-over that cannot go out", () => {
  it("falls back to the brand's team when the in-thread reply is refused", async () => {
    mockReplyToLead.mockRejectedValue(new FakeReplyToLeadError("human_took_over", 409, "a person answered"));
    const result = await escalateReply(INPUT);
    expect(mockSendEmail).toHaveBeenCalledTimes(2);
    expect(result).toMatchObject({ handoff: "team" });
  });

  it("falls back to the brand's team when the reply is unreadable, and says so", async () => {
    mockLoadHistory.mockResolvedValue({ history: { ...HISTORY, notes: ["unreadable"] }, latestReply: null });
    const result = await escalateReply(INPUT);
    expect(mockComplete).not.toHaveBeenCalled();
    expect(mockReplyToLead).not.toHaveBeenCalled();
    expect(mockSendEmail.mock.calls[0][0].metadata.text).toContain("Their reply could not be read");
    expect(result).toMatchObject({ handoff: "team", replyRead: false });
  });
});

// ─── A reply the responder never answers (off-topic, referral) ──────────────

describe("handThreadToHuman", () => {
  const THREAD = {
    instantlyCampaignId: "ic-1",
    campaignId: "camp-1",
    leadEmail: "jakub@marktize.com",
    brandId: "brand-1",
    orgId: "org-1",
    userId: "user-1",
    runId: "run-1",
    stopReason: "off topic",
  };

  it("stops the ladder and emails NOBODY when the brand pays for no AI answers", async () => {
    const result = await handThreadToHuman({ ...THREAD, notifyTeam: null });
    expect(mockSendEmail).not.toHaveBeenCalled();
    expect(mockListMembers).not.toHaveBeenCalled();
    expect(mockHandFollowups).toHaveBeenCalledWith({ orgId: "org-1", leadRowId: "lead-row-1", reason: "off topic" });
    expect(result).toMatchObject({ handoff: "none", handedTo: null, followupsStopped: true });
  });

  it("tells the brand's team, in off-topic words, when it pays for AI answers", async () => {
    const result = await handThreadToHuman({ ...THREAD, notifyTeam: "off_topic" });
    expect(mockSendEmail).toHaveBeenCalledTimes(2);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.recipientEmail).toBe("owner@docdinners.com");
    expect(params.metadata.text).toContain("about something other than your offer");
    expect(result).toMatchObject({ handoff: "team", followupsStopped: true });
  });
});

describe("refusals", () => {
  it("refuses an empty question", async () => {
    await expect(escalateReply({ ...INPUT, question: "  " })).rejects.toBeInstanceOf(EscalateReplyError);
  });

  it("refuses an unknown campaign", async () => {
    mockLoadCampaign.mockResolvedValue(null);
    await expect(escalateReply(INPUT)).rejects.toMatchObject({ code: "campaign_not_found" });
  });
});

describe("pure helpers", () => {
  it("turns the draft into paragraphs and removes em-dashes", () => {
    expect(handoffTextToHtml("Hi Bob,\n\nOne — two.\nthree\n\nBest,")).toBe(
      "<p>Hi Bob,</p><p>One, two.<br>three</p><p>Best,</p>",
    );
  });

  it("quotes the conversation newest first", () => {
    const html = renderQuotedHistory(HISTORY.messages as never);
    expect(html.indexOf("Comparable")).toBeLessThan(html.indexOf("cold 1"));
    expect(html).toContain("michael@clinic.com wrote:");
  });
});
