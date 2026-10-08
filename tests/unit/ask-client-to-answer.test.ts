import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockReturning = vi.fn();
const mockSet = vi.fn();
const mockScope = vi.fn();
const mockResponders = vi.fn();
const mockMembers = vi.fn();
const mockComplete = vi.fn();
const mockSendEmail = vi.fn();
const mockFindLead = vi.fn();
const mockHistory = vi.fn();

const whereObj = {
  returning: (...a: unknown[]) => mockReturning(...a),
  then: (onF: (v: unknown) => unknown, onR?: (e: unknown) => unknown) => Promise.resolve(undefined).then(onF, onR),
};

vi.mock("../../src/db", () => ({
  db: {
    update: () => ({
      set: (v: unknown) => {
        mockSet(v);
        return { where: () => whereObj };
      },
    }),
  },
}));

vi.mock("../../src/lib/campaign-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/campaign-client")>();
  return {
    ...actual,
    getCampaignTriggerScope: (...a: unknown[]) => mockScope(...a),
    findOngoingResponderCampaigns: (...a: unknown[]) => mockResponders(...a),
  };
});
vi.mock("../../src/lib/client-org-client", () => ({
  listOrgMembers: (...a: unknown[]) => mockMembers(...a),
  getExternalOrgId: async () => null,
}));
vi.mock("../../src/lib/chat-client", () => ({ orgComplete: (...a: unknown[]) => mockComplete(...a) }));
vi.mock("../../src/lib/email-client", () => ({ sendEmail: (...a: unknown[]) => mockSendEmail(...a) }));
vi.mock("../../src/lib/lead-client", () => ({ findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a) }));
vi.mock("../../src/lib/prospect-history", () => ({
  loadHistoryWithLatestReply: (...a: unknown[]) => mockHistory(...a),
  REPLY_WAIT_SHORT_MS: [],
}));

import {
  answerRequestRecipients,
  cleanSummaryLine,
  maybeAskClientToAnswer,
  renderAnswerRequest,
  replySubject,
  replyWordsOnly,
  tidyBody,
} from "../../src/lib/ask-client-to-answer";
import { isResponderCampaign } from "../../src/lib/campaign-client";
import type { ThreadMessage } from "../../src/lib/forward-positive-reply";
import { stripQuotedHistory } from "../../src/lib/self-send/qualify-reply";

// ─── Fixtures (Doug, Shockwavecenters, 2026-10-08) ──────────────────────────

const sent1: ThreadMessage = {
  direction: "outbound",
  from: "Kevin Lourd <kevin@shockwave-mail.com>",
  to: "drdoug@prohealthdoc.com",
  date: "2026-10-01T14:00:00.000Z",
  subject: "Shockwave therapy ROI at Pro Health",
  bodyText: "Hi Doug, first email.",
};
const sent2: ThreadMessage = { ...sent1, date: "2026-10-04T14:00:00.000Z", subject: "Re: Shockwave therapy ROI at Pro Health", bodyText: "Hi Doug, follow-up." };
const reply: ThreadMessage = {
  direction: "inbound",
  from: "Doug Arvanitis <drdoug@prohealthdoc.com>",
  to: "kevin@shockwave-mail.com",
  date: "2026-10-08T13:02:00.000Z",
  subject: "Re: Shockwave therapy ROI at Pro Health",
  bodyText: "I would be curious to see the difference between my clinical protocols and yours",
};

const campaign = {
  instantlyCampaignId: "54fb99ff-4976-4ccc-be41-47b5b5e86ca3",
  campaignId: "3922c8e1-3405-46af-8a56-1eef3f221b19",
  orgId: "org-uuid",
  userId: "user-uuid",
  runId: "run-uuid",
  brandIds: ["a179bbd9-8eed-4dba-9338-78125922b0c6"],
};

// ─── Pure ────────────────────────────────────────────────────────────────────

describe("replySubject", () => {
  it("answers under the thread's own subject, one Re:", () => {
    expect(replySubject("Re: RE: Fwd: Shockwave therapy ROI at Pro Health")).toBe("Re: Shockwave therapy ROI at Pro Health");
    expect(replySubject("Shockwave therapy ROI at Pro Health")).toBe("Re: Shockwave therapy ROI at Pro Health");
  });
});

describe("cleanSummaryLine", () => {
  it("drops what it cannot send, never makes one up", () => {
    expect(cleanSummaryLine("NONE")).toBeNull();
    expect(cleanSummaryLine("")).toBeNull();
    expect(cleanSummaryLine(null)).toBeNull();
    expect(cleanSummaryLine("line one\nline two")).toBeNull();
  });
  it("removes dashes and closes the sentence", () => {
    expect(cleanSummaryLine("Doug asked how his protocols compare — with yours")).toBe("Doug asked how his protocols compare, with yours.");
  });
});

describe("isResponderCampaign", () => {
  it("a conversation_to_* leg that writes back is a responder; the phone ring and sourcing are not", () => {
    expect(isResponderCampaign({ legKey: "conversation_to_meeting_booked", featureSlug: "ai-meeting-booking" })).toBe(true);
    expect(isResponderCampaign({ legKey: "conversation_to_booking_call", featureSlug: "ai-instant-call" })).toBe(false);
    expect(isResponderCampaign({ legKey: "start_to_conversation", featureSlug: "sales-cold-email-outreach" })).toBe(false);
    expect(isResponderCampaign({ legKey: "start_to_lead_found", featureSlug: "sourcing-apollo-cold-filters" })).toBe(false);
    expect(isResponderCampaign({ legKey: null, featureSlug: null })).toBe(false);
  });
});

describe("renderAnswerRequest (owner copy, locked 2026-10-08)", () => {
  const content = renderAnswerRequest({
    clientFirstName: "David",
    leadEmail: "drdoug@prohealthdoc.com",
    leadFullName: "Doug Arvanitis",
    leadFirstName: "Doug",
    company: "Pro Health",
    summaryLine: "He asked how his clinical protocols compare with yours.",
    reply,
    earlier: [sent1, sent2, reply],
  });

  it("is the owner's copy word for word", () => {
    expect(content.subject).toBe("Re: Shockwave therapy ROI at Pro Health");
    expect(content.text.startsWith(
      [
        "Hi David,",
        "",
        "Good news: Doug Arvanitis at Pro Health is interested and is waiting for an answer.",
        "He asked how his clinical protocols compare with yours.",
        "",
        "You can answer by hitting Reply on this email. It adds Doug's email in To:, under the same subject, with the conversation below.",
        "",
        "Thanks,",
        "Kevin",
        "",
        "---",
        "From: Doug Arvanitis <drdoug@prohealthdoc.com>",
      ].join("\n"),
    )).toBe(true);
  });

  it("puts their reply verbatim on top, then every email newest first, each with sender and date", () => {
    const below = content.text.split("\n---\n")[1];
    const iReply = below.indexOf("I would be curious to see the difference between my clinical protocols and yours");
    const iSent2 = below.indexOf("Hi Doug, follow-up.");
    const iSent1 = below.indexOf("Hi Doug, first email.");
    expect(iReply).toBeGreaterThanOrEqual(0);
    expect(iReply).toBeLessThan(iSent2);
    expect(iSent2).toBeLessThan(iSent1);
    expect(below.match(/I would be curious/g)).toHaveLength(1);
    expect(below).toContain("From: Kevin Lourd <kevin@shockwave-mail.com>");
    expect(below).toContain("Date: Oct 1, 2026");
  });

  it("no em-dash, no 'Could you', and the HTML is the same text escaped", () => {
    for (const s of [content.text, content.subject]) {
      expect(s).not.toMatch(/[—–]/);
      expect(s).not.toContain("Could you");
    }
    expect(content.html).toContain("&lt;drdoug@prohealthdoc.com&gt;");
  });

  it("drops the summary line, the company and the names when it does not have them", () => {
    const bare = renderAnswerRequest({
      clientFirstName: null, leadEmail: "x@y.com", leadFullName: null, leadFirstName: null,
      company: null, summaryLine: null, reply, earlier: [],
    });
    expect(bare.text).toContain("Hi,\n\nGood news: x@y.com is interested and is waiting for an answer.\n\nYou can answer");
    expect(bare.text).toContain("It adds their email in To:");
  });
});

describe("the reply quoting the thread (Doug, prod 2026-10-08)", () => {
  const quoting: ThreadMessage = {
    ...reply,
    bodyText: [
      "Hello Scott,", "", "", "", "I would be curious to see the difference between my clinical protocols and yours.", "", "",
      "Doug Arvanitis, D.C.", "", "",
      "From: Scott Miller <scott@axionmilestone.com>", "To: <drdoug@prohealthdoc.com>", "Date: Thu, 08 Oct 2026 08:09:35 -0400",
      "Subject: Re: Shockwave therapy ROI at Pro Health", "", "Hi Doug, follow-up.", "",
      "On Thu, October 1, 2026 12:11 PM, Scott Miller wrote:", "Hi Doug, first email.",
    ].join("\n"),
  };
  const content = renderAnswerRequest({
    clientFirstName: "David", leadEmail: "drdoug@prohealthdoc.com", leadFullName: "Doug Arvanitis",
    leadFirstName: "Doug", company: "Pro Health", summaryLine: null, reply: quoting, earlier: [sent1, sent2, quoting],
  });

  it("shows the conversation ONCE: their words on top, every earlier email once below", () => {
    const below = content.text.split("\n---\n")[1];
    expect(below.match(/Hi Doug, follow-up\./g)).toHaveLength(1);
    expect(below.match(/Hi Doug, first email\./g)).toHaveLength(1);
    expect(below.startsWith("From: Doug Arvanitis <drdoug@prohealthdoc.com>")).toBe(true);
    expect(below).toContain("Hello Scott,\n\nI would be curious to see the difference between my clinical protocols and yours.\n\nDoug Arvanitis, D.C.\n\nFrom: Kevin Lourd");
  });

  it("a reply with nothing quoted is unchanged; a body that is only a quote is kept as written", () => {
    expect(replyWordsOnly("Yes please.", stripQuotedHistory)).toBe("Yes please.");
    expect(replyWordsOnly("> quoted only", stripQuotedHistory)).toBe("> quoted only");
  });
});

describe("tidyBody", () => {
  it("keeps every word, folds runs of blank lines and trailing spaces", () => {
    expect(tidyBody("Hello Scott,\n\n\n\nI would be curious.\n\n\n\n\nDoug  \n\u200B\n")).toBe("Hello Scott,\n\nI would be curious.\n\nDoug");
  });
});

describe("answerRequestRecipients", () => {
  it("every member, the agency inbox in Bcc once; no member = the agency inbox", () => {
    expect(answerRequestRecipients([{ email: "a@c.com", firstName: "A" }, { email: "b@c.com", firstName: null }], "growth@distribute.you")).toEqual([
      { email: "a@c.com", firstName: "A", bcc: ["growth@distribute.you"] },
      { email: "b@c.com", firstName: null, bcc: [] },
    ]);
    expect(answerRequestRecipients([], "growth@distribute.you")).toEqual([{ email: "growth@distribute.you", firstName: null, bcc: [] }]);
  });
});

// ─── The decision ────────────────────────────────────────────────────────────

describe("maybeAskClientToAnswer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_NOTIFICATION_EMAIL = "growth@distribute.you";
    mockScope.mockResolvedValue({ brandId: campaign.brandIds[0], offerId: "offer-1" });
    mockResponders.mockResolvedValue([]);
    mockReturning.mockResolvedValue([{ id: "row" }]);
    mockMembers.mockResolvedValue([{ email: "shockwavecenters@gmail.com", firstName: "David" }]);
    mockFindLead.mockResolvedValue({ id: "l1", firstName: "Doug", lastName: "Arvanitis", name: "Doug Arvanitis", company: "Pro Health" });
    mockHistory.mockResolvedValue({
      history: { items: [sent1, sent2, reply].map((message) => ({ type: "message", message })), notes: [] },
      latestReply: reply,
    });
    mockComplete.mockResolvedValue({ content: "Doug asked how the clinical protocols at Pro Health compare with yours." });
    mockSendEmail.mockResolvedValue(undefined);
  });

  it("no responder running: asks every member, Reply-To = the prospect, subject Re: <thread>", async () => {
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com");

    expect(outcome.sent).toBe(true);
    expect(mockResponders).toHaveBeenCalledWith({ orgId: "org-uuid", brandId: campaign.brandIds[0], offerId: "offer-1" });
    expect(mockSendEmail).toHaveBeenCalledTimes(1);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("positive-reply-answer-request");
    expect(params.recipientEmail).toBe("shockwavecenters@gmail.com");
    expect(params.replyToEmail).toBe("drdoug@prohealthdoc.com");
    expect(params.bccEmails).toEqual(["growth@distribute.you"]);
    expect(params.metadata.subject).toBe("Re: Shockwave therapy ROI at Pro Health");
    expect(params.metadata.text).toContain("Hi David,");
    expect(params.metadata.text).toContain("Doug asked how the clinical protocols at Pro Health compare with yours.");
    // org-billed on the campaign row's run, never haiku
    const [completeParams, identity] = mockComplete.mock.calls[0];
    expect(completeParams.model).not.toBe("haiku");
    // sonnet answers 400 to any sampling parameter (prod 2026-10-08: line dropped)
    expect(completeParams).not.toHaveProperty("temperature");
    expect(identity).toMatchObject({ orgId: "org-uuid", runId: "run-uuid" });
  });

  it("a responder running: sends nothing and takes no claim", async () => {
    mockResponders.mockResolvedValue(["responder-campaign"]);
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com");
    expect(outcome).toEqual({ sent: false, reason: "responder_running" });
    expect(mockSendEmail).not.toHaveBeenCalled();
    expect(mockReturning).not.toHaveBeenCalled();
  });

  it("once per thread: a taken claim sends nothing", async () => {
    mockReturning.mockResolvedValue([]);
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com");
    expect(outcome).toEqual({ sent: false, reason: "already_asked" });
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("an unreadable responder answer sends nothing (never read as 'nobody answers')", async () => {
    mockResponders.mockRejectedValue(new Error("campaign-service 503"));
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com");
    expect(outcome).toEqual({ sent: false, reason: "responder_read_failed" });
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("the summary line is dropped, not invented, when the model fails", async () => {
    mockComplete.mockRejectedValue(new Error("chat down"));
    await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com");
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.text).toContain("is waiting for an answer.\n\nYou can answer");
  });

  it("no member could be emailed: the claim is released", async () => {
    mockSendEmail.mockRejectedValue(new Error("postmark down"));
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com");
    expect(outcome).toEqual({ sent: false, reason: "send_failed" });
    expect(mockSet).toHaveBeenLastCalledWith(expect.objectContaining({ clientAnswerRequestedAt: null }));
  });
});
