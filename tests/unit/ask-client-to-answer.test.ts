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
const mockHandThread = vi.fn();
vi.mock("../../src/lib/hand-followups-to-human", () => ({
  handThreadFollowupsToHuman: (...a: unknown[]) => mockHandThread(...a),
}));
const mockBrandStatus = vi.fn();
vi.mock("../../src/lib/brand-account-status", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  fetchBrandAccountStatus: (...a: unknown[]) => mockBrandStatus(...a),
}));
const mockBrand = vi.fn();
vi.mock("../../src/lib/brand-client", () => ({ getBrandHandoffContext: (...a: unknown[]) => mockBrand(...a) }));
vi.mock("../../src/lib/prospect-history", () => ({
  loadHistoryWithLatestReply: (...a: unknown[]) => mockHistory(...a),
  REPLY_WAIT_SHORT_MS: [],
}));

import {
  answerRequestRecipients,
  latestProspectMessage,
  ourAnswerAfter,
  renderStaffCheckIn,
  templateCheckIn,
  cleanCheckIn,
  CHECK_IN_PROMISE_WORDING,
  threadSender,
  cleanSummaryLine,
  maybeAskClientToAnswer,
  renderAnswerRequest,
  replySubject,
  replyWordsOnly,
  tidyBody,
} from "../../src/lib/ask-client-to-answer";
import { isResponderCampaign } from "../../src/lib/campaign-client";
import { relationshipForStatus } from "../../src/lib/brand-account-status";
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

/** A day after Doug wrote: a fresh question. */
const DOUG_NOW = new Date("2026-10-09T10:00:00.000Z");

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

describe("renderAnswerRequest (owner copy 2026-10-08 + who line and date 2026-10-11)", () => {
  const content = renderAnswerRequest({
    clientFirstName: "David",
    leadEmail: "drdoug@prohealthdoc.com",
    leadFullName: "Doug Arvanitis",
    leadFirstName: "Doug",
    company: "Pro Health",
    summaryLine: "He asked how his clinical protocols compare with yours.",
    brandName: "Shockwavecenters",
    reply,
    earlier: [sent1, sent2, reply],
  });

  it("is the owner's copy word for word", () => {
    expect(content.subject).toBe("Re: Shockwave therapy ROI at Pro Health");
    expect(content.text.startsWith(
      [
        "Client: Shockwavecenters · Prospect: Doug Arvanitis (Pro Health)",
        "",
        "Hi David,",
        "",
        "Good news: Doug Arvanitis at Pro Health is interested and is waiting for an answer.",
        "They wrote on Oct 8, 2026.",
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
      company: null, summaryLine: null, brandName: null, reply, earlier: [],
    });
    expect(bare.text.startsWith("Client: unknown brand · Prospect: x@y.com\n\nHi,\n\nGood news: x@y.com is interested and is waiting for an answer.\nThey wrote on Oct 8, 2026.\n\nYou can answer")).toBe(true);
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
    leadFirstName: "Doug", company: "Pro Health", summaryLine: null, brandName: "Shockwavecenters", reply: quoting, earlier: [sent1, sent2, quoting],
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
      { email: "a@c.com", firstName: "A", cc: ["growth@distribute.you"] },
      { email: "b@c.com", firstName: null, cc: [] },
    ]);
    expect(answerRequestRecipients([], "growth@distribute.you")).toEqual([{ email: "growth@distribute.you", firstName: null, cc: [] }]);
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
    mockBrand.mockResolvedValue({ name: "Shockwavecenters", rep: { email: null, firstName: null, role: null } });
  });

  it("no responder running: asks every member, Reply-To = the prospect, subject Re: <thread>", async () => {
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com", { asOf: DOUG_NOW });

    expect(outcome.sent).toBe(true);
    expect(mockResponders).toHaveBeenCalledWith({ orgId: "org-uuid", brandId: campaign.brandIds[0], offerId: "offer-1" });
    expect(mockSendEmail).toHaveBeenCalledTimes(1);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("positive-reply-answer-request");
    expect(params.recipientEmail).toBe("shockwavecenters@gmail.com");
    expect(params.replyToEmail).toBe("drdoug@prohealthdoc.com");
    expect(params.ccEmails).toEqual(["growth@distribute.you"]);
    expect(params.bccEmails).toBeUndefined();
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

  it("once the client is asked, the thread is HANDED in lead-service: no responder switched on later answers it", async () => {
    await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com", { asOf: DOUG_NOW });
    expect(mockHandThread).toHaveBeenCalledTimes(1);
    expect(mockHandThread.mock.calls[0][0]).toMatchObject({
      orgId: "org-uuid",
      campaignId: campaign.campaignId,
      leadEmail: "drdoug@prohealthdoc.com",
    });
    expect(mockHandThread.mock.calls[0][0].reason).toContain("shockwavecenters@gmail.com");
  });

  it("a responder running: sends nothing and takes no claim", async () => {
    mockResponders.mockResolvedValue(["responder-campaign"]);
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com", { asOf: DOUG_NOW });
    expect(outcome).toEqual({ sent: false, reason: "responder_running" });
    expect(mockSendEmail).not.toHaveBeenCalled();
    expect(mockReturning).not.toHaveBeenCalled();
    expect(mockHandThread).not.toHaveBeenCalled();
  });

  it("once per thread: a taken claim sends nothing", async () => {
    mockReturning.mockResolvedValue([]);
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com", { asOf: DOUG_NOW });
    expect(outcome).toEqual({ sent: false, reason: "already_asked" });
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("an unreadable responder answer sends nothing (never read as 'nobody answers')", async () => {
    mockResponders.mockRejectedValue(new Error("campaign-service 503"));
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com", { asOf: DOUG_NOW });
    expect(outcome).toEqual({ sent: false, reason: "responder_read_failed" });
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("the summary line is dropped, not invented, when the model fails", async () => {
    mockComplete.mockRejectedValue(new Error("chat down"));
    await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com", { asOf: DOUG_NOW });
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.text).toContain("They wrote on Oct 8, 2026.\n\nYou can answer");
  });

  it("no member could be emailed: the claim is released", async () => {
    mockSendEmail.mockRejectedValue(new Error("postmark down"));
    const outcome = await maybeAskClientToAnswer(campaign, "drdoug@prohealthdoc.com", { asOf: DOUG_NOW });
    expect(outcome).toEqual({ sent: false, reason: "send_failed" });
    expect(mockSet).toHaveBeenLastCalledWith(expect.objectContaining({ clientAnswerRequestedAt: null }));
    // Nobody was told, so nobody owns it: the thread is NOT handed.
    expect(mockHandThread).not.toHaveBeenCalled();
  });
});

// ─── The two threads that confused the owner (2026-10-11) ────────────────────

/** Dr. Gallego, Doc Dinners: our cold email, our follow-up, HER question, then OUR manual bump. */
const gallego = {
  cold: { direction: "outbound", from: "fallon@agileconsultco.com", to: "drgallego@shinwellness.com", date: "2026-07-28T13:02:35.000Z", subject: "Doctor dinners in Phoenix", bodyText: "Hi Dr. Gallego, first email." },
  followUp: { direction: "outbound", from: "fallon@agileconsultco.com", to: "drgallego@shinwellness.com", date: "2026-07-31T13:13:42.000Z", subject: "Re: Doctor dinners in Phoenix", bodyText: "Hi Dr. Gallego, follow-up." },
  question: { direction: "inbound", from: "drgallego@shinwellness.com", to: "fallon@agileconsultco.com", date: "2026-08-02T15:38:55.000Z", subject: "Re: Doctor dinners in Phoenix", bodyText: "Yes can you please send me more info please?\n\nOn Fri, Jul 31, 2026 Fallon wrote:\n> Hi Dr. Gallego, follow-up." },
  bump: { direction: "outbound", from: "fallon@agileconsultco.com", to: "drgallego@shinwellness.com", date: "2026-09-04T17:14:04.000Z", subject: "Re: Doctor dinners in Phoenix", bodyText: "Hello Dr. Gallego, Just checking if you had news from the DocDinners team..." },
} satisfies Record<string, ThreadMessage>;
const gallegoThread: ThreadMessage[] = [gallego.cold, gallego.followUp, gallego.question, gallego.bump];

/** Américo Vargas, ObraCam: our emails, HIS question, nothing after. */
const vargas = {
  cold: { direction: "outbound", from: "Kevin <kevin@growdistribute.com>", to: "ava@befco-sa.cl", date: "2026-09-22T12:00:00.000Z", subject: "Cámaras para obras", bodyText: "Hola Américo, primer correo." },
  question: { direction: "inbound", from: "Américo Vargas <ava@befco-sa.cl>", to: "kevin@growdistribute.com", date: "2026-09-29T11:20:36.000Z", subject: "Re: Cámaras para obras", bodyText: "Hola, envíame el informe de muestra y los costos." },
} satisfies Record<string, ThreadMessage>;
const vargasThread: ThreadMessage[] = [vargas.cold, vargas.question];

describe("AC1: they wrote = the prospect's own latest message, dated when THEY wrote", () => {
  it("Gallego: our Sep 4 bump is never quoted as her question", () => {
    expect(latestProspectMessage(gallegoThread)).toBe(gallego.question);
  });
  it("Gallego: even when our bump is filed INBOUND (a copy into the thread), it is ours", () => {
    const bumpFiledInbound = { ...gallego.bump, direction: "inbound" as const };
    expect(latestProspectMessage([gallego.cold, gallego.followUp, gallego.question, bumpFiledInbound])).toBe(gallego.question);
  });
  it("Vargas: his Sep 29 question", () => {
    expect(latestProspectMessage(vargasThread)).toBe(vargas.question);
  });
});

describe("AC2: a message from us after their question = a person handled it", () => {
  it("Gallego: Fallon's Unibox bump on Sep 4 answers the Aug 2 question", () => {
    expect(ourAnswerAfter(gallegoThread, gallego.question)).toBe(gallego.bump);
  });
  it("Gallego: counted even when filed inbound", () => {
    const bumpFiledInbound = { ...gallego.bump, direction: "inbound" as const };
    expect(ourAnswerAfter([gallego.cold, gallego.question, bumpFiledInbound], gallego.question)).toBe(bumpFiledInbound);
  });
  it("Vargas: nothing from us after his question", () => {
    expect(ourAnswerAfter(vargasThread, vargas.question)).toBeNull();
  });
});

describe("AC3 + AC4: the staff check-in for an OLD question (Vargas, 12 days)", () => {
  const content = renderStaffCheckIn({
    brandName: "ObraCam",
    leadEmail: "ava@befco-sa.cl",
    leadFullName: "Américo Vargas",
    company: "Befco",
    question: vargas.question,
    earlier: vargasThread,
    senderAddress: "kevin@growdistribute.com",
    checkIn: "Hola Américo,\n\nTe escribo para saber si pudiste hablar con ObraCam sobre el informe y los costos. Para cualquier otra cosa, escribe directamente a ObraCam.\n\nKevin",
  });

  it("opens with one line naming the client and the prospect, then what to do", () => {
    expect(content.text.startsWith(
      [
        "Client: ObraCam · Prospect: Américo Vargas (Befco)",
        "",
        "Américo Vargas asked a question on Sep 29, 2026.",
        "Nobody answered in this thread.",
        "Someone may have answered another way, so check in first.",
        "",
        "Send this in the thread, from kevin@growdistribute.com:",
        "",
        "Hola Américo,",
      ].join("\n"),
    )).toBe(true);
    expect(content.subject).toBe("Check in with Américo Vargas (ObraCam)");
  });

  it("names the brand, never 'The client'; no dashes", () => {
    expect(content.text).toContain("ObraCam was not emailed about this.");
    expect(content.text).not.toMatch(/the client/i);
    expect(content.text.split("\n---\n")[0]).not.toMatch(/[—–]/);
    expect(content.subject).not.toMatch(/[—–]/);
  });

  it("quotes his words, dated Sep 29, below the line", () => {
    const below = content.text.split("\n---\n")[1];
    expect(below.startsWith("From: Américo Vargas <ava@befco-sa.cl>\nDate: Sep 29, 2026")).toBe(true);
    expect(below).toContain("envíame el informe de muestra y los costos");
  });

  it("the template check-in (model down) is short, names the brand, signed as the thread's sender", () => {
    expect(threadSender(vargasThread)).toEqual({ address: "kevin@growdistribute.com", firstName: "Kevin" });
    expect(threadSender(gallegoThread)).toEqual({ address: "fallon@agileconsultco.com", firstName: "Fallon" });
    expect(templateCheckIn({ leadFirstName: "Américo", brandName: "ObraCam", senderFirstName: "Kevin", relationship: "not_working_with_us" })).toBe(
      "Hi Américo,\n\nFollowing up on your question. Did you get what you needed? For anything else, please reach ObraCam directly.\n\nKevin",
    );
  });
});

describe("maybeAskClientToAnswer on the two real threads", () => {
  const vargasCampaign = { ...campaign, instantlyCampaignId: "self:45edbc68-1608-4b5a-90a1-4b3d1eb437ee" };
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_NOTIFICATION_EMAIL = "growth@distribute.you";
    mockReturning.mockResolvedValue([{ id: "row" }]);
    mockMembers.mockResolvedValue([{ email: "owner@obracam.cl", firstName: "Rodrigo" }]);
    mockSendEmail.mockResolvedValue(undefined);
  });

  it("Gallego (question, then our manual bump): nothing sent, claim released, thread not handed", async () => {
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Dr.", lastName: "Gallego", company: "Shin Wellness" });
    mockBrand.mockResolvedValue({ name: "Doc Dinners", rep: { email: null, firstName: null, role: null } });
    mockHistory.mockResolvedValue({
      history: { items: gallegoThread.map((message) => ({ type: "message", message })), notes: [] },
      latestReply: gallego.question,
    });
    const outcome = await maybeAskClientToAnswer(campaign, "drgallego@shinwellness.com", {
      because: "overdue",
      asOf: new Date("2026-10-11T10:26:00Z"),
    });
    expect(outcome).toEqual({ sent: false, reason: "answered_in_thread" });
    expect(mockSendEmail).not.toHaveBeenCalled();
    expect(mockSet).toHaveBeenLastCalledWith(expect.objectContaining({ clientAnswerRequestedAt: null }));
    expect(mockHandThread).not.toHaveBeenCalled();
  });

  it("Vargas, 12 days later: staff only, a check-in in his language; the client is NOT emailed", async () => {
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Américo", lastName: "Vargas", company: "Befco" });
    mockBrand.mockResolvedValue({ name: "ObraCam", rep: { email: null, firstName: null, role: null } });
    mockHistory.mockResolvedValue({
      history: { items: vargasThread.map((message) => ({ type: "message", message })), notes: [] },
      latestReply: vargas.question,
    });
    mockComplete.mockResolvedValue({
      content: "Hola Américo,\n\nTe escribo para saber si pudiste hablar con ObraCam sobre el informe y los costos. Para cualquier otra cosa, escribe directamente a ObraCam.\n\nKevin",
    });
    const outcome = await maybeAskClientToAnswer(vargasCampaign, "ava@befco-sa.cl", {
      because: "overdue",
      asOf: new Date("2026-10-11T10:12:00Z"),
    });
    expect(outcome).toMatchObject({ sent: true, to: "staff", recipients: ["growth@distribute.you"] });
    expect(mockSendEmail).toHaveBeenCalledTimes(1);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("unanswered-question-check-in");
    expect(params.recipientEmail).toBe("growth@distribute.you");
    expect(params.ccEmails).toBeUndefined();
    expect(params.replyToEmail).toBeUndefined();
    expect(mockMembers).not.toHaveBeenCalled();
    expect(params.metadata.text.split("\n")[0]).toBe("Client: ObraCam · Prospect: Américo Vargas (Befco)");
    expect(params.metadata.text).toContain("Américo Vargas asked a question on Sep 29, 2026.");
    expect(params.metadata.text).toContain("Hola Américo,\n\nTe escribo para saber si pudiste hablar con ObraCam");
    // the model is told his own words, never haiku, no sampling parameter
    const [completeParams] = mockComplete.mock.calls[0];
    expect(completeParams.message).toContain("envíame el informe de muestra y los costos");
    expect(completeParams.systemPrompt).toContain("language the prospect wrote in");
    expect(completeParams.model).not.toBe("haiku");
    expect(completeParams).not.toHaveProperty("temperature");
    // follow-ups handed to a person, once per thread (the claim is kept)
    expect(mockHandThread).toHaveBeenCalledTimes(1);
    expect(mockSet).not.toHaveBeenCalledWith(expect.objectContaining({ clientAnswerRequestedAt: null }));
  });

  it("Vargas, model down: the English template check-in, still staff only", async () => {
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Américo", lastName: "Vargas", company: "Befco" });
    mockBrand.mockResolvedValue({ name: "ObraCam", rep: { email: null, firstName: null, role: null } });
    mockHistory.mockResolvedValue({
      history: { items: vargasThread.map((message) => ({ type: "message", message })), notes: [] },
      latestReply: vargas.question,
    });
    mockComplete.mockRejectedValue(new Error("chat down"));
    await maybeAskClientToAnswer(vargasCampaign, "ava@befco-sa.cl", { because: "overdue", asOf: new Date("2026-10-11T10:12:00Z") });
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.recipientEmail).toBe("growth@distribute.you");
    expect(params.metadata.text).toContain("Did you get what you needed? For anything else, please reach ObraCam directly.\n\nKevin");
  });

  it("Vargas, the day after he wrote: the client is asked (staff in Cc), and the email names ObraCam and him first", async () => {
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Américo", lastName: "Vargas", company: "Befco" });
    mockBrand.mockResolvedValue({ name: "ObraCam", rep: { email: null, firstName: null, role: null } });
    mockHistory.mockResolvedValue({
      history: { items: vargasThread.map((message) => ({ type: "message", message })), notes: [] },
      latestReply: vargas.question,
    });
    mockComplete.mockResolvedValue({ content: "Américo asked for the sample report and the costs." });
    const outcome = await maybeAskClientToAnswer(vargasCampaign, "ava@befco-sa.cl", { because: "overdue", asOf: new Date("2026-09-30T12:00:00Z") });
    expect(outcome).toMatchObject({ sent: true, to: "client" });
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("positive-reply-answer-request");
    expect(params.recipientEmail).toBe("owner@obracam.cl");
    expect(params.ccEmails).toEqual(["growth@distribute.you"]);
    expect(params.metadata.text.startsWith("Client: ObraCam · Prospect: Américo Vargas (Befco)\n\nHi Rodrigo,")).toBe(true);
    expect(params.metadata.text).toContain("They wrote on Sep 29, 2026.");
  });
});

// ─── Follow-up 2026-10-12: she wrote AGAIN after our bump (prospect, us, prospect) ──

/** Dr. Gallego's Sep 14 objection: filed by our IMAP poller, after Fallon's Sep 4 bump. */
const gallegoObjection: ThreadMessage = {
  direction: "inbound",
  from: "\"Dr. Wendy Gallego\" <drgallego@shinwellness.com>",
  to: "fallon@agileconsultco.com",
  date: "2026-09-14T22:51:09.000Z",
  subject: "Re: Doctor dinners in Phoenix",
  bodyText: "If this is the same company I spoke to, the ROI wasn't great... You can let me know if this is something different?",
};
const gallegoFullThread: ThreadMessage[] = [...gallegoThread, gallegoObjection];

/** The same shape on our own sender: our dispatches, her replies from IMAP. */
const selfSendCampaign = { ...campaign, instantlyCampaignId: "self:7d1b6a10-0000-4000-8000-000000000001" };
const selfSendThread: ThreadMessage[] = [
  { ...gallego.cold, from: "fallon@agileconsultco.com" },
  { ...gallego.question, from: "\"Dr. Wendy Gallego\" <drgallego@shinwellness.com>" },
  { ...gallego.bump, from: "fallon@agileconsultco.com" },
  gallegoObjection,
];

describe("prospect, us, prospect: her NEWEST message counts, our earlier bump does not answer it", () => {
  for (const [transport, thread] of [
    ["Instantly (mirror + IMAP copy)", gallegoFullThread],
    ["self-send (dispatch log + IMAP)", selfSendThread],
  ] as const) {
    it(`${transport}: the Sep 14 objection is her latest message and nobody answered it`, () => {
      const latest = latestProspectMessage(thread);
      expect(latest).toBe(gallegoObjection);
      expect(ourAnswerAfter(thread, latest!)).toBeNull();
    });
  }
});

describe("maybeAskClientToAnswer on Gallego's real thread (Sep 14 objection unanswered)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_NOTIFICATION_EMAIL = "growth@distribute.you";
    mockReturning.mockResolvedValue([{ id: "row" }]);
    mockSendEmail.mockResolvedValue(undefined);
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Wendy", lastName: "Gallego", company: "Shin Wellness" });
    mockBrand.mockResolvedValue({ name: "Doc Dinners", rep: { email: null, firstName: null, role: null } });
    mockComplete.mockResolvedValue({ content: "Hi Wendy,\n\nThis is a different team. Happy to share what changed.\n\nFallon" });
  });

  for (const [transport, camp, thread] of [
    ["Instantly", campaign, gallegoFullThread],
    ["self-send", selfSendCampaign, selfSendThread],
  ] as const) {
    it(`${transport}: staff get a check-in quoting HER Sep 14 words, dated Sep 14; the client is not emailed`, async () => {
      mockHistory.mockResolvedValue({
        history: { items: thread.map((message) => ({ type: "message", message })), notes: [] },
        latestReply: gallegoObjection,
      });
      const outcome = await maybeAskClientToAnswer(camp, "drgallego@shinwellness.com", {
        because: "overdue",
        asOf: new Date("2026-10-12T12:00:00Z"),
      });
      expect(outcome).toMatchObject({ sent: true, to: "staff", recipients: ["growth@distribute.you"] });
      expect(mockSendEmail).toHaveBeenCalledTimes(1);
      const [params] = mockSendEmail.mock.calls[0];
      expect(params.eventType).toBe("unanswered-question-check-in");
      expect(params.recipientEmail).toBe("growth@distribute.you");
      expect(mockMembers).not.toHaveBeenCalled();
      const text: string = params.metadata.text;
      expect(text.split("\n")[0]).toBe("Client: Doc Dinners · Prospect: Wendy Gallego (Shin Wellness)");
      expect(text).toContain("asked a question on Sep 14, 2026.");
      const below = text.split("\n---\n")[1];
      expect(below).toContain("Date: Sep 14, 2026");
      expect(below.indexOf("the ROI wasn't great")).toBeLessThan(below.indexOf("Just checking if you had news"));
      // the model drafts the check-in from HER newest words, never Fallon's bump
      const [completeParams] = mockComplete.mock.calls[0];
      expect(completeParams.message).toContain("the ROI wasn't great");
    });
  }
});

// ─── Follow-up 2026-10-12: the check-in promises nothing, and knows if the brand churned ──

describe("the staff check-in never promises what only the brand can deliver", () => {
  const PROMISES = ["this week", "I will send", "I can set up", "esta semana", "te los hago llegar", "cette semaine", "je te les fais parvenir"];

  it("the template promises nothing, for an active brand and a churned one", () => {
    for (const relationship of ["working_with_us", "not_working_with_us", "unknown"] as const) {
      const text = templateCheckIn({ leadFirstName: "Wendy", brandName: "Doc Dinners", senderFirstName: "Fallon", relationship });
      expect(CHECK_IN_PROMISE_WORDING.test(text)).toBe(false);
      for (const p of PROMISES) expect(text.toLowerCase()).not.toContain(p.toLowerCase());
    }
  });

  it("active brand: the door stays open for the brand; churned or unknown: the prospect is pointed to the brand", () => {
    expect(templateCheckIn({ leadFirstName: "Wendy", brandName: "Doc Dinners", senderFirstName: "Fallon", relationship: "working_with_us" })).toBe(
      "Hi Wendy,\n\nFollowing up on your question. Did you get what you needed from Doc Dinners? They can follow up with you directly.\n\nFallon",
    );
    expect(templateCheckIn({ leadFirstName: "Américo", brandName: "ObraCam", senderFirstName: "Kevin", relationship: "unknown" })).toContain(
      "please reach ObraCam directly",
    );
  });

  it("a model draft that promises a deliverable or a time frame is dropped for the template (the owner's own example included)", () => {
    expect(cleanCheckIn("Hola Américo,\n\nSinon je te les fais parvenir cette semaine.\n\nKevin")).toBeNull();
    expect(cleanCheckIn("Hola Américo,\n\n¿Pudiste hablar con ObraCam? Si no, te los hago llegar esta semana.\n\nKevin")).toBeNull();
    expect(cleanCheckIn("Hi Doug,\n\nIf not, I can set up the intro this week.\n\nScott")).toBeNull();
    expect(cleanCheckIn("Hi Wendy,\n\nDid you get what you needed from Doc Dinners? They can follow up with you directly.\n\nFallon")).not.toBeNull();
  });

  it("the account status is read as the producer states it", () => {
    expect(relationshipForStatus("active")).toBe("working_with_us");
    expect(relationshipForStatus("reactive_only")).toBe("working_with_us");
    expect(relationshipForStatus("out_of_credit")).toBe("working_with_us");
    expect(relationshipForStatus("paused")).toBe("not_working_with_us");
    expect(relationshipForStatus("inactive")).toBe("not_working_with_us");
    expect(relationshipForStatus("no_payment_method")).toBe("not_working_with_us");
    expect(relationshipForStatus("unknown")).toBe("unknown");
    expect(relationshipForStatus(undefined)).toBe("unknown");
  });
});

describe("maybeAskClientToAnswer: the check-in adapts to the brand's status", () => {
  const vargasCampaign = { ...campaign, instantlyCampaignId: "self:45edbc68-1608-4b5a-90a1-4b3d1eb437ee" };
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_NOTIFICATION_EMAIL = "growth@distribute.you";
    mockReturning.mockResolvedValue([{ id: "row" }]);
    mockSendEmail.mockResolvedValue(undefined);
  });

  it("churned brand (ObraCam paused): the model is told it no longer works with us, and never told to promise", async () => {
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Américo", lastName: "Vargas", company: "Befco" });
    mockBrand.mockResolvedValue({ name: "ObraCam", rep: { email: null, firstName: null, role: null } });
    mockBrandStatus.mockResolvedValue({ status: "paused", relationship: "not_working_with_us" });
    mockHistory.mockResolvedValue({
      history: { items: vargasThread.map((message) => ({ type: "message", message })), notes: [] },
      latestReply: vargas.question,
    });
    mockComplete.mockResolvedValue({ content: "Hola Américo,\n\n¿Recibiste lo que necesitabas? Para cualquier otra cosa, escribe directamente a ObraCam.\n\nKevin" });
    await maybeAskClientToAnswer(vargasCampaign, "ava@befco-sa.cl", { because: "overdue", asOf: new Date("2026-10-12T10:00:00Z") });
    expect(mockBrandStatus).toHaveBeenCalledWith("org-uuid", campaign.brandIds[0]);
    const [completeParams] = mockComplete.mock.calls[0];
    expect(completeParams.message).toContain("Brand still works with us: no (account status: paused)");
    expect(completeParams.systemPrompt).toContain("point the prospect to the brand directly");
    expect(completeParams.systemPrompt).toContain("Never promise");
    expect(completeParams.systemPrompt).not.toMatch(/this week|set it up|set up the intro|sending the info/i);
    // the status is for the model, never for the prospect (a live draft said "ObraCam ya no trabaja con nosotros")
    expect(completeParams.systemPrompt).toContain("never say whether the brand works with us");
    // room to reason before writing: at 300 a long objection came back empty
    expect(completeParams.maxTokens).toBeGreaterThanOrEqual(1200);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.text).toContain("escribe directamente a ObraCam");
  });

  it("active brand (Doc Dinners): the model is told the brand still works with us", async () => {
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Wendy", lastName: "Gallego", company: "Shin Wellness" });
    mockBrand.mockResolvedValue({ name: "Doc Dinners", rep: { email: null, firstName: null, role: null } });
    mockBrandStatus.mockResolvedValue({ status: "active", relationship: "working_with_us" });
    mockHistory.mockResolvedValue({
      history: { items: gallegoFullThread.map((message) => ({ type: "message", message })), notes: [] },
      latestReply: gallegoObjection,
    });
    mockComplete.mockResolvedValue({ content: "Hi Wendy,\n\nI hear you on the ROI. Doc Dinners can follow up with you directly.\n\nFallon" });
    await maybeAskClientToAnswer(campaign, "drgallego@shinwellness.com", { because: "overdue", asOf: new Date("2026-10-12T12:00:00Z") });
    const [completeParams] = mockComplete.mock.calls[0];
    expect(completeParams.message).toContain("Brand still works with us: yes (account status: active)");
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.text).toContain("Doc Dinners can follow up with you directly.");
  });

  it("an unreadable status is 'unknown': nothing is invented, the template points to the brand", async () => {
    mockFindLead.mockResolvedValue({ id: "l", firstName: "Américo", lastName: "Vargas", company: "Befco" });
    mockBrand.mockResolvedValue({ name: "ObraCam", rep: { email: null, firstName: null, role: null } });
    mockBrandStatus.mockRejectedValue(new Error("features-service 503"));
    mockHistory.mockResolvedValue({
      history: { items: vargasThread.map((message) => ({ type: "message", message })), notes: [] },
      latestReply: vargas.question,
    });
    mockComplete.mockRejectedValue(new Error("chat down"));
    await maybeAskClientToAnswer(vargasCampaign, "ava@befco-sa.cl", { because: "overdue", asOf: new Date("2026-10-12T10:00:00Z") });
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.text).toContain("please reach ObraCam directly");
  });
});
