import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockReturning = vi.fn();
const mockUpdate = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockListEmails = vi.fn();
const mockSendEmail = vi.fn();
const mockExecute = vi.fn();
const mockFetchLeadConversation = vi.fn();
const mockSet = vi.fn();
const mockExternalOrgId = vi.fn();
const mockFindLead = vi.fn();

// A drizzle-ish update builder: .set().where() returns an object that is both
// awaitable (release: `await db.update()...where()`) and has .returning() (claim).
const whereObj = {
  returning: (...a: unknown[]) => mockReturning(...a),
  then: (onF: (v: unknown) => unknown, onR?: (e: unknown) => unknown) =>
    Promise.resolve(undefined).then(onF, onR),
};

vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    update: (...a: unknown[]) => {
      mockUpdate(...a);
      return {
        set: (v: unknown) => {
          mockSet(v);
          return { where: () => whereObj };
        },
      };
    },
  },
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveInstantlyApiKey(...a),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  listEmails: (...a: unknown[]) => mockListEmails(...a),
}));

vi.mock("../../src/lib/lead-conversation", () => ({
  fetchLeadConversation: (...a: unknown[]) => mockFetchLeadConversation(...a),
}));

const mockBrandContext = vi.fn();
vi.mock("../../src/lib/brand-client", () => ({
  getBrandHandoffContext: (...a: unknown[]) => mockBrandContext(...a),
  getSalesRep: async () => ({ email: null, phone: null }),
}));

vi.mock("../../src/lib/client-org-client", () => ({
  getExternalOrgId: (...a: unknown[]) => mockExternalOrgId(...a),
}));

vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
}));

vi.mock("../../src/lib/email-client", () => ({
  sendEmail: (...a: unknown[]) => mockSendEmail(...a),
}));

import {
  isPositiveQualification,
  POSITIVE_QUALIFICATION_EVENT_TYPES,
  htmlToText,
  selectThreadMessages,
  threadSubject,
  formatThreadDate,
  maybeForwardPositiveReply,
  type ForwardPositiveReplyCampaign,
} from "../../src/lib/forward-positive-reply";
import { REPLY_CLASSIFICATION_MAP } from "../../src/lib/silver-promote";

const campaign: ForwardPositiveReplyCampaign = {
  instantlyCampaignId: "inst-camp-1",
  campaignId: "camp-1",
  orgId: "org-1",
  userId: "user-1",
  runId: "run-1",
  brandIds: ["brand-1"],
};

function record(overrides: Record<string, unknown>) {
  return {
    id: "e",
    campaign_id: "inst-camp-1",
    lead: "lead@x.com",
    lead_id: null,
    eaccount: "amy@distribute.com",
    ue_type: 1,
    step: "step-1",
    timestamp_email: "2026-07-14T10:00:00.000Z",
    ...overrides,
  };
}

describe("isPositiveQualification / positive set", () => {
  it("is true for the three buying positive kinds; a referral is escalated instead", () => {
    expect(isPositiveQualification("lead_interested")).toBe(true);
    expect(isPositiveQualification("lead_referral")).toBe(false);
    expect(isPositiveQualification("lead_info_requested")).toBe(true);
    expect(isPositiveQualification("lead_meeting_requested")).toBe(true);
    // Negative / neutral / non-qualified → never
    expect(isPositiveQualification("lead_not_interested")).toBe(false);
    expect(isPositiveQualification("lead_out_of_office")).toBe(false);
    expect(isPositiveQualification("reply_received")).toBe(false);
    expect(isPositiveQualification("email_opened")).toBe(false);
    // Deal progress is not a reply kind at all any more.
    expect(isPositiveQualification("lead_meeting_booked")).toBe(false);
    expect(isPositiveQualification("lead_closed")).toBe(false);
  });

  // The forwarding set and the coarse metric map answer DIFFERENT questions, so
  // they are no longer the same set. Forwarding = "is this worth a human's
  // eyes"; the coarse map = "was this a buying signal we count and price". A
  // referral is the one kind where those answers differ: forward it, but never
  // report it as the customer's sales interest.
  it("covers every REPLY_CLASSIFICATION_MAP 'positive' entry, and nothing else", () => {
    const positiveFromMap = Object.entries(REPLY_CLASSIFICATION_MAP)
      .filter(([, v]) => v === "positive")
      .map(([k]) => k)
      .sort();
    for (const kind of positiveFromMap) {
      expect(POSITIVE_QUALIFICATION_EVENT_TYPES.has(kind)).toBe(true);
    }
    const forwardedButNotPositive = [...POSITIVE_QUALIFICATION_EVENT_TYPES]
      .filter((k) => !positiveFromMap.includes(k))
      .sort();
    expect(forwardedButNotPositive).toEqual([]);
  });

  it("does NOT forward a referral: it is escalated to a person instead (one email per reply)", () => {
    expect(isPositiveQualification("lead_referral")).toBe(false);
    expect(REPLY_CLASSIFICATION_MAP.lead_referral).toBe("neutral");
  });
});

describe("htmlToText", () => {
  it("strips tags, turns breaks into newlines, decodes common entities", () => {
    const out = htmlToText(
      "<p>Hi Amy,</p><p>Yes let's talk &amp; meet.<br>Best</p><style>x{}</style>",
    );
    expect(out).toContain("Hi Amy,");
    expect(out).toContain("Yes let's talk & meet.");
    expect(out).toContain("Best");
    expect(out).not.toContain("<");
    expect(out).not.toContain("style");
  });

  it("separates paragraphs with an EMPTY line, a <br> with one newline", () => {
    const out = htmlToText(
      "<p>Hi Julia,</p><p>First paragraph.</p><p>Line one<br>line two</p><p>--</p><p>Amy Moore<br>Distribute.you</p>",
    );
    expect(out).toBe(
      "Hi Julia,\n\nFirst paragraph.\n\nLine one\nline two\n\n--\n\nAmy Moore\nDistribute.you",
    );
  });

  it("keeps a client's one-<div>-per-line reply on single lines", () => {
    expect(htmlToText("<div>Yes</div><div>send details</div><div><br></div><div>Bob</div>")).toBe(
      "Yes\nsend details\n\nBob",
    );
  });

  it("never stacks more than one empty line", () => {
    expect(htmlToText("<p>a</p><p><br></p><p>b</p>")).toBe("a\n\nb");
  });
});

describe("selectThreadMessages", () => {
  it("orders oldest→newest, labels direction, skips scheduled (ue_type 4)", () => {
    const msgs = selectThreadMessages([
      record({
        ue_type: 2,
        timestamp_email: "2026-07-14T12:00:00.000Z",
        from_address_email: "lead@x.com",
        to_address_email_list: "amy@distribute.com",
        subject: "Re: hi",
        body: { text: "Sounds great!" },
      }),
      record({
        ue_type: 1,
        timestamp_email: "2026-07-14T10:00:00.000Z",
        from_address_email: "amy@distribute.com",
        to_address_email_list: "lead@x.com",
        subject: "hi",
        body: { html: "<p>Hello there</p>" },
      }),
      record({ ue_type: 4, timestamp_email: "2026-07-14T14:00:00.000Z" }),
    ]);

    expect(msgs).toHaveLength(2);
    expect(msgs[0].direction).toBe("outbound");
    expect(msgs[0].bodyText).toBe("Hello there");
    expect(msgs[1].direction).toBe("inbound");
    expect(msgs[1].from).toBe("lead@x.com");
    expect(msgs[1].bodyText).toBe("Sounds great!");
  });
});

describe("thread helpers", () => {
  const msgs = selectThreadMessages([
    record({
      ue_type: 1,
      from_address_email: "amy@distribute.com",
      to_address_email_list: "lead@x.com",
      subject: "Functional medicine",
      body: { text: "Hello" },
    }),
    record({
      ue_type: 2,
      timestamp_email: "2026-07-14T12:00:00.000Z",
      from_address_email: "lead@x.com",
      to_address_email_list: "amy@distribute.com",
      subject: "Re: Functional medicine",
      body: { text: "Interested!" },
    }),
  ]);

  it("threadSubject = the newest real subject", () => {
    expect(threadSubject(msgs)).toBe("Re: Functional medicine");
    expect(threadSubject([])).toBe("(no subject)");
  });

  it("formatThreadDate is readable UTC", () => {
    expect(formatThreadDate("2026-07-13T17:57:13.748Z")).toContain("Jul 13, 2026");
    expect(formatThreadDate("2026-07-13T17:57:13.748Z")).toContain("UTC");
  });

});

describe("maybeForwardPositiveReply (the celebration)", () => {
  const NOW = { background: false, waitsMs: [] as number[] };

  beforeEach(() => {
    mockReturning.mockReset();
    mockUpdate.mockReset();
    mockResolveInstantlyApiKey.mockReset();
    mockListEmails.mockReset();
    mockSendEmail.mockReset();
    mockExecute.mockReset();
    mockFetchLeadConversation.mockReset();
    mockBrandContext.mockReset();
    mockSet.mockReset();
    mockExternalOrgId.mockReset();
    mockFindLead.mockReset();
    mockExternalOrgId.mockResolvedValue("org_3Jv0lYZiVaymxPEuyEbrbXs8hpv");
    mockFindLead.mockResolvedValue({ id: "346a5c86-ac49-4321-a873-a63c830b3217", company: null });
    mockExecute.mockResolvedValue({ rows: [] });
    mockFetchLeadConversation.mockRejectedValue(new Error("campaign-service down"));
    mockReturning.mockResolvedValue([{ id: "row-1" }]); // claim won by default
    mockResolveInstantlyApiKey.mockResolvedValue({ key: "api-key-1", keySource: "org" });
    mockBrandContext.mockResolvedValue({
      name: "Doc Dinners",
      rep: { email: "rep@docdinners.com", firstName: null, role: null },
    });
    mockListEmails.mockResolvedValue([
      record({ ue_type: 1, timestamp_email: "2026-07-08T13:00:00.000Z", subject: "Partnership?", body: { text: "cold 1" } }),
      record({
        ue_type: 2,
        timestamp_email: "2026-07-13T17:00:00.000Z",
        from_address_email: "lead@x.com",
        to_address_email_list: "amy@distribute.com",
        subject: "Re: Partnership?",
        body: { text: "1. What results?\n2. <b>Guarantee</b> terms?\n\nMichael" },
      }),
    ]);
    mockSendEmail.mockResolvedValue(undefined);
  });

  it("celebrates to the brand's rep, the agency inbox in Bcc, with the reply verbatim", async () => {
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", NOW);

    expect(mockUpdate).toHaveBeenCalledTimes(1); // claim only
    expect(mockSendEmail).toHaveBeenCalledTimes(1);
    const [params, identity] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("positive-reply-celebration");
    expect(params.recipientEmail).toBe("rep@docdinners.com");
    expect(params.bccEmails).toEqual(["kevin@distribute.you"]);
    expect(params.ccEmails).toBeUndefined();
    // complete and verbatim, escaped (the engine interpolates raw)
    expect(params.metadata.html).toContain("1. What results?\n2. &lt;b&gt;Guarantee&lt;/b&gt; terms?\n\nMichael");
    expect(params.metadata.text).toContain("1. What results?\n2. <b>Guarantee</b> terms?");
    // Short: the earlier emails live behind the button, not in the email.
    expect(params.metadata.html).not.toContain("cold 1");
    expect(params.metadata.html).toContain("#2563EB");
    expect(params.metadata.subject).toContain("Doc Dinners");
    for (const body of [params.metadata.html, params.metadata.text, params.metadata.subject]) {
      expect(body).not.toContain("\u2014");
    }
    expect(identity.orgId).toBe("org-1");
  });

  it("a brand that names no rep celebrates to the agency inbox alone", async () => {
    mockBrandContext.mockResolvedValue({ name: "Doc Dinners", rep: { email: null, firstName: null, role: null } });
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", NOW);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.recipientEmail).toBe("kevin@distribute.you");
    expect(params.bccEmails).toBeUndefined();
  });

  it("non-positive event: no claim, no send", async () => {
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_not_interested", NOW);
    expect(mockUpdate).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("already celebrated (claim lost): no send", async () => {
    mockReturning.mockResolvedValue([]);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_meeting_requested", NOW);
    expect(mockListEmails).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("platform send (null orgId): no claim, no send", async () => {
    await maybeForwardPositiveReply({ ...campaign, orgId: null }, "lead@x.com", "lead_interested", NOW);
    expect(mockUpdate).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("send fails: releases the claim and never throws", async () => {
    mockSendEmail.mockRejectedValue(new Error("email gateway down"));
    await expect(
      maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", NOW),
    ).resolves.toBeUndefined();
    expect(mockUpdate).toHaveBeenCalledTimes(2); // claim + release
  });

  it("links the conversation on the dashboard person page, under the CLERK org id", async () => {
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_info_requested", NOW);
    expect(mockExternalOrgId).toHaveBeenCalledWith("org-1");
    const [params] = mockSendEmail.mock.calls[0];
    const href =
      "https://dashboard.distribute.you/v2/orgs/org_3Jv0lYZiVaymxPEuyEbrbXs8hpv/brands/brand-1/people/346a5c86-ac49-4321-a873-a63c830b3217";
    expect(params.metadata.html).toContain(`href="${href}"`);
    expect(params.metadata.subject).toContain("asked for more information");
    expect(params.metadata.subject).not.toContain("\u{1F389}");
  });

  it("an unresolvable org still sends, the button opening the dashboard home", async () => {
    mockExternalOrgId.mockRejectedValue(new Error("client-service down"));
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", NOW);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.html).toContain('href="https://dashboard.distribute.you/v2"');
  });

  it("a MEETING request claims its own column (and the general one when free), and sends the dedicated email", async () => {
    mockReturning.mockResolvedValue([{ id: "row-1", tookGeneral: true }]);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_meeting_requested", NOW);
    expect(mockSet.mock.calls[0][0]).toHaveProperty("meetingRequestCelebratedAt");
    expect(mockSet.mock.calls[0][0]).toHaveProperty("positiveReplyForwardedAt");
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.subject).toContain("wants to book a call");
  });

  it("a failed MEETING send after an earlier celebration gives back ONLY the meeting claim", async () => {
    mockReturning.mockResolvedValue([{ id: "row-1", tookGeneral: false }]);
    mockSendEmail.mockRejectedValue(new Error("email gateway down"));
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_meeting_requested", NOW);
    const release = mockSet.mock.calls[1][0];
    expect(release).toHaveProperty("meetingRequestCelebratedAt", null);
    expect(release).not.toHaveProperty("positiveReplyForwardedAt");
  });

  it("an info or interest reply claims only the general column (never the meeting one)", async () => {
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_info_requested", NOW);
    expect(mockSet.mock.calls[0][0]).toHaveProperty("positiveReplyForwardedAt");
    expect(mockSet.mock.calls[0][0]).not.toHaveProperty("meetingRequestCelebratedAt");
  });

  it("a reply that cannot be read is SAID, never summarized", async () => {
    mockListEmails.mockResolvedValue([
      record({ ue_type: 1, subject: "Partnership?", body: { text: "cold 1" } }),
    ]);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", NOW);
    const [params] = mockSendEmail.mock.calls[0];
    expect(params.metadata.html).toContain("Their reply could not be read here");
    expect(params.metadata.text).toContain("Their reply could not be read here");
  });
});
