import { describe, it, expect, vi, beforeEach } from "vitest";

const mockGetSalesRepPhone = vi.fn();
const mockFindLead = vi.fn();
const mockRequestReveal = vi.fn();
const mockReadReveal = vi.fn();
const mockPlaceCall = vi.fn();
const mockFetchMirrored = vi.fn();
const mockFetchSelfSendThread = vi.fn();

const claimed: { rows: Array<{ id: string }> } = { rows: [{ id: "row-1" }] };
const updates: Array<Record<string, unknown>> = [];

vi.mock("../../src/db", () => ({
  db: {
    update: () => ({
      set: (values: Record<string, unknown>) => {
        updates.push(values);
        return {
          where: () => ({
            returning: async () => claimed.rows,
            then: (resolve: (v: unknown) => unknown) => Promise.resolve(undefined).then(resolve),
          }),
        };
      },
    }),
  },
}));

vi.mock("../../src/lib/brand-client", () => ({
  getSalesRepPhone: (...a: unknown[]) => mockGetSalesRepPhone(...a),
}));

vi.mock("../../src/lib/lead-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
}));

vi.mock("../../src/lib/apollo-client", () => ({
  requestPhoneReveal: (...a: unknown[]) => mockRequestReveal(...a),
  readPhoneReveal: (...a: unknown[]) => mockReadReveal(...a),
}));

vi.mock("../../src/lib/twilio-client", () => ({
  placeCall: (...a: unknown[]) => mockPlaceCall(...a),
}));

vi.mock("../../src/lib/mirror-emails", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  fetchMirroredEmailRecords: (...a: unknown[]) => mockFetchMirrored(...a),
}));

vi.mock("../../src/lib/self-send/thread", () => ({
  fetchSelfSendThread: (...a: unknown[]) => mockFetchSelfSendThread(...a),
}));

const {
  maybeRingRepOnSalesInterest,
  connectNumberFor,
  latestInboundText,
  spokenName,
  buildCallReply,
  isRevealSettled,
  revealPhoneWithinBudget,
  PHONE_REVEAL_WAIT_MS,
  REPLY_TEXT_UNAVAILABLE,
} = await import("../../src/lib/ring-rep-on-sales-interest");
const { isSalesInterestQualification } = await import(
  "../../src/lib/trigger-sales-interest-campaign"
);
const { REPLY_KINDS, POSITIVE_REPLY_KINDS } = await import("../../src/lib/reply-kind");

const CAMPAIGN = {
  instantlyCampaignId: "inst-camp-1",
  campaignId: "camp-1",
  orgId: "org-1",
  userId: "user-1",
  runId: "run-1",
  brandIds: ["brand-1"],
};

const LEAD = {
  id: "lc-1",
  email: "prospect@example.com",
  apolloPersonId: "apollo-person-1",
  name: "Dana Reid",
  company: "Acme",
};

const FOUND = {
  revealId: "rev-1",
  apolloPersonId: "apollo-person-1",
  status: "found" as const,
  mobilePhone: "+15551230000",
  dncStatus: null,
  doNotCall: false,
  phoneNumbers: [],
  failureReason: null,
  creditsConsumed: 8,
  requestedAt: null,
  completedAt: null,
};

const INBOUND_THREAD = [
  {
    direction: "outbound" as const,
    from: "amy@sender.com",
    to: "prospect@example.com",
    date: "2026-09-01T09:00:00.000Z",
    subject: "Hello",
    bodyText: "our pitch",
  },
  {
    direction: "inbound" as const,
    from: "prospect@example.com",
    to: "amy@sender.com",
    date: "2026-09-02T09:00:00.000Z",
    subject: "Re: Hello",
    bodyText: "Yes, very interested — send pricing.",
  },
];

/** The mirrored bronze payloads `selectThreadMessages` turns into that thread. */
const MIRRORED_RECORDS = [
  {
    ue_type: 1,
    timestamp_email: "2026-09-01T09:00:00.000Z",
    from_address_email: "amy@sender.com",
    to_address_email_list: "prospect@example.com",
    subject: "Hello",
    body: { text: "our pitch" },
  },
  {
    ue_type: 2,
    timestamp_email: "2026-09-02T09:00:00.000Z",
    from_address_email: "prospect@example.com",
    to_address_email_list: "amy@sender.com",
    subject: "Re: Hello",
    body: { text: "Yes, very interested — send pricing." },
  },
];

beforeEach(() => {
  vi.resetAllMocks();
  updates.length = 0;
  claimed.rows = [{ id: "row-1" }];
  mockGetSalesRepPhone.mockResolvedValue("+15559990000");
  mockFindLead.mockResolvedValue(LEAD);
  mockRequestReveal.mockResolvedValue(FOUND);
  mockPlaceCall.mockResolvedValue({
    success: true,
    callId: "call-1",
    costName: "twilio-voice-us",
    connectOffered: true,
  });
  mockFetchMirrored.mockResolvedValue(MIRRORED_RECORDS);
});

describe("the gate", () => {
  it("is the SAME predicate the campaign trigger and the follow-up enqueue use", async () => {
    for (const kind of REPLY_KINDS) {
      vi.resetAllMocks();
      mockGetSalesRepPhone.mockResolvedValue("+15559990000");
      mockFindLead.mockResolvedValue(LEAD);
      mockRequestReveal.mockResolvedValue(FOUND);
      mockPlaceCall.mockResolvedValue({
        success: true,
        callId: "call-1",
        costName: "c",
        connectOffered: true,
      });
      mockFetchMirrored.mockResolvedValue(MIRRORED_RECORDS);

      await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", kind);

      expect(mockPlaceCall.mock.calls.length > 0).toBe(
        isSalesInterestQualification(kind),
      );
    }
  });

  it("does NOT ring on a referral, which forwards to the agency inbox but is not a buyer", async () => {
    // The forward set is deliberately wider than this one; ringing a rep to say
    // "a buyer is interested" about somebody who said they are the wrong person
    // is exactly what that divergence prevents.
    expect(POSITIVE_REPLY_KINDS).toContain("lead_referral");
    expect(isSalesInterestQualification("lead_referral")).toBe(false);

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_referral");
    expect(mockPlaceCall).not.toHaveBeenCalled();
    expect(mockGetSalesRepPhone).not.toHaveBeenCalled();
  });
});

describe("a brand with no number", () => {
  it("is untouched — no claim, no lookup, no reveal, no call", async () => {
    mockGetSalesRepPhone.mockResolvedValue(null);

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    expect(mockPlaceCall).not.toHaveBeenCalled();
    expect(mockFindLead).not.toHaveBeenCalled();
    expect(mockRequestReveal).not.toHaveBeenCalled();
    expect(updates).toEqual([]);
  });

  it("is also the answer when the campaign names no brand", async () => {
    await maybeRingRepOnSalesInterest(
      { ...CAMPAIGN, brandIds: [] },
      "prospect@example.com",
      "lead_interested",
    );
    expect(mockGetSalesRepPhone).not.toHaveBeenCalled();
    expect(mockPlaceCall).not.toHaveBeenCalled();
  });
});

describe("the call", () => {
  it("offers the connection when a usable number arrived", async () => {
    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    expect(mockPlaceCall).toHaveBeenCalledTimes(1);
    const body = mockPlaceCall.mock.calls[0][0];
    expect(body.to).toBe("+15559990000");
    expect(body.connectTo).toBe("+15551230000");
    expect(body.reply).toEqual({
      name: "Dana Reid",
      company: "Acme",
      message: "Yes, very interested — send pricing.",
    });
    expect(body.brandId).toBe("brand-1");
    expect(body.campaignId).toBe("camp-1");
    expect(body.parentRunId).toBe("run-1");
  });

  it("still happens, WITHOUT a connect number, when Apollo has none", async () => {
    mockRequestReveal.mockResolvedValue({ ...FOUND, status: "not_found", mobilePhone: null });

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    expect(mockPlaceCall).toHaveBeenCalledTimes(1);
    expect(mockPlaceCall.mock.calls[0][0].connectTo).toBeUndefined();
  });

  it("still happens, WITHOUT a connect number, when the reveal itself fails", async () => {
    mockRequestReveal.mockRejectedValue(new Error("apollo down"));

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    expect(mockPlaceCall).toHaveBeenCalledTimes(1);
    expect(mockPlaceCall.mock.calls[0][0].connectTo).toBeUndefined();
  });

  it("NEVER dials a number flagged do-not-call — it announces the absence instead", async () => {
    mockRequestReveal.mockResolvedValue({
      ...FOUND,
      doNotCall: true,
      dncStatus: "do_not_call",
    });

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    expect(mockPlaceCall).toHaveBeenCalledTimes(1);
    const body = mockPlaceCall.mock.calls[0][0];
    expect(body.connectTo).toBeUndefined();
    // and the number is nowhere on the request at all
    expect(JSON.stringify(body)).not.toContain("+15551230000");
  });

  it("still happens when the lead cannot be identified — named by the address they wrote from", async () => {
    mockFindLead.mockResolvedValue(null);

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    const body = mockPlaceCall.mock.calls[0][0];
    expect(body.reply.name).toBe("prospect@example.com");
    expect(body.connectTo).toBeUndefined();
    expect(mockRequestReveal).not.toHaveBeenCalled();
  });

  it("states that the reply text is unavailable rather than inventing one", async () => {
    mockFetchMirrored.mockResolvedValue([]);

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    expect(mockPlaceCall.mock.calls[0][0].reply.message).toBe(REPLY_TEXT_UNAVAILABLE);
  });

  it("reads a self-send sequence's words from OUR bronze, not from Instantly's mirror", async () => {
    mockFetchSelfSendThread.mockResolvedValue(INBOUND_THREAD);

    await maybeRingRepOnSalesInterest(
      { ...CAMPAIGN, instantlyCampaignId: "self:abc" },
      "prospect@example.com",
      "lead_interested",
    );

    expect(mockFetchSelfSendThread).toHaveBeenCalledWith("self:abc");
    expect(mockFetchMirrored).not.toHaveBeenCalled();
    expect(mockPlaceCall.mock.calls[0][0].reply.message).toBe(
      "Yes, very interested — send pricing.",
    );
  });
});

describe("at most once per lead", () => {
  it("claims BEFORE any external call, and a replay rings nobody", async () => {
    claimed.rows = [];

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    expect(mockPlaceCall).not.toHaveBeenCalled();
    expect(mockFindLead).not.toHaveBeenCalled();
    expect(mockRequestReveal).not.toHaveBeenCalled();
  });

  it("releases the claim when the call could not be placed, so a later signal re-attempts", async () => {
    mockPlaceCall.mockRejectedValue(new Error("twilio 502"));

    await maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested");

    // claim set, then released back to null
    expect(updates.length).toBe(2);
    expect(updates[0].salesInterestCallAt).toBeInstanceOf(Date);
    expect(updates[1].salesInterestCallAt).toBeNull();
  });

  it("never throws, whatever fails — the qualification is the primary job", async () => {
    mockGetSalesRepPhone.mockRejectedValue(new Error("brand-service down"));
    await expect(
      maybeRingRepOnSalesInterest(CAMPAIGN, "prospect@example.com", "lead_interested"),
    ).resolves.toBeUndefined();
    expect(mockPlaceCall).not.toHaveBeenCalled();
  });
});

describe("the bounded wait", () => {
  it("returns as soon as the number arrives, without burning the budget", async () => {
    mockRequestReveal.mockResolvedValue({ ...FOUND, status: "pending", mobilePhone: null });
    mockReadReveal.mockResolvedValueOnce(FOUND);

    let clock = 0;
    const reveal = await revealPhoneWithinBudget(
      "apollo-person-1",
      { orgId: "org-1", userId: "user-1", runId: "run-1" },
      { now: () => clock, sleep: async (ms) => { clock += ms; } },
    );

    expect(reveal.status).toBe("found");
    expect(mockReadReveal).toHaveBeenCalledTimes(1);
    expect(clock).toBeLessThan(PHONE_REVEAL_WAIT_MS);
  });

  it("gives up at the budget and hands back what it last saw — still pending", async () => {
    const pending = { ...FOUND, status: "pending" as const, mobilePhone: null };
    mockRequestReveal.mockResolvedValue(pending);
    mockReadReveal.mockResolvedValue(pending);

    let clock = 0;
    const reveal = await revealPhoneWithinBudget(
      "apollo-person-1",
      { orgId: "org-1", userId: "user-1", runId: "run-1" },
      { now: () => clock, sleep: async (ms) => { clock += ms; } },
    );

    expect(reveal.status).toBe("pending");
    expect(connectNumberFor(reveal)).toBeNull();
    expect(clock).toBeGreaterThanOrEqual(PHONE_REVEAL_WAIT_MS);
    // 90s at 5s a poll
    expect(mockReadReveal).toHaveBeenCalledTimes(PHONE_REVEAL_WAIT_MS / 5_000);
  });

  it("does not poll at all when the first answer already settles it", async () => {
    mockRequestReveal.mockResolvedValue(FOUND);
    const reveal = await revealPhoneWithinBudget("apollo-person-1", {
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });
    expect(reveal.status).toBe("found");
    expect(mockReadReveal).not.toHaveBeenCalled();
  });
});

describe("pure helpers", () => {
  it("isRevealSettled: only pending is worth waiting on", () => {
    expect(isRevealSettled("pending")).toBe(false);
    for (const s of ["found", "not_found", "failed"] as const) {
      expect(isRevealSettled(s)).toBe(true);
    }
  });

  it("connectNumberFor: found + dialable + a number, or null", () => {
    expect(connectNumberFor(null)).toBeNull();
    expect(connectNumberFor({ ...FOUND, status: "pending" })).toBeNull();
    expect(connectNumberFor({ ...FOUND, status: "not_found" })).toBeNull();
    expect(connectNumberFor({ ...FOUND, status: "failed" })).toBeNull();
    expect(connectNumberFor({ ...FOUND, doNotCall: true })).toBeNull();
    expect(connectNumberFor({ ...FOUND, mobilePhone: null })).toBeNull();
    expect(connectNumberFor({ ...FOUND, mobilePhone: "  " })).toBeNull();
    expect(connectNumberFor(FOUND)).toBe("+15551230000");
  });

  it("latestInboundText: the prospect's own last words, never ours", () => {
    expect(latestInboundText(INBOUND_THREAD)).toBe("Yes, very interested — send pricing.");
    expect(latestInboundText([INBOUND_THREAD[0]])).toBeNull();
    expect(latestInboundText([])).toBeNull();
    expect(
      latestInboundText([{ ...INBOUND_THREAD[1], bodyText: "(no body)" }]),
    ).toBeNull();
  });

  it("spokenName / buildCallReply: a name when we hold one, the address otherwise", () => {
    expect(spokenName("a@b.com", null)).toBe("a@b.com");
    expect(spokenName("a@b.com", { ...LEAD, name: "  " })).toBe("a@b.com");
    expect(spokenName("a@b.com", LEAD)).toBe("Dana Reid");

    expect(buildCallReply("a@b.com", null, null)).toEqual({
      name: "a@b.com",
      message: REPLY_TEXT_UNAVAILABLE,
    });
    expect(buildCallReply("a@b.com", { ...LEAD, company: null }, "hi")).toEqual({
      name: "Dana Reid",
      message: "hi",
    });
  });
});
