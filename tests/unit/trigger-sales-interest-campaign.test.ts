import { describe, it, expect, vi, beforeEach } from "vitest";

const mockGetCampaignTriggerScope = vi.fn();
const mockRecordTriggerEvent = vi.fn();
const mockFindResponders = vi.fn();
const mockFindLead = vi.fn();
const mockReadThread = vi.fn();

vi.mock("../../src/lib/campaign-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  getCampaignTriggerScope: (...args: unknown[]) => mockGetCampaignTriggerScope(...args),
  recordTriggerEvent: (...args: unknown[]) => mockRecordTriggerEvent(...args),
  findOngoingResponderCampaigns: (...args: unknown[]) => mockFindResponders(...args),
}));
vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...args: unknown[]) => mockFindLead(...args),
}));
vi.mock("../../src/lib/positive-reply-item", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  readReplyThread: (...args: unknown[]) => mockReadThread(...args),
}));

const {
  maybeTriggerSalesInterestCampaign,
  triggerSalesInterestLeg,
  isSalesInterestQualification,
  POSITIVE_REPLY_TRIGGER_ID,
} = await import("../../src/lib/trigger-sales-interest-campaign");
const { POSITIVE_REPLY_KINDS, REPLY_KIND_CLASSIFICATION, REPLY_KINDS } = await import(
  "../../src/lib/reply-kind"
);

const CAMPAIGN = {
  instantlyCampaignId: "inst-camp-1",
  campaignId: "camp-1",
  orgId: "org-1",
  leadId: "lead-1",
};

const SCOPE = { brandId: "brand-1", offerId: "offer-1" };

const THREAD = [
  { direction: "outbound" as const, from: "amy@s.com", to: "p@x.com", date: "2026-10-01T09:00:00.000Z", subject: "Hi", bodyText: "our pitch\n--\nAmy Moore" },
  { direction: "inbound" as const, from: "p@x.com", to: "amy@s.com", date: "2026-10-02T09:00:00.000Z", subject: "Re: Hi", bodyText: "Yes, interested.\n\n> our pitch" },
];

function event(over: Record<string, unknown> = {}) {
  return {
    event: {
      id: "evt-1",
      triggerId: "positive_reply_received",
      status: "done",
      outcome: "skipped",
      skipReason: "no_campaign",
      ranCampaignIds: [],
      detail: { legKeys: [], triggered: [], skipped: [] },
      ...over,
    },
    replayed: false,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockGetCampaignTriggerScope.mockResolvedValue(SCOPE);
  mockRecordTriggerEvent.mockResolvedValue(event());
  mockFindResponders.mockResolvedValue(["responder-1"]);
  mockFindLead.mockResolvedValue({ id: "lc-1", email: "p@x.com", name: "Dana Reid" });
  mockReadThread.mockResolvedValue(THREAD);
});

// ─── which replies count as a sales interest (pure) ───────────────────────────

describe("isSalesInterestQualification", () => {
  // The step is labelled "Sales interest": a buyer answers and a conversation
  // opens. That is exactly what the coarse map reports as `positive`, and it is
  // the same signal the fleet already prices as the brand's sales interest.
  it.each([["lead_interested"], ["lead_info_requested"], ["lead_meeting_requested"]])(
    "fires on %s",
    (kind) => {
      expect(isSalesInterestQualification(kind)).toBe(true);
    },
  );

  // ⚠️ The divergence from POSITIVE_REPLY_KINDS is the point. "Not me, but talk
  // to X" is worth a human's eyes (it forwards) and is NOT this person opening a
  // conversation — firing a funded meeting-booking campaign at someone who just
  // said they are the wrong person is the mistake the map exists to prevent.
  it("does NOT fire on lead_referral, even though it forwards to the agency inbox", () => {
    expect(isSalesInterestQualification("lead_referral")).toBe(false);
    expect(POSITIVE_REPLY_KINDS).toContain("lead_referral");
  });

  it("keys on the classification map rather than a second hand-written list", () => {
    for (const kind of REPLY_KINDS) {
      expect(isSalesInterestQualification(kind)).toBe(
        REPLY_KIND_CLASSIFICATION[kind] === "positive",
      );
    }
  });

  it.each([["lead_not_interested"], ["auto_reply_received"], ["email_sent"], ["nonsense"]])(
    "does not fire on %s",
    (eventType) => {
      expect(isSalesInterestQualification(eventType)).toBe(false);
    },
  );
});

// ─── the occurrence ──────────────────────────────────────────────────────────

describe("the positive reply is reported to the trigger door", () => {
  it("names the trigger, the scope, the person and carries their cleaned words", async () => {
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested");
    expect(mockGetCampaignTriggerScope).toHaveBeenCalledWith("camp-1", "org-1");
    expect(mockReadThread).toHaveBeenCalledWith("inst-camp-1");
    expect(mockRecordTriggerEvent).toHaveBeenCalledTimes(1);
    const arg = mockRecordTriggerEvent.mock.calls[0][0];
    expect(arg).toMatchObject({
      orgId: "org-1",
      triggerId: "positive_reply_received",
      brandId: "brand-1",
      offerId: "offer-1",
      leadId: "lead-1",
      requestedByCampaignId: "camp-1",
      person: { email: "p@x.com", name: "Dana Reid" },
      // LOCKED with the AI Instant Call workflow: {channel, replyText, priorMessages newest-first}.
      // channel "email": the instant call rings only on an email reply (INSTANT-CALL-EMAIL-ONLY-1016).
      item: {
        channel: "email",
        replyText: "Yes, interested.",
        priorMessages: [{ direction: "outbound", text: "our pitch" }],
      },
    });
    expect(arg.idempotencyKey).toMatch(/^positive_reply_received:[0-9a-f]{64}$/);
    expect(POSITIVE_REPLY_TRIGGER_ID).toBe("positive_reply_received");
  });

  it("ONE occurrence per reply: a replay or a re-qualification of the same words sends the same key", async () => {
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested");
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "P@X.com ", "lead_meeting_requested");
    const [a, b] = mockRecordTriggerEvent.mock.calls.map((c) => c[0].idempotencyKey);
    expect(a).toBe(b);
  });

  it("a NEW reply later in the thread is a new occurrence", async () => {
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested");
    mockReadThread.mockResolvedValueOnce([
      ...THREAD,
      { ...THREAD[1], date: "2026-10-05T09:00:00.000Z", bodyText: "Can we talk Friday?" },
    ]);
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_meeting_requested");
    const [a, b] = mockRecordTriggerEvent.mock.calls.map((c) => c[0].idempotencyKey);
    expect(a).not.toBe(b);
  });

  it("a caller-stated key wins (a redraft is a new occurrence of the same reply)", async () => {
    await triggerSalesInterestLeg(CAMPAIGN, "p@x.com", { idempotencyKey: "positive_reply_received:redraft:r-1" });
    expect(mockRecordTriggerEvent.mock.calls[0][0].idempotencyKey).toBe("positive_reply_received:redraft:r-1");
  });

  it("still reports, without the words, when the thread cannot be read", async () => {
    mockReadThread.mockRejectedValueOnce(new Error("db down"));
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested");
    expect(mockRecordTriggerEvent.mock.calls[0][0].item).toEqual({ channel: "email", replyText: null, priorMessages: [] });
  });

  it("still reports, without a name, when lead-service cannot be read", async () => {
    mockFindLead.mockRejectedValueOnce(new Error("lead 500"));
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested");
    expect(mockRecordTriggerEvent.mock.calls[0][0].person).toEqual({ email: "p@x.com" });
  });

  it("reports nothing on a reply that is not a sales interest", async () => {
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_referral")).toBeUndefined();
    expect(mockRecordTriggerEvent).not.toHaveBeenCalled();
  });

  it("reports nothing for a platform send (no caller campaign) or an org-less one", async () => {
    expect(await maybeTriggerSalesInterestCampaign({ ...CAMPAIGN, campaignId: null }, "p@x.com", "lead_interested")).toBe("unknown");
    expect(await maybeTriggerSalesInterestCampaign({ ...CAMPAIGN, orgId: null }, "p@x.com", "lead_interested")).toBe("unknown");
    expect(mockRecordTriggerEvent).not.toHaveBeenCalled();
  });

  it("a thread on no offer: nothing reported, nobody answers", async () => {
    mockGetCampaignTriggerScope.mockResolvedValueOnce({ brandId: "brand-1", offerId: null });
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("nobody");
    expect(mockRecordTriggerEvent).not.toHaveBeenCalled();
  });

  it("never throws when campaign-service fails, and says so loudly", async () => {
    mockRecordTriggerEvent.mockRejectedValueOnce(new Error("campaign-service POST /internal/trigger-events failed: 502"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("unknown");
    expect(warn.mock.calls.some((c) => String(c[0]).includes("502"))).toBe(true);
    warn.mockRestore();
  });
});

// ─── what the step trigger's answer fed: does an email responder answer? ─────

describe("responder verdict, read off the recorded occurrence", () => {
  it("a responder ran: answers", async () => {
    mockRecordTriggerEvent.mockResolvedValueOnce(event({ outcome: "ran", ranCampaignIds: ["responder-1"] }));
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("answers");
  });

  it("a responder run already in flight: answers", async () => {
    mockRecordTriggerEvent.mockResolvedValueOnce(
      event({ detail: { legKeys: [], triggered: [], skipped: [{ campaignId: "responder-1", legKey: null, reason: "run_in_flight", detail: "" }] } }),
    );
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("answers");
  });

  it("a replayed occurrence answers as the first one did", async () => {
    mockRecordTriggerEvent.mockResolvedValueOnce({ ...event({ outcome: "ran", ranCampaignIds: ["responder-1"] }), replayed: true });
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("answers");
  });

  it("only the PHONE pipe ran (AI Instant Call answers no email): nobody", async () => {
    mockRecordTriggerEvent.mockResolvedValueOnce(event({ outcome: "ran", ranCampaignIds: ["aic-1"] }));
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("nobody");
  });

  it("the responder is out of money (owed until room returns): nobody answers NOW", async () => {
    mockRecordTriggerEvent.mockResolvedValueOnce(
      event({ status: "pending", outcome: null, skipReason: null, detail: { legKeys: [], triggered: [], skipped: [{ campaignId: "responder-1", legKey: null, reason: "unfunded", detail: "" }] } }),
    );
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("nobody");
  });

  it("no pipe bought: nobody", async () => {
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("nobody");
  });

  it("no campaign asked yet (person not known to the brand yet): unknown, never nobody", async () => {
    mockRecordTriggerEvent.mockResolvedValueOnce(event({ status: "pending", outcome: null, skipReason: null, detail: { person: {}, item: {} } }));
    expect(await maybeTriggerSalesInterestCampaign(CAMPAIGN, "p@x.com", "lead_interested")).toBe("unknown");
  });
});
