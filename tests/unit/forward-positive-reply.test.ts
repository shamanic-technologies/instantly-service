import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockReturning = vi.fn();
const mockUpdate = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockListEmails = vi.fn();
const mockSendEmail = vi.fn();

// A drizzle-ish update builder: .set().where() returns an object that is both
// awaitable (release: `await db.update()...where()`) and has .returning() (claim).
const whereObj = {
  returning: (...a: unknown[]) => mockReturning(...a),
  then: (onF: (v: unknown) => unknown, onR?: (e: unknown) => unknown) =>
    Promise.resolve(undefined).then(onF, onR),
};

vi.mock("../../src/db", () => ({
  db: {
    update: (...a: unknown[]) => {
      mockUpdate(...a);
      return { set: () => ({ where: () => whereObj }) };
    },
  },
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveInstantlyApiKey(...a),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  listEmails: (...a: unknown[]) => mockListEmails(...a),
}));

vi.mock("../../src/lib/email-client", () => ({
  sendEmail: (...a: unknown[]) => mockSendEmail(...a),
}));

import {
  isPositiveQualification,
  POSITIVE_QUALIFICATION_EVENT_TYPES,
  htmlToText,
  selectThreadMessages,
  renderThreadText,
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
  it("is true only for Instantly's positive qualification events", () => {
    expect(isPositiveQualification("lead_interested")).toBe(true);
    expect(isPositiveQualification("lead_meeting_booked")).toBe(true);
    expect(isPositiveQualification("lead_closed")).toBe(true);
    // Negative / neutral / non-qualified → never
    expect(isPositiveQualification("lead_not_interested")).toBe(false);
    expect(isPositiveQualification("lead_out_of_office")).toBe(false);
    expect(isPositiveQualification("reply_received")).toBe(false);
    expect(isPositiveQualification("email_opened")).toBe(false);
  });

  it("stays in lockstep with REPLY_CLASSIFICATION_MAP's 'positive' entries", () => {
    const positiveFromMap = Object.entries(REPLY_CLASSIFICATION_MAP)
      .filter(([, v]) => v === "positive")
      .map(([k]) => k)
      .sort();
    expect([...POSITIVE_QUALIFICATION_EVENT_TYPES].sort()).toEqual(positiveFromMap);
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

describe("renderThreadText", () => {
  it("includes lead, campaign, qualification, and every message from/to/body", () => {
    const msgs = selectThreadMessages([
      record({
        ue_type: 1,
        from_address_email: "amy@distribute.com",
        to_address_email_list: "lead@x.com",
        body: { text: "Hello" },
      }),
      record({
        ue_type: 2,
        timestamp_email: "2026-07-14T12:00:00.000Z",
        from_address_email: "lead@x.com",
        to_address_email_list: "amy@distribute.com",
        body: { text: "Interested!" },
      }),
    ]);
    const text = renderThreadText(msgs, {
      leadEmail: "lead@x.com",
      campaignId: "camp-1",
      qualification: "lead_interested",
    });
    expect(text).toContain("Lead: lead@x.com");
    expect(text).toContain("Campaign: camp-1");
    expect(text).toContain("lead_interested");
    expect(text).toContain("Hello");
    expect(text).toContain("Interested!");
    expect(text).toContain("amy@distribute.com");
  });

  it("degrades gracefully when the thread is empty", () => {
    const text = renderThreadText([], {
      leadEmail: "lead@x.com",
      campaignId: "camp-1",
      qualification: "lead_interested",
    });
    expect(text).toContain("Lead: lead@x.com");
    expect(text).toContain("not yet available");
  });
});

describe("maybeForwardPositiveReply", () => {
  beforeEach(() => {
    mockReturning.mockReset();
    mockUpdate.mockReset();
    mockResolveInstantlyApiKey.mockReset();
    mockListEmails.mockReset();
    mockSendEmail.mockReset();
    mockReturning.mockResolvedValue([{ id: "row-1" }]); // claim won by default
    mockResolveInstantlyApiKey.mockResolvedValue({ key: "api-key-1", keySource: "org" });
    mockListEmails.mockResolvedValue([
      record({
        ue_type: 2,
        from_address_email: "lead@x.com",
        to_address_email_list: "amy@distribute.com",
        body: { text: "Yes, interested!" },
      }),
    ]);
    mockSendEmail.mockResolvedValue(undefined);
  });

  it("positive event: claims, fetches thread, forwards to the agency inbox", async () => {
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested");

    expect(mockUpdate).toHaveBeenCalledTimes(1); // claim only (no release)
    expect(mockListEmails).toHaveBeenCalledWith("api-key-1", {
      campaignId: "inst-camp-1",
    });
    expect(mockSendEmail).toHaveBeenCalledTimes(1);
    const [params, identity] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("positive-reply-forward");
    expect(params.recipientEmail).toBe("kevin@distribute.you");
    expect(params.metadata.thread).toContain("Yes, interested!");
    expect(params.metadata.leadEmail).toBe("lead@x.com");
    expect(identity.orgId).toBe("org-1");
  });

  it("non-positive event: no claim, no send", async () => {
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_not_interested");
    expect(mockUpdate).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("already forwarded (claim lost): no send", async () => {
    mockReturning.mockResolvedValue([]); // claim lost
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_closed");
    expect(mockListEmails).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("platform send (null orgId): no claim, no send", async () => {
    await maybeForwardPositiveReply(
      { ...campaign, orgId: null },
      "lead@x.com",
      "lead_interested",
    );
    expect(mockUpdate).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("send fails: releases the claim and never throws (fail-soft)", async () => {
    mockSendEmail.mockRejectedValue(new Error("email gateway down"));
    await expect(
      maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested"),
    ).resolves.toBeUndefined();
    // claim + release = 2 updates
    expect(mockUpdate).toHaveBeenCalledTimes(2);
  });
});
