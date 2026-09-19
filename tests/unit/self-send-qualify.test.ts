import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockPlatformComplete = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({
  platformComplete: (...a: unknown[]) => mockPlatformComplete(...a),
  platformJudgment: vi.fn(),
}));

// The shadow measurement runs BESIDE the classifier and decides nothing. Mocked
// here so these tests assert what `qualifyReply` RETURNS, independently of it.
const mockShadowJudgeReply = vi.fn();
vi.mock("../../src/lib/shadow-judgment", () => ({
  shadowJudgeReply: (...a: unknown[]) => mockShadowJudgeReply(...a),
}));

import {
  QUALIFICATION_EVENT_TYPES,
  isQualificationEventType,
  parseQualification,
  stripQuotedHistory,
} from "../../src/lib/self-send/qualify-reply";
import { REPLY_CLASSIFICATION_MAP } from "../../src/lib/silver-promote";
import { qualifyReply } from "../../src/lib/self-send/qualify-reply";

beforeEach(() => {
  vi.resetAllMocks();
});

/**
 * The shadow measurement is launched DETACHED through a dynamic import, so it
 * lands a few ticks after `qualifyReply` has already returned. Drain those ticks
 * before asserting on it — and after every test, so one test's detached call
 * cannot be read as the next test's.
 */
async function drainDetached(): Promise<void> {
  for (let i = 0; i < 5; i += 1) await new Promise((r) => setTimeout(r, 0));
}

afterEach(drainDetached);

describe("qualifyReply", () => {
  // The cheapest model in the catalogue that does a short closed-set pick
  // reliably. The spend is chat-service's, on a platform run — this sweep has no
  // inbound org request, so there is no customer to bill for classifying a reply
  // to our own outreach.
  it("classifies on deepseek-flash, with reasoning off and JSON out", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "",
      json: { classification: "lead_interested" },
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-v4-flash",
    });

    await qualifyReply("I would be interested, send me the costs");

    expect(mockPlatformComplete).toHaveBeenCalledWith(
      expect.objectContaining({
        provider: "deepseek",
        model: "deepseek-flash",
        responseFormat: "json",
        temperature: 0,
        disableThinking: true,
      }),
    );
  });

  // An unusable answer must not become a fabricated neutral — a wrong "neutral"
  // on a hot reply reads as a real judgement.
  it("returns null rather than a label it did not obtain", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "I think it is probably positive?",
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-v4-flash",
    });

    expect(await qualifyReply("hello")).toBeNull();
  });
});

describe("QUALIFICATION_EVENT_TYPES", () => {
  // Emitting a label outside the silver vocabulary would write an event that no
  // reader maps to a sentiment — invisible in every stat.
  it("only contains labels silver already knows how to classify", () => {
    for (const type of QUALIFICATION_EVENT_TYPES) {
      expect(REPLY_CLASSIFICATION_MAP[type]).toBeDefined();
    }
  });

  // Deal progress is an outcome someone records in the lead-outcomes service,
  // not something a reply's text can honestly support — and it is no longer a
  // reply kind at all, so it is not in the map either.
  it("excludes deal progress entirely", () => {
    expect(REPLY_CLASSIFICATION_MAP.lead_closed).toBeUndefined();
    expect(REPLY_CLASSIFICATION_MAP.lead_meeting_booked).toBeUndefined();
    expect(QUALIFICATION_EVENT_TYPES).not.toContain("lead_closed");
    expect(QUALIFICATION_EVENT_TYPES).not.toContain("lead_meeting_booked");
  });

  it("can produce every positive distinction a reader needs", () => {
    expect(QUALIFICATION_EVENT_TYPES).toContain("lead_referral");
    expect(QUALIFICATION_EVENT_TYPES).toContain("lead_info_requested");
    expect(QUALIFICATION_EVENT_TYPES).toContain("lead_meeting_requested");
  });

  it("can produce every sentiment silver distinguishes", () => {
    const sentiments = new Set(
      QUALIFICATION_EVENT_TYPES.map((t) => REPLY_CLASSIFICATION_MAP[t]),
    );
    expect(sentiments).toEqual(new Set(["positive", "negative", "neutral"]));
  });
});

describe("isQualificationEventType", () => {
  it("accepts a known label", () => {
    expect(isQualificationEventType("lead_interested")).toBe(true);
  });

  it.each([["lead_closed"], ["interested"], ["POSITIVE"], [""], [null], [42]])(
    "refuses %p",
    (value) => {
      expect(isQualificationEventType(value)).toBe(false);
    },
  );
});

describe("stripQuotedHistory", () => {
  // The model must judge what THEY wrote, not our own email quoted back.
  it("cuts at a > quote marker", () => {
    expect(
      stripQuotedHistory("Sounds good, send details.\n\n> Our original pitch\n> more"),
    ).toBe("Sounds good, send details.");
  });

  it("cuts at the On ... wrote: attribution", () => {
    expect(
      stripQuotedHistory("Not for us.\n\nOn Mon, 16 Aug 2026, Amy Moore wrote:\nOur pitch"),
    ).toBe("Not for us.");
  });

  it("cuts at an Original Message separator", () => {
    expect(
      stripQuotedHistory("Wrong person.\n\n----- Original Message -----\nblah"),
    ).toBe("Wrong person.");
  });

  it("leaves a reply with no quoted history intact", () => {
    expect(stripQuotedHistory("Interested, tell me more.")).toBe(
      "Interested, tell me more.",
    );
  });

  it("returns empty for a reply that is nothing but quoted history", () => {
    expect(stripQuotedHistory("> only the quote")).toBe("");
  });
});

describe("parseQualification", () => {
  it("reads the structured json field", () => {
    expect(parseQualification({ json: { classification: "lead_interested" } })).toBe(
      "lead_interested",
    );
  });

  it("falls back to parsing the raw content", () => {
    expect(
      parseQualification({ content: '{"classification":"lead_not_interested"}' }),
    ).toBe("lead_not_interested");
  });

  // Defaulting to neutral would put a fabricated judgement into the gold stats,
  // and a wrong "neutral" on a hot reply is worse than an absent one.
  it.each([
    [{ json: { classification: "enthusiastic" } }],
    [{ json: {} }],
    [{ content: "The lead seems interested." }],
    [{ content: "" }],
    [{}],
  ])("returns null rather than guessing for %p", (result) => {
    expect(parseQualification(result)).toBeNull();
  });

  it("refuses a label outside the accepted set even when well-formed", () => {
    expect(parseQualification({ json: { classification: "lead_closed" } })).toBeNull();
  });
});

describe("the shadow judgment runs beside the classifier and decides nothing", () => {
  // The whole contract of the measurement: whatever the judgment engine says —
  // including nothing at all — the stored classification is what the LLM said.
  it("returns the LLM's label unchanged, and hands the shadow the same text", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "",
      json: { classification: "lead_not_interested" },
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-flash",
    });

    const label = await qualifyReply("Not for us, thanks.\n\n> our quoted email", {
      instantlyCampaignId: "self:abc",
      leadEmail: "joe@x.com",
      source: "imap_poller",
    });

    expect(label).toBe("lead_not_interested");
    await drainDetached();

    expect(mockShadowJudgeReply).toHaveBeenCalledWith({
      instantlyCampaignId: "self:abc",
      leadEmail: "joe@x.com",
      source: "imap_poller",
      // The SAME text the LLM judged: quoted history already stripped.
      replyText: "Not for us, thanks.",
      llmClassification: "lead_not_interested",
    });
  });

  it("records the absence too, so an unusable classification is still countable", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "not json at all",
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-flash",
    });

    expect(await qualifyReply("hello")).toBeNull();
    await drainDetached();

    expect(mockShadowJudgeReply).toHaveBeenCalledWith(
      expect.objectContaining({ llmClassification: null }),
    );
  });

  // Never awaited: one caller runs inside Instantly's webhook, where a slow
  // delivery counts toward disabling the whole subscription.
  it("does NOT wait for the shadow before returning", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "",
      json: { classification: "lead_interested" },
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-flash",
    });
    // A shadow that never settles must not hold the classification up.
    mockShadowJudgeReply.mockImplementation(() => new Promise(() => {}));

    expect(await qualifyReply("interested!")).toBe("lead_interested");
  });
});
