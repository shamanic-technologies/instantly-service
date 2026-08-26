import { describe, it, expect } from "vitest";
import {
  ACCEPTED_QUALIFICATION_STATUSES,
  AUTOMATED_REPLY_KINDS,
  DEAL_PROGRESS_TO_REPLY_KIND,
  LEGACY_DEAL_PROGRESS_STATUSES,
  NEGATIVE_REPLY_KINDS,
  POSITIVE_REPLY_KINDS,
  REPLY_KINDS,
  REPLY_KIND_CLASSIFICATION,
  isDealProgressEventType,
  isReplyKind,
  isSequenceStoppingReplyKind,
  resolveReplyKind,
} from "../../src/lib/reply-kind";

describe("the reply-kind vocabulary", () => {
  it("expresses the four positive distinctions a reader has to tell apart", () => {
    expect([...POSITIVE_REPLY_KINDS]).toEqual([
      "lead_interested",
      "lead_referral",
      "lead_info_requested",
      "lead_meeting_requested",
    ]);
  });

  it("carries no deal progress at all", () => {
    expect(REPLY_KINDS).not.toContain("lead_meeting_booked");
    expect(REPLY_KINDS).not.toContain("lead_closed");
    expect(REPLY_KINDS).not.toContain("lead_meeting_completed");
    expect(isReplyKind("lead_meeting_booked")).toBe(false);
  });

  it("classifies every kind, and only positive kinds as positive", () => {
    for (const kind of REPLY_KINDS) {
      expect(REPLY_KIND_CLASSIFICATION[kind]).toBeDefined();
    }
    const positive = Object.entries(REPLY_KIND_CLASSIFICATION)
      .filter(([, v]) => v === "positive")
      .map(([k]) => k)
      .sort();
    expect(positive).toEqual([...POSITIVE_REPLY_KINDS].sort());
  });
});

describe("resolveReplyKind", () => {
  it("is the identity on the vocabulary", () => {
    for (const kind of REPLY_KINDS) {
      expect(resolveReplyKind(kind)).toBe(kind);
    }
  });

  // The domain fact: someone whose reply was qualified as a booked meeting or a
  // closed-won deal had by definition replied positively. `lead_interested` is
  // the strongest claim that supports on its own — resolving to
  // `lead_meeting_requested` would re-encode the deal axis into the reply axis.
  it.each([
    ["lead_meeting_booked"],
    ["lead_closed"],
    ["lead_meeting_completed"],
  ])("resolves the deal-progress value %s to lead_interested", (status) => {
    expect(resolveReplyKind(status)).toBe("lead_interested");
    expect(isDealProgressEventType(status)).toBe(true);
  });

  it("throws on anything it cannot resolve — never a silent fallback", () => {
    expect(() => resolveReplyKind("lead_unicorn")).toThrow(/Unknown reply qualification status/);
    expect(() => resolveReplyKind("")).toThrow();
  });

  it("resolves every status the write path accepts", () => {
    for (const status of ACCEPTED_QUALIFICATION_STATUSES) {
      expect(isReplyKind(resolveReplyKind(status))).toBe(true);
    }
  });
});

describe("the write path while the consoles migrate", () => {
  it("still accepts the two deal-progress values", () => {
    expect([...LEGACY_DEAL_PROGRESS_STATUSES]).toEqual(["lead_meeting_booked", "lead_closed"]);
    for (const status of LEGACY_DEAL_PROGRESS_STATUSES) {
      expect(ACCEPTED_QUALIFICATION_STATUSES).toContain(status);
    }
  });

  it("accepts the vocabulary plus exactly those two, nothing else", () => {
    expect([...ACCEPTED_QUALIFICATION_STATUSES].sort()).toEqual(
      [...REPLY_KINDS, ...LEGACY_DEAL_PROGRESS_STATUSES].sort(),
    );
  });

  it("keeps lead_meeting_completed OUT of the write path — it was never hand-set", () => {
    expect(ACCEPTED_QUALIFICATION_STATUSES).not.toContain("lead_meeting_completed");
    expect(DEAL_PROGRESS_TO_REPLY_KIND.lead_meeting_completed).toBe("lead_interested");
  });
});

describe("isSequenceStoppingReplyKind", () => {
  it("stops on every human reply, whatever its sentiment", () => {
    for (const kind of [...POSITIVE_REPLY_KINDS, ...NEGATIVE_REPLY_KINDS, "lead_neutral" as const]) {
      expect(isSequenceStoppingReplyKind(kind)).toBe(true);
    }
  });

  // A machine answering is not the prospect engaging: stopping would end the
  // outreach AND refund the spend for someone who never replied.
  it("never stops on an automated reply", () => {
    for (const kind of AUTOMATED_REPLY_KINDS) {
      expect(isSequenceStoppingReplyKind(kind)).toBe(false);
    }
  });
});
