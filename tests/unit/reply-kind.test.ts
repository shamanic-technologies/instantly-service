import { describe, it, expect } from "vitest";
import {
  ACCEPTED_QUALIFICATION_STATUSES,
  AUTOMATED_REPLY_KINDS,
  DEAL_PROGRESS_TO_REPLY_KIND,
  DISQUALIFYING_REPLY_KINDS,
  LEGACY_DEAL_PROGRESS_STATUSES,
  NEGATIVE_REPLY_KINDS,
  POSITIVE_REPLY_KINDS,
  REPLY_KINDS,
  REPLY_KIND_CLASSIFICATION,
  isDealProgressEventType,
  isDisqualifyingReplyKind,
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

  // A "no" about the MOMENT and a "no" about the PERSON are acted on
  // completely differently: the first lead is recycled, the second is
  // disqualified. Collapsing a job change into `lead_not_interested` is the
  // conflation that turns the negative bucket into a dumping ground.
  it("separates the permanent, person-level negatives from the temporary one", () => {
    expect([...NEGATIVE_REPLY_KINDS]).toEqual([
      "lead_not_interested",
      "lead_wrong_person",
      "lead_changed_job",
    ]);
    expect(isReplyKind("lead_changed_job")).toBe(true);
    expect(REPLY_KIND_CLASSIFICATION.lead_changed_job).toBe("negative");
  });

  // Two different statements: one says we picked the wrong contact, the other
  // says the right contact left the role. A person who once held it would read
  // the first back as false.
  it("keeps a job change distinct from a wrong contact and from a decline", () => {
    expect(new Set(NEGATIVE_REPLY_KINDS).size).toBe(NEGATIVE_REPLY_KINDS.length);
    expect(resolveReplyKind("lead_changed_job")).toBe("lead_changed_job");
  });

  it("carries no deal progress at all", () => {
    expect(REPLY_KINDS).not.toContain("lead_meeting_booked");
    expect(REPLY_KINDS).not.toContain("lead_closed");
    expect(REPLY_KINDS).not.toContain("lead_meeting_completed");
    expect(isReplyKind("lead_meeting_booked")).toBe(false);
  });

  it("classifies every kind", () => {
    for (const kind of REPLY_KINDS) {
      expect(REPLY_KIND_CLASSIFICATION[kind]).toBeDefined();
    }
  });

  // The coarse projection is what becomes the customer's reported SALES
  // INTEREST downstream. A referral is worth reading (it still forwards) but it
  // is not this person's buying interest, so it must not be counted or priced
  // as one. This is the one deliberate divergence from POSITIVE_REPLY_KINDS.
  it("reports a referral as neutral, not positive", () => {
    expect(REPLY_KIND_CLASSIFICATION.lead_referral).toBe("neutral");
    expect(POSITIVE_REPLY_KINDS).toContain("lead_referral");
  });

  it("counts only the buying-interest kinds as positive", () => {
    const positive = Object.entries(REPLY_KIND_CLASSIFICATION)
      .filter(([, v]) => v === "positive")
      .map(([k]) => k)
      .sort();
    expect(positive).toEqual([
      "lead_info_requested",
      "lead_interested",
      "lead_meeting_requested",
    ]);
  });

  it("classifies every reply kind exactly as intended", () => {
    expect(REPLY_KIND_CLASSIFICATION).toEqual({
      lead_interested: "positive",
      lead_referral: "neutral",
      lead_info_requested: "positive",
      lead_meeting_requested: "positive",
      lead_not_interested: "negative",
      lead_wrong_person: "negative",
      lead_changed_job: "negative",
      lead_neutral: "neutral",
      lead_out_of_office: "neutral",
      auto_reply_received: "neutral",
    });
  });

  // A referral is a human reply, so it stops the sequence like every other one.
  it("still stops the sequence on a referral", () => {
    expect(isSequenceStoppingReplyKind("lead_referral")).toBe(true);
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
  it("stops on a job change — a person stating it has replied", () => {
    expect(isSequenceStoppingReplyKind("lead_changed_job")).toBe(true);
  });

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

describe("disqualifying reply kinds — a 'no' about the PERSON, not the MOMENT", () => {
  it("disqualifies only the two kinds that are permanent facts about the person", () => {
    expect(isDisqualifyingReplyKind("lead_wrong_person")).toBe(true);
    expect(isDisqualifyingReplyKind("lead_changed_job")).toBe(true);
  });

  it("does NOT disqualify a lead who simply declines today — that lead stays recyclable", () => {
    expect(isDisqualifyingReplyKind("lead_not_interested")).toBe(false);
  });

  it("disqualifies nothing outside the negative kinds", () => {
    for (const kind of REPLY_KINDS) {
      if (isDisqualifyingReplyKind(kind)) {
        expect(NEGATIVE_REPLY_KINDS).toContain(kind);
      }
    }
  });

  it("changes nothing about the coarse classification — all three negatives stay 'negative'", () => {
    // The finer reading is ADDITIVE. A consumer that reads only the coarse
    // value (features-service counts stats off it) sees no difference at all.
    for (const kind of NEGATIVE_REPLY_KINDS) {
      expect(REPLY_KIND_CLASSIFICATION[kind]).toBe("negative");
    }
  });

  it("leaves the disqualifying kinds sequence-stopping, like every other human reply", () => {
    for (const kind of DISQUALIFYING_REPLY_KINDS) {
      expect(isSequenceStoppingReplyKind(kind)).toBe(true);
    }
  });
});
