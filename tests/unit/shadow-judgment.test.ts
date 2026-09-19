import { describe, it, expect, vi, beforeEach } from "vitest";

const mockPlatformJudgment = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({
  platformJudgment: (...a: unknown[]) => mockPlatformJudgment(...a),
  platformComplete: vi.fn(),
}));

const mockInsertValues = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({
      values: async (v: unknown) => {
        mockInsertValues(v);
      },
    }),
  },
}));

import {
  buildReplyKindQuestion,
  enginesAgreed,
  isKnownReplyKind,
  recordShadowJudgment,
  shadowJudgeReply,
  REPLY_KIND_QUESTION_KEY,
} from "../../src/lib/shadow-judgment";
import {
  QUALIFICATION_EVENT_TYPES,
  REPLY_KIND_CRITERIA,
  REPLY_KIND_JUDGMENT_INSTRUCTIONS,
  SYSTEM_PROMPT,
} from "../../src/lib/self-send/qualify-reply";

beforeEach(() => {
  vi.resetAllMocks();
});

/** Collapse whitespace so a line-wrapped prompt still matches a single-line criterion. */
function normalize(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

describe("the question is the EXISTING vocabulary, not a second one", () => {
  // A disagreement only means something if both engines answered the same
  // question. If the criteria drift from the prompt, the measurement starts
  // reporting the wording rather than the judgement — silently.
  it("offers exactly the reply kinds this service knows, and no others", () => {
    const question = buildReplyKindQuestion();
    expect(Object.keys(question.criteria).sort()).toEqual(
      [...QUALIFICATION_EVENT_TYPES].sort(),
    );
  });

  it("describes every label with the sentence SYSTEM_PROMPT already gives it", () => {
    const prompt = normalize(SYSTEM_PROMPT);
    for (const label of QUALIFICATION_EVENT_TYPES) {
      const criterion = REPLY_KIND_CRITERIA[label];
      const what = typeof criterion === "string" ? criterion : criterion.what;
      expect(
        prompt.includes(normalize(what)),
        `criterion for ${label} is not in SYSTEM_PROMPT verbatim`,
      ).toBe(true);
    }
  });

  it("takes its worked examples verbatim from SYSTEM_PROMPT too", () => {
    const prompt = normalize(SYSTEM_PROMPT);
    for (const label of QUALIFICATION_EVENT_TYPES) {
      const criterion = REPLY_KIND_CRITERIA[label];
      if (typeof criterion === "string") continue;
      for (const example of criterion.examples ?? []) {
        expect(prompt.includes(normalize(example)), `example "${example}" absent`).toBe(true);
      }
    }
  });

  it("carries the judging rules, including the paragraph about our OWN footer", () => {
    const prompt = normalize(SYSTEM_PROMPT);
    // Load-bearing: every email we send ends with our unsubscribe line, which
    // the reply quotes back. An engine not told this reads OUR words as THEIR
    // removal request — the one label with a legal consequence.
    expect(REPLY_KIND_JUDGMENT_INSTRUCTIONS).toContain("That is OUR footer");
    for (const sentence of [
      "Judge only what the reply says.",
      "That is OUR footer, not their request",
    ]) {
      expect(normalize(REPLY_KIND_JUDGMENT_INSTRUCTIONS)).toContain(normalize(sentence));
      expect(prompt).toContain(normalize(sentence));
    }
  });

  it("asks a CHOICE question, so the answer carries a distribution", () => {
    expect(buildReplyKindQuestion().type).toBe("choice");
  });
});

describe("enginesAgreed", () => {
  it("is true only when both engines named the same label", () => {
    expect(enginesAgreed("lead_interested", "lead_interested")).toBe(true);
    expect(enginesAgreed("lead_interested", "lead_neutral")).toBe(false);
  });

  // An absence is not a disagreement. Counting it as one would inflate the very
  // number this table exists to measure.
  it("is NULL when either side is absent, never false", () => {
    expect(enginesAgreed(null, "lead_interested")).toBeNull();
    expect(enginesAgreed("lead_interested", null)).toBeNull();
    expect(enginesAgreed(null, null)).toBeNull();
  });
});

describe("isKnownReplyKind", () => {
  it("accepts a label from the vocabulary and refuses anything else", () => {
    expect(isKnownReplyKind("lead_interested")).toBe(true);
    expect(isKnownReplyKind("lead_meeting_booked")).toBe(false);
    expect(isKnownReplyKind(undefined)).toBe(false);
  });
});

describe("recordShadowJudgment", () => {
  function answer(choice: string, confidence: number) {
    return {
      model: "jev-latest",
      answers: {
        [REPLY_KIND_QUESTION_KEY]: {
          type: "choice" as const,
          choice,
          confidence,
          probabilities: { [choice]: confidence },
        },
      },
      usage: { inputTokens: 412, outputTokens: 0 },
    };
  }

  it("records BOTH answers, the confidence, the distribution and the token count", async () => {
    mockPlatformJudgment.mockResolvedValue(answer("lead_interested", 0.93));

    await recordShadowJudgment({
      replyText: "Sounds good, I am interested",
      llmClassification: "lead_interested",
      instantlyCampaignId: "self:abc",
      leadEmail: "joe@x.com",
      source: "imap_poller",
    });

    expect(mockInsertValues).toHaveBeenCalledWith(
      expect.objectContaining({
        instantlyCampaignId: "self:abc",
        leadEmail: "joe@x.com",
        source: "imap_poller",
        llmClassification: "lead_interested",
        judgmentClassification: "lead_interested",
        judgmentConfidence: 0.93,
        judgmentProbabilities: { lead_interested: 0.93 },
        judgmentModel: "jev-latest",
        judgmentInputTokens: 412,
        agreed: true,
      }),
    );
  });

  it("records a disagreement as one", async () => {
    mockPlatformJudgment.mockResolvedValue(answer("lead_opt_out_requested", 0.61));

    await recordShadowJudgment({
      replyText: "No interest, please stop sending emails.",
      llmClassification: "lead_not_interested",
    });

    expect(mockInsertValues).toHaveBeenCalledWith(
      expect.objectContaining({
        llmClassification: "lead_not_interested",
        judgmentClassification: "lead_opt_out_requested",
        agreed: false,
        source: "unattributed",
      }),
    );
  });

  // A vendor failure is a fact worth counting. A silently missing row would read
  // as "we never classified that reply".
  it("writes the row WITH the vendor's message, then rethrows, when the engine fails", async () => {
    mockPlatformJudgment.mockRejectedValue(new Error("returned 429: rate limited"));

    await expect(
      recordShadowJudgment({ replyText: "hi", llmClassification: "lead_neutral" }),
    ).rejects.toThrow("429");

    expect(mockInsertValues).toHaveBeenCalledWith(
      expect.objectContaining({
        llmClassification: "lead_neutral",
        error: expect.stringContaining("429"),
      }),
    );
  });

  // Coercing an unreadable answer would put a fabricated judgement into the
  // very table meant to tell us whether the engines agree.
  it("refuses to record a label outside the vocabulary", async () => {
    mockPlatformJudgment.mockResolvedValue(answer("lead_meeting_booked", 0.99));

    await expect(
      recordShadowJudgment({ replyText: "hi", llmClassification: "lead_neutral" }),
    ).rejects.toThrow("unreadable");

    const row = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(row.judgmentClassification).toBeUndefined();
    expect(row.error).toContain("lead_meeting_booked");
    // The token count is still recorded — the call happened and was billed.
    expect(row.judgmentInputTokens).toBe(412);
  });
});

describe("shadowJudgeReply", () => {
  // The one place in this repo where swallowing is correct — and it must be
  // LOUD, with enough detail to tell a vendor error from a bug of ours.
  it("never rejects, and logs the failure with the lead and the campaign", async () => {
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    mockPlatformJudgment.mockRejectedValue(new Error("returned 502: TypeSafe failed"));

    expect(() =>
      shadowJudgeReply({
        replyText: "hi",
        llmClassification: "lead_neutral",
        leadEmail: "joe@x.com",
        instantlyCampaignId: "self:abc",
        source: "reply_opt_out",
      }),
    ).not.toThrow();

    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    const line = logged.mock.calls.map((c) => String(c[0])).join(" ");
    expect(line).toContain("joe@x.com");
    expect(line).toContain("self:abc");
    expect(line).toContain("reply_opt_out");
    expect(line).toContain("the stored classification is unaffected");
    logged.mockRestore();
  });

  it("returns void synchronously, so it cannot add latency to the caller", () => {
    mockPlatformJudgment.mockResolvedValue({
      model: "jev-latest",
      answers: {},
      usage: { inputTokens: 1, outputTokens: 0 },
    });
    vi.spyOn(console, "error").mockImplementation(() => {});
    expect(
      shadowJudgeReply({ replyText: "hi", llmClassification: null }),
    ).toBeUndefined();
  });
});
