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

// What we sent them is read from the DB; mocked so each test states it.
const mockLoadOurOutreach = vi.fn();
vi.mock("../../src/lib/self-send/our-outreach", () => ({
  loadOurOutreach: (...a: unknown[]) => mockLoadOurOutreach(...a),
}));

// The org's reply-judging run of the day (COST-ACCOUNTING-1023), mocked.
const mockJudgingRun = vi.fn();
vi.mock("../../src/lib/reply-judging-run", () => ({
  replyJudgingRunForSequence: (...a: unknown[]) => mockJudgingRun(...a),
}));

import {
  QUALIFICATION_EVENT_TYPES,
  isQualificationEventType,
  parseQualification,
  stripQuotedHistory,
  SYSTEM_PROMPT,
} from "../../src/lib/self-send/qualify-reply";
import { REPLY_CLASSIFICATION_MAP } from "../../src/lib/silver-promote";
import { qualifyReply } from "../../src/lib/self-send/qualify-reply";
import { htmlToText } from "../../src/lib/forward-positive-reply";
import {
  BOTTOM_POSTED_STOP_REPLY,
  ONLY_OUR_QUOTED_THREAD,
} from "../helpers/bottom-posted-reply";

const JUDGING_RUN = "9d0c1b2a-1111-4222-8333-444455556666";
/** Every reply is judged on a sequence: it names the reply-judging run. */
const SEQ = { instantlyCampaignId: "self:seq" };

beforeEach(() => {
  vi.resetAllMocks();
  mockJudgingRun.mockResolvedValue(JUDGING_RUN);
  mockLoadOurOutreach.mockResolvedValue(null);
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

    await qualifyReply("I would be interested, send me the costs", SEQ);

    expect(mockPlatformComplete).toHaveBeenCalledWith(
      expect.objectContaining({
        provider: "deepseek",
        model: "deepseek-flash",
        responseFormat: "json",
        temperature: 0,
        disableThinking: true,
      }),
      { runId: JUDGING_RUN },
    );
  });

  it("is made FOR the org's reply-judging run when the sequence is known (COST-ACCOUNTING-1023)", async () => {
    mockJudgingRun.mockResolvedValue("4c8b7a1e-1111-4222-8333-444455556666");
    mockLoadOurOutreach.mockResolvedValue(null);
    mockPlatformComplete.mockResolvedValue({ content: "", json: { classification: "lead_interested" }, tokensInput: 1, tokensOutput: 1, model: "x" });

    await qualifyReply("Interested", { ...SEQ, instantlyCampaignId: "self:abc" });

    expect(mockJudgingRun).toHaveBeenCalledWith("self:abc");
    expect(mockPlatformComplete.mock.calls.at(-1)?.[1]).toEqual({ runId: "4c8b7a1e-1111-4222-8333-444455556666" });
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

    expect(await qualifyReply("hello", SEQ)).toBeNull();
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

  // Prod 2026-10-01: cutting at the first marker stripped this reply to NOTHING.
  it("keeps what they wrote BELOW a >-quoted block (bottom-posting)", () => {
    const words = stripQuotedHistory(BOTTOM_POSTED_STOP_REPLY);
    expect(words).toBe("STOP!");
    // None of OUR quoted mail survives: not the attribution, not either footer.
    expect(words).not.toMatch(/wrote:/);
    expect(words).not.toMatch(/Reply "stop"/i);
    expect(words).not.toMatch(/unsubscribe/i);
    expect(words).not.toMatch(/Naomi/);
  });

  it("keeps interleaved answers between quoted lines", () => {
    expect(
      stripQuotedHistory(
        "On Mon, 16 Aug 2026, Amy Moore wrote:\n> Open to a call?\nNo.\n> Someone else?\nTry Bob.\n",
      ),
    ).toBe("No.\nTry Bob.");
  });

  it("drops an attribution the client wrapped over two lines", () => {
    expect(
      stripQuotedHistory(
        "On Thu, September 24, 2026 1:01 PM, Naomi Martin\n<naomi@x.com> wrote:\n> pitch\n\nstop",
      ),
    ).toBe("stop");
  });

  // Gmail's plain-text part: the name and the email wrapped away, "wrote:" alone.
  it("drops Gmail's wrapped attribution with a bare wrote: line", () => {
    expect(
      stripQuotedHistory(
        "stop\n\nOn Mon, Sep 28, 2026 at 8:17 AM Kevin Lourd\nwrote:\n\n> Hi Tim,\n> pitch\n\n--\nTim Blake",
      ),
    ).toBe("stop");
  });

  it("keeps a signature ABOVE the quote, as before (top-posting unchanged)", () => {
    expect(stripQuotedHistory("No thanks.\n\n--\nDr. E\n\nOn Thu, Sep 24, 2026, K wrote:\n> pitch")).toBe(
      "No thanks.\n\n--\nDr. E",
    );
  });

  it("returns empty for our quoted thread with nothing of theirs", () => {
    expect(stripQuotedHistory(ONLY_OUR_QUOTED_THREAD)).toBe("");
  });

  // An unprefixed quote has no end marker: everything under it stays cut.
  it("still cuts everything under an attribution with no > block", () => {
    expect(
      stripQuotedHistory(
        "Thanks.\n\nOn Mon, 16 Aug 2026, Amy Moore wrote:\nOur pitch\n\nNot relevant? Reply \"stop\" and I won't email you again.",
      ),
    ).toBe("Thanks.");
  });

  it("keeps a bottom-posted answer in an HTML-only reply (blockquote)", () => {
    const text = htmlToText(
      '<div>On Thu, Oct 1, 2026, Naomi Martin wrote:<br></div><blockquote type="cite"><p>Open to an intro?</p><p>Not relevant? Reply "stop" and I won\'t email you again.</p></blockquote><div>STOP!</div><div>Pam</div>',
    );
    expect(stripQuotedHistory(text)).toBe("STOP!\nPam");
  });
});

describe("qualifyReply on a bottom-posted reply", () => {
  it("sends the classifier THEIR words and records the stop", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "",
      json: { classification: "lead_opt_out_requested" },
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-v4-flash",
    });

    const kind = await qualifyReply(BOTTOM_POSTED_STOP_REPLY, { ...SEQ,
      subject: "Re: Patient dinner events in Tulsa",
    });

    expect(kind).toBe("lead_opt_out_requested");
    const sent = mockPlatformComplete.mock.calls[0][0].message as string;
    expect(sent).toContain("STOP!");
    expect(sent).not.toMatch(/Reply "stop"/i);
  });

  it("never asks the classifier about our quoted footer alone", async () => {
    const kind = await qualifyReply(ONLY_OUR_QUOTED_THREAD, { ...SEQ,
      subject: "Re: Patient dinner events in Tulsa",
    });
    expect(kind).toBeNull();
    expect(mockPlatformComplete).not.toHaveBeenCalled();
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

    const label = await qualifyReply("Not for us, thanks.\n\n> our quoted email", { ...SEQ,
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

    expect(await qualifyReply("hello", SEQ)).toBeNull();
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

    expect(await qualifyReply("interested!", SEQ)).toBe("lead_interested");
  });
});

describe("a reply about something other than the offer", () => {
  it("accepts lead_off_topic as a label", () => {
    expect(QUALIFICATION_EVENT_TYPES).toContain("lead_off_topic");
    expect(parseQualification({ json: { classification: "lead_off_topic" } })).toBe("lead_off_topic");
  });

  it("hands the model the thread's subject, since a short reply says nothing about the topic", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "",
      json: { classification: "lead_off_topic" },
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-v4-flash",
    });

    const kind = await qualifyReply("can you explain?\n\nOn Mon, Sep 28, 2026 Kevin wrote:\n> hi", { ...SEQ,
      subject: "Re: Marktize + Doc Dinners partnership?",
    });

    expect(kind).toBe("lead_off_topic");
    const [{ message, systemPrompt }] = mockPlatformComplete.mock.calls[0];
    expect(message).toBe("Subject: Re: Marktize + Doc Dinners partnership?\n\ncan you explain?");
    expect(systemPrompt).toContain("- lead_off_topic — ");
    // The subject is context, not something they wrote.
    expect(systemPrompt).toContain('When the message starts with "Subject:"');
  });

  it("sends the reply exactly as before when no subject is known", async () => {
    mockPlatformComplete.mockResolvedValue({ content: "", json: { classification: "lead_interested" } });
    await qualifyReply("I would be interested", SEQ);
    expect(mockPlatformComplete.mock.calls[0][0].message).toBe("I would be interested");
  });

  it("keeps the subject out of the shadow measurement's context", async () => {
    mockPlatformComplete.mockResolvedValue({ content: "", json: { classification: "lead_neutral" } });
    await qualifyReply("ok", { ...SEQ, leadEmail: "a@b.com", subject: "Re: x" });
    await drainDetached();
    const [arg] = mockShadowJudgeReply.mock.calls[0];
    expect(arg).not.toHaveProperty("subject");
    expect(arg.leadEmail).toBe("a@b.com");
  });
});

describe("a reply whose only words are its subject", () => {
  it("classifies the subject they typed when the body is empty", async () => {
    mockPlatformComplete.mockResolvedValue({ content: "", json: { classification: "lead_opt_out_requested" } });
    const kind = await qualifyReply("\n> On 09/24/2026 Zoey wrote:\n> Hey Michelle", { ...SEQ, subject: "Stop" });
    expect(kind).toBe("lead_opt_out_requested");
    expect(mockPlatformComplete.mock.calls[0][0].message).toBe("Stop");
  });

  it("never reads OUR subject echoed back (Re:) as their words", async () => {
    const kind = await qualifyReply("> quoted only", { ...SEQ, subject: "Re: More patients for your clinic" });
    expect(kind).toBeNull();
    expect(mockPlatformComplete).not.toHaveBeenCalled();
  });
});

/**
 * prod 2026-10-13, DefiniteJobs: our email offered free job listings to an
 * employer under "Hiring at Carter & Co this month?", the reply "yes we are
 * hiring two warehouse operatives, a free first listing sounds good, can we
 * have a quick call this week?" was judged a job enquiry (lead_off_topic)
 * because the classifier saw the subject and the reply, never what we sell.
 */
describe("the classifier reads what we sent them", () => {
  const OUR_EMAIL =
    "Hi Sam,\n\nDefiniteJobs puts UK vacancies in front of local job seekers.\n\nWould a free first listing help?";

  it("frames their reply with our email, read for THIS sequence", async () => {
    mockLoadOurOutreach.mockResolvedValue(OUR_EMAIL);
    mockPlatformComplete.mockResolvedValue({ content: "", json: { classification: "lead_meeting_requested" } });

    const kind = await qualifyReply("Hi, yes we are hiring two warehouse operatives in Leeds. A quick call this week?", { ...SEQ,
      instantlyCampaignId: "self:abc",
      leadEmail: "sam@carter.co",
      subject: "Re: Hiring at Carter & Co this month?",
    });

    expect(kind).toBe("lead_meeting_requested");
    expect(mockLoadOurOutreach).toHaveBeenCalledWith("self:abc");
    const [{ message, systemPrompt }] = mockPlatformComplete.mock.calls[0];
    expect(message).toBe(
      `Subject: Re: Hiring at Carter & Co this month?\n\nOur email:\n${OUR_EMAIL}\n\nTheir reply:\nHi, yes we are hiring two warehouse operatives in Leeds. A quick call this week?`,
    );
    expect(systemPrompt).toContain('When the message carries an "Our email:" section');
    // "Off topic" is judged against what we sell, not against a fixed topic list.
    expect(systemPrompt).toContain("when our email itself offers what the reply talks about");
  });

  it("sends the reply exactly as before when we hold no email for the thread", async () => {
    mockLoadOurOutreach.mockResolvedValue(null);
    mockPlatformComplete.mockResolvedValue({ content: "", json: { classification: "lead_interested" } });
    await qualifyReply("I would be interested", { ...SEQ, instantlyCampaignId: "inst-1", subject: "Re: x" });
    expect(mockPlatformComplete.mock.calls[0][0].message).toBe("Subject: Re: x\n\nI would be interested");
  });

  // PLATFORM-CALLER-RUN-1024: judging a reply is production; with no
  // sequence there is no run to judge it for, so nothing is asked.
  it("refuses a reply with no sequence: no run, no call", async () => {
    await expect(
      qualifyReply("I would be interested", {} as unknown as Parameters<typeof qualifyReply>[1]),
    ).rejects.toThrow("no reply-judging run");
    expect(mockPlatformComplete).not.toHaveBeenCalled();
  });

  it("a judging run that cannot be resolved asks nothing (never sent as nobody's work)", async () => {
    mockJudgingRun.mockRejectedValue(new Error("runs-service down"));
    await expect(qualifyReply("I would be interested", SEQ)).rejects.toThrow("runs-service down");
    expect(mockPlatformComplete).not.toHaveBeenCalled();
  });

  it("fails loud when our email cannot be read, rather than judging blind", async () => {
    mockLoadOurOutreach.mockRejectedValue(new Error("db down"));
    await expect(qualifyReply("yes please", { ...SEQ, instantlyCampaignId: "self:abc" })).rejects.toThrow("db down");
    expect(mockPlatformComplete).not.toHaveBeenCalled();
  });

  it("frames the subject THEY typed with our email too", async () => {
    mockLoadOurOutreach.mockResolvedValue(OUR_EMAIL);
    mockPlatformComplete.mockResolvedValue({ content: "", json: { classification: "lead_opt_out_requested" } });
    await qualifyReply("> quoted only", { ...SEQ, instantlyCampaignId: "self:abc", subject: "Stop" });
    expect(mockPlatformComplete.mock.calls[0][0].message).toBe(`Our email:\n${OUR_EMAIL}\n\nTheir reply:\nStop`);
  });

  it("knows a competitor's customer is a decline, not one of ours", () => {
    const prompt = SYSTEM_PROMPT;
    expect(prompt).toContain("Using the same kind of product bought from ANOTHER company is not this");
  });
});
