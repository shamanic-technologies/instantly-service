import { describe, expect, it } from "vitest";
import {
  ABSENCE_SYSTEM_PROMPT,
  absenceJudgmentMessage,
  parseStatedReturn,
  resolveAbsence,
  type StatedReturn,
} from "../../src/lib/self-send/absence";
import {
  isHeldForAbsence,
  nextDueStep,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";

/**
 * OOO-RETURN-DATE-1101. Fixtures copied from the three prod cases (CeXO AI,
 * 2026-10) plus Rafael's Spanish one: the model's reading is given, the rest is
 * ours.
 */

const at = (iso: string) => new Date(iso);
const stated = (s: Partial<StatedReturn> & Pick<StatedReturn, "month" | "day" | "meaning">): StatedReturn => ({
  year: null,
  ...s,
});

describe("resolveAbsence: from the stated date to the hold", () => {
  it("Rajesh: 'on PTO from 10/16 to 10/26' read 10/21 = back 10/27, nothing before 10/28", () => {
    const r = resolveAbsence(stated({ month: 10, day: 26, meaning: "away_through" }), at("2026-10-21T12:15:52Z"));
    expect(r.outcome).toBe("return_date");
    expect(r.firstDayBack).toBe("2026-10-27");
    expect(r.holdUntil?.toISOString()).toBe("2026-10-28T00:00:00.000Z");
  });

  it("Shaikha: 'will return on 1st November 2026' = back 11/01, nothing before 11/02", () => {
    const r = resolveAbsence(stated({ year: 2026, month: 11, day: 1, meaning: "back_on" }), at("2026-10-26T04:12:10Z"));
    expect(r.firstDayBack).toBe("2026-11-01");
    expect(r.holdUntil?.toISOString()).toBe("2026-11-02T00:00:00.000Z");
  });

  it("Matti: 'out of office until 2nd Nov' = away through 11/02, nothing before 11/04", () => {
    const r = resolveAbsence(stated({ month: 11, day: 2, meaning: "away_through" }), at("2026-10-26T06:17:27Z"));
    expect(r.firstDayBack).toBe("2026-11-03");
    expect(r.holdUntil?.toISOString()).toBe("2026-11-04T00:00:00.000Z");
  });

  it("Rafael: 'hasta el 23 de Octubre' = nothing before 10/25", () => {
    const r = resolveAbsence(stated({ month: 10, day: 23, meaning: "away_through" }), at("2026-10-14T09:00:00Z"));
    expect(r.holdUntil?.toISOString()).toBe("2026-10-25T00:00:00.000Z");
  });

  it("a stale autoresponder: 'until September 21' read in October is NOT next September", () => {
    const r = resolveAbsence(stated({ month: 9, day: 21, meaning: "away_through" }), at("2026-10-26T10:00:00Z"));
    expect(r).toEqual({ outcome: "stale", firstDayBack: null, holdUntil: null });
  });

  it("a stale date WITH its year is stale too", () => {
    const r = resolveAbsence(stated({ year: 2026, month: 9, day: 21, meaning: "away_through" }), at("2026-10-26T10:00:00Z"));
    expect(r.outcome).toBe("stale");
  });

  it("a year turn: 'until January 5' read on December 20 is next January", () => {
    const r = resolveAbsence(stated({ month: 1, day: 5, meaning: "away_through" }), at("2026-12-20T10:00:00Z"));
    expect(r.firstDayBack).toBe("2027-01-06");
  });

  it("back today ('until yesterday') holds nothing", () => {
    const r = resolveAbsence(stated({ year: 2026, month: 10, day: 25, meaning: "away_through" }), at("2026-10-26T10:00:00Z"));
    expect(r.outcome).toBe("stale");
  });

  it("no date / unreadable / impossible day hold nothing", () => {
    expect(resolveAbsence(null, at("2026-10-26T10:00:00Z")).outcome).toBe("no_date");
    expect(resolveAbsence(undefined, at("2026-10-26T10:00:00Z")).outcome).toBe("unreadable");
    expect(resolveAbsence(stated({ year: 2026, month: 2, day: 30, meaning: "back_on" }), at("2026-01-26T10:00:00Z")).outcome).toBe("no_date");
  });

  it("an absence beyond a year is no date", () => {
    expect(resolveAbsence(stated({ year: 2028, month: 1, day: 5, meaning: "back_on" }), at("2026-10-26T10:00:00Z")).outcome).toBe("no_date");
  });
});

describe("parseStatedReturn: what the model answered", () => {
  it("reads the json field and the content string", () => {
    expect(parseStatedReturn({ json: { date: { year: null, month: 11, day: 2 }, meaning: "away_through" } })).toEqual({
      year: null,
      month: 11,
      day: 2,
      meaning: "away_through",
    });
    expect(parseStatedReturn({ content: '{"date":{"year":2026,"month":11,"day":1},"meaning":"back_on"}' })?.year).toBe(2026);
  });

  it("no date = null (judged: nothing to hold)", () => {
    expect(parseStatedReturn({ json: { date: null, meaning: null } })).toBeNull();
  });

  it("anything else = undefined (unreadable)", () => {
    expect(parseStatedReturn({ content: "I think November" })).toBeUndefined();
    expect(parseStatedReturn({ json: { date: { month: "11", day: 2 }, meaning: "away_through" } })).toBeUndefined();
    expect(parseStatedReturn({ json: { date: { month: 11, day: 2 }, meaning: "soon" } })).toBeUndefined();
  });
});

describe("the judgment request", () => {
  it("gives the message date (to resolve 'back Monday') and never asks the model to guess a year", () => {
    const message = absenceJudgmentMessage({
      receivedAt: at("2026-10-21T12:15:52Z"),
      subject: "Automatic reply: Proving AI value at McKesson?",
      text: "I am on PTO from 10/16 to 10/26.",
    });
    expect(message).toContain("Message date: 2026-10-21 (Wednesday)");
    expect(message).toContain("I am on PTO from 10/16 to 10/26.");
    expect(ABSENCE_SYSTEM_PROMPT).toContain("never guess it");
    expect(ABSENCE_SYSTEM_PROMPT).toContain("any language");
  });
});

// ── The dispatch hold ──────────────────────────────────────────────────────

/** Rajesh's sequence: step 1 sent 10/21 12:15, step 2 due 3 days later, step 3 7 days after step 2. */
const rajesh = (over: Partial<PendingSequence> = {}): PendingSequence => ({
  instantlyCampaignId: "self:69535282-400f-456a-8475-fe4a46f298c6",
  leadEmail: "prospect@example.com",
  accountEmail: "sender@ours.example",
  provisionedSteps: [2, 3],
  lastSentStep: 1,
  lastSentAt: at("2026-10-21T12:15:00Z"),
  stepDelays: [3, 7, null],
  timezone: "America/Chicago",
  awayUntil: at("2026-10-28T00:00:00Z"),
  ...over,
});

const capacity: AccountCapacity = {
  accountEmail: "sender@ours.example",
  mailbox: "sender@ours.example",
  cap: 50,
  recentSustainedDaily: 50,
  sentToday: 0,
};

describe("dispatch: nothing leaves before the person is back", () => {
  it("AC1: step 2 due 10/24 waits; it leaves on 10/28, step 3 keeps its 7 days after it", () => {
    // 10/24 (Saturday) 15:00 UTC = 10:00 Chicago: due by cadence, held.
    expect(nextDueStep(rajesh(), at("2026-10-24T15:00:00Z"))).toBeNull();
    expect(nextDueStep(rajesh(), at("2026-10-27T15:00:00Z"))).toBeNull();
    // 10/28 15:00 UTC: back + 1 day.
    expect(nextDueStep(rajesh(), at("2026-10-28T15:00:00Z"))?.step).toBe(2);
    // Step 2 really went out 10/28 15:00: step 3 is due 7 days after IT.
    const afterStep2 = rajesh({ provisionedSteps: [3], lastSentStep: 2, lastSentAt: at("2026-10-28T15:00:00Z") });
    expect(nextDueStep(afterStep2, at("2026-11-04T14:59:00Z"))).toBeNull();
    expect(nextDueStep(afterStep2, at("2026-11-04T15:00:00Z"))?.step).toBe(3);
  });

  it("the full selection holds them too (and sends without the hold: control)", () => {
    const asOf = at("2026-10-24T15:00:00Z");
    expect(selectDueSteps([rajesh()], [capacity], asOf).selected).toHaveLength(0);
    expect(selectDueSteps([rajesh({ awayUntil: null })], [capacity], asOf).selected.map((s) => s.step)).toEqual([2]);
  });

  it("Rafael: back before the step is due = unchanged", () => {
    const rafael = rajesh({ lastSentAt: at("2026-10-14T12:00:00Z"), stepDelays: [14, 7, null], awayUntil: at("2026-10-25T00:00:00Z") });
    expect(nextDueStep(rafael, at("2026-10-28T15:00:00Z"))?.dueAt.toISOString()).toBe("2026-10-28T12:00:00.000Z");
  });

  it("a past hold (stale, or already back) changes nothing", () => {
    const seq = rajesh({ awayUntil: at("2026-10-20T00:00:00Z") });
    expect(isHeldForAbsence(seq, at("2026-10-24T15:00:00Z"))).toBe(false);
    expect(nextDueStep(seq, at("2026-10-24T15:00:00Z"))?.step).toBe(2);
  });

  it("a new follow-up sequence to the same person waits as well (the hold is per address)", () => {
    const followUp = rajesh({ instantlyCampaignId: "self:new", provisionedSteps: [1], lastSentStep: null, lastSentAt: null, queuedAt: at("2026-10-23T10:00:00Z") });
    expect(nextDueStep(followUp, at("2026-10-24T15:00:00Z"))).toBeNull();
  });

  it("an answer owed to the person's own action is never held: acting proves they are there", () => {
    const answer = rajesh({ provisionedSteps: [1], lastSentStep: null, lastSentAt: null, answersPerson: true });
    expect(isHeldForAbsence(answer, at("2026-10-24T15:00:00Z"))).toBe(false);
  });
});
