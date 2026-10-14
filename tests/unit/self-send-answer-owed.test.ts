import { describe, it, expect } from "vitest";

import {
  isAnswerOwed,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";

// FORM-ANSWER-LATENCY-1014: an email that answers a person's own action (its
// campaign runs a REACTIVE leg: a form fill, a pushed email) is owed NOW, like a
// reply. A cold (proactive) first touch keeps every gate it had.

// Monday 10:00 America/Chicago (inside the window).
const MONDAY_IN_WINDOW = new Date("2026-08-17T15:00:00Z");
// Monday 03:00 America/Chicago (window shut).
const MONDAY_NIGHT = new Date("2026-08-17T08:00:00Z");
// Sunday 10:00 America/Chicago (no sending day).
const SUNDAY = new Date("2026-08-16T15:00:00Z");

function sequence(overrides: Partial<PendingSequence> = {}): PendingSequence {
  return {
    instantlyCampaignId: "camp-cold",
    leadEmail: "prospect@example.com",
    accountEmail: "amy@saviolabsco.com",
    provisionedSteps: [1, 2, 3],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [2, 5, null],
    timezone: "America/Chicago",
    ...overrides,
  };
}

const answer = (overrides: Partial<PendingSequence> = {}) =>
  sequence({
    instantlyCampaignId: "camp-answer",
    leadEmail: "kevin+form@distribute.you",
    provisionedSteps: [1],
    stepDelays: [null],
    answersPerson: true,
    ...overrides,
  });

function capacity(overrides: Partial<AccountCapacity> = {}): AccountCapacity {
  return {
    accountEmail: "amy@saviolabsco.com",
    mailbox: "amy@saviolabsco.com",
    cap: 50,
    recentSustainedDaily: 50,
    sentToday: 0,
    adoptsFirstEmails: true,
    ...overrides,
  };
}

const ids = (selection: ReturnType<typeof selectDueSteps>) =>
  selection.selected.map((s) => s.instantlyCampaignId);

describe("isAnswerOwed", () => {
  it("is the FIRST email of a sequence answering a person, nothing else", () => {
    expect(isAnswerOwed(answer())).toBe(true);
    expect(isAnswerOwed(sequence())).toBe(false);
    expect(isAnswerOwed(answer({ lastSentStep: 1, lastSentAt: MONDAY_IN_WINDOW }))).toBe(false);
  });
});

describe("selectDueSteps — an answer owed to a person leaves at once", () => {
  it("sends the answer when the person's business hours are shut; the cold first touch still waits", () => {
    const selection = selectDueSteps([sequence(), answer()], [capacity()], MONDAY_NIGHT);
    expect(ids(selection)).toEqual(["camp-answer"]);
  });

  it("sends the answer on a Sunday; the cold first touch still rests", () => {
    const selection = selectDueSteps([sequence(), answer()], [capacity()], SUNDAY);
    expect(ids(selection)).toEqual(["camp-answer"]);
  });

  it("puts the answer ahead of older cold steps when the mailbox has room for ONE", () => {
    const oldCold = sequence({ queuedAt: new Date("2026-08-01T00:00:00Z") });
    const selection = selectDueSteps(
      [oldCold, answer({ queuedAt: MONDAY_IN_WINDOW })],
      [capacity({ sentToday: 49 })],
      MONDAY_IN_WINDOW,
    );
    expect(ids(selection)).toEqual(["camp-answer"]);
  });

  it("still respects the mailbox cap: no room anywhere = not sent now", () => {
    const selection = selectDueSteps([answer()], [capacity({ sentToday: 50 })], MONDAY_NIGHT);
    expect(selection.selected).toEqual([]);
    expect(selection.dueBeforeCapacity).toBe(1);
  });

  it("moves the answer to a production mailbox with room when its own is full", () => {
    const selection = selectDueSteps(
      [answer()],
      [
        capacity({ sentToday: 50 }),
        capacity({ accountEmail: "zoe@other.com", mailbox: "zoe@other.com" }),
      ],
      MONDAY_NIGHT,
    );
    expect(selection.selected).toEqual([
      expect.objectContaining({ instantlyCampaignId: "camp-answer", accountEmail: "zoe@other.com", rehomedFrom: "amy@saviolabsco.com" }),
    ]);
  });

  it("skips a silenced mailbox for an answer like for any step (then rehomes it)", () => {
    const selection = selectDueSteps(
      [answer()],
      [capacity(), capacity({ accountEmail: "zoe@other.com", mailbox: "zoe@other.com" })],
      MONDAY_NIGHT,
      new Set(["amy@saviolabsco.com"]),
    );
    expect(selection.skippedSilenced).toBe(1);
    expect(selection.selected[0]).toMatchObject({ accountEmail: "zoe@other.com" });
  });

  it("holds the answer's LATER steps to the normal window (only the answer is owed now)", () => {
    const followup = answer({
      provisionedSteps: [2],
      stepDelays: [0, null],
      lastSentStep: 1,
      lastSentAt: new Date("2026-08-10T15:00:00Z"),
    });
    expect(selectDueSteps([followup], [capacity()], MONDAY_NIGHT).selected).toEqual([]);
  });

  // AC2 control: without the stamp, nothing about a cold first touch changed.
  it("leaves a cold first touch exactly as before: in window sent, out of window and on Sunday held", () => {
    expect(ids(selectDueSteps([sequence()], [capacity()], MONDAY_IN_WINDOW))).toEqual(["camp-cold"]);
    expect(selectDueSteps([sequence()], [capacity()], MONDAY_NIGHT).selected).toEqual([]);
    expect(selectDueSteps([sequence()], [capacity()], SUNDAY).selected).toEqual([]);
  });
});
