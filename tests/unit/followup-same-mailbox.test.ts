import { describe, it, expect } from "vitest";

import {
  holdSecondEmailToPerson,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";

// FOLLOWUP-SAME-MAILBOX-1125 (owner rule: a person our brand already emailed
// gets a follow-up IN THEIR THREAD or nothing; no-go: never a second email to a
// person the same day from two mailboxes).

// Wednesday 2026-11-25 11:00 America/New_York (inside the window).
const NOW = new Date("2026-11-25T16:00:00Z");
const CHRISTINE = "christine.mcginn@alphaocg.com";
const THREAD_MAILBOX = "kevin@maildistribute.com"; // in_recovery, still sends
const POOL_MAILBOX = "kevin.lourd@growthagency.group"; // in_production

function sequence(overrides: Partial<PendingSequence> = {}): PendingSequence {
  return {
    instantlyCampaignId: "self:followup",
    leadEmail: CHRISTINE,
    accountEmail: THREAD_MAILBOX,
    provisionedSteps: [1],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [null],
    timezone: "America/New_York",
    queuedAt: new Date("2026-11-25T15:59:34Z"),
    continuesThread: true,
    ...overrides,
  };
}

function capacity(overrides: Partial<AccountCapacity> = {}): AccountCapacity {
  return {
    accountEmail: THREAD_MAILBOX,
    mailbox: THREAD_MAILBOX,
    cap: 20,
    recentSustainedDaily: 50,
    sentToday: 0,
    // A recovering mailbox takes no new person.
    adoptsFirstEmails: false,
    ...overrides,
  };
}

const pool = (overrides: Partial<AccountCapacity> = {}) =>
  capacity({ accountEmail: POOL_MAILBOX, mailbox: POOL_MAILBOX, cap: 50, adoptsFirstEmails: true, ...overrides });

describe("FOLLOWUP-SAME-MAILBOX-1125: a thread continuation leaves from its thread's mailbox", () => {
  it("Christine replay: the follow-up pinned to her recovering thread mailbox is selected there, never moved", () => {
    const selection = selectDueSteps([sequence()], [capacity(), pool()], NOW);
    expect(selection.selected).toHaveLength(1);
    expect(selection.selected[0]!.accountEmail).toBe(THREAD_MAILBOX);
    expect(selection.selected[0]!.rehomedFrom).toBeUndefined();
    expect(selection.rehomed).toBe(0);
  });

  it("thread mailbox at its daily cap: the follow-up WAITS (no other mailbox, no new thread)", () => {
    const selection = selectDueSteps(
      [sequence()],
      [capacity({ sentToday: 20 }), pool()],
      NOW,
    );
    expect(selection.selected).toHaveLength(0);
    expect(selection.rehomed).toBe(0);
  });

  it("control: a FIRST email (opens a thread) on a full mailbox is moved to the pool", () => {
    const selection = selectDueSteps(
      [sequence({ continuesThread: false })],
      [capacity({ sentToday: 20 }), pool()],
      NOW,
    );
    expect(selection.selected.map((s) => s.accountEmail)).toEqual([POOL_MAILBOX]);
  });
});

describe("FOLLOWUP-SAME-MAILBOX-1125: one email per person per day across sequences", () => {
  const followUp = sequence({ instantlyCampaignId: "self:e3a85cbd" });
  const otherCampaign = sequence({
    instantlyCampaignId: "self:0df3e086",
    accountEmail: "kevin.lourd@growthagency.media",
    continuesThread: false,
    queuedAt: new Date("2026-11-25T16:00:42Z"),
  });

  it("Christine replay (two sequences due in one run, 2 s apart on 2026-11-25): ONE goes, the thread continuation", () => {
    const { kept, held } = holdSecondEmailToPerson([otherCampaign, followUp], [], NOW);
    expect(kept.map((s) => s.instantlyCampaignId)).toEqual(["self:e3a85cbd"]);
    expect(held).toBe(1);
  });

  it("a person emailed by ANOTHER sequence within the day waits; that sequence's own next step is its cadence's", () => {
    const recent = [
      { leadEmail: CHRISTINE.toUpperCase(), instantlyCampaignId: "self:e3a85cbd", sentAt: new Date(NOW.getTime() - 60_000) },
    ];
    const { kept, held } = holdSecondEmailToPerson([otherCampaign, followUp], recent, NOW);
    expect(kept.map((s) => s.instantlyCampaignId)).toEqual(["self:e3a85cbd"]);
    expect(held).toBe(1);
  });

  it("an email older than a day holds nobody", () => {
    const recent = [
      { leadEmail: CHRISTINE, instantlyCampaignId: "self:old", sentAt: new Date(NOW.getTime() - 25 * 3600_000) },
    ];
    const { kept } = holdSecondEmailToPerson([followUp], recent, NOW);
    expect(kept).toHaveLength(1);
  });

  it("an answer owed to the person's own action is never held, and holds the cold sequence of the same run", () => {
    const answer = sequence({ instantlyCampaignId: "self:answer", continuesThread: false, answersPerson: true });
    const recent = [{ leadEmail: CHRISTINE, instantlyCampaignId: "self:x", sentAt: new Date(NOW.getTime() - 60_000) }];
    expect(holdSecondEmailToPerson([answer], recent, NOW).kept).toHaveLength(1);
    const { kept } = holdSecondEmailToPerson([followUp, answer], [], NOW);
    expect(kept.map((s) => s.instantlyCampaignId)).toEqual(["self:answer"]);
  });

  it("different people are untouched; a sequence not due now holds nobody", () => {
    const other = sequence({ instantlyCampaignId: "self:paul", leadEmail: "paul@smartstorage.ie" });
    const notDue = sequence({
      instantlyCampaignId: "self:later",
      lastSentStep: 1,
      lastSentAt: new Date(NOW.getTime() - 3600_000),
      provisionedSteps: [2],
      stepDelays: [3, null],
    });
    const { kept, held } = holdSecondEmailToPerson([followUp, other, notDue], [], NOW);
    expect(held).toBe(0);
    expect(kept).toHaveLength(3);
  });
});
