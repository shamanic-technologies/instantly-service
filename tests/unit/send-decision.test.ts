import { describe, expect, it } from "vitest";

import {
  mailboxRoom,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";
import { buildSendDecision, SEND_DECISION_SCHEMA } from "../../src/lib/self-send/send-decision";

// FACTORY-RECORD-SEND-1028: the record of WHY a send left when it did is built
// from the very objects the selection decided on, and its room figure is the
// one the selection spends.

// Monday 10:00 in America/Chicago: inside the prospect-local send window.
const NOW = new Date("2026-08-17T15:00:00Z");
const DAY = 24 * 60 * 60 * 1000;

const capacities: AccountCapacity[] = [
  // Two aliases of ONE mailbox: limit = MIN, volume and sends = SUM.
  { accountEmail: "amy@a.test", mailbox: "box@a.test", cap: 45, recentSustainedDaily: 30, sentToday: 4, adoptsFirstEmails: true },
  { accountEmail: "amy.m@a.test", mailbox: "box@a.test", cap: 40, recentSustainedDaily: 20, sentToday: 3 },
  { accountEmail: "bob@b.test", mailbox: "bob@b.test", cap: 50, recentSustainedDaily: 50, sentToday: 0 },
];

const firstEmail: PendingSequence = {
  instantlyCampaignId: "self:first",
  leadEmail: "new@lead.test",
  accountEmail: "amy@a.test",
  provisionedSteps: [1, 2, 3],
  lastSentStep: null,
  lastSentAt: null,
  stepDelays: [3, 7, null],
  timezone: null,
  queuedAt: new Date(NOW.getTime() - 2 * 60 * 60 * 1000),
  verdict: "valid",
};

const followUp: PendingSequence = {
  instantlyCampaignId: "self:follow",
  leadEmail: "old@lead.test",
  accountEmail: "amy.m@a.test",
  provisionedSteps: [2, 3],
  lastSentStep: 1,
  lastSentAt: new Date(NOW.getTime() - 4 * DAY),
  stepDelays: [3, 7, null],
  timezone: "America/New_York",
  verdict: "valid",
};

describe("buildSendDecision", () => {
  const selection = selectDueSteps([firstEmail, followUp], capacities, NOW);

  it("the selection sends both, from the same real mailbox", () => {
    expect(selection.selected.map((s) => s.instantlyCampaignId).sort()).toEqual([
      "self:first",
      "self:follow",
    ]);
  });

  it("records a FIRST email: due when queued, default zone flagged, mailbox room as selected", () => {
    const step = selection.selected.find((s) => s.instantlyCampaignId === "self:first")!;
    const sentAt = new Date(NOW.getTime() + 5_000);
    const decision = buildSendDecision({
      trigger: "sweep",
      step,
      sequence: firstEmail,
      capacities,
      selected: selection.selected,
      selectedAt: NOW,
      sentAt,
    });

    expect(decision.schema).toBe(SEND_DECISION_SCHEMA);
    expect(decision.selectedAt).toBe(NOW.toISOString());
    expect(decision.sentAt).toBe(sentAt.toISOString());
    expect(decision.step).toEqual({
      step: 1,
      dueAt: firstEmail.queuedAt!.toISOString(),
      isFirstEmail: true,
      answerOwed: false,
      rehomedFrom: null,
      cadence: { rule: "first_email_due_when_queued", queuedAt: firstEmail.queuedAt!.toISOString() },
    });
    expect(decision.window).toMatchObject({
      timezone: "America/Chicago",
      timezoneIsDefault: true,
      isSendingDay: true,
      withinLocalWindow: true,
    });
    // limit = min(45, 40), ramp volume = 30 + 20, sent = 4 + 3.
    expect(decision.mailbox.login).toBe("box@a.test");
    expect(decision.mailbox.room).toEqual(mailboxRoom(capacities).get("box@a.test"));
    expect(decision.mailbox.room).toMatchObject({ limit: 40, sustainedDaily: 50, sentToday: 7 });
    expect(decision.mailbox.aliases.map((a) => a.accountEmail).sort()).toEqual(["amy.m@a.test", "amy@a.test"]);
    expect(decision.sequence).toBe(firstEmail);
    // JSON-safe: what the table stores.
    expect(() => JSON.stringify(decision)).not.toThrow();
  });

  it("records a FOLLOW-UP: the gap after the last sent step, its own zone, the steps placed before it", () => {
    const step = selection.selected.find((s) => s.instantlyCampaignId === "self:follow")!;
    const decision = buildSendDecision({
      trigger: "sweep",
      step,
      sequence: followUp,
      capacities,
      selected: selection.selected,
      selectedAt: NOW,
      sentAt: NOW,
    });

    expect(decision.step.isFirstEmail).toBe(false);
    expect(decision.step.cadence).toEqual({
      rule: "gap_after_last_sent_step",
      lastSentStep: 1,
      lastSentAt: followUp.lastSentAt!.toISOString(),
      gapDays: 3,
    });
    expect(decision.step.dueAt).toBe(new Date(followUp.lastSentAt!.getTime() + 3 * DAY).toISOString());
    expect(decision.window).toMatchObject({ timezone: "America/New_York", timezoneIsDefault: false });
    const index = selection.selected.indexOf(step);
    expect(decision.mailbox.selectedBeforeInRun).toBe(index);
  });

  it("mailboxRoom is the room the selection spends: a mailbox sends exactly its room, no more", () => {
    const room = mailboxRoom(capacities).get("box@a.test")!.roomAtStart;
    const many: PendingSequence[] = Array.from({ length: room + 5 }, (_, i) => ({
      ...firstEmail,
      instantlyCampaignId: `self:${String(i).padStart(3, "0")}`,
      leadEmail: `p${i}@lead.test`,
    }));
    const onlyThisMailbox = capacities.filter((c) => c.mailbox === "box@a.test");
    expect(selectDueSteps(many, onlyThisMailbox, NOW).selected).toHaveLength(room);
  });
});
