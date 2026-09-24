import { describe, it, expect } from "vitest";

import {
  nextDueStep,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";

// Monday 10:00 America/Chicago — inside the default prospect window.
const NOW = new Date("2026-08-17T15:00:00Z");
const DAY = 24 * 60 * 60 * 1000;

function seq(over: Partial<PendingSequence> = {}): PendingSequence {
  return {
    instantlyCampaignId: "camp-1",
    leadEmail: "p@x.com",
    accountEmail: "kevin@growthagency.ch",
    provisionedSteps: [1, 2, 3],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [3, 7, null],
    ...over,
  };
}

function cap(over: Partial<AccountCapacity> = {}): AccountCapacity {
  const accountEmail = over.accountEmail ?? "kevin@growthagency.ch";
  const c = over.cap ?? 20;
  return {
    accountEmail,
    mailbox: accountEmail,
    cap: c,
    recentSustainedDaily: c,
    sentToday: 0,
    ...over,
  };
}

/** An overdue followup on the given mailbox. */
function followup(i: number, accountEmail: string): PendingSequence {
  return seq({
    instantlyCampaignId: `fu-${i}`,
    leadEmail: `fu${i}@x.com`,
    accountEmail,
    provisionedSteps: [2, 3],
    lastSentStep: 1,
    lastSentAt: new Date(NOW.getTime() - 5 * DAY),
  });
}

describe("a first email is due from when the lead was handed to us", () => {
  it("dates a never-sent first email at queuedAt, not now", () => {
    const queuedAt = new Date(NOW.getTime() - 14 * DAY);
    expect(nextDueStep(seq({ queuedAt }), NOW)?.dueAt).toEqual(queuedAt);
  });

  it("never dates it in the future", () => {
    const future = new Date(NOW.getTime() + DAY);
    expect(nextDueStep(seq({ queuedAt: future }), NOW)?.dueAt).toEqual(NOW);
  });

  it("CONTROL: without queuedAt it keeps the old reading (due now)", () => {
    expect(nextDueStep(seq(), NOW)?.dueAt).toEqual(NOW);
  });

  it("a two-week-old first email sorts ahead of a followup that came due two days ago", () => {
    const old = seq({
      instantlyCampaignId: "first-old",
      queuedAt: new Date(NOW.getTime() - 14 * DAY),
    });
    // Due at lastSent(−5d) + 3d = −2d.
    const fu = followup(1, "kevin@growthagency.ch");
    const out = selectDueSteps([fu, old], [cap({ cap: 1, recentSustainedDaily: 1 })], NOW);
    expect(out.selected.map((s) => s.instantlyCampaignId)).toEqual(["first-old"]);
  });
});

describe("a first email its mailbox cannot send is moved to a production mailbox with room", () => {
  const full = cap({ accountEmail: "kevin@growthagency.ch", cap: 20, sentToday: 20 });
  const prod = cap({
    accountEmail: "amy@saviolabsco.com",
    cap: 50,
    recentSustainedDaily: 50,
    adoptsFirstEmails: true,
  });

  it("moves a stranded FIRST email, marking where it came from", () => {
    const out = selectDueSteps([seq()], [full, prod], NOW);
    expect(out.rehomed).toBe(1);
    expect(out.selected).toEqual([
      expect.objectContaining({
        instantlyCampaignId: "camp-1",
        step: 1,
        accountEmail: "amy@saviolabsco.com",
        rehomedFrom: "kevin@growthagency.ch",
      }),
    ]);
  });

  it("NEVER moves a followup — its prospect already heard from one mailbox", () => {
    const out = selectDueSteps([followup(1, "kevin@growthagency.ch")], [full, prod], NOW);
    expect(out.selected).toEqual([]);
    expect(out.rehomed).toBe(0);
  });

  it("only a production mailbox adopts", () => {
    const recovering = cap({ accountEmail: "other@gandi.ch", cap: 50, recentSustainedDaily: 50 });
    const out = selectDueSteps([seq()], [full, recovering], NOW);
    expect(out.selected).toEqual([]);
    expect(out.rehomed).toBe(0);
  });

  it("serves the target's own due steps first, then adopts with what is left", () => {
    const tight = cap({
      accountEmail: "amy@saviolabsco.com",
      cap: 2,
      recentSustainedDaily: 2,
      adoptsFirstEmails: true,
    });
    const own = [followup(1, "amy@saviolabsco.com"), followup(2, "amy@saviolabsco.com")];
    const stranded = seq({ queuedAt: new Date(NOW.getTime() - 20 * DAY) });
    const out = selectDueSteps([stranded, ...own], [full, tight], NOW);
    expect(out.selected.map((s) => s.instantlyCampaignId).sort()).toEqual(["fu-1", "fu-2"]);
    expect(out.rehomed).toBe(0);
  });

  it("moves first emails off a SILENCED mailbox too", () => {
    const roomy = cap({ accountEmail: "kevin@growthagency.ch", cap: 20 });
    const out = selectDueSteps(
      [seq()],
      [roomy, prod],
      NOW,
      new Set(["kevin@growthagency.ch"]),
    );
    expect(out.skippedSilenced).toBe(1);
    expect(out.selected[0]).toMatchObject({ accountEmail: "amy@saviolabsco.com" });
  });

  it("moves a first email whose mailbox has no credential at all", () => {
    const out = selectDueSteps([seq({ accountEmail: "ghost@nowhere.com" })], [prod], NOW);
    expect(out.blockedNoCapacityRow).toBe(1);
    expect(out.selected[0]).toMatchObject({
      accountEmail: "amy@saviolabsco.com",
      rehomedFrom: "ghost@nowhere.com",
    });
  });

  it("spreads over the adopters by room, oldest first, and stops when nobody has room", () => {
    const a = cap({ accountEmail: "a@p.com", cap: 2, recentSustainedDaily: 2, adoptsFirstEmails: true });
    const b = cap({ accountEmail: "b@p.com", cap: 1, recentSustainedDaily: 1, adoptsFirstEmails: true });
    const sequences = Array.from({ length: 5 }, (_, i) =>
      seq({
        instantlyCampaignId: `s-${i}`,
        leadEmail: `s${i}@x.com`,
        queuedAt: new Date(NOW.getTime() - (10 - i) * DAY),
      }),
    );
    const out = selectDueSteps(sequences, [full, a, b], NOW);
    expect(out.rehomed).toBe(3);
    expect(out.selected.map((s) => s.instantlyCampaignId)).toEqual(["s-0", "s-1", "s-2"]);
    expect(out.selected.map((s) => s.accountEmail).sort()).toEqual(["a@p.com", "a@p.com", "b@p.com"]);
  });

  it("CONTROL: a first email whose own mailbox has room stays put", () => {
    const roomy = cap({ accountEmail: "kevin@growthagency.ch", cap: 20 });
    const out = selectDueSteps([seq()], [roomy, prod], NOW);
    expect(out.selected[0]).toMatchObject({ accountEmail: "kevin@growthagency.ch" });
    expect(out.selected[0]?.rehomedFrom).toBeUndefined();
    expect(out.rehomed).toBe(0);
  });
});
