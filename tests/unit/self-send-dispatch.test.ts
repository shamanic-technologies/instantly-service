import { describe, it, expect } from "vitest";

import {
  classifyPermanentFailure,
  nextDueStep,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";
import { STEP_GAP_CALENDAR_DAYS } from "../../src/lib/sending-forecast";

// A MONDAY. The original fixture was 2026-08-16, a Sunday — so the whole suite
// was quietly asserting weekend behaviour, which is exactly the bug this file now
// guards against.
const NOW = new Date("2026-08-17T12:00:00Z");
const DAY = 24 * 60 * 60 * 1000;

function sequence(overrides: Partial<PendingSequence> = {}): PendingSequence {
  return {
    instantlyCampaignId: "camp-1",
    leadEmail: "prospect@example.com",
    accountEmail: "amy@saviolabsco.com",
    provisionedSteps: [1, 2, 3],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [2, 5, null],
    ...overrides,
  };
}

// ─── nextDueStep ──────────────────────────────────────────────────────────────

describe("nextDueStep", () => {
  it("makes a never-contacted lead due immediately", () => {
    const due = nextDueStep(sequence(), NOW);
    expect(due).toMatchObject({ step: 1, dueAt: NOW });
  });

  it("returns null when nothing is left to send", () => {
    expect(nextDueStep(sequence({ provisionedSteps: [] }), NOW)).toBeNull();
  });

  it("holds a followup until its own gap has elapsed", () => {
    // step 1 sent 1 day ago, gap 1→2 is 2 days ⇒ not yet.
    const notYet = nextDueStep(
      sequence({
        provisionedSteps: [2, 3],
        lastSentStep: 1,
        lastSentAt: new Date(NOW.getTime() - 1 * DAY),
      }),
      NOW,
    );
    expect(notYet).toBeNull();

    const due = nextDueStep(
      sequence({
        provisionedSteps: [2, 3],
        lastSentStep: 1,
        lastSentAt: new Date(NOW.getTime() - 2 * DAY),
      }),
      NOW,
    );
    expect(due).toMatchObject({ step: 2 });
  });

  // A step two hops out must wait for BOTH gaps, not just the immediate one.
  it("chains every hop when an intermediate step was skipped", () => {
    const base = {
      provisionedSteps: [3],
      lastSentStep: 1,
      lastSentAt: new Date(NOW.getTime() - 6 * DAY),
      stepDelays: [2, 5, null],
    };

    // gaps 1→2 (2d) + 2→3 (5d) = 7 days; only 6 have passed.
    expect(nextDueStep(sequence(base), NOW)).toBeNull();

    expect(
      nextDueStep(
        sequence({ ...base, lastSentAt: new Date(NOW.getTime() - 7 * DAY) }),
        NOW,
      ),
    ).toMatchObject({ step: 3 });
  });

  it("falls back to the shared gap constant for a missing delay", () => {
    const due = nextDueStep(
      sequence({
        provisionedSteps: [4],
        lastSentStep: 3,
        lastSentAt: new Date(NOW.getTime() - STEP_GAP_CALENDAR_DAYS * DAY),
        stepDelays: [2, 5, null],
      }),
      NOW,
    );
    expect(due).toMatchObject({ step: 4 });
  });

  it("sends only the earliest outstanding step, never two at once", () => {
    const due = nextDueStep(
      sequence({
        provisionedSteps: [3, 2],
        lastSentStep: 1,
        lastSentAt: new Date(NOW.getTime() - 30 * DAY),
      }),
      NOW,
    );
    expect(due?.step).toBe(2);
  });

  // A ledger that disagrees with itself must not re-send a real email.
  it("returns null when the outstanding step was already sent", () => {
    expect(
      nextDueStep(
        sequence({
          provisionedSteps: [1],
          lastSentStep: 2,
          lastSentAt: new Date(NOW.getTime() - 30 * DAY),
        }),
        NOW,
      ),
    ).toBeNull();
  });

  it("reports an overdue step's real due date, not now", () => {
    const lastSentAt = new Date(NOW.getTime() - 10 * DAY);
    const due = nextDueStep(
      sequence({ provisionedSteps: [2], lastSentStep: 1, lastSentAt }),
      NOW,
    );
    expect(due?.dueAt).toEqual(new Date(lastSentAt.getTime() + 2 * DAY));
  });
});

// ─── selectDueSteps ───────────────────────────────────────────────────────────

describe("selectDueSteps", () => {
  const capacity = (over: Partial<AccountCapacity> = {}): AccountCapacity => ({
    accountEmail: "amy@saviolabsco.com",
    cap: 45,
    sentToday: 0,
    ...over,
  });

  it("clips to the room left on the mailbox", () => {
    const sequences = Array.from({ length: 5 }, (_, i) =>
      sequence({ instantlyCampaignId: `camp-${i}`, leadEmail: `p${i}@x.com` }),
    );

    const selected = selectDueSteps(sequences, [capacity({ cap: 10, sentToday: 8 })], NOW);
    expect(selected).toHaveLength(2);
  });

  it("sends the most overdue step first", () => {
    const sequences = [
      sequence({
        instantlyCampaignId: "fresh",
        provisionedSteps: [2],
        lastSentStep: 1,
        lastSentAt: new Date(NOW.getTime() - 2 * DAY),
      }),
      sequence({
        instantlyCampaignId: "stale",
        provisionedSteps: [2],
        lastSentStep: 1,
        lastSentAt: new Date(NOW.getTime() - 30 * DAY),
      }),
    ];

    const selected = selectDueSteps(sequences, [capacity({ cap: 1 })], NOW);
    expect(selected.map((s) => s.instantlyCampaignId)).toEqual(["stale"]);
  });

  // Inventing capacity for an unknown account is how a fresh mailbox gets pushed
  // past what Gmail accepts — the exact failure the age ramp exists to prevent.
  it("treats an account with no capacity row as having NO room", () => {
    const selected = selectDueSteps([sequence()], [], NOW);
    expect(selected).toEqual([]);
  });

  it("caps each mailbox independently", () => {
    const sequences = [
      sequence({ instantlyCampaignId: "a1", accountEmail: "a@x.com" }),
      sequence({ instantlyCampaignId: "a2", accountEmail: "a@x.com" }),
      sequence({ instantlyCampaignId: "b1", accountEmail: "b@x.com" }),
    ];

    const selected = selectDueSteps(
      sequences,
      [
        capacity({ accountEmail: "a@x.com", cap: 1 }),
        capacity({ accountEmail: "b@x.com", cap: 5 }),
      ],
      NOW,
    );

    expect(selected.map((s) => s.instantlyCampaignId).sort()).toEqual(["a1", "b1"]);
  });

  it("skips a saturated mailbox entirely", () => {
    const selected = selectDueSteps(
      [sequence()],
      [capacity({ cap: 45, sentToday: 45 })],
      NOW,
    );
    expect(selected).toEqual([]);
  });

  it("is deterministic for steps that came due at the same instant", () => {
    const sequences = ["c", "a", "b"].map((id) =>
      sequence({ instantlyCampaignId: id, leadEmail: `${id}@x.com` }),
    );

    const first = selectDueSteps(sequences, [capacity()], NOW);
    const second = selectDueSteps([...sequences].reverse(), [capacity()], NOW);

    expect(first.map((s) => s.instantlyCampaignId)).toEqual(["a", "b", "c"]);
    expect(second.map((s) => s.instantlyCampaignId)).toEqual(["a", "b", "c"]);
  });
});

// ─── classifyPermanentFailure ─────────────────────────────────────────────────

describe("classifyPermanentFailure", () => {
  // Recording a sender-side refusal as a bounce would poison a reachable
  // prospect's record with a fact about OUR mailbox, permanently.
  it("reads Gmail's daily sending limit as a SENDER problem, never a bounce", () => {
    expect(
      classifyPermanentFailure("550-5.4.5 Daily user sending limit exceeded", 550),
    ).toBe("sender");
  });

  it("reads a policy block as a SENDER problem", () => {
    expect(classifyPermanentFailure("550 5.7.1 Message blocked", 550)).toBe("sender");
  });

  it("reads a dead recipient as a bounce", () => {
    expect(classifyPermanentFailure("550 5.1.1 No such user here", 550)).toBe("recipient");
    expect(classifyPermanentFailure("550 5.1.2 Host unknown", 550)).toBe("recipient");
  });

  it("reads a full mailbox as a bounce — it is about the recipient", () => {
    expect(classifyPermanentFailure("552 5.2.2 Mailbox full", 552)).toBe("recipient");
  });

  it("reads a bare 550 with no enhanced code as a bounce", () => {
    expect(classifyPermanentFailure("550 No such user", 550)).toBe("recipient");
  });

  // Default to the side that never poisons lead data.
  it("defaults an unrecognised permanent failure to SENDER", () => {
    expect(classifyPermanentFailure("", null)).toBe("sender");
    expect(classifyPermanentFailure("500 Syntax error", 500)).toBe("sender");
    expect(classifyPermanentFailure("554 Transaction failed", 554)).toBe("sender");
  });
});

// ─── Weekday gate ─────────────────────────────────────────────────────────────

describe("selectDueSteps — sending calendar", () => {
  const SATURDAY = new Date("2026-08-15T12:00:00Z");
  const SUNDAY = new Date("2026-08-16T12:00:00Z");
  const MONDAY = new Date("2026-08-17T12:00:00Z");

  const capacity = { accountEmail: "amy@saviolabsco.com", cap: 45, sentToday: 0 };

  it("confirms the fixture days really are what they claim", () => {
    expect(SATURDAY.getUTCDay()).toBe(6);
    expect(SUNDAY.getUTCDay()).toBe(0);
    expect(MONDAY.getUTCDay()).toBe(1);
  });

  // Every campaign in the fleet is created Mon-Fri, and both transports run on
  // the same mailboxes — diverging would change a mailbox's behaviour purely
  // because of which pipe a lead was assigned to.
  it.each([
    ["Saturday", SATURDAY],
    ["Sunday", SUNDAY],
  ])("sends nothing on a %s, even with a badly overdue step", (_label, day) => {
    const overdue = sequence({
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date(day.getTime() - 60 * DAY),
    });

    expect(selectDueSteps([overdue], [capacity], day)).toEqual([]);
  });

  // The weekly placement test runs Saturday precisely because mailboxes are
  // otherwise empty and can absorb a ~30-50 seed spike.
  it("leaves the Saturday placement-test slot completely free", () => {
    const many = Array.from({ length: 20 }, (_, i) =>
      sequence({ instantlyCampaignId: `c-${i}`, leadEmail: `p${i}@x.com` }),
    );
    expect(selectDueSteps(many, [capacity], SATURDAY)).toHaveLength(0);
  });

  // Nothing is lost — a weekend-due step simply waits, and Monday drains the
  // backlog most-overdue-first.
  it("carries a weekend-due step over to Monday", () => {
    const dueOnSaturday = sequence({
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date(SATURDAY.getTime() - 2 * DAY),
    });

    expect(selectDueSteps([dueOnSaturday], [capacity], SATURDAY)).toEqual([]);

    const onMonday = selectDueSteps([dueOnSaturday], [capacity], MONDAY);
    expect(onMonday).toHaveLength(1);
    expect(onMonday[0]!.step).toBe(2);
  });

  it("is unchanged on a weekday", () => {
    expect(selectDueSteps([sequence()], [capacity], MONDAY)).toHaveLength(1);
  });
});
