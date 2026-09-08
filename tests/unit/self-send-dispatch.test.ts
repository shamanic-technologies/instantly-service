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
// Monday 10:00 in America/Chicago — the fleet default zone, inside the
// prospect-local 08:00-17:00 window every campaign schedule carries.
const NOW = new Date("2026-08-17T15:00:00Z");
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

/** The steps a run would send — `selectDueSteps` also reports why it clipped. */
const pick = (
  sequences: readonly PendingSequence[],
  capacities: readonly AccountCapacity[],
  asOf: Date,
) => selectDueSteps(sequences, capacities, asOf).selected;

describe("selectDueSteps", () => {
  // `mailbox` defaults to the address, which is the Primeforge case (the address
  // IS the SMTP login). A test that wants the Gandi alias case sets it apart.
  const capacity = (over: Partial<AccountCapacity> = {}): AccountCapacity => {
    const accountEmail = over.accountEmail ?? "amy@saviolabsco.com";
    const cap = over.cap ?? 45;
    // `recentSustainedDaily` defaults to the cap itself: a mailbox already sending at
    // its limit, so the volume ramp is saturated and `cap` means exactly what it
    // says. The ramp gets its own cases below rather than colouring every test.
    return {
      accountEmail,
      mailbox: accountEmail,
      cap,
      recentSustainedDaily: cap,
      sentToday: 0,
      ...over,
    };
  };


  it("clips to the room left on the mailbox", () => {
    const sequences = Array.from({ length: 5 }, (_, i) =>
      sequence({ instantlyCampaignId: `camp-${i}`, leadEmail: `p${i}@x.com` }),
    );

    const selected = pick(sequences, [capacity({ cap: 10, sentToday: 8 })], NOW);
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

    const selected = pick(sequences, [capacity({ cap: 1 })], NOW);
    expect(selected.map((s) => s.instantlyCampaignId)).toEqual(["stale"]);
  });

  // Inventing capacity for an unknown account is how a fresh mailbox gets pushed
  // past what Gmail accepts — the exact failure the age ramp exists to prevent.
  it("treats an account with no capacity row as having NO room", () => {
    const selected = pick([sequence()], [], NOW);
    expect(selected).toEqual([]);
  });

  it("caps each mailbox independently", () => {
    const sequences = [
      sequence({ instantlyCampaignId: "a1", accountEmail: "a@x.com" }),
      sequence({ instantlyCampaignId: "a2", accountEmail: "a@x.com" }),
      sequence({ instantlyCampaignId: "b1", accountEmail: "b@x.com" }),
    ];

    const selected = pick(
      sequences,
      [
        capacity({ accountEmail: "a@x.com", cap: 1 }),
        capacity({ accountEmail: "b@x.com", cap: 5 }),
      ],
      NOW,
    );

    expect(selected.map((s) => s.instantlyCampaignId).sort()).toEqual(["a1", "b1"]);
  });

  // ── Aliases share one mailbox, so they share one day's quota ───────────────
  //
  // A Gandi domain is ONE real mailbox carrying several aliases, and we hold a
  // sending account per alias: 154 accounts on 44 mailboxes in prod. Budgeting
  // per address hands that single mailbox five times its quota, which the relay
  // answers with `450 4.7.1 Too many mail per day for sasl <user>` — per SASL
  // USER, not per alias. `growthagency.forum` had three aliases in production at
  // 50/day each against one 50/day mailbox.

  it("spends ONE quota across every alias of the same mailbox", () => {
    const sequences = Array.from({ length: 6 }, (_, i) =>
      sequence({
        instantlyCampaignId: `camp-${i}`,
        leadEmail: `p${i}@x.com`,
        // Three aliases, round-robin, all on one Gandi mailbox.
        accountEmail: ["kevin@ga.forum", "kevinl@ga.forum", "klourd@ga.forum"][i % 3],
      }),
    );

    const selected = pick(
      sequences,
      [
        capacity({ accountEmail: "kevin@ga.forum", mailbox: "kevin@ga.forum", cap: 2 }),
        capacity({ accountEmail: "kevinl@ga.forum", mailbox: "kevin@ga.forum", cap: 2 }),
        capacity({ accountEmail: "klourd@ga.forum", mailbox: "kevin@ga.forum", cap: 2 }),
      ],
      NOW,
    );

    // Two — the mailbox's cap. Keyed per address this was six.
    expect(selected).toHaveLength(2);
  });

  it("counts what an alias ALREADY sent against its mailbox's quota", () => {
    const selected = pick(
      [sequence({ accountEmail: "kevin@ga.forum" })],
      [
        capacity({ accountEmail: "kevin@ga.forum", mailbox: "kevin@ga.forum", cap: 5, sentToday: 0 }),
        // A sibling alias already spent the mailbox's whole allowance today.
        capacity({ accountEmail: "klourd@ga.forum", mailbox: "kevin@ga.forum", cap: 5, sentToday: 5 }),
      ],
      NOW,
    );

    expect(selected).toEqual([]);
  });

  it("takes the LOWEST cap among a mailbox's aliases", () => {
    const sequences = Array.from({ length: 4 }, (_, i) =>
      sequence({ instantlyCampaignId: `c${i}`, leadEmail: `p${i}@x.com`, accountEmail: "kevin@ga.forum" }),
    );

    const selected = pick(
      sequences,
      [
        capacity({ accountEmail: "kevin@ga.forum", mailbox: "kevin@ga.forum", cap: 3 }),
        // An operator lowered one alias; they meant it for the mailbox.
        capacity({ accountEmail: "klourd@ga.forum", mailbox: "kevin@ga.forum", cap: 1 }),
      ],
      NOW,
    );

    expect(selected).toHaveLength(1);
  });

  it("leaves distinct mailboxes independent (the Primeforge case is unchanged)", () => {
    const sequences = [
      sequence({ instantlyCampaignId: "a", accountEmail: "amy@saviolabsco.com" }),
      sequence({ instantlyCampaignId: "b", accountEmail: "ezekiel@plainsignalco.com" }),
    ];

    const selected = pick(
      sequences,
      [
        capacity({ accountEmail: "amy@saviolabsco.com", cap: 1 }),
        capacity({ accountEmail: "ezekiel@plainsignalco.com", cap: 1 }),
      ],
      NOW,
    );

    expect(selected.map((s) => s.instantlyCampaignId).sort()).toEqual(["a", "b"]);
  });

  it("skips a saturated mailbox entirely", () => {
    const selected = pick(
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

    const first = pick(sequences, [capacity()], NOW);
    const second = pick([...sequences].reverse(), [capacity()], NOW);

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

// ─── Prospect-local send window ───────────────────────────────────────────────

describe("selectDueSteps — the prospect's own business hours", () => {
  const capacity = {
    accountEmail: "amy@saviolabsco.com",
    mailbox: "amy@saviolabsco.com",
    cap: 45,
    // Already sending at its limit, so the volume ramp is saturated and the cap
    // is the operator limit — this block is about the window, not the ramp.
    recentSustainedDaily: 45,
    sentToday: 0,
  };
  const due = () => sequence({ provisionedSteps: [1] });

  // On the Instantly transport the campaign schedule holds the send until the
  // prospect's business hours. Here we ARE the scheduler, so without this gate a
  // lead's first email fires at whatever hour the hourly cron happens to run.
  it("holds a step until the lead's local window OPENS", () => {
    // Monday 07:00 in Chicago — the campaign schedule opens at 08:00.
    const beforeOpen = new Date("2026-08-17T12:00:00Z");
    expect(pick([due()], [capacity], beforeOpen)).toEqual([]);
    // ...and one hour later it goes.
    const afterOpen = new Date("2026-08-17T13:00:00Z");
    expect(pick([due()], [capacity], afterOpen)).toHaveLength(1);
  });

  it("stops once the lead's local window CLOSES", () => {
    // Monday 17:00 in Chicago — the window is half-open, so this is shut.
    const afterClose = new Date("2026-08-17T22:00:00Z");
    expect(pick([due()], [capacity], afterClose)).toEqual([]);
  });

  it("uses each lead's OWN zone, so one sends while another waits", () => {
    // 2026-08-17T13:30Z is 08:30 in Chicago (open) and 06:30 in Los Angeles
    // (shut) — the same instant, two different answers.
    const asOf = new Date("2026-08-17T13:30:00Z");
    const chicago = sequence({
      instantlyCampaignId: "c-chi",
      leadEmail: "chi@x.com",
      provisionedSteps: [1],
      timezone: "America/Chicago",
    });
    const pacific = sequence({
      instantlyCampaignId: "c-pac",
      leadEmail: "pac@x.com",
      provisionedSteps: [1],
      timezone: "America/Los_Angeles",
    });
    const picked = pick([chicago, pacific], [capacity], asOf);
    expect(picked.map((d) => d.instantlyCampaignId)).toEqual(["c-chi"]);
  });

  it("holds a lead whose local day is a weekend even though ours is not", () => {
    // Friday 2026-08-21 22:30Z is already SATURDAY in Auckland.
    const fridayHere = new Date("2026-08-21T22:30:00Z");
    expect(fridayHere.getUTCDay()).toBe(5);
    const nz = sequence({ provisionedSteps: [1], timezone: "Pacific/Auckland" });
    expect(pick([nz], [capacity], fridayHere)).toEqual([]);
  });

  // The UTC gate is the outer floor and this one is stricter, never looser: a
  // lead whose local window opens while it is still the weekend HERE waits for
  // the next UTC sending day. Capacity books the earlier of the two, so a send
  // arrives on its booked day or after it — never before.
  it("still refuses a lead whose local window is open on OUR weekend", () => {
    // Sunday 2026-08-16 20:30Z is Monday 08:30 in Auckland — open for the
    // prospect, but the fleet-wide weekend gate holds it anyway.
    const sundayHere = new Date("2026-08-16T20:30:00Z");
    expect(sundayHere.getUTCDay()).toBe(0);
    const nz = sequence({ provisionedSteps: [1], timezone: "Pacific/Auckland" });
    expect(pick([nz], [capacity], sundayHere)).toEqual([]);
  });

  it("falls back to the fleet default zone rather than guessing, when none is stored", () => {
    const asOf = new Date("2026-08-17T13:30:00Z"); // 08:30 Chicago
    const noZone = sequence({ provisionedSteps: [1], timezone: null });
    expect(pick([noZone], [capacity], asOf)).toHaveLength(1);
  });
});

// ─── Weekday gate ─────────────────────────────────────────────────────────────

describe("selectDueSteps — sending calendar", () => {
  const SATURDAY = new Date("2026-08-15T15:00:00Z");
  const SUNDAY = new Date("2026-08-16T15:00:00Z");
  const MONDAY = new Date("2026-08-17T15:00:00Z");

  const capacity = {
    accountEmail: "amy@saviolabsco.com",
    mailbox: "amy@saviolabsco.com",
    cap: 45,
    // Already sending at its limit, so the volume ramp is saturated and the cap
    // is the operator limit — this block is about the window, not the ramp.
    recentSustainedDaily: 45,
    sentToday: 0,
  };

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

    expect(pick([overdue], [capacity], day)).toEqual([]);
  });

  // The weekly placement test runs Saturday precisely because mailboxes are
  // otherwise empty and can absorb a ~30-50 seed spike.
  it("leaves the Saturday placement-test slot completely free", () => {
    const many = Array.from({ length: 20 }, (_, i) =>
      sequence({ instantlyCampaignId: `c-${i}`, leadEmail: `p${i}@x.com` }),
    );
    expect(pick(many, [capacity], SATURDAY)).toHaveLength(0);
  });

  // Nothing is lost — a weekend-due step simply waits, and Monday drains the
  // backlog most-overdue-first.
  it("carries a weekend-due step over to Monday", () => {
    const dueOnSaturday = sequence({
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date(SATURDAY.getTime() - 2 * DAY),
    });

    expect(pick([dueOnSaturday], [capacity], SATURDAY)).toEqual([]);

    const onMonday = pick([dueOnSaturday], [capacity], MONDAY);
    expect(onMonday).toHaveLength(1);
    expect(onMonday[0]!.step).toBe(2);
  });

  it("is unchanged on a weekday", () => {
    expect(pick([sequence()], [capacity], MONDAY)).toHaveLength(1);
  });
});

// ─── The cap ramps on VOLUME, at MAILBOX grain ───────────────────────────────
//
// Two failures this pair guards, both of which look like a healthy worker:
//
//   - Ramping each ADDRESS and taking the minimum holds a five-alias mailbox to
//     the cap earned by a fifth of its traffic, so it can never grow. That is
//     the weekly rewind this change removes, re-created one level down.
//   - Reporting only the post-clip count makes a throttled fleet read as an idle
//     one. Prod 2026-09-07: `due: 0` every hour for eight days while 1,208
//     sequences waited on a first email.

describe("selectDueSteps — the volume ramp is applied per real mailbox", () => {
  const NOW = new Date("2026-08-17T15:00:00Z"); // a Monday, inside the window

  const alias = (address: string, over: Partial<AccountCapacity> = {}): AccountCapacity => ({
    accountEmail: address,
    mailbox: "kevin@ga.forum",
    cap: 50,
    recentSustainedDaily: 4,
    sentToday: 0,
    ...over,
  });

  const leads = (n: number) =>
    Array.from({ length: n }, (_, i) =>
      sequence({
        instantlyCampaignId: `camp-${i}`,
        leadEmail: `p${i}@x.com`,
        accountEmail: i % 5 === 0 ? "kevin@ga.forum" : `alias${i % 5}@ga.forum`,
      }),
    );

  it("SUMS the aliases' volumes, so a busy mailbox is not held to one alias's share", () => {
    // Five aliases at 4/day each: the MAILBOX demonstrably carried 20, so it may
    // attempt 30. Ramping per alias would have offered max(5, 6) = 6.
    const capacities = [
      alias("kevin@ga.forum"),
      alias("alias1@ga.forum"),
      alias("alias2@ga.forum"),
      alias("alias3@ga.forum"),
      alias("alias4@ga.forum"),
    ];
    expect(pick(leads(40), capacities, NOW)).toHaveLength(30);
  });

  it("still takes the MINIMUM operator limit across the aliases", () => {
    // Volume says 30; an operator who lowered one alias to 7 meant it for the
    // mailbox behind it, so 7 wins.
    const capacities = [
      alias("kevin@ga.forum", { cap: 7 }),
      alias("alias1@ga.forum"),
      alias("alias2@ga.forum"),
      alias("alias3@ga.forum"),
      alias("alias4@ga.forum"),
    ];
    expect(pick(leads(40), capacities, NOW)).toHaveLength(7);
  });

  it("floors a mailbox that has sent nothing rather than granting it a full cap", () => {
    const cold = [alias("kevin@ga.forum", { recentSustainedDaily: 0 })];
    expect(pick(leads(40).map((s) => ({ ...s, accountEmail: "kevin@ga.forum" })), cold, NOW))
      .toHaveLength(5);
  });
});

describe("selectDueSteps — a throttled run is distinguishable from an idle one", () => {
  const NOW = new Date("2026-08-17T15:00:00Z");

  const leads = (n: number, accountEmail: string) =>
    Array.from({ length: n }, (_, i) =>
      sequence({ instantlyCampaignId: `c-${i}`, leadEmail: `p${i}@x.com`, accountEmail }),
    );

  it("reports how many steps were due BEFORE capacity clipped them", () => {
    const capacity: AccountCapacity = {
      accountEmail: "amy@saviolabsco.com",
      mailbox: "amy@saviolabsco.com",
      cap: 50,
      recentSustainedDaily: 0, // cold ⇒ floored at 5
      sentToday: 0,
    };
    const out = selectDueSteps(leads(40, "amy@saviolabsco.com"), [capacity], NOW);
    expect(out.selected).toHaveLength(5);
    expect(out.dueBeforeCapacity).toBe(40);
    expect(out.blockedNoCapacityRow).toBe(0);
  });

  it("counts steps stranded on a mailbox we hold no credential for", () => {
    // These are not slow, they are abandoned: no cap will ever grow into them.
    // ~600 sequences sat in exactly this state without a single log line.
    const out = selectDueSteps(leads(12, "nocreds@ga.forum"), [], NOW);
    expect(out.selected).toEqual([]);
    expect(out.dueBeforeCapacity).toBe(12);
    expect(out.blockedNoCapacityRow).toBe(12);
  });

  it("reports zeros on a weekend rather than a count nothing will act on", () => {
    const SATURDAY = new Date("2026-08-15T15:00:00Z");
    const out = selectDueSteps(leads(9, "amy@saviolabsco.com"), [], SATURDAY);
    expect(out).toEqual({ selected: [], dueBeforeCapacity: 0, blockedNoCapacityRow: 0 });
  });
});
