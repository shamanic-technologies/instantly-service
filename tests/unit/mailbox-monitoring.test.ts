/**
 * MAILBOX-MONITORING-1019: the monitoring reads at account / mailbox / domain /
 * fleet grain, and the inbox-placement staff alert.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

import { entityKey, inScope, parseEntityScope } from "../../src/lib/ops/entity-scope";
import { mapEntityPeriods, mapSentPeriods } from "../../src/lib/ops/sent-per-period";
import { countSeeds, summarizePlacementTests, type PlacementSeed } from "../../src/lib/ops/placement-tests";
import { buildCapacityDaily, projectMailboxCapacity } from "../../src/lib/ops/capacity-daily";
import type { CapacityHistoryByMailboxPoint } from "../../src/lib/capacity-history";
import {
  buildSchedule,
  listQueuedSteps,
  planSeedTests,
  planWarmups,
  theoreticalStepDate,
} from "../../src/lib/ops/schedule";
import type { QueuedSequenceDetail } from "../../src/lib/account-sending-stats";
import {
  PLACEMENT_ALERT_INBOX_PCT_BAR,
  alertEventId,
  alertMetadata,
  applyEvaluations,
  evaluatePlacement,
  runPlacementAlert,
  type Episode,
  type SeedSenderDay,
} from "../../src/lib/placement-alert";

function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockExecute.mockResolvedValue(pgResult([]));
});

describe("entity scope", () => {
  it("parses the three grains lower-cased and refuses an unknown groupBy", () => {
    expect(parseEntityScope({ domain: "X.com", groupBy: "mailbox" })).toEqual({ scope: { domain: "x.com" }, groupBy: "mailbox" });
    expect(parseEntityScope({ groupBy: "brand" })).toEqual({ error: "groupBy must be account|mailbox|domain" });
  });

  it("an alias belongs to its login's mailbox and to its own domain", () => {
    expect(entityKey("mailbox", "Kevin@x.com", "eric@x.com")).toBe("eric@x.com");
    expect(entityKey("mailbox", "eric@x.com", null)).toBe("eric@x.com");
    expect(entityKey("domain", "kevin@x.com", "eric@x.com")).toBe("x.com");
    expect(inScope({ mailbox: "eric@x.com" }, "kevin@x.com", "eric@x.com")).toBe(true);
    expect(inScope({ mailbox: "eric@x.com", domain: "y.com" }, "kevin@x.com", "eric@x.com")).toBe(false);
  });
});

describe("sent per period, per entity", () => {
  it("fills every period per entity and the entities add up to the fleet", () => {
    const fleet = mapSentPeriods(
      [
        { period_start: "2026-10-18", period_end: "2026-10-19", in_progress: false, to_leads: 3, warmup: 2, seeds: 1 },
        { period_start: "2026-10-19", period_end: "2026-10-20", in_progress: true, to_leads: 1, warmup: 0, seeds: 4 },
      ],
      { grain: "day", since: null, asOf: "2026-10-19T12:00:00Z" },
    );
    const entities = mapEntityPeriods(fleet.periods, [
      { entity_key: "b@x.com", period_start: "2026-10-18", to_leads: 1, warmup: 2, seeds: 0 },
      { entity_key: "a@x.com", period_start: "2026-10-18", to_leads: 2, warmup: 0, seeds: 1 },
      { entity_key: "a@x.com", period_start: "2026-10-19", to_leads: 1, warmup: 0, seeds: 4 },
    ]);
    expect(entities.map((e) => e.key)).toEqual(["a@x.com", "b@x.com"]);
    expect(entities[1].periods).toHaveLength(2);
    expect(entities[1].periods[1]).toMatchObject({ periodStart: "2026-10-19", inProgress: true, toLeads: 0 });
    fleet.periods.forEach((p, i) => {
      for (const k of ["toLeads", "warmup", "seeds"] as const) {
        expect(entities.reduce((n, e) => n + e.periods[i][k], 0)).toBe(p[k]);
      }
    });
  });
});

describe("placement tests", () => {
  it("summarizes silver rows per test and per inbox provider, newest first", () => {
    const tests = summarizePlacementTests([
      { test_id: "seed:a", account_email: "s@x.com", recipient_esp: 1, tested_at: "2026-10-18T07:00:00Z", seed_total: 10, inbox_count: 4, spam_count: 5, missing_count: 1 },
      { test_id: "seed:a", account_email: "s@x.com", recipient_esp: 4, tested_at: "2026-10-18T07:00:00Z", seed_total: 2, inbox_count: 0, spam_count: 2, missing_count: 0 },
      { test_id: "seed:b", account_email: "t@x.com", recipient_esp: 2, tested_at: "2026-10-19T07:00:00Z", seed_total: 3, inbox_count: 3, spam_count: 0, missing_count: 0 },
    ]);
    expect(tests.map((t) => t.testId)).toEqual(["seed:b", "seed:a"]);
    expect(tests[1].byProvider.google).toMatchObject({ seeds: 10, inbox: 4, spam: 5, missing: 1, inboxPct: 40 });
    expect(tests[1].byProvider.microsoftConsumer.spam).toBe(2);
    expect(tests[0].byProvider.microsoft.inboxPct).toBe(100);
  });

  it("counts a never-found seed as missing and leaves out a seed that never left", () => {
    const base: Omit<PlacementSeed, "placement" | "dispatchOutcome"> = {
      sender: "s@x.com", senderMailbox: "s@x.com", senderDomain: "x.com", receiver: "r@g.com", provider: "google",
      providerLabel: "Google", folder: null, msInference: null, msScl: null, dispatchedAt: null, observedAt: null,
      spfPass: null, dkimPass: null, dmarcPass: null,
    };
    const counted = countSeeds([
      { ...base, placement: "inbox", dispatchOutcome: "sent" },
      { ...base, placement: "missing", dispatchOutcome: "sent" },
      { ...base, placement: null, dispatchOutcome: "permanent" },
    ]);
    expect(counted.total).toMatchObject({ seeds: 2, inbox: 1, missing: 1, inboxPct: 50 });
    expect(counted.bySender).toHaveLength(1);
  });
});

describe("capacity per day", () => {
  const mb = (addresses: string[], operatorLimit: number, sustained: number, rampCap: number) => ({
    addresses, operatorLimit, sustained, rampCap, capacity: Math.min(operatorLimit, rampCap),
  });
  const history: CapacityHistoryByMailboxPoint[] = [
    { date: "2026-10-18", inProductionCount: 2, mailboxes: new Map([["eric@x.com", mb(["eric@x.com", "kev@x.com"], 50, 10, 15)], ["a@y.com", mb(["a@y.com"], 50, 40, 50)]]) },
    { date: "2026-10-19", inProductionCount: 3, mailboxes: new Map([["eric@x.com", mb(["eric@x.com", "kev@x.com"], 50, 12, 18)], ["a@y.com", mb(["a@y.com"], 50, 40, 50)]]) },
  ];
  const loginOf = new Map([["eric@x.com", "eric@x.com"], ["kev@x.com", "eric@x.com"], ["a@y.com", null]]);
  const asOf = new Date("2026-10-19T12:00:00Z");

  it("projects the ramp forward from today's volume, never past the operator limit", () => {
    const future = projectMailboxCapacity(history[1], 5);
    const caps = future.map((d) => d.mailboxes.get("eric@x.com")!.capacity);
    expect(future[0].date).toBe("2026-10-20");
    expect(caps[0]).toBeGreaterThan(18);
    for (let i = 1; i < caps.length; i++) expect(caps[i]).toBeGreaterThanOrEqual(caps[i - 1]);
    expect(Math.max(...caps)).toBeLessThanOrEqual(50);
  });

  it("mailbox and domain rows add up to the fleet; account rows carry their mailbox's figure", () => {
    const byMailbox = buildCapacityDaily({ asOf, history, horizonDays: 3, scope: {}, groupBy: "mailbox", loginOf });
    const byDomain = buildCapacityDaily({ asOf, history, horizonDays: 3, scope: {}, groupBy: "domain", loginOf });
    expect(byMailbox.days.map((d) => d.phase)).toEqual(["past", "today", "future", "future", "future"]);
    byMailbox.days.forEach((d, i) => {
      expect(byMailbox.entities!.reduce((n, e) => n + e.days[i].capacity, 0)).toBe(d.capacity);
      expect(byDomain.entities!.reduce((n, e) => n + e.days[i].capacity, 0)).toBe(d.capacity);
    });
    const byAccount = buildCapacityDaily({ asOf, history, horizonDays: 0, scope: { domain: "x.com" }, groupBy: "account", loginOf });
    expect(byAccount.additive).toBe(false);
    expect(byAccount.entities!.map((e) => [e.key, e.days[1].capacity])).toEqual([["eric@x.com", 18], ["kev@x.com", 18]]);
    expect(byAccount.days[1].capacity).toBe(18);
  });
});

describe("schedule: queued steps at their theoretical date", () => {
  const asOf = new Date("2026-10-19T12:00:00Z");
  const seq = (o: Partial<QueuedSequenceDetail>): QueuedSequenceDetail => ({
    account: "kev@x.com", lastSentStep: null, lastSentAt: null, provisionedSteps: [1, 2, 3], stepDelays: [3, 7, 0],
    timezone: null, queuedAt: null, instantlyCampaignId: "self:1", leadEmail: "p@q.com", orgId: "o", campaignId: "c", ...o,
  });

  it("chains the configured delays from the previous send; a never-sent sequence from its assignment day", () => {
    const contacted = seq({ lastSentStep: 1, lastSentAt: new Date("2026-10-16T09:00:00Z"), provisionedSteps: [2, 3] });
    expect(theoreticalStepDate(contacted, 2)!.toISOString()).toBe("2026-10-19T09:00:00.000Z");
    expect(theoreticalStepDate(contacted, 3)!.toISOString()).toBe("2026-10-26T09:00:00.000Z");
    const fresh = seq({ queuedAt: new Date("2026-10-19T08:00:00Z") });
    expect(theoreticalStepDate(fresh, 1)!.toISOString()).toBe("2026-10-19T08:00:00.000Z");
    expect(theoreticalStepDate(fresh, 2)!.toISOString()).toBe("2026-10-22T08:00:00.000Z");
    expect(theoreticalStepDate(seq({}), 1)).toBeNull();
  });

  it("buckets steps on their day, overdue apart, and per-mailbox rows add up to the fleet", () => {
    const loginOf = new Map<string, string | null>([["kev@x.com", "eric@x.com"], ["a@y.com", null]]);
    const steps = listQueuedSteps(
      [
        seq({ lastSentStep: 1, lastSentAt: new Date("2026-10-16T09:00:00Z"), provisionedSteps: [2, 3] }),
        seq({ account: "a@y.com", instantlyCampaignId: "self:2", lastSentStep: 1, lastSentAt: new Date("2026-10-01T09:00:00Z"), provisionedSteps: [2] }),
        seq({ account: "a@y.com", instantlyCampaignId: "self:3", queuedAt: new Date("2026-10-19T08:00:00Z"), provisionedSteps: [1, 2] }),
      ],
      loginOf,
      asOf,
    );
    expect(steps.find((s) => s.instantlyCampaignId === "self:2")!.overdue).toBe(true);
    expect(steps.find((s) => s.instantlyCampaignId === "self:1")!.mailbox).toBe("eric@x.com");
    const schedule = buildSchedule({
      asOf, horizonDays: 7, scope: {}, groupBy: "mailbox", loginOf, steps,
      warmups: [{ mailbox: "eric@x.com", day: "2026-10-19", planned: 4 }],
      seeds: [{ account: "a@y.com", day: "2026-10-25", expectedSeeds: 12 }],
    });
    expect(schedule.totals).toEqual({ overdueQueuedSteps: 1, undatedQueuedSteps: 0, beyondHorizonQueuedSteps: 0 });
    expect(schedule.days[0]).toMatchObject({ date: "2026-10-19", isToday: true, queuedSteps: 2, queuedFirstEmails: 1, queuedFollowUps: 1, warmupsPlanned: 4 });
    expect(schedule.days[3]).toMatchObject({ date: "2026-10-22", queuedFirstEmails: 1 });
    expect(schedule.days[6]).toMatchObject({ date: "2026-10-25", seedTestsPlanned: 1, seedsPlanned: 12 });
    expect(schedule.days[7]).toMatchObject({ date: "2026-10-26", queuedFollowUps: 1 });
    schedule.days.forEach((d, i) => {
      for (const k of ["queuedSteps", "warmupsPlanned", "seedsPlanned"] as const) {
        expect(schedule.entities!.reduce((n, e) => n + e.days[i][k], 0)).toBe(d[k]);
      }
    });
    const one = buildSchedule({ asOf, horizonDays: 7, scope: { account: "kev@x.com" }, groupBy: null, loginOf, steps, warmups: [{ mailbox: "eric@x.com", day: "2026-10-19", planned: 4 }], seeds: [] });
    expect(one.days[0]).toMatchObject({ queuedSteps: 1, warmupsPlanned: 4 });
  });

  it("plans seed tests on the cadence and warmups on the projected cap", () => {
    const plan = planSeedTests(
      [
        { account: "a@y.com", lastTestedAt: new Date("2026-10-18T07:00:00Z"), lastSeeds: 12 },
        { account: "new@y.com", lastTestedAt: null, lastSeeds: null },
      ],
      asOf,
      "2026-11-05",
    );
    expect(plan.filter((p) => p.account === "a@y.com").map((p) => p.day)).toEqual(["2026-10-25", "2026-11-01"]);
    expect(plan.find((p) => p.account === "new@y.com")).toMatchObject({ day: "2026-10-19", expectedSeeds: 12 });
    const warm = planWarmups([{ mailbox: "eric@x.com", operatorLimit: 50, sustained: 50 }], asOf, 2);
    expect(warm.map((w) => w.planned)).toEqual([4, 4, 4]);
  });
});

describe("inbox placement alert", () => {
  const loginOf = new Map<string, string | null>();
  const inProduction = new Set(["good@p.com", "bad@p.com"]);
  const day = (d: string, sender: string, inbox: number, spam: number, esp = 1): SeedSenderDay => ({ day: d, esp, sender, seeds: inbox + spam, inbox, spam });

  it("FIRES: production mailboxes the cut still lets mail Google land below the bar", () => {
    const evals = evaluatePlacement({
      rows: [day("2026-10-19", "good@p.com", 10, 15), day("2026-10-19", "legacy@g.com", 0, 40)],
      health: [{ day: "2026-10-19", refused: [], mostlySpam: { google: [] } }],
      inProduction,
      loginOf,
    });
    expect(evals[0]).toMatchObject({ provider: "google", measured: true, below: true, mailboxesCut: 0 });
    expect(evals[0].allowed.inboxPct).toBe(40);
    expect(evals[0].fleet.found).toBe(65);
    const written = applyEvaluations(evals, new Map());
    expect(written).toHaveLength(1);
    expect(alertEventId(written[0])).toBe("google:2026-10-19");
    expect(alertMetadata(written[0])).toEqual({
      provider: "Google", measuredOn: "2026-10-19", inboxPct: "40.0", seeds: "25",
      thresholdPct: String(PLACEMENT_ALERT_INBOX_PCT_BAR), mailboxesCut: "0", dashboardUrl: "https://dashboard.distribute.you/v2/orgs/org_3CL2MrIGuvlW9YHd5RgRgNoMAMS/brands/f4d73dab-1f9d-49b2-b16e-63ecde76a5eb/monitoring/emails/inbox-providers",
    });
  });

  it("DOES NOT FIRE: the cut already took the bad mailbox off Google, the rest are in the inbox", () => {
    const evals = evaluatePlacement({
      rows: [day("2026-10-19", "good@p.com", 24, 1), day("2026-10-19", "bad@p.com", 0, 30), day("2026-10-19", "legacy@g.com", 0, 40)],
      health: [{ day: "2026-10-19", refused: [], mostlySpam: { google: ["bad@p.com"] } }],
      inProduction,
      loginOf,
    });
    expect(evals[0]).toMatchObject({ below: false, mailboxesCut: 1 });
    expect(evals[0].allowed.inboxPct).toBe(96);
    expect(applyEvaluations(evals, new Map())).toEqual([]);
  });

  it("does not fire on too few found seeds", () => {
    const evals = evaluatePlacement({ rows: [day("2026-10-19", "good@p.com", 1, 9)], health: [], inProduction, loginOf });
    expect(evals[0]).toMatchObject({ measured: false, below: false });
    expect(applyEvaluations(evals, new Map())).toEqual([]);
  });

  it("once per fall: a day still below extends the open episode; a recovery closes it; a new fall opens a new one", () => {
    const below = (d: string) => evaluatePlacement({ rows: [day(d, "good@p.com", 5, 20)], health: [], inProduction, loginOf });
    const above = (d: string) => evaluatePlacement({ rows: [day(d, "good@p.com", 25, 0)], health: [], inProduction, loginOf });
    const open: Episode = applyEvaluations(below("2026-10-18"), new Map())[0];
    const sent = { ...open, alertStatus: "sent" as const };
    const extended = applyEvaluations(below("2026-10-19"), new Map([["google", sent]]));
    expect(extended).toHaveLength(1);
    expect(extended[0]).toMatchObject({ startedOn: "2026-10-18", lastBelowOn: "2026-10-19", alertStatus: "sent", closedOn: null });
    const closed = applyEvaluations(above("2026-10-20"), new Map([["google", extended[0]]]));
    expect(closed[0]).toMatchObject({ startedOn: "2026-10-18", closedOn: "2026-10-20" });
    const again = applyEvaluations(below("2026-10-22"), new Map());
    expect(alertEventId(again[0])).toBe("google:2026-10-22");
  });

  it("runPlacementAlert sends a new episode once and never re-sends an alerted one", async () => {
    const send = vi.fn().mockResolvedValue({ sent: true, duplicate: false });
    const seeds = pgResult([{ day: "2026-10-19", esp: 1, sender: "good@p.com", seeds: 25, inbox: 5, spam: 20 }]);
    const accounts = pgResult([{ email: "good@p.com", mailbox_login: null, lifecycle_status: "in_production" }]);
    // pass 1: no open episode
    mockExecute
      .mockResolvedValueOnce(seeds)
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(accounts)
      .mockResolvedValueOnce(pgResult([]));
    const first = await runPlacementAlert({ dryRun: false, asOf: new Date("2026-10-19T12:00:00Z"), send });
    expect(first.alerted).toEqual(["google:2026-10-19"]);
    expect(send).toHaveBeenCalledTimes(1);

    // pass 2: the episode is open and already alerted
    mockExecute.mockReset();
    mockExecute
      .mockResolvedValueOnce(seeds)
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(accounts)
      .mockResolvedValueOnce(
        pgResult([{ provider: "google", started_on: "2026-10-19", last_below_on: "2026-10-19", closed_on: null, inbox_pct: "20.0", found_seeds: 25, threshold_pct: "80.0", mailboxes_cut: 0, alert_status: "sent" }]),
      )
      .mockResolvedValue(pgResult([]));
    const second = await runPlacementAlert({ dryRun: false, asOf: new Date("2026-10-19T13:00:00Z"), send });
    expect(second.alerted).toEqual([]);
    expect(second.wouldAlert).toEqual([]);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("dry run writes and sends nothing", async () => {
    const send = vi.fn();
    mockExecute
      .mockResolvedValueOnce(pgResult([{ day: "2026-10-19", esp: 1, sender: "good@p.com", seeds: 25, inbox: 5, spam: 20 }]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([{ email: "good@p.com", mailbox_login: null, lifecycle_status: "in_production" }]))
      .mockResolvedValueOnce(pgResult([]));
    const tick = await runPlacementAlert({ dryRun: true, asOf: new Date("2026-10-19T12:00:00Z"), send });
    expect(tick.wouldAlert.map((w) => w.eventId)).toEqual(["google:2026-10-19"]);
    expect(send).not.toHaveBeenCalled();
    expect(mockExecute).toHaveBeenCalledTimes(4);
  });
});
