/**
 * The future, from today on (MAILBOX-MONITORING-1019): per day and per
 * account / mailbox / domain, what is planned to leave.
 *
 *   queued steps   every un-sent step of every queued sequence at its
 *                  THEORETICAL date: the previous send plus the configured step
 *                  delay (chained), the same nominal cadence the ops queue
 *                  breakdown uses (`delayForGap`). A never-sent sequence is
 *                  anchored on the day it was assigned (its first email is owed
 *                  that day). Deliberately NOT the real send date: no capacity,
 *                  no sending window, no weekend, no holiday (owner's call). A
 *                  step whose theoretical date has passed is OVERDUE, counted
 *                  apart, never moved to a made-up day.
 *   warmups        the warmup rule re-applied (`warmupBudgetFor`) to the cap the
 *                  ramp projects for each day, for the mailboxes that sent warmup
 *                  in the last 7 days (the mesh's members). Each day = sends
 *                  planned; replies (~1/3 of received) come on top.
 *   seed tests     the seed cadence re-applied (`projectNextSeedTest`, then every
 *                  interval, on the next non-sending day) for every address of the
 *                  testable pool, each test expected to carry as many seeds as
 *                  the address's latest one (never tested: the median of the
 *                  latest tests, the receivers being the same).
 *
 * Nothing here decides anything: every figure is a rule the service runs.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { IN_PRODUCTION_DAILY_LIMIT } from "../account-lifecycle";
import { fetchQueuedSequenceDetails, type QueuedSequenceDetail } from "../account-sending-stats";
import { fetchTestablePoolEmails } from "../account-lifecycle-sync";
import { fetchRecentDailyVolume, sustainedForMailbox } from "../recent-send-volume";
import { MS_PER_DAY, delayForGap } from "../sending-forecast";
import { SEED_TEST_INTERVAL_DAYS } from "../seed-placement/due";
import { WARMUP_MAX_PER_DAY, warmupBudgetFor } from "../warmup/plan";
import { entityKey, inScope, mailboxOf, type EntityGrain, type EntityScope } from "./entity-scope";
import { nextNonSendingDay, projectNextSeedTest, rampProjection } from "./pure";

export const SCHEDULE_HORIZON_DEFAULT_DAYS = 30;
export const SCHEDULE_HORIZON_MAX_DAYS = 90;

const dayOf = (d: Date) => d.toISOString().slice(0, 10);

// ─── Queued steps at their theoretical date ──────────────────────────────────

export interface QueuedStep {
  instantlyCampaignId: string;
  leadEmail: string;
  orgId: string | null;
  campaignId: string | null;
  account: string;
  mailbox: string;
  domain: string;
  step: number;
  /** First email of the sequence (nothing sent yet), else a follow-up. */
  isFirstEmail: boolean;
  /** The previous send this date chains from; null for a never-sent sequence (anchored on `queuedAt`). */
  anchoredOn: string | null;
  theoreticalAt: string | null;
  theoreticalDay: string | null;
  overdue: boolean;
}

/** Pure: the theoretical date of one un-sent step. Null only when nothing anchors it. */
export function theoreticalStepDate(seq: QueuedSequenceDetail, step: number): Date | null {
  const delays = seq.stepDelays ?? [];
  if (seq.lastSentStep !== null && seq.lastSentAt) {
    let days = 0;
    for (let hop = seq.lastSentStep; hop < step; hop++) days += delayForGap(hop, delays);
    return new Date(seq.lastSentAt.getTime() + days * MS_PER_DAY);
  }
  if (!seq.queuedAt) return null;
  let days = 0;
  for (let hop = 1; hop < step; hop++) days += delayForGap(hop, delays);
  return new Date(seq.queuedAt.getTime() + days * MS_PER_DAY);
}

/** Pure: every queued step of these sequences, oldest theoretical date first. */
export function listQueuedSteps(
  sequences: readonly QueuedSequenceDetail[],
  loginOf: ReadonlyMap<string, string | null>,
  asOf: Date,
): QueuedStep[] {
  const today = dayOf(asOf);
  const out: QueuedStep[] = [];
  for (const seq of sequences) {
    const account = seq.account.trim().toLowerCase();
    for (const step of [...seq.provisionedSteps].sort((a, b) => a - b)) {
      const at = theoreticalStepDate(seq, step);
      const day = at ? dayOf(at) : null;
      out.push({
        instantlyCampaignId: seq.instantlyCampaignId,
        leadEmail: seq.leadEmail,
        orgId: seq.orgId,
        campaignId: seq.campaignId,
        account,
        mailbox: mailboxOf(account, loginOf.get(account)),
        domain: entityKey("domain", account, null),
        step,
        isFirstEmail: seq.lastSentStep === null,
        anchoredOn: seq.lastSentAt ? seq.lastSentAt.toISOString() : null,
        theoreticalAt: at ? at.toISOString() : null,
        theoreticalDay: day,
        overdue: day !== null && day < today,
      });
    }
  }
  return out.sort(
    (a, b) =>
      (a.theoreticalAt ?? "").localeCompare(b.theoreticalAt ?? "") ||
      a.instantlyCampaignId.localeCompare(b.instantlyCampaignId) ||
      a.step - b.step,
  );
}

// ─── Planned seed tests ──────────────────────────────────────────────────────

export interface SeedPlan {
  account: string;
  day: string;
  expectedSeeds: number;
}

/** Pure: each address's seed tests inside the horizon. */
export function planSeedTests(
  addresses: readonly { account: string; lastTestedAt: Date | null; lastSeeds: number | null }[],
  asOf: Date,
  horizonEnd: string,
): SeedPlan[] {
  // A never-tested address will get what a tested one gets (same receivers):
  // the median of the latest tests.
  const known = addresses.map((a) => a.lastSeeds).filter((n): n is number => n !== null && n > 0).sort((a, b) => a - b);
  const median = known.length ? known[Math.floor(known.length / 2)] : 0;
  const out: SeedPlan[] = [];
  for (const a of addresses) {
    let day = projectNextSeedTest(a.lastTestedAt, asOf).expectedAt;
    const expectedSeeds = a.lastSeeds !== null && a.lastSeeds > 0 ? a.lastSeeds : median;
    for (let guard = 0; guard < 60 && day <= horizonEnd; guard++) {
      out.push({ account: a.account, day, expectedSeeds });
      const elapses = new Date(Date.parse(`${day}T00:00:00Z`) + SEED_TEST_INTERVAL_DAYS * MS_PER_DAY);
      const next = nextNonSendingDay(elapses);
      if (next <= day) break;
      day = next;
    }
  }
  return out;
}

// ─── Planned warmups ─────────────────────────────────────────────────────────

export interface WarmupPlanDay {
  mailbox: string;
  day: string;
  planned: number;
}

/** Pure: per mesh mailbox, the warmup budget of each day from the ramp's projected cap. */
export function planWarmups(
  mailboxes: readonly { mailbox: string; operatorLimit: number; sustained: number }[],
  asOf: Date,
  horizonDays: number,
): WarmupPlanDay[] {
  const out: WarmupPlanDay[] = [];
  for (const m of mailboxes) {
    const path = rampProjection(m.sustained, IN_PRODUCTION_DAILY_LIMIT, asOf, horizonDays + 1);
    for (let d = 0; d <= horizonDays; d++) {
      const rampCap = (path[d] ?? path[path.length - 1]).cap;
      const cap = Math.min(m.operatorLimit, rampCap);
      out.push({
        mailbox: m.mailbox,
        day: dayOf(new Date(asOf.getTime() + d * MS_PER_DAY)),
        planned: warmupBudgetFor(cap, { fillsHeadroom: m.operatorLimit <= WARMUP_MAX_PER_DAY }),
      });
    }
  }
  return out;
}

// ─── The per-day read ────────────────────────────────────────────────────────

export interface ScheduleDay {
  date: string;
  /** True for today: what is left of it is still planned. */
  isToday: boolean;
  queuedSteps: number;
  queuedFirstEmails: number;
  queuedFollowUps: number;
  warmupsPlanned: number;
  seedTestsPlanned: number;
  seedsPlanned: number;
}

export interface ScheduleTotals {
  /** Steps whose theoretical date is before today (late against the cadence). */
  overdueQueuedSteps: number;
  /** Steps with no anchor at all (no previous send, no assignment date). */
  undatedQueuedSteps: number;
  /** Steps whose theoretical date falls after the horizon. */
  beyondHorizonQueuedSteps: number;
}

/** The lifecycle statuses a monitoring read can be narrowed to (`?lifecycle=`). */
export const LIFECYCLE_FILTERS = ["in_production", "in_recovery", "deactivated_by_instantly", "deactivated_by_user"] as const;
export type LifecycleFilter = (typeof LIFECYCLE_FILTERS)[number];

/** Each address's CURRENT lifecycle status, lower-cased keys. */
export async function loadLifecycleOf(): Promise<Map<string, string | null>> {
  const rows = rowsOf<{ email: string; lifecycle_status: string | null }>(
    await db.execute(sql`SELECT lower(email) AS email, lifecycle_status FROM instantly_accounts`),
  );
  return new Map(rows.map((r) => [r.email, r.lifecycle_status]));
}

export interface Schedule {
  asOf: string;
  scope: EntityScope;
  /** Present when the read was narrowed to one lifecycle status. */
  lifecycle?: LifecycleFilter;
  horizonDays: number;
  days: ScheduleDay[];
  totals: ScheduleTotals;
  groupBy?: EntityGrain;
  entities?: { key: string; days: ScheduleDay[]; totals: ScheduleTotals }[];
}

function emptyDays(asOf: Date, horizonDays: number): ScheduleDay[] {
  return Array.from({ length: horizonDays + 1 }, (_, d) => ({
    date: dayOf(new Date(asOf.getTime() + d * MS_PER_DAY)),
    isToday: d === 0,
    queuedSteps: 0,
    queuedFirstEmails: 0,
    queuedFollowUps: 0,
    warmupsPlanned: 0,
    seedTestsPlanned: 0,
    seedsPlanned: 0,
  }));
}

/** Pure: roll queued steps, warmups and seed tests up to days, at one scope and optional grain. */
export function buildSchedule(input: {
  asOf: Date;
  horizonDays: number;
  scope: EntityScope;
  groupBy: EntityGrain | null;
  loginOf: ReadonlyMap<string, string | null>;
  steps: readonly QueuedStep[];
  warmups: readonly WarmupPlanDay[];
  seeds: readonly SeedPlan[];
  /** Keep only addresses whose CURRENT lifecycle status is this one (null = every address). */
  lifecycle?: LifecycleFilter | null;
  lifecycleOf?: ReadonlyMap<string, string | null>;
}): Schedule {
  const { asOf, horizonDays } = input;
  const lifecycleOk = (address: string) =>
    !input.lifecycle || (input.lifecycleOf?.get(address.toLowerCase()) ?? null) === input.lifecycle;
  // A warmup belongs to a mailbox: kept when one of its addresses has the status.
  const mailboxLifecycleOk = (mailbox: string) => {
    if (!input.lifecycle) return true;
    if (lifecycleOk(mailbox)) return true;
    for (const [address, login] of input.loginOf) if (mailboxOf(address, login) === mailbox && lifecycleOk(address)) return true;
    return false;
  };
  const index = new Map(emptyDays(asOf, horizonDays).map((d, i) => [d.date, i]));
  const buckets = new Map<string, { days: ScheduleDay[]; totals: ScheduleTotals }>();
  const bucket = (key: string) => {
    const b = buckets.get(key) ?? {
      days: emptyDays(asOf, horizonDays),
      totals: { overdueQueuedSteps: 0, undatedQueuedSteps: 0, beyondHorizonQueuedSteps: 0 },
    };
    buckets.set(key, b);
    return b;
  };
  const keysFor = (account: string, mailbox: string): string[] => {
    const keys = ["*"];
    if (input.groupBy === "account") keys.push(account);
    else if (input.groupBy === "mailbox") keys.push(mailbox);
    else if (input.groupBy === "domain") keys.push(entityKey("domain", account, null));
    return keys;
  };

  for (const s of input.steps) {
    if (!inScope(input.scope, s.account, input.loginOf.get(s.account) ?? s.mailbox)) continue;
    if (!lifecycleOk(s.account)) continue;
    for (const key of keysFor(s.account, s.mailbox)) {
      const b = bucket(key);
      if (s.theoreticalDay === null) b.totals.undatedQueuedSteps += 1;
      else if (s.overdue) b.totals.overdueQueuedSteps += 1;
      else {
        const i = index.get(s.theoreticalDay);
        if (i === undefined) {
          b.totals.beyondHorizonQueuedSteps += 1;
          continue;
        }
        b.days[i].queuedSteps += 1;
        if (s.isFirstEmail) b.days[i].queuedFirstEmails += 1;
        else b.days[i].queuedFollowUps += 1;
      }
    }
  }

  // A warmup belongs to a MAILBOX; at the account grain it is listed under the
  // mailbox's own address only when that address exists (never split across aliases).
  for (const w of input.warmups) {
    const i = index.get(w.day);
    if (i === undefined) continue;
    if (!inScope(input.scope, w.mailbox, w.mailbox) && !inMailboxScope(input.scope, w.mailbox, input.loginOf)) continue;
    if (!mailboxLifecycleOk(w.mailbox)) continue;
    for (const key of keysFor(w.mailbox, w.mailbox)) bucket(key).days[i].warmupsPlanned += w.planned;
  }

  for (const p of input.seeds) {
    const i = index.get(p.day);
    if (i === undefined) continue;
    const mailbox = mailboxOf(p.account, input.loginOf.get(p.account));
    if (!inScope(input.scope, p.account, input.loginOf.get(p.account) ?? mailbox)) continue;
    if (!lifecycleOk(p.account)) continue;
    for (const key of keysFor(p.account, mailbox)) {
      const day = bucket(key).days[i];
      day.seedTestsPlanned += 1;
      day.seedsPlanned += p.expectedSeeds;
    }
  }

  const fleet = buckets.get("*") ?? bucket("*");
  const served: Schedule = {
    asOf: asOf.toISOString(),
    scope: input.scope,
    ...(input.lifecycle ? { lifecycle: input.lifecycle } : {}),
    horizonDays,
    days: fleet.days,
    totals: fleet.totals,
  };
  if (!input.groupBy) return served;
  return {
    ...served,
    groupBy: input.groupBy,
    entities: [...buckets.keys()]
      .filter((k) => k !== "*")
      .sort()
      .map((key) => ({ key, ...buckets.get(key)! })),
  };
}

/** A mailbox is in an account/domain scope when one of its addresses is. */
function inMailboxScope(scope: EntityScope, mailbox: string, loginOf: ReadonlyMap<string, string | null>): boolean {
  for (const [address, login] of loginOf) {
    if (mailboxOf(address, login) === mailbox && inScope(scope, address, login)) return true;
  }
  return false;
}

// ─── IO ──────────────────────────────────────────────────────────────────────

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

async function loadLoginOf(): Promise<Map<string, string | null>> {
  const rows = rowsOf<{ email: string; mailbox_login: string | null }>(
    await db.execute(sql`SELECT lower(email) AS email, lower(mailbox_login) AS mailbox_login FROM instantly_accounts`),
  );
  return new Map(rows.map((r) => [r.email, r.mailbox_login]));
}

export async function readQueuedSteps(asOf: Date = new Date()): Promise<{ loginOf: Map<string, string | null>; steps: QueuedStep[] }> {
  const [sequences, loginOf] = await Promise.all([fetchQueuedSequenceDetails(), loadLoginOf()]);
  return { loginOf, steps: listQueuedSteps(sequences, loginOf, asOf) };
}

async function loadWarmupMesh(loginOf: ReadonlyMap<string, string | null>) {
  const [mesh, limits, volume] = await Promise.all([
    db.execute(sql`
      SELECT DISTINCT lower(sender_mailbox) AS mailbox FROM warmup_dispatches
      WHERE outcome = 'sent' AND dispatched_at >= now() - interval '7 days'
    `),
    db.execute(sql`SELECT lower(email) AS email, daily_limit FROM instantly_accounts`),
    fetchRecentDailyVolume(),
  ]);
  const limitByMailbox = new Map<string, number>();
  const addressesByMailbox = new Map<string, string[]>();
  for (const r of rowsOf<{ email: string; daily_limit: number | null }>(limits)) {
    const mailbox = mailboxOf(r.email, loginOf.get(r.email));
    const limit = r.daily_limit === null ? 0 : Number(r.daily_limit);
    const known = limitByMailbox.get(mailbox);
    limitByMailbox.set(mailbox, known === undefined ? limit : Math.min(known, limit));
    addressesByMailbox.set(mailbox, [...(addressesByMailbox.get(mailbox) ?? []), r.email]);
  }
  return rowsOf<{ mailbox: string }>(mesh).map(({ mailbox }) => ({
    mailbox,
    operatorLimit: limitByMailbox.get(mailbox) ?? IN_PRODUCTION_DAILY_LIMIT,
    sustained: sustainedForMailbox(volume, addressesByMailbox.get(mailbox) ?? [mailbox]),
  }));
}

async function loadSeedCandidates() {
  const pool = (await fetchTestablePoolEmails()).map((e) => e.toLowerCase());
  if (pool.length === 0) return [];
  const latest = rowsOf<{ account: string; tested_at: unknown; seeds: number | string }>(
    await db.execute(sql`
      SELECT DISTINCT ON (lower(account_email)) lower(account_email) AS account, tested_at,
             (SELECT sum(r2.seed_total) FROM instantly_placement_results r2
               WHERE r2.test_id = r.test_id AND lower(r2.account_email) = lower(r.account_email)) AS seeds
      FROM instantly_placement_results r
      WHERE r.test_id LIKE 'seed:%'
      ORDER BY lower(account_email), tested_at DESC
    `),
  );
  const byAccount = new Map(latest.map((r) => [r.account, r]));
  return pool.map((account) => {
    const r = byAccount.get(account);
    return {
      account,
      lastTestedAt: r?.tested_at ? new Date(String(r.tested_at instanceof Date ? r.tested_at.toISOString() : r.tested_at)) : null,
      lastSeeds: r ? Number(r.seeds) : null,
    };
  });
}

export async function readSchedule(opts: {
  scope: EntityScope;
  groupBy: EntityGrain | null;
  horizonDays: number;
  lifecycle?: LifecycleFilter | null;
  asOf?: Date;
}): Promise<Schedule> {
  const asOf = opts.asOf ?? new Date();
  const { loginOf, steps } = await readQueuedSteps(asOf);
  const [mesh, seedCandidates, lifecycleOf] = await Promise.all([
    loadWarmupMesh(loginOf),
    loadSeedCandidates(),
    opts.lifecycle ? loadLifecycleOf() : Promise.resolve(new Map<string, string | null>()),
  ]);
  const horizonEnd = dayOf(new Date(asOf.getTime() + opts.horizonDays * MS_PER_DAY));
  return buildSchedule({
    asOf,
    horizonDays: opts.horizonDays,
    scope: opts.scope,
    groupBy: opts.groupBy,
    loginOf,
    steps,
    warmups: planWarmups(mesh, asOf, opts.horizonDays),
    seeds: planSeedTests(seedCandidates, asOf, horizonEnd),
    lifecycle: opts.lifecycle ?? null,
    lifecycleOf,
  });
}
