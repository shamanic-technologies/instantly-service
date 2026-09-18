/**
 * Pure projections the ops reads serve beside the facts.
 *
 * Every function here is a re-application of a rule the service already runs
 * (the ramp, the evidence expiry, the seed cadence, the rate card) to a date
 * or a row, so a dashboard can say WHEN something will happen from the same
 * arithmetic that will make it happen. None of them decides anything.
 */

import {
  DELIVERY_EVIDENCE_MAX_AGE_DAYS,
  IN_PRODUCTION_DAILY_LIMIT,
  RAMP_GROWTH_FACTOR,
  rampCapForVolume,
} from "../account-lifecycle";
import { decideSeedTestDue, type SeedDueDecision } from "../seed-placement/due";
import { SENDING_WEEKDAYS } from "../sending-calendar";

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * The cap trajectory from today's sustained volume to the ceiling, assuming
 * the mailbox USES the room it is given each day (the ramp only grows on
 * measured volume, so this is the fastest honest path, not a promise).
 */
export function rampProjection(
  sustainedDaily: number,
  ceiling: number = IN_PRODUCTION_DAILY_LIMIT,
  asOf: Date = new Date(),
  maxDays = 30,
): Array<{ date: string; cap: number }> {
  const out: Array<{ date: string; cap: number }> = [];
  let sustained = sustainedDaily;
  for (let day = 0; day < maxDays; day++) {
    const cap = rampCapForVolume(sustained, ceiling);
    out.push({ date: new Date(asOf.getTime() + day * DAY_MS).toISOString().slice(0, 10), cap });
    if (cap >= ceiling) break;
    // Using the whole cap today makes tomorrow's sustained at least today's cap;
    // the ramp itself grows by RAMP_GROWTH_FACTOR a day from there.
    sustained = Math.max(sustained * RAMP_GROWTH_FACTOR, cap);
  }
  return out;
}

/** When the latest placement evidence stops counting, per `DELIVERY_EVIDENCE_MAX_AGE_DAYS`. */
export function evidenceExpiresAt(testedAt: Date | string | null): string | null {
  if (!testedAt) return null;
  const t = new Date(testedAt);
  if (Number.isNaN(t.getTime())) return null;
  return new Date(t.getTime() + DELIVERY_EVIDENCE_MAX_AGE_DAYS * DAY_MS).toISOString();
}

/** The next UTC day that is NOT a sending day (the seed harness sends at weekends), on or after `asOf`. */
export function nextNonSendingDay(asOf: Date): string {
  for (let day = 0; day < 8; day++) {
    const d = new Date(asOf.getTime() + day * DAY_MS);
    if (!SENDING_WEEKDAYS.includes(d.getUTCDay())) return d.toISOString().slice(0, 10);
  }
  return asOf.toISOString().slice(0, 10);
}

export interface SeedProjection extends SeedDueDecision {
  /** The day the next seed test is expected to run, from the cadence rules. */
  expectedAt: string;
}

/** Re-apply `decideSeedTestDue` and turn its verdict into a date. */
export function projectNextSeedTest(lastTestedAt: Date | null, asOf: Date): SeedProjection {
  const decision = decideSeedTestDue(lastTestedAt, asOf);
  if (decision.due) return { ...decision, expectedAt: asOf.toISOString().slice(0, 10) };
  if (decision.reason === "recently_tested" && lastTestedAt) {
    // The interval elapses on a given day; the test then waits for the weekend.
    const elapses = new Date(lastTestedAt.getTime() + 6 * DAY_MS);
    return { ...decision, expectedAt: nextNonSendingDay(elapses > asOf ? elapses : asOf) };
  }
  return { ...decision, expectedAt: nextNonSendingDay(asOf) };
}

export interface PooledDelivery {
  inboxCount: number;
  seedTotal: number;
  /** Σinbox / Σseeds across the pooled accounts; null when nothing was measured. */
  inboxPct: number | null;
  /** Newest test among them. */
  testedAt: string | null;
  measuredAccounts: number;
}

/** Pool several accounts' latest tests into one figure, the same Σ/Σ the gate uses. */
export function poolDelivery(
  rows: Array<{ inboxCount: number; seedTotal: number; testedAt: Date | string | null }>,
): PooledDelivery {
  let inbox = 0;
  let seeds = 0;
  let newest: Date | null = null;
  let measured = 0;
  for (const r of rows) {
    if (r.seedTotal <= 0) continue;
    measured += 1;
    inbox += r.inboxCount;
    seeds += r.seedTotal;
    const t = r.testedAt ? new Date(r.testedAt) : null;
    if (t && !Number.isNaN(t.getTime()) && (!newest || t > newest)) newest = t;
  }
  return {
    inboxCount: inbox,
    seedTotal: seeds,
    inboxPct: seeds > 0 ? Math.round((inbox * 1000) / seeds) / 10 : null,
    testedAt: newest ? newest.toISOString() : null,
    measuredAccounts: measured,
  };
}

export interface CostEstimate {
  cents: number;
  currency: string;
  /** Always `estimate` — derived from the rate card, never a vendor charge. */
  source: "estimate";
  since: string;
  months: number;
}

/**
 * What a recurring rate has cost since a start date, in whole elapsed months
 * PLUS the current one — a subscription is billed at the start of a period.
 * Honest about what it is: a rate × a duration, not a ledger.
 */
export function estimatePaidToDate(
  monthlyCents: number,
  currency: string,
  since: Date | string | null,
  asOf: Date,
): CostEstimate | null {
  if (!since) return null;
  const start = new Date(since);
  if (Number.isNaN(start.getTime()) || start > asOf) return null;
  const months =
    (asOf.getUTCFullYear() - start.getUTCFullYear()) * 12 +
    (asOf.getUTCMonth() - start.getUTCMonth()) +
    1;
  return { cents: Math.round(monthlyCents * months), currency, source: "estimate", since: start.toISOString(), months };
}

/** Per-1000 rate from two counts; null when the base is empty (never a fabricated 0). */
export function ratePerMille(numerator: number, base: number): number | null {
  return base > 0 ? Math.round((numerator * 10000) / base) / 10 : null;
}
