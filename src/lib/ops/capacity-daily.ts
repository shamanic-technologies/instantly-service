/**
 * Capacity per day, past and future, and the ramp that produced it
 * (MAILBOX-MONITORING-1019). Staff read `GET /internal/ops/capacity-daily`.
 *
 * PAST (first lifecycle event .. today): `fetchCapacityHistoryByMailbox`, the
 * same reconstruction `/internal/audit/capacity-history` sums, so the fleet line
 * here IS that series. Per real mailbox, as-of each day: its in_production
 * addresses, the operator limit (min across them), the volume it had sustained
 * over the ramp window, the ramp cap that volume earned, and
 * capacity = min(limit, ramp cap). Today is `phase: "today"`: its sustained
 * volume includes today so far.
 *
 * FUTURE (tomorrow .. +horizonDays): the ramp rule re-applied (`rampProjection`)
 * from today's sustained volume, for the mailboxes in production TODAY, holding
 * today's operator limit. It assumes the mailbox uses the room it is given (the
 * fastest honest path, never a promise) and that nothing changes state.
 *
 * Grains: a mailbox's figures add up to its domain and the fleet. At the ACCOUNT
 * grain each address carries its MAILBOX's figure (the quota is the mailbox's,
 * shared by its aliases), so account rows do not add (`additive: false`).
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import {
  IN_PRODUCTION_DAILY_LIMIT,
  RAMP_FLOOR_PER_DAY,
  RAMP_GROWTH_FACTOR,
  RAMP_VOLUME_WINDOW_DAYS,
} from "../account-lifecycle";
import { fetchCapacityHistoryByMailbox, type CapacityHistoryByMailboxPoint, type MailboxCapacityOnDay } from "../capacity-history";
import { getOrSetCachedStats } from "../stats-cache";
import { domainOf, inScope, type EntityGrain, type EntityScope } from "./entity-scope";
import { rampProjection } from "./pure";

const DAY_MS = 86_400_000;
export const CAPACITY_HORIZON_DEFAULT_DAYS = 30;
export const CAPACITY_HORIZON_MAX_DAYS = 90;

export interface CapacityDay {
  date: string;
  phase: "past" | "today" | "future";
  /** What could be sent that day (min of operator limit and ramp cap, summed per mailbox). */
  capacity: number;
  /** What the ramp alone allowed (summed per mailbox). */
  rampCap: number;
  /** The operator limits (summed per mailbox). */
  operatorLimit: number;
  /** Volume sustained over the ramp window ending that day (summed per mailbox); future = the projection's assumption. */
  sustained: number;
  mailboxesInProduction: number;
  addressesInProduction: number;
}

export interface CapacityDaily {
  asOf: string;
  scope: EntityScope;
  horizonDays: number;
  rampRules: {
    floorPerDay: number;
    growthFactor: number;
    volumeWindowDays: number;
    ceilingPerDay: number;
    rule: string;
  };
  days: CapacityDay[];
  groupBy?: EntityGrain;
  /** False at the account grain: aliases share one mailbox's quota. */
  additive?: boolean;
  entities?: { key: string; days: CapacityDay[] }[];
}

function emptyDay(date: string, phase: CapacityDay["phase"]): CapacityDay {
  return { date, phase, capacity: 0, rampCap: 0, operatorLimit: 0, sustained: 0, mailboxesInProduction: 0, addressesInProduction: 0 };
}

function addMailbox(day: CapacityDay, m: MailboxCapacityOnDay): void {
  day.capacity += m.capacity;
  day.rampCap += m.rampCap;
  day.operatorLimit += m.operatorLimit;
  day.sustained += m.sustained;
  day.mailboxesInProduction += 1;
  day.addressesInProduction += m.addresses.length;
}

/**
 * Pure: extend each mailbox in production on the LAST history day into the
 * future with the ramp rule. Returns the future days' per-mailbox figures.
 */
export function projectMailboxCapacity(
  today: CapacityHistoryByMailboxPoint,
  horizonDays: number,
): { date: string; mailboxes: Map<string, MailboxCapacityOnDay> }[] {
  const start = Date.parse(`${today.date}T00:00:00Z`);
  const out = Array.from({ length: horizonDays }, (_, i) => ({
    date: new Date(start + (i + 1) * DAY_MS).toISOString().slice(0, 10),
    mailboxes: new Map<string, MailboxCapacityOnDay>(),
  }));
  for (const [mailbox, m] of today.mailboxes) {
    // Day 0 of the projection is today; it reproduces today's ramp cap.
    const path = rampProjection(m.sustained, IN_PRODUCTION_DAILY_LIMIT, new Date(start), horizonDays + 1);
    let sustained = m.sustained;
    for (let d = 1; d <= horizonDays; d++) {
      const rampCap = (path[d] ?? path[path.length - 1]).cap;
      // The projection's own assumption: yesterday's room was used.
      sustained = Math.max(sustained, (path[d - 1] ?? path[path.length - 1]).cap);
      out[d - 1].mailboxes.set(mailbox, {
        addresses: m.addresses,
        operatorLimit: m.operatorLimit,
        sustained,
        rampCap,
        capacity: Math.min(m.operatorLimit, rampCap),
      });
    }
  }
  return out;
}

/** Pure: the served shape from per-mailbox days. */
export function buildCapacityDaily(input: {
  asOf: Date;
  history: CapacityHistoryByMailboxPoint[];
  horizonDays: number;
  scope: EntityScope;
  groupBy: EntityGrain | null;
  loginOf: ReadonlyMap<string, string | null>;
}): CapacityDaily {
  const todayKey = input.asOf.toISOString().slice(0, 10);
  const last = input.history[input.history.length - 1];
  const all: { date: string; phase: CapacityDay["phase"]; mailboxes: Map<string, MailboxCapacityOnDay> }[] = [
    ...input.history.map((p) => ({ date: p.date, phase: (p.date === todayKey ? "today" : "past") as CapacityDay["phase"], mailboxes: p.mailboxes })),
    ...(last ? projectMailboxCapacity(last, input.horizonDays) : []).map((p) => ({ ...p, phase: "future" as const })),
  ];

  // A mailbox is in scope when any of its addresses is.
  const mailboxInScope = (mailbox: string, m: MailboxCapacityOnDay) =>
    m.addresses.some((a) => inScope(input.scope, a, input.loginOf.get(a) ?? mailbox));

  const days = all.map((p) => {
    const day = emptyDay(p.date, p.phase);
    for (const [mailbox, m] of p.mailboxes) if (mailboxInScope(mailbox, m)) addMailbox(day, m);
    return day;
  });

  const served: CapacityDaily = {
    asOf: input.asOf.toISOString(),
    scope: input.scope,
    horizonDays: input.horizonDays,
    rampRules: {
      floorPerDay: RAMP_FLOOR_PER_DAY,
      growthFactor: RAMP_GROWTH_FACTOR,
      volumeWindowDays: RAMP_VOLUME_WINDOW_DAYS,
      ceilingPerDay: IN_PRODUCTION_DAILY_LIMIT,
      rule: `cap = min(operator limit, max(${RAMP_FLOOR_PER_DAY}, round(${RAMP_GROWTH_FACTOR} x volume sustained over the last ${RAMP_VOLUME_WINDOW_DAYS} days)))`,
    },
    days,
  };
  if (!input.groupBy) return served;

  const grain = input.groupBy;
  const series = new Map<string, CapacityDay[]>();
  all.forEach((p, i) => {
    for (const [mailbox, m] of p.mailboxes) {
      if (!mailboxInScope(mailbox, m)) continue;
      const keys =
        grain === "mailbox"
          ? [mailbox]
          : grain === "domain"
            ? [domainOf(mailbox)]
            : m.addresses.filter((a) => inScope(input.scope, a, input.loginOf.get(a) ?? mailbox));
      for (const key of keys) {
        const rows = series.get(key) ?? all.map((q) => emptyDay(q.date, q.phase));
        addMailbox(rows[i], grain === "account" ? { ...m, addresses: [key] } : m);
        series.set(key, rows);
      }
    }
  });
  return {
    ...served,
    groupBy: grain,
    additive: grain !== "account",
    entities: [...series.keys()].sort().map((key) => ({ key, days: series.get(key)! })),
  };
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

export async function readCapacityDaily(opts: {
  scope: EntityScope;
  groupBy: EntityGrain | null;
  horizonDays: number;
  asOf?: Date;
}): Promise<CapacityDaily> {
  const asOf = opts.asOf ?? new Date();
  const [addresses, first] = await Promise.all([
    db.execute(sql`SELECT lower(email) AS email, lower(mailbox_login) AS mailbox_login FROM instantly_accounts`),
    db.execute(sql`SELECT to_char(min(created_at), 'YYYY-MM-DD') AS first FROM instantly_account_lifecycle_events`),
  ]);
  const loginOf = new Map(
    rowsOf<{ email: string; mailbox_login: string | null }>(addresses).map((r) => [r.email, r.mailbox_login]),
  );
  const mailboxOfMap = new Map<string, string>();
  for (const [email, login] of loginOf) if (login) mailboxOfMap.set(email, login);
  const firstDay = rowsOf<{ first: string | null }>(first)[0]?.first ?? null;
  // Since inception (the first lifecycle event), capped by the history read.
  const days = firstDay
    ? Math.min(365, Math.floor((Date.parse(`${asOf.toISOString().slice(0, 10)}T00:00:00Z`) - Date.parse(`${firstDay}T00:00:00Z`)) / DAY_MS) + 1)
    : 1;
  // One reconstruction serves every scope and grain: cached apart from them.
  const history = await getOrSetCachedStats(
    `capacity-history-by-mailbox:${days}`,
    () => fetchCapacityHistoryByMailbox(days, mailboxOfMap),
    10 * 60 * 1000,
    { refreshAfterMs: 5 * 60 * 1000 },
  );
  return buildCapacityDaily({ asOf, history, horizonDays: opts.horizonDays, scope: opts.scope, groupBy: opts.groupBy, loginOf });
}
