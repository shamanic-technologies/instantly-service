/**
 * What each mailbox has ACTUALLY been sending — the input the daily cap ramps on.
 *
 * ⚠️ This exists because the ramp used to key on TIME and the time it keyed on
 * was resettable. `capForAccount` scaled the limit by how long ago the account
 * last changed lifecycle state; the weekly placement test moves the delivery
 * score, the score moves the state, and the state reset the counter — so the
 * ramp was rewound every Saturday and never finished. Measured in prod
 * 2026-09-07: 197 of 197 self-send mailboxes anchored on a lifecycle flip, an
 * average apparent age of 21 days against a real age of 102, the whole fleet
 * pinned at a cap of 5-13, and 1,208 sequences waiting to send a first email.
 *
 * Volume cannot be rewound by a state transition, because it is a measured fact
 * about what left the building.
 *
 * One loader, one number per sending address, so every consumer of the cap
 * (assignment, dispatch, warmup, the Instantly limit sync, the ops table) reads
 * the same figure and none of them re-derives it.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { RAMP_VOLUME_WINDOW_DAYS } from "./account-lifecycle";

/** account address → UTC day (`YYYY-MM-DD`) → messages that address sent. */
export type DailyVolume = Map<string, Map<string, number>>;

/** `db.execute` resolves a QueryResult on node-postgres, never a bare array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * What each sending address sent, per UTC day, over the last
 * {@link RAMP_VOLUME_WINDOW_DAYS} days.
 *
 * Per DAY rather than a single peak, because the quota belongs to the relay
 * login and several aliases share one: only per-day totals let a caller holding
 * the alias map compute the MAILBOX's real figure. See {@link sustainedForMailbox}.
 *
 * ⚠️ All three sources count, and leaving any of them out breaks the ramp for
 * the mailboxes that need it most. Gmail's per-user quota does not care which
 * of our jobs put a message on the wire, and a mailbox in recovery sends NO
 * outreach at all — its warmup and placement-test traffic is the only volume it
 * has, so counting outreach alone would pin it at the floor for good and it
 * could never arrive in production at a usable rate.
 *
 * A WINDOW rather than yesterday alone: the fleet does not send at weekends, so
 * reading yesterday on a Monday sees Sunday's zero and resets every mailbox —
 * the same weekly rewind this change removes. An address absent from the result
 * has sent nothing; the caller reads that as 0, which the ramp floors at
 * `RAMP_FLOOR_PER_DAY`.
 */
export async function fetchRecentDailyVolume(
  windowDays: number = RAMP_VOLUME_WINDOW_DAYS,
): Promise<DailyVolume> {
  const since = sql.raw(`now() - interval '${Math.max(1, Math.floor(windowDays))} days'`);

  const result = await db.execute(sql`
    WITH per_day AS (
      SELECT account_email, day, SUM(n) AS n
      FROM (
        SELECT e.account_email                        AS account_email,
               date_trunc('day', e.timestamp)         AS day,
               COUNT(*)                               AS n
          FROM instantly_events e
         WHERE e.event_type = 'email_sent'
           AND e.inferred = false
           AND e.account_email IS NOT NULL
           AND e.timestamp >= ${since}
         GROUP BY 1, 2
        UNION ALL
        SELECT w.sender_email,
               date_trunc('day', w.dispatched_at),
               COUNT(*)
          FROM warmup_dispatches w
         WHERE w.outcome = 'sent'
           AND w.dispatched_at >= ${since}
         GROUP BY 1, 2
        UNION ALL
        SELECT s.sender_email,
               date_trunc('day', s.dispatched_at),
               COUNT(*)
          FROM seed_placement_dispatches s
         WHERE s.outcome = 'sent'
           AND s.dispatched_at >= ${since}
         GROUP BY 1, 2
      ) sources
      GROUP BY 1, 2
    )
    SELECT account_email AS "accountEmail",
           to_char(day, 'YYYY-MM-DD') AS "day",
           n::int AS "n"
      FROM per_day
  `);

  const volume: DailyVolume = new Map();
  for (const row of rowsOf(result)) {
    const email = String(row.accountEmail ?? "").trim().toLowerCase();
    const day = String(row.day ?? "");
    if (!email || !day) continue;
    const days = volume.get(email) ?? new Map<string, number>();
    days.set(day, (days.get(day) ?? 0) + Number(row.n ?? 0));
    volume.set(email, days);
  }
  return volume;
}

/**
 * The SECOND-highest of a set of daily totals, or 0 when there are fewer than two.
 *
 * ⚠️ The single highest day is the wrong statistic, and prod says so plainly:
 * the weekly seed placement test dispatches its whole fleet in ONE burst (1,780
 * seeds on 2026-09-05), so every mailbox's peak day is that burst. Measured at
 * mailbox grain, the highest day averaged 78 and the second 51 — reading the
 * peak would have handed 41 of 43 mailboxes their full cap on the strength of
 * one artificial day, which is exactly the 0-to-50 pattern the ramp exists to
 * prevent, arriving through the back door.
 *
 * The second-highest is the cheapest robust answer: a mailbox that reached a
 * volume TWICE has sustained it, one that spiked once has not. A mailbox with a
 * single day of history reads 0 and starts at the floor, then climbs from there
 * as it uses the room — so it is self-starting, not trapped.
 */
function secondHighest(values: Iterable<number>): number {
  let first = 0;
  let second = 0;
  for (const n of values) {
    if (n > first) {
      second = first;
      first = n;
    } else if (n > second) {
      second = n;
    }
  }
  return second;
}

/**
 * The volume this ADDRESS has sustained, or 0 when it has barely sent.
 *
 * For a caller that does not know which addresses share a mailbox. It
 * under-states an alias fleet, which is the safe direction.
 */
export function sustainedFor(volume: DailyVolume, accountEmail: string): number {
  const days = volume.get(accountEmail.trim().toLowerCase());
  return days === undefined ? 0 : secondHighest(days.values());
}

/**
 * The volume a real MAILBOX has sustained — over its DAILY TOTALS across every
 * address that authenticates as it.
 *
 * ⚠️ Summing each alias's own figure instead would OVER-state the mailbox
 * whenever two aliases were busy on different days, and over-stating is the one
 * direction a quota ramp must not err in — it is what the relay answers with
 * `450 4.7.1 Too many mail per day for sasl <user>`, per SASL user and not per
 * alias. Per-day totals first, then the statistic.
 */
export function sustainedForMailbox(
  volume: DailyVolume,
  addresses: Iterable<string>,
): number {
  const byDay = new Map<string, number>();
  for (const address of addresses) {
    const days = volume.get(address.trim().toLowerCase());
    if (days === undefined) continue;
    for (const [day, n] of days) byDay.set(day, (byDay.get(day) ?? 0) + n);
  }
  return secondHighest(byDay.values());
}

/**
 * The volume a set of daily totals sustained over the {@link RAMP_VOLUME_WINDOW_DAYS}
 * days ENDING on `endDay` (inclusive) — the same statistic {@link sustainedFor}
 * applies to today, evaluated at an arbitrary past day.
 *
 * Exists so the capacity-over-time series can show what each day's cap ACTUALLY
 * was rather than the limits the accounts happened to carry. A day with fewer
 * than two days of history behind it reads 0, which the ramp floors — the honest
 * answer for a mailbox nobody had measured yet.
 */
export function sustainedOn(
  byDay: ReadonlyMap<string, number>,
  endDay: string,
  windowDays: number = RAMP_VOLUME_WINDOW_DAYS,
): number {
  const end = Date.parse(`${endDay}T00:00:00Z`);
  if (Number.isNaN(end)) return 0;
  const start = end - (Math.max(1, Math.floor(windowDays)) - 1) * 86_400_000;
  const inWindow: number[] = [];
  for (const [day, n] of byDay) {
    const at = Date.parse(`${day}T00:00:00Z`);
    if (!Number.isNaN(at) && at >= start && at <= end) inWindow.push(n);
  }
  return secondHighest(inWindow);
}

/** Per-day totals for one real MAILBOX, summing every address that authenticates as it. */
export function dailyTotalsForMailbox(
  volume: DailyVolume,
  addresses: Iterable<string>,
): Map<string, number> {
  const byDay = new Map<string, number>();
  for (const address of addresses) {
    const days = volume.get(address.trim().toLowerCase());
    if (days === undefined) continue;
    for (const [day, n] of days) byDay.set(day, (byDay.get(day) ?? 0) + n);
  }
  return byDay;
}
