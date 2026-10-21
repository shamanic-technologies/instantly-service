/**
 * Capacity-over-time (Gold, derived-on-read). Reconstructs the fleet's
 * `in_production` daily capacity for each of the last N days from the append-only
 * Bronze layers:
 *   - instantly_account_lifecycle_events → each account's STATUS as-of any day
 *     (the latest transition with created_at <= end-of-day).
 *   - each account's daily_limit as-of any day = the LATEST, at or before
 *     end-of-day, of an Instantly snapshot (`instantly_accounts_raw.fetched_at`)
 *     and a lifecycle flip's `daily_limit_after` (the limit reconcileLifecycle
 *     put in force). The snapshots stopped on 2026-10-12 (Instantly plan
 *     cancelled) while our own limit kept moving on flips: reading snapshots
 *     alone replayed the 10-12 limit on every later day (CAPACITY-FROZEN-LIMIT-
 *     1021; flips after an account's last snapshot backfilled by migration 0088
 *     from measured values). A day with neither is "limit unknown": counted in
 *     `limitUnknownAddresses`, contributing 0, never invented.
 *
 * ⚠️ For each day, `dailyCapacity` is what the fleet could REALLY have sent that
 * day, not the sum of the limits its accounts carried. The old form was
 * `Σ daily_limit` over the in_production pool, which overstated the fleet twice:
 * it ignored the RAMP (a mailbox is only offered `rampCapForVolume(what it has
 * been sending)`), and it counted per ADDRESS when several aliases share one
 * relay login and therefore ONE quota. Measured 2026-09-08, the live point read
 * **2,980/day** against roughly **1,100-1,400** once both were applied.
 *
 * So each day is summed over real MAILBOXES: the operator limit is the MINIMUM
 * across a mailbox's aliases, and the ramp reads the volume that mailbox had
 * sustained over the {@link RAMP_VOLUME_WINDOW_DAYS} days ending THAT day —
 * evaluated as-of the day, so the series shows the ramp climbing rather than
 * today's ramp painted over the past. `inProductionCount` stays an ACCOUNT
 * count: it answers "how many senders passed the gate", a different question
 * from "how much could go out".
 *
 * ⚠️ Warmup and seed-placement mail only exist from 2026-09-05, so days before
 * that carry outreach volume alone. Older points are therefore lower — that IS
 * what those days could send on the evidence that existed, not a gap to patch.
 *
 * Days are UTC calendar days. No fabrication — a day before an account's first
 * event contributes 0, and a day after the account vanished (`absent_since`)
 * contributes 0 too.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import {
  IN_PRODUCTION_DAILY_LIMIT,
  RAMP_VOLUME_WINDOW_DAYS,
  rampCapForVolume,
} from "./account-lifecycle";
import {
  dailyTotalsForMailbox,
  fetchRecentDailyVolume,
  sustainedOn,
} from "./recent-send-volume";

export interface CapacityHistoryPoint {
  /** YYYY-MM-DD (UTC). */
  date: string;
  inProductionCount: number;
  dailyCapacity: number;
}

/**
 * `days` is clamped to [1, 365]. Returns one point per UTC day, oldest first.
 *
 * `mailboxOf` maps a sending address to the login it authenticates as. An
 * address it does not know is its own mailbox — the Primeforge case, and the
 * safe reading: the account is never silently dropped from the total.
 */
export async function fetchCapacityHistory(
  days: number,
  mailboxOf: ReadonlyMap<string, string> = new Map(),
): Promise<CapacityHistoryPoint[]> {
  return (await fetchCapacityHistoryByMailbox(days, mailboxOf)).map((p) => ({
    date: p.date,
    inProductionCount: p.inProductionCount,
    dailyCapacity: [...p.mailboxes.values()].reduce((n, m) => n + m.capacity, 0),
  }));
}

/** One mailbox on one day of the capacity history. */
export interface MailboxCapacityOnDay {
  /** Its in_production addresses that day. */
  addresses: string[];
  /** The operator limit: the minimum known `daily_limit` across those addresses (0 when none is known). */
  operatorLimit: number;
  /** Its addresses whose limit that day is unknown (no snapshot, no flip): not invented. */
  limitUnknownAddresses: string[];
  /** Volume the mailbox had sustained over the ramp window ending that day. */
  sustained: number;
  /** What the ramp allowed on that volume (`rampCapForVolume`). */
  rampCap: number;
  /** min(operatorLimit, rampCap): what it could send that day. */
  capacity: number;
}

export interface CapacityHistoryByMailboxPoint {
  date: string;
  inProductionCount: number;
  mailboxes: Map<string, MailboxCapacityOnDay>;
}

/**
 * The same reconstruction, kept per real mailbox (MAILBOX-MONITORING-1019): the
 * monitoring read rolls it up to domain and fleet and drills into the mailbox.
 * `fetchCapacityHistory` is its fleet sum, so both serve ONE figure.
 */
export async function fetchCapacityHistoryByMailbox(
  days: number,
  mailboxOf: ReadonlyMap<string, string> = new Map(),
): Promise<CapacityHistoryByMailboxPoint[]> {
  const window = Math.max(1, Math.min(365, Math.floor(days)));
  // One extra ramp-window of volume, so the OLDEST point in the series is
  // measured against a full window rather than a truncated one.
  const volume = await fetchRecentDailyVolume(window + RAMP_VOLUME_WINDOW_DAYS);
  const result = await db.execute(sql`
    WITH days AS (
      SELECT generate_series(
        (CURRENT_DATE - (${window}::int - 1) * INTERVAL '1 day'),
        CURRENT_DATE,
        INTERVAL '1 day'
      )::date AS day
    ),
    accounts AS (
      SELECT DISTINCT account_email FROM instantly_account_lifecycle_events
    ),
    per AS (
      SELECT
        d.day,
        a.account_email,
        (SELECT e.to_status
           FROM instantly_account_lifecycle_events e
          WHERE e.account_email = a.account_email
            AND e.created_at < (d.day + INTERVAL '1 day')
          ORDER BY e.created_at DESC
          LIMIT 1) AS status,
        (SELECT u.daily_limit FROM (
           (SELECT r.daily_limit, r.fetched_at AS at
              FROM instantly_accounts_raw r
             WHERE r.account_email = a.account_email
               AND r.fetched_at < (d.day + INTERVAL '1 day')
             ORDER BY r.fetched_at DESC
             LIMIT 1)
           UNION ALL
           (SELECT f.daily_limit_after, f.created_at AS at
              FROM instantly_account_lifecycle_events f
             WHERE f.account_email = a.account_email
               AND f.daily_limit_after IS NOT NULL
               AND f.created_at < (d.day + INTERVAL '1 day')
             ORDER BY f.created_at DESC
             LIMIT 1)
         ) u ORDER BY u.at DESC LIMIT 1) AS daily_limit
      FROM days d CROSS JOIN accounts a
      -- An address that has left our estate (absent_since) stops counting from the
      -- day AFTER it vanished: its last lifecycle event still says in_production
      -- (reconcile only re-derives present accounts), so without this the 22
      -- cancelled DFY boxes read "in production" forever (ACCOUNTS-PROD-TABS-1021).
      WHERE NOT EXISTS (
        SELECT 1 FROM instantly_accounts ia
         WHERE lower(ia.email) = lower(a.account_email)
           AND ia.absent_since IS NOT NULL
           AND ia.absent_since < d.day
      )
    )
    SELECT
      to_char(day, 'YYYY-MM-DD') AS "date",
      account_email                AS "accountEmail",
      status                       AS "status",
      daily_limit                  AS "dailyLimit"
    FROM per
    WHERE status = 'in_production'
    ORDER BY day
  `);

  const rows: Record<string, unknown>[] = Array.isArray(result)
    ? (result as Record<string, unknown>[])
    : (((result as { rows?: unknown }).rows as Record<string, unknown>[]) ?? []);

  // Per-day totals for each real mailbox, computed once and reused for every
  // point — the alias grouping does not change from one day to the next.
  const dailyTotalsCache = new Map<string, Map<string, number>>();
  const addressesByMailbox = new Map<string, Set<string>>();
  const mailboxFor = (email: string) =>
    mailboxOf.get(email.trim().toLowerCase()) ?? email.trim().toLowerCase();
  for (const address of volume.keys()) {
    const mailbox = mailboxFor(address);
    const group = addressesByMailbox.get(mailbox) ?? new Set<string>();
    group.add(address);
    addressesByMailbox.set(mailbox, group);
  }

  const byDate = new Map<
    string,
    {
      accounts: number;
      limitByMailbox: Map<string, number | null>;
      unknownByMailbox: Map<string, string[]>;
      addressesByMailbox: Map<string, string[]>;
    }
  >();
  for (const row of rows) {
    const date = String(row.date);
    const email = String(row.accountEmail ?? "");
    if (!email) continue;
    const point = byDate.get(date) ?? {
      accounts: 0,
      limitByMailbox: new Map(),
      unknownByMailbox: new Map(),
      addressesByMailbox: new Map(),
    };
    point.accounts += 1;
    const mailbox = mailboxFor(email);
    // An address seen in the account history but never in the volume map still
    // belongs to its mailbox — otherwise its aliases would be invisible to the
    // grouping and the mailbox would be counted once per address.
    const group = addressesByMailbox.get(mailbox) ?? new Set<string>();
    group.add(email.trim().toLowerCase());
    addressesByMailbox.set(mailbox, group);
    const limit = row.dailyLimit === null || row.dailyLimit === undefined ? null : Number(row.dailyLimit);
    const known = point.limitByMailbox.get(mailbox) ?? null;
    point.limitByMailbox.set(mailbox, limit === null ? known : known === null ? limit : Math.min(known, limit));
    if (limit === null) {
      point.unknownByMailbox.set(mailbox, [...(point.unknownByMailbox.get(mailbox) ?? []), email.trim().toLowerCase()]);
    }
    point.addressesByMailbox.set(mailbox, [...(point.addressesByMailbox.get(mailbox) ?? []), email.trim().toLowerCase()]);
    byDate.set(date, point);
  }

  const totalsFor = (mailbox: string) => {
    const cached = dailyTotalsCache.get(mailbox);
    if (cached !== undefined) return cached;
    const totals = dailyTotalsForMailbox(volume, addressesByMailbox.get(mailbox) ?? []);
    dailyTotalsCache.set(mailbox, totals);
    return totals;
  };

  // ⚠️ Emit EVERY day in the window, not only the days that had a production
  // account. The row-per-account query returns nothing for a day the fleet was
  // entirely out of production, and dropping those would leave holes in the
  // series that read as missing data rather than as the zero they are.
  const series: CapacityHistoryByMailboxPoint[] = [];
  const today = Date.parse(`${new Date().toISOString().slice(0, 10)}T00:00:00Z`);
  for (let i = window - 1; i >= 0; i -= 1) {
    const date = new Date(today - i * 86_400_000).toISOString().slice(0, 10);
    const point = byDate.get(date);
    if (point === undefined) {
      series.push({ date, inProductionCount: 0, mailboxes: new Map() });
      continue;
    }
    const mailboxes = new Map<string, MailboxCapacityOnDay>();
    for (const [mailbox, knownLimit] of point.limitByMailbox) {
      const limit = knownLimit ?? 0;
      const sustained = sustainedOn(totalsFor(mailbox), date);
      const rampCap = rampCapForVolume(sustained, IN_PRODUCTION_DAILY_LIMIT);
      mailboxes.set(mailbox, {
        addresses: (point.addressesByMailbox.get(mailbox) ?? []).sort(),
        operatorLimit: limit,
        limitUnknownAddresses: (point.unknownByMailbox.get(mailbox) ?? []).sort(),
        sustained,
        rampCap,
        capacity: Math.min(limit, rampCap),
      });
    }
    series.push({ date, inProductionCount: point.accounts, mailboxes });
  }
  return series;
}
