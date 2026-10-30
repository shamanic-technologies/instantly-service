/**
 * The send pool per day: how many mailboxes the sender could hand a NEW email
 * to, per send pool, stored as a daily series (SEND-POOL-VENDOR-402-1030).
 *
 * 2026-10-29 15:00 UTC → 2026-10-30 18:30 UTC every org's new emails were
 * refused ("No active Instantly accounts available") while Monitoring read "35
 * in production": nothing measured the pool the sender actually draws from.
 * This samples it through the sender's OWN selection (`fetchSendablePool`), 60 s
 * after boot then every 15 min, and keeps per UTC day and pool:
 *   - `sendableMin`: the day's lowest sample, so a few hours at 0 still read 0;
 *   - `sendableLast` + `byVendor`: the latest sample.
 * Served as `sendPoolHistory` on `GET /internal/ops/infra` (red on Monitoring
 * when a pool hits 0). A day with no sample is null, never a fake 0.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { errorMessage } from "./db-error.js";
import { fetchSendablePool } from "./send-lead";
import type { CallerInfo } from "./key-client";

export const SEND_POOL_SHARED = "shared";
export const SEND_POOL_HISTORY_DAYS = 90;
export const SEND_POOL_SAMPLE_INTERVAL_MS = 15 * 60 * 1000;
export const SEND_POOL_FIRST_SAMPLE_MS = 60 * 1000;

export interface SendPoolNow {
  pool: string;
  sendable: number;
  byVendor: Record<string, number>;
}

function rowsOf<T>(result: unknown): T[] {
  const r = result as { rows?: T[] } | T[];
  return Array.isArray(r) ? r : (r.rows ?? []);
}

/** Pure: sendable mailboxes per vendor (`null` vendor = no inventory row). */
export function countByVendor(accounts: readonly { infraProvider: string | null }[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const a of accounts) {
    const k = a.infraProvider ?? "unclassified";
    out[k] = (out[k] ?? 0) + 1;
  }
  return out;
}

/** One row per send pool now: the shared pool plus every reserved feature slug. */
export async function loadSendPools(caller: CallerInfo): Promise<SendPoolNow[]> {
  const reserved = rowsOf<{ slug: string }>(
    await db.execute(sql`SELECT DISTINCT feature_slug AS slug FROM instantly_account_feature_policy ORDER BY 1`),
  ).map((r) => String(r.slug));
  return Promise.all(
    [null, ...reserved].map(async (slug) => {
      const pool = await fetchSendablePool(slug, caller);
      return { pool: slug ?? SEND_POOL_SHARED, sendable: pool.length, byVendor: countByVendor(pool) };
    }),
  );
}

export async function recordSendPools(pools: readonly SendPoolNow[], asOf: Date): Promise<void> {
  const day = asOf.toISOString().slice(0, 10);
  for (const p of pools) {
    await db.execute(sql`
      INSERT INTO send_pool_daily (day, pool, sendable_min, sendable_last, by_vendor, samples, updated_at)
      VALUES (${day}::date, ${p.pool}, ${p.sendable}, ${p.sendable}, ${JSON.stringify(p.byVendor)}::jsonb, 1, now())
      ON CONFLICT (day, pool) DO UPDATE SET
        sendable_min = LEAST(send_pool_daily.sendable_min, EXCLUDED.sendable_min),
        sendable_last = EXCLUDED.sendable_last,
        by_vendor = EXCLUDED.by_vendor,
        samples = send_pool_daily.samples + 1,
        updated_at = EXCLUDED.updated_at
    `);
  }
}

export interface SendPoolDay {
  day: string;
  /** Null = no sample that day (never a fake 0). */
  sendableMin: number | null;
  sendableLast: number | null;
  byVendor: Record<string, number> | null;
}

export interface SendPoolHistory {
  pool: string;
  days: SendPoolDay[];
}

interface StoredRow {
  day: string;
  pool: string;
  sendableMin: unknown;
  sendableLast: unknown;
  byVendor: unknown;
}

/** Pure: one series per pool seen in the window, every day emitted, oldest first. */
export function buildSendPoolHistory(rows: readonly StoredRow[], fromDay: string, toDay: string): SendPoolHistory[] {
  const pools = [...new Set(rows.map((r) => r.pool))].sort((a, b) =>
    a === SEND_POOL_SHARED ? -1 : b === SEND_POOL_SHARED ? 1 : a.localeCompare(b),
  );
  const key = (pool: string, day: string) => `${pool}|${day}`;
  const byKey = new Map(rows.map((r) => [key(r.pool, r.day), r]));
  return pools.map((pool) => {
    const days: SendPoolDay[] = [];
    for (let t = Date.parse(`${fromDay}T00:00:00Z`); t <= Date.parse(`${toDay}T00:00:00Z`); t += 86_400_000) {
      const day = new Date(t).toISOString().slice(0, 10);
      const r = byKey.get(key(pool, day));
      days.push(
        r
          ? {
              day,
              sendableMin: Number(r.sendableMin),
              sendableLast: Number(r.sendableLast),
              byVendor: Object.fromEntries(
                Object.entries((r.byVendor ?? {}) as Record<string, unknown>).map(([k, v]) => [k, Number(v)]),
              ),
            }
          : { day, sendableMin: null, sendableLast: null, byVendor: null },
      );
    }
    return { pool, days };
  });
}

export async function loadSendPoolHistory(
  days: number = SEND_POOL_HISTORY_DAYS,
  asOf: Date = new Date(),
): Promise<SendPoolHistory[]> {
  const toDay = asOf.toISOString().slice(0, 10);
  const fromDay = new Date(Date.parse(`${toDay}T00:00:00Z`) - (days - 1) * 86_400_000).toISOString().slice(0, 10);
  const rows = rowsOf<StoredRow>(
    await db.execute(sql`
      SELECT to_char(day, 'YYYY-MM-DD') AS day, pool,
             sendable_min AS "sendableMin", sendable_last AS "sendableLast", by_vendor AS "byVendor"
      FROM send_pool_daily
      WHERE day >= ${fromDay}::date AND day <= ${toDay}::date
    `),
  );
  return buildSendPoolHistory(rows, fromDay, toDay);
}

// ─── In-process sampler ──────────────────────────────────────────────────────

const CALLER: CallerInfo = { method: "POST", path: "/internal/send-pool-daily" };
let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function sendPoolSampleTick(): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const pools = await loadSendPools(CALLER);
      await recordSendPools(pools, new Date());
      console.log(`[instantly-service] send-pool-daily: ${JSON.stringify(pools.map((p) => [p.pool, p.sendable]))}`);
    } catch (error: unknown) {
      console.error(`[instantly-service] send-pool-daily: sample FAILED: ${errorMessage(error)}`);
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startSendPoolDailyWorker(): void {
  if (timer) return;
  setTimeout(() => void sendPoolSampleTick(), SEND_POOL_FIRST_SAMPLE_MS).unref?.();
  timer = setInterval(() => void sendPoolSampleTick(), SEND_POOL_SAMPLE_INTERVAL_MS);
  timer.unref?.();
}
