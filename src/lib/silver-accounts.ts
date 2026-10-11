/**
 * The account list, read from OUR tables instead of a live Instantly
 * `GET /accounts`.
 *
 * The Instantly Email Outreach plan is cancelled (2026-10-12): its API may stop
 * answering, and the ops table and the sending forecast must not go down with
 * it. Each account is its LAST snapshot payload (`instantly_accounts_raw`,
 * written by accounts-sync while Instantly answers), with the two fields WE own
 * laid over it from silver:
 *
 *   - `daily_limit`: for a mailbox we send from ourselves, the reconcile writes
 *     it locally on a lifecycle flip (no Instantly PATCH exists any more), so
 *     the snapshot copy can be stale;
 *   - `status`: silver's `instantly_status`.
 *
 * Only rows still present (`absent_since IS NULL`) are returned, the same set
 * `GET /accounts` would list. Fails loud on a DB error.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import type { Account } from "./instantly-client";

function rowsOf(result: unknown): Record<string, unknown>[] {
  return ((result as { rows?: Record<string, unknown>[] }).rows ?? []) as Record<string, unknown>[];
}

/** Pure: lay silver's own columns over the last snapshot payload. */
export function accountFromSilver(row: {
  email: string;
  payload: Record<string, unknown> | null;
  dailyLimit: number | null;
  instantlyStatus: number | null;
}): Account {
  const base = (row.payload ?? {}) as Partial<Account>;
  return {
    ...base,
    email: row.email,
    warmup_status: base.warmup_status ?? 0,
    status: row.instantlyStatus ?? base.status ?? 0,
    ...(row.dailyLimit === null ? {} : { daily_limit: row.dailyLimit }),
  } as Account;
}

export async function listAccountsFromSilver(): Promise<Account[]> {
  const result = await db.execute(sql`
    SELECT a.email            AS "email",
           a.daily_limit      AS "dailyLimit",
           a.instantly_status AS "instantlyStatus",
           r.payload          AS "payload"
    FROM instantly_accounts a
    LEFT JOIN LATERAL (
      SELECT payload FROM instantly_accounts_raw r
      WHERE r.account_email = a.email
      ORDER BY r.fetched_at DESC
      LIMIT 1
    ) r ON true
    WHERE a.absent_since IS NULL
    ORDER BY a.email
  `);
  return rowsOf(result).map((r) =>
    accountFromSilver({
      email: String(r.email),
      payload: (r.payload ?? null) as Record<string, unknown> | null,
      dailyLimit: r.dailyLimit === null || r.dailyLimit === undefined ? null : Number(r.dailyLimit),
      instantlyStatus:
        r.instantlyStatus === null || r.instantlyStatus === undefined ? null : Number(r.instantlyStatus),
    }),
  );
}
