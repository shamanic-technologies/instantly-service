/**
 * Which of these addresses has bounced on one of OUR sends — fleet-wide.
 *
 * A bounce is a fact about the ADDRESS, not about the sender: the mailbox does
 * not exist (or refuses mail) whoever writes to it. So unlike an opt-out, which
 * is consent given to one org and is read per org, this read is NOT org-scoped.
 * The consumer is human-service's serve path, which must never hand lead-service
 * a person our own sends already proved unreachable, under any org or brand
 * (human-service#73: 275 addresses re-served after a recorded bounce).
 *
 * ⚠️ READ FROM SILVER (`instantly_events`), NOT GOLD. The house rule elsewhere is
 * to read gold, because gold carries precedence logic (manual statements,
 * withdrawals, reply-kind resolution) a fresh aggregate would re-implement. A
 * bounce has none: gold's `bounced` is a bare `BOOL_OR(event_type =
 * 'email_bounced')` over these same rows. Silver is the better source on both
 * remaining axes: it keeps bounces on org-less platform sends that gold never
 * projects (29 addresses in prod on 2026-10-03), and `instantly_events_lead_email_idx`
 * serves the lookup in ~0.1 ms where gold has no `lead_email`-leading index and
 * seq-scans (~55 ms) on every serve.
 *
 * HARD vs SOFT. No column distinguishes them, and none needs to: an
 * `email_bounced` event here is already a PERMANENT failure. The self-send
 * poller promotes only a permanent DSN (`classifyPermanentFailure`,
 * `isTransientDeliveryReport`), the delayed-DSN backfill deleted every event a
 * temporary delay had produced, and Instantly's own `email_bounced` webhook is
 * its hard-bounce signal. So every row is treated as a hard bounce.
 *
 * Addresses are stored lowercased and trimmed (0 exceptions in prod), and the
 * input is normalized the same way before the lookup.
 *
 * Fails loud: the route answers a DB error with a 500, never an empty list —
 * an empty list is exactly "nobody bounced", the wrong answer that looks right.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";

export interface BouncedEmail {
  email: string;
  firstBouncedAt: string;
}

/** node-postgres resolves `db.execute` to a QueryResult object, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export function normalizeBounceEmail(email: string): string {
  return email.trim().toLowerCase();
}

export async function findBouncedEmails(emails: string[]): Promise<BouncedEmail[]> {
  const normalized = [
    ...new Set(emails.map(normalizeBounceEmail).filter((e) => e.length > 0)),
  ];
  if (normalized.length === 0) return [];

  const result = await db.execute(sql`
    SELECT lead_email AS email, MIN("timestamp") AS first_bounced_at
    FROM instantly_events
    WHERE event_type = 'email_bounced'
      AND lead_email = ANY(${sql.param(normalized)}::text[])
    GROUP BY lead_email
  `);
  return rowsOf(result).map((r) => ({
    email: String(r.email),
    firstBouncedAt: new Date(r.first_bounced_at as string | Date).toISOString(),
  }));
}
