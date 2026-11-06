/**
 * The domain policy (`instantly_domain_policy`) as a TAG every ops row can carry
 * (SENDING-INFRA-AUDIT-1106 item 14): a domain pulled out of cold outreach used to
 * read "deactivated" like any other, with nothing about the plan to cancel it.
 *
 * `retiring` rows are domains the owner CANCELLED at the vendor (owner 2026-11-06:
 * Primeforge removes them at the end of the current Google Workspace month), so they
 * are served as `state: "cancelled"` with `removedAt` = the first instant of next
 * month (UTC). `brand` rows are our own brand/product domains, never cold.
 */
import { sql } from "drizzle-orm";
import { db } from "../../db";

export interface DomainExclusion {
  /** The policy row's reason as stored: `brand` | `retiring` | any future value. */
  reason: string;
  /** The policy row's note as stored (who/why/when), null when none. */
  note: string | null;
  /** What a person reads: `cancelled` (retiring: cancelled at the vendor), `brand` (never cold), else the reason. */
  state: string;
  /** When the vendor removes it (cancelled only): the end of the current month, ISO; null otherwise. */
  removedAt: string | null;
}

/** First instant of the month after `asOf`, UTC. */
export function endOfMonthUtc(asOf: Date): string {
  return new Date(Date.UTC(asOf.getUTCFullYear(), asOf.getUTCMonth() + 1, 1)).toISOString();
}

export function toExclusion(reason: string, note: string | null, asOf: Date): DomainExclusion {
  const cancelled = reason === "retiring";
  return { reason, note, state: cancelled ? "cancelled" : reason, removedAt: cancelled ? endOfMonthUtc(asOf) : null };
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

/** domain (lower-cased) -> its exclusion. */
export async function loadDomainExclusions(asOf: Date = new Date()): Promise<Map<string, DomainExclusion>> {
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`SELECT lower(domain) AS domain, reason, note FROM instantly_domain_policy`),
  );
  return new Map(
    rows.map((r) => [String(r.domain), toExclusion(String(r.reason), r.note === null || r.note === undefined ? null : String(r.note), asOf)]),
  );
}

/**
 * Addresses on a CANCELLED domain whose mailbox owes no prospect step any more:
 * nothing left to protect, so no warmup and no placement test is spent on them
 * (1,214 warmup + 257 tests in 7 days against 88 prospect emails on 2026-11-06).
 * Lower-cased. A mailbox still draining follow-ups keeps both until it is done.
 */
export async function loadDrainedCancelledAddresses(): Promise<Set<string>> {
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT lower(a.email) AS email
      FROM instantly_accounts a
      JOIN instantly_domain_policy p ON lower(p.domain) = lower(split_part(a.email, '@', 2))
      WHERE p.reason = 'retiring'
        AND NOT EXISTS (
          SELECT 1
          FROM instantly_accounts sib
          JOIN instantly_campaigns c ON lower(c.account_email) = lower(sib.email)
          JOIN sequence_costs sc ON sc.instantly_campaign_id = c.instantly_campaign_id
          WHERE COALESCE(sib.mailbox_login, lower(sib.email)) = COALESCE(a.mailbox_login, lower(a.email))
            AND c.status = 'active'
            AND sc.status = 'provisioned'
        )
    `),
  );
  return new Set(rows.map((r) => String(r.email)));
}
