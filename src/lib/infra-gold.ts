/**
 * Gold reads over the provider inventory (issue #555, PR 2).
 *
 * IO only — every derivation lives in `infra-pricing.ts` so it can be unit
 * tested without a database. Silver is read, never written.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import type { InventoryDomain, PriceRate } from "./infra-pricing";

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function toDate(value: unknown): Date | null {
  if (!value) return null;
  const parsed = new Date(value as string);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

/**
 * Every (provider, domain) row, joined with what the fleet does on it.
 *
 * Three joins deserve a note:
 * - Mailboxes are counted from what the VENDOR reports, not from Instantly. The
 *   two genuinely differ: the legacy Gandi domains run dozens of Instantly
 *   accounts against a single Gandi mailbox, because the mail is relayed. That
 *   difference is a finding, not a bug to paper over.
 * - Instantly accounts exclude ghosts (`absent_since`), so a deleted mailbox
 *   stops counting the moment the accounts-sync notices.
 * - Sends count REAL dispatches only (`inferred = false`) over a trailing 30
 *   days, which is the denominator for cost-per-email.
 */
export async function loadInventoryDomains(): Promise<InventoryDomain[]> {
  const result = await db.execute(sql`
    WITH vendor_mailboxes AS (
      SELECT provider, domain, count(*)::int AS mailbox_count
        FROM infra_mailboxes
       WHERE absent_since IS NULL
       GROUP BY provider, domain
    ),
    sending_accounts AS (
      SELECT split_part(email, '@', 2) AS domain,
             count(*)::int AS account_count,
             count(*) FILTER (WHERE lifecycle_status = 'in_production')::int AS in_production_count
        FROM instantly_accounts
       WHERE absent_since IS NULL
       GROUP BY 1
    ),
    recent_sends AS (
      SELECT split_part(account_email, '@', 2) AS domain, count(*)::int AS sent_last_30d
        FROM instantly_events
       WHERE event_type = 'email_sent'
         AND inferred = false
         AND account_email IS NOT NULL
         AND timestamp >= now() - interval '30 days'
       GROUP BY 1
    )
    SELECT d.provider,
           d.domain,
           d.role,
           d.status,
           d.expires_at,
           d.autorenew,
           d.deletion_scheduled,
           d.cancelled_at,
           d.absent_since,
           d.price_cents,
           d.price_currency,
           COALESCE(vm.mailbox_count, 0)       AS mailbox_count,
           COALESCE(sa.account_count, 0)       AS account_count,
           COALESCE(sa.in_production_count, 0) AS in_production_count,
           COALESCE(rs.sent_last_30d, 0)       AS sent_last_30d
      FROM infra_domains d
      LEFT JOIN vendor_mailboxes vm ON vm.provider = d.provider AND vm.domain = d.domain
      LEFT JOIN sending_accounts sa ON sa.domain = d.domain
      LEFT JOIN recent_sends rs     ON rs.domain = d.domain
     ORDER BY d.domain, d.provider
  `);

  return rowsOf<Record<string, unknown>>(result).map((row) => ({
    provider: String(row.provider),
    domain: String(row.domain),
    role: String(row.role),
    status: row.status === null || row.status === undefined ? null : String(row.status),
    expiresAt: toDate(row.expires_at),
    autorenew: row.autorenew === null || row.autorenew === undefined ? null : Boolean(row.autorenew),
    deletionScheduled: Boolean(row.deletion_scheduled),
    cancelledAt: toDate(row.cancelled_at),
    absentSince: toDate(row.absent_since),
    priceCents: row.price_cents === null || row.price_cents === undefined ? null : Number(row.price_cents),
    priceCurrency:
      row.price_currency === null || row.price_currency === undefined ? null : String(row.price_currency),
    mailboxCount: Number(row.mailbox_count ?? 0),
    instantlyAccountCount: Number(row.account_count ?? 0),
    inProductionCount: Number(row.in_production_count ?? 0),
    sentLast30d: Number(row.sent_last_30d ?? 0),
  }));
}

/**
 * The rate card, reduced to the row in force TODAY per key. A rate change adds a
 * row rather than editing one, so past figures stay reproducible — this read is
 * what makes that non-retroactive storage usable.
 */
export async function loadEffectiveRates(asOf: Date): Promise<PriceRate[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT ON (provider, scope, item)
           provider, scope, item, unit_cents, currency, source, note
      FROM infra_price_rates
     WHERE effective_from <= ${asOf}
     ORDER BY provider, scope, item, effective_from DESC
  `);

  return rowsOf<Record<string, unknown>>(result).map((row) => ({
    provider: String(row.provider),
    scope: String(row.scope) as PriceRate["scope"],
    item: String(row.item ?? ""),
    unitCents: Number(row.unit_cents),
    currency: String(row.currency),
    source: String(row.source) as PriceRate["source"],
    note: row.note === null || row.note === undefined ? null : String(row.note),
  }));
}
