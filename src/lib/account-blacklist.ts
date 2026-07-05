/**
 * Staff manual per-account blacklist ("rest an account") — IO glue for the
 * account-health toggle (POST /internal/audit/account-blacklist).
 *
 * The manual flag lives on `instantly_accounts.manually_blacklisted` (keyed on
 * email). It is threaded into `classifyAccountBlock` (as the highest-precedence
 * "manual" reason) so the live send gate (`filterHealthyAccounts`) excludes a
 * blacklisted account from NEW sends AND the account-health audit view reports
 * it `blocked / blockReason:"manual"` — one source of truth, no divergence.
 *
 * Blacklisting does NOT touch the account's Instantly `daily_limit` (max send),
 * so already-queued emails keep draining; it only raises the WARMUP daily volume
 * (see BLACKLIST_WARMUP_DAILY_LIMIT) to recover reputation. Re-allowing restores
 * the warmup volume (ALLOWED_WARMUP_DAILY_LIMIT) and clears the flag.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { instantlyAccounts } from "../db/schema";

/** Warmup daily send volume when an account is manually blacklisted (warm harder). */
export const BLACKLIST_WARMUP_DAILY_LIMIT = 50;

/** Warmup daily send volume restored when an account is re-allowed. */
export const ALLOWED_WARMUP_DAILY_LIMIT = 10;

interface EmailRow {
  email: string;
}

function rowsOf(result: unknown): EmailRow[] {
  if (!result) return [];
  return Array.isArray(result)
    ? (result as EmailRow[])
    : (((result as { rows?: EmailRow[] }).rows) ?? []);
}

/**
 * The set of account emails staff have manually blacklisted
 * (`manually_blacklisted = true`). Loaded once per send / audit call and passed
 * into `classifyAccountBlock` / `filterHealthyAccounts` / `buildAccountHealth`.
 * Empty set when none — never fabricated.
 */
export async function fetchManuallyBlacklistedEmails(): Promise<Set<string>> {
  const result = await db.execute(
    sql`SELECT email FROM instantly_accounts WHERE manually_blacklisted = true`,
  );
  return new Set(rowsOf(result).map((r) => r.email).filter(Boolean));
}

/**
 * Upsert the manual-blacklist flag on the account row (keyed by email). The row
 * may not exist yet (accounts live in Instantly; `instantly_accounts` is a
 * sparse local mirror) — insert with defaults on first toggle, update otherwise.
 * `manually_blacklisted_at` is stamped when blacklisting, cleared when allowing.
 */
export async function setAccountManualBlacklist(
  email: string,
  blacklisted: boolean,
  now: Date = new Date(),
): Promise<void> {
  const at = blacklisted ? now : null;
  await db
    .insert(instantlyAccounts)
    .values({ email, manuallyBlacklisted: blacklisted, manuallyBlacklistedAt: at })
    .onConflictDoUpdate({
      target: instantlyAccounts.email,
      set: { manuallyBlacklisted: blacklisted, manuallyBlacklistedAt: at, updatedAt: now },
    });
}
