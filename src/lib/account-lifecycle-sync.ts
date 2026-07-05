/**
 * Per-account LIFECYCLE — IO glue (Bronze/Silver/Gold).
 *
 * Pure derivation lives in lib/account-lifecycle.ts. This module does the IO:
 *   - snapshotAccounts     — Bronze: full Instantly GET /accounts snapshot +
 *                            Silver: upsert the health columns + name.
 *   - reconcileLifecycle   — Gold: recompute each account's lifecycle_status from
 *                            (silver health + latest placement delivery +
 *                            domain_policy); on a CHANGE, write a lifecycle event,
 *                            update silver, and PATCH the Instantly warmup. Runs
 *                            after the accounts-sync AND after a placement sync.
 *                            Idempotent (writes an event ONLY on an actual change).
 *   - fetchInProductionAccounts — Silver read for the live send gate.
 *   - fetchLifecycleByEmail     — Silver read for account-health + sending-forecast.
 *   - fetchTestablePoolEmails   — Silver read for placement-test seeding.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import {
  instantlyAccounts,
  instantlyAccountsRaw,
  instantlyAccountLifecycleEvents,
} from "../db/schema";
import { listAccounts, setWarmupDailyLimit, type Account } from "./instantly-client";
import {
  deriveLifecycle,
  warmupDailyForStatus,
  emailDomain,
  isDeliveryFull,
  type LifecycleStatus,
  type LifecycleReason,
} from "./account-lifecycle";

function rowsOf<T = Record<string, unknown>>(result: unknown): T[] {
  if (!result) return [];
  return Array.isArray(result)
    ? (result as T[])
    : (((result as { rows?: T[] }).rows) ?? []);
}

// ─── Bronze + Silver: accounts snapshot ─────────────────────────────────────

export interface SnapshotSummary {
  synced: number;
}

/**
 * Full snapshot of Instantly GET /accounts → Bronze (append-only history) +
 * Silver (upsert current health cols + name). Read-only against Instantly (spends
 * no quota). Fails loud on any Instantly error.
 */
export async function snapshotAccounts(apiKey: string): Promise<SnapshotSummary> {
  const accounts = await listAccounts(apiKey);
  const now = new Date();

  for (const a of accounts) {
    // Bronze: one immutable row per (account, fetch).
    await db.insert(instantlyAccountsRaw).values({
      accountEmail: a.email,
      status: a.status,
      warmupScore: a.stat_warmup_score ?? null,
      dailyLimit: a.daily_limit ?? null,
      providerCode: a.provider_code ?? null,
      payload: a as unknown as Record<string, unknown>,
      fetchedAt: now,
    });

    // Silver: upsert the health snapshot + name. Lifecycle cols are owned by
    // reconcileLifecycle — never touched here.
    const warmupEnabled = a.warmup_status === 1;
    const statusText = a.status > 0 ? "active" : "inactive";
    await db
      .insert(instantlyAccounts)
      .values({
        email: a.email,
        warmupEnabled,
        status: statusText,
        dailySendLimit: a.daily_limit ?? null,
        instantlyStatus: a.status,
        warmupScore: a.stat_warmup_score ?? null,
        dailyLimit: a.daily_limit ?? null,
        providerCode: a.provider_code ?? null,
        firstName: a.first_name ?? null,
        lastName: a.last_name ?? null,
      })
      .onConflictDoUpdate({
        target: instantlyAccounts.email,
        set: {
          warmupEnabled,
          status: statusText,
          dailySendLimit: a.daily_limit ?? null,
          instantlyStatus: a.status,
          warmupScore: a.stat_warmup_score ?? null,
          dailyLimit: a.daily_limit ?? null,
          providerCode: a.provider_code ?? null,
          firstName: a.first_name ?? null,
          lastName: a.last_name ?? null,
          updatedAt: now,
        },
      });
  }

  return { synced: accounts.length };
}

// ─── Gold reads ─────────────────────────────────────────────────────────────

/** Brand/product domains from instantly_domain_policy. */
export async function fetchDomainPolicy(): Promise<Set<string>> {
  const result = await db.execute(sql`SELECT domain FROM instantly_domain_policy`);
  return new Set(
    rowsOf<{ domain: string }>(result)
      .map((r) => (r.domain ?? "").toLowerCase())
      .filter(Boolean),
  );
}

/** One account's latest-test placement delivery, blended across ESPs. */
export interface AccountDelivery {
  /** Σ inbox_count over the latest test's (account, ESP) rows. */
  inboxCount: number;
  /** Σ seed_total over the same rows. */
  seedTotal: number;
  /** Rounded inbox %, or null when never tested. */
  deliveryPct: number | null;
  /** True ⇔ 100% inbox across ALL ESPs of the latest test (inbox == seed, seed > 0). */
  full: boolean;
}

/**
 * Latest placement delivery per account, summed across ESPs. `delivery === 100`
 * (full) ⇔ every (account, ESP) row of the latest test is inbox == seed:
 * because inbox_i ≤ seed_i for every ESP i, Σinbox == Σseed forces per-ESP
 * equality, so the blended equality is the exact "100% across all ESPs" rule.
 * Accounts never tested are ABSENT from the map (→ delivery unknown → in_recovery).
 */
export async function fetchLatestDeliveryByAccount(): Promise<Map<string, AccountDelivery>> {
  const result = await db.execute(sql`
    WITH latest AS (
      SELECT DISTINCT ON (account_email) account_email, test_id
      FROM instantly_placement_results
      ORDER BY account_email, tested_at DESC, test_id DESC
    )
    SELECT
      r.account_email AS "accountEmail",
      SUM(r.inbox_count)::int AS "inboxCount",
      SUM(r.seed_total)::int AS "seedTotal"
    FROM instantly_placement_results r
    JOIN latest l
      ON l.account_email = r.account_email AND l.test_id = r.test_id
    GROUP BY r.account_email
  `);

  const map = new Map<string, AccountDelivery>();
  for (const r of rowsOf<{ accountEmail: string; inboxCount: number; seedTotal: number }>(
    result,
  )) {
    const inboxCount = Number(r.inboxCount);
    const seedTotal = Number(r.seedTotal);
    const full = isDeliveryFull([{ inboxCount, seedTotal }]);
    const deliveryPct = seedTotal > 0 ? Math.round((inboxCount * 100) / seedTotal) : null;
    map.set(r.accountEmail, { inboxCount, seedTotal, deliveryPct, full });
  }
  return map;
}

export interface LifecycleView {
  status: LifecycleStatus | null;
  reason: string | null;
  updatedAt: string | null;
}

/** Current lifecycle projection per account (for account-health + forecast). */
export async function fetchLifecycleByEmail(): Promise<Map<string, LifecycleView>> {
  const result = await db.execute(sql`
    SELECT email AS "email",
           lifecycle_status AS "status",
           lifecycle_reason AS "reason",
           lifecycle_updated_at AS "updatedAt"
    FROM instantly_accounts
  `);
  const map = new Map<string, LifecycleView>();
  for (const r of rowsOf<{
    email: string;
    status: string | null;
    reason: string | null;
    updatedAt: string | Date | null;
  }>(result)) {
    map.set(r.email, {
      status: (r.status as LifecycleStatus | null) ?? null,
      reason: r.reason ?? null,
      updatedAt: r.updatedAt ? new Date(r.updatedAt).toISOString() : null,
    });
  }
  return map;
}

/**
 * The live-send pool: Instantly Account-shaped objects for every silver account
 * currently `in_production`. Read PURELY from silver — no live listAccounts on
 * the send hot-path. `signature` is left undefined so the send path derives the
 * per-account default signature from first/last name.
 */
export async function fetchInProductionAccounts(): Promise<Account[]> {
  const result = await db.execute(sql`
    SELECT email AS "email",
           first_name AS "firstName",
           last_name AS "lastName",
           instantly_status AS "instantlyStatus",
           warmup_score AS "warmupScore",
           daily_limit AS "dailyLimit",
           provider_code AS "providerCode"
    FROM instantly_accounts
    WHERE lifecycle_status = 'in_production'
  `);
  return rowsOf<{
    email: string;
    firstName: string | null;
    lastName: string | null;
    instantlyStatus: number | null;
    warmupScore: number | null;
    dailyLimit: number | null;
    providerCode: number | null;
  }>(result).map((r) => ({
    email: r.email,
    warmup_status: 0,
    status: r.instantlyStatus ?? 1,
    first_name: r.firstName ?? undefined,
    last_name: r.lastName ?? undefined,
    signature: undefined,
    stat_warmup_score: r.warmupScore ?? undefined,
    daily_limit: r.dailyLimit ?? undefined,
    provider_code: r.providerCode ?? undefined,
  }));
}

/**
 * Emails eligible to be placement-tested: everything active + not brand-blocked,
 * i.e. lifecycle_status IN ('in_recovery', 'in_production'). This BREAKS the
 * bootstrap deadlock — a fresh account starts in_recovery (delivery unknown), so
 * it MUST be testable to ever earn the delivery == 100 that promotes it to
 * in_production. Seeding placement tests from in_production only would never test
 * (and never promote) a recovering account.
 */
export async function fetchTestablePoolEmails(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT email FROM instantly_accounts
    WHERE lifecycle_status IN ('in_recovery', 'in_production')
  `);
  return rowsOf<{ email: string }>(result)
    .map((r) => r.email)
    .filter(Boolean);
}

// ─── Gold: reconcile ────────────────────────────────────────────────────────

export interface ReconcileLifecycleSummary {
  scanned: number;
  changed: number;
  warmupPatched: number;
  failed: number;
}

interface SilverAccountRow {
  email: string;
  instantlyStatus: number | null;
  warmupScore: number | null;
  dailyLimit: number | null;
  lifecycleStatus: string | null;
}

/**
 * Recompute every account's lifecycle from (silver health snapshot + latest
 * placement delivery + domain_policy). On a CHANGE:
 *   1. PATCH the Instantly warmup FIRST (10/day in_production, 50/day recovery /
 *      deactivated_by_user, untouched for deactivated_by_instantly) — fail loud
 *      per account; on a PATCH error we count `failed` and SKIP the persist (no
 *      half-applied state — next run retries).
 *   2. Insert a lifecycle event (the audit trail + capacity-history raw material).
 *   3. Update the silver lifecycle projection.
 * Idempotent: an account whose derived status equals its current status is a
 * no-op (no event, no PATCH). Never touches the campaign daily_limit.
 */
export async function reconcileLifecycle(
  apiKey: string,
): Promise<ReconcileLifecycleSummary> {
  const [accountsResult, domainPolicy, deliveryByEmail] = await Promise.all([
    db.execute(sql`
      SELECT email AS "email",
             instantly_status AS "instantlyStatus",
             warmup_score AS "warmupScore",
             daily_limit AS "dailyLimit",
             lifecycle_status AS "lifecycleStatus"
      FROM instantly_accounts
    `),
    fetchDomainPolicy(),
    fetchLatestDeliveryByAccount(),
  ]);

  const accounts = rowsOf<SilverAccountRow>(accountsResult);
  let changed = 0;
  let warmupPatched = 0;
  let failed = 0;

  for (const row of accounts) {
    const currentStatus = (row.lifecycleStatus as LifecycleStatus | null) ?? null;
    const healthScore = Number(row.warmupScore ?? 0);
    const delivery = deliveryByEmail.get(row.email);
    const deliveryValue = delivery ? (delivery.full ? 100 : delivery.deliveryPct) : null;
    const deliveryPctSnapshot = delivery?.deliveryPct ?? null;

    const { status, reason } = deriveLifecycle({
      instantlyStatus: Number(row.instantlyStatus ?? 0),
      domain: emailDomain(row.email),
      healthScore,
      delivery: deliveryValue,
      domainPolicy,
    });

    if (status === currentStatus) continue; // idempotent — nothing changed

    // Reactivation: an account leaving deactivated_by_instantly (Instantly
    // re-enabled it) reports `reactivated` instead of the raw derived reason.
    const eventReason: LifecycleReason =
      currentStatus === "deactivated_by_instantly" &&
      (status === "in_production" || status === "in_recovery")
        ? "reactivated"
        : reason;

    const warmupTarget = warmupDailyForStatus(status);

    try {
      // ORDERING (load-bearing): PATCH Instantly warmup FIRST. On failure we do
      // NOT persist the event/status (no half-applied state) — next run retries.
      if (warmupTarget !== null) {
        await setWarmupDailyLimit(apiKey, row.email, warmupTarget);
        warmupPatched += 1;
      }
    } catch (error: unknown) {
      failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[account-lifecycle] warmup PATCH failed for ${row.email} → ${status}: ${message}`,
      );
      continue;
    }

    const now = new Date();
    await db.insert(instantlyAccountLifecycleEvents).values({
      accountEmail: row.email,
      fromStatus: currentStatus,
      toStatus: status,
      reason: eventReason,
      healthScore,
      deliveryPct: deliveryPctSnapshot,
      dailyLimit: row.dailyLimit ?? null,
      createdAt: now,
    });
    await db
      .update(instantlyAccounts)
      .set({
        lifecycleStatus: status,
        lifecycleReason: eventReason,
        lifecycleUpdatedAt: now,
        updatedAt: now,
      })
      .where(sql`${instantlyAccounts.email} = ${row.email}`);

    changed += 1;
    console.log(
      `[account-lifecycle] ${row.email}: ${currentStatus ?? "(new)"} → ${status} (${eventReason})`,
    );
  }

  return { scanned: accounts.length, changed, warmupPatched, failed };
}
