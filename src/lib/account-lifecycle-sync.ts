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
import {
  listAccounts,
  setWarmupDailyLimit,
  setDailyLimit,
  type Account,
} from "./instantly-client";
import {
  deriveLifecycle,
  warmupDailyForStatus,
  dailyLimitForStatus,
  emailDomain,
  isDeliveryAtBar,
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
    const timestampCreated = a.timestamp_created ? new Date(a.timestamp_created) : null;
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
        timestampCreated,
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
          timestampCreated,
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

/** One account's latest-test placement delivery. */
export interface AccountDelivery {
  /** Σ inbox_count over the latest test's (account, ESP) rows. */
  inboxCount: number;
  /** Σ seed_total over the same rows. */
  seedTotal: number;
  /** Rounded BLENDED inbox % — display/snapshot only, never the gate. */
  deliveryPct: number | null;
  /**
   * The GATE: true ⇔ EVERY (account, ESP) row of the latest test inboxed at
   * >= PRODUCTION_DELIVERY_PCT_BAR. Per-ESP, never the blended pct above — see
   * {@link isDeliveryAtBar}.
   */
  atBar: boolean;
}

/**
 * Latest placement delivery per account. Returns ONE ROW PER (account, ESP) of the
 * account's latest test and applies the per-ESP bar in JS via `isDeliveryAtBar` —
 * the SQL must NOT pre-sum the ESPs, because at a 95 bar a blended average hides a
 * failing Gmail behind a passing Outlook (the sum-equivalence only held at 100).
 * The blended `deliveryPct` is still computed, for the event snapshot / display.
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
      r.recipient_esp AS "recipientEsp",
      r.inbox_count::int AS "inboxCount",
      r.seed_total::int AS "seedTotal"
    FROM instantly_placement_results r
    JOIN latest l
      ON l.account_email = r.account_email AND l.test_id = r.test_id
  `);

  const espRowsByAccount = new Map<string, { inboxCount: number; seedTotal: number }[]>();
  for (const r of rowsOf<{ accountEmail: string; inboxCount: number; seedTotal: number }>(
    result,
  )) {
    const list = espRowsByAccount.get(r.accountEmail) ?? [];
    list.push({ inboxCount: Number(r.inboxCount), seedTotal: Number(r.seedTotal) });
    espRowsByAccount.set(r.accountEmail, list);
  }

  const map = new Map<string, AccountDelivery>();
  for (const [email, espRows] of espRowsByAccount) {
    const inboxCount = espRows.reduce((s, r) => s + r.inboxCount, 0);
    const seedTotal = espRows.reduce((s, r) => s + r.seedTotal, 0);
    const deliveryPct = seedTotal > 0 ? Math.round((inboxCount * 100) / seedTotal) : null;
    map.set(email, { inboxCount, seedTotal, deliveryPct, atBar: isDeliveryAtBar(espRows) });
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
 * currently `in_production`, FILTERED to the pool reserved for `featureSlug`.
 * Read PURELY from silver — no live listAccounts on the send hot-path.
 * `signature` is left undefined so the send path derives the per-account default
 * signature from first/last name.
 *
 * Feature carve-out (instantly_account_feature_policy):
 *   - featureSlug is a RESERVED slug (present in the policy table) → pool = the
 *     accounts reserved to exactly that slug (e.g. sales-crm-email-outreach → the
 *     3 CRM accounts).
 *   - featureSlug is non-reserved OR null → pool = the UNRESERVED accounts (the
 *     whole in_production fleet minus every reserved account). This is the shared
 *     default pool serving the 5 cold-email features + any untagged send.
 * "Reserved slugs" is derived from the table itself (no hardcoded constant), so
 * reserving another feature is a data change, never a deploy.
 */
export async function fetchInProductionAccounts(
  featureSlug?: string | null,
): Promise<Account[]> {
  const slug = featureSlug ?? null;
  const result = await db.execute(sql`
    SELECT a.email AS "email",
           a.first_name AS "firstName",
           a.last_name AS "lastName",
           a.instantly_status AS "instantlyStatus",
           a.warmup_score AS "warmupScore",
           a.daily_limit AS "dailyLimit",
           a.provider_code AS "providerCode",
           a.timestamp_created AS "timestampCreated"
    FROM instantly_accounts a
    LEFT JOIN instantly_account_feature_policy p ON p.account_email = a.email
    WHERE a.lifecycle_status = 'in_production'
      AND CASE
            WHEN ${slug}::text IN (
              SELECT feature_slug FROM instantly_account_feature_policy
            )
              THEN p.feature_slug = ${slug}::text
            ELSE p.account_email IS NULL
          END
  `);
  return rowsOf<{
    email: string;
    firstName: string | null;
    lastName: string | null;
    instantlyStatus: number | null;
    warmupScore: number | null;
    dailyLimit: number | null;
    providerCode: number | null;
    timestampCreated: string | Date | null;
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
    timestamp_created: r.timestampCreated
      ? new Date(r.timestampCreated).toISOString()
      : undefined,
  }));
}

/**
 * How old an account must be before the weekly placement test seeds it. A test
 * sends ~30-50 seed emails from the mailbox; running that on a days-old account
 * measures its warmup, not its deliverability, and spends Growth-sub quota.
 */
export const TESTABLE_MIN_AGE_DAYS = 7;

/**
 * Emails eligible to be placement-tested by the weekly test: active + not
 * brand-blocked (lifecycle_status IN ('in_recovery', 'in_production')) AND at
 * least {@link TESTABLE_MIN_AGE_DAYS} days old.
 *
 * Including `in_recovery` BREAKS the bootstrap deadlock — a new account starts
 * in_recovery (delivery unknown), so it MUST be testable to ever earn the
 * delivery score that promotes it. Seeding from in_production only would never
 * test (and never promote) a recovering account.
 *
 * The age floor keeps seed quota off mailboxes too new to have a meaningful
 * result: a brand-new mailbox has barely warmed, so testing it in week one
 * measures nothing and burns the Growth-sub quota. An account with no
 * `timestamp_created` (not yet backfilled) is treated as old enough — the
 * pre-backfill behaviour.
 */
export async function fetchTestablePoolEmails(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT email FROM instantly_accounts
    WHERE lifecycle_status IN ('in_recovery', 'in_production')
      AND (
        timestamp_created IS NULL
        OR timestamp_created <= now() - make_interval(days => ${TESTABLE_MIN_AGE_DAYS})
      )
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
  dailyLimitPatched: number;
  /** Accounts whose STATUS was unchanged but whose stale `lifecycle_reason` was refreshed. */
  reasonsRefreshed: number;
  failed: number;
}

interface SilverAccountRow {
  email: string;
  instantlyStatus: number | null;
  warmupScore: number | null;
  dailyLimit: number | null;
  lifecycleStatus: string | null;
  lifecycleReason: string | null;
}

/**
 * Recompute every account's lifecycle from (silver health snapshot + latest
 * placement delivery + domain_policy). On a CHANGE:
 *   1. PATCH the Instantly warmup FIRST (5/day in_production, 30/day recovery /
 *      deactivated_by_user, untouched for deactivated_by_instantly) — fail loud
 *      per account; on a PATCH error we count `failed` and SKIP the persist (no
 *      half-applied state — next run retries).
 *   2. PATCH the campaign daily_limit (45 in_production, 20 in_recovery; untouched
 *      for deactivated_* so the queue drains). Paired with warmup so total = 50/day
 *      (under Gmail's per-user daily sending limit).
 *   3. Insert a lifecycle event (the audit trail + capacity-history raw material).
 *   4. Update the silver lifecycle projection.
 * Idempotent: an account whose derived status equals its current status writes NO
 * event and makes NO Instantly PATCH — but its silver `lifecycle_reason` IS
 * refreshed when the derived reason moved (the reason was previously only ever
 * written on a flip, so it went stale and contradicted the health/delivery columns
 * shown beside it).
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
             lifecycle_status AS "lifecycleStatus",
             lifecycle_reason AS "lifecycleReason"
      FROM instantly_accounts
    `),
    fetchDomainPolicy(),
    fetchLatestDeliveryByAccount(),
  ]);

  const accounts = rowsOf<SilverAccountRow>(accountsResult);
  let changed = 0;
  let warmupPatched = 0;
  let dailyLimitPatched = 0;
  let reasonsRefreshed = 0;
  let failed = 0;

  for (const row of accounts) {
    const currentStatus = (row.lifecycleStatus as LifecycleStatus | null) ?? null;
    const healthScore = Number(row.warmupScore ?? 0);
    const delivery = deliveryByEmail.get(row.email);
    const deliveryPctSnapshot = delivery?.deliveryPct ?? null;

    const { status, reason } = deriveLifecycle({
      instantlyStatus: Number(row.instantlyStatus ?? 0),
      domain: emailDomain(row.email),
      healthScore,
      deliveryAtBar: delivery ? delivery.atBar : null,
      domainPolicy,
    });

    if (status === currentStatus) {
      // Status unchanged → no transition, so NO lifecycle event and NO Instantly
      // PATCH. But the stored REASON can still be stale: it is only ever written
      // on a flip, so an account that entered in_recovery on `health_below_bar`
      // and has since recovered its health (still held back by delivery) kept
      // displaying `health_below_bar` next to a health of 100 — a self-
      // contradictory ops surface. Refresh the reason column alone.
      // `lifecycleUpdatedAt` is deliberately NOT touched: reactivate-accounts
      // reads it as the "deactivated for >= 24h" proxy, and a reason refresh is
      // not a state change. A stored `reactivated` is legitimately overwritten
      // by the derived reason — silver answers "why is it in this state NOW",
      // while "how it got here" stays in the lifecycle EVENT audit trail.
      if (currentStatus !== null && reason !== row.lifecycleReason) {
        await db
          .update(instantlyAccounts)
          .set({ lifecycleReason: reason, updatedAt: new Date() })
          .where(sql`${instantlyAccounts.email} = ${row.email}`);
        reasonsRefreshed += 1;
      }
      continue; // idempotent — status unchanged
    }

    // Reactivation: an account leaving deactivated_by_instantly (Instantly
    // re-enabled it) reports `reactivated` instead of the raw derived reason.
    const eventReason: LifecycleReason =
      currentStatus === "deactivated_by_instantly" &&
      (status === "in_production" || status === "in_recovery")
        ? "reactivated"
        : reason;

    const warmupTarget = warmupDailyForStatus(status);
    const dailyLimitTarget = dailyLimitForStatus(status);

    try {
      // ORDERING (load-bearing): PATCH Instantly FIRST. On failure we do NOT
      // persist the event/status (no half-applied state) — next run retries.
      if (warmupTarget !== null) {
        await setWarmupDailyLimit(apiKey, row.email, warmupTarget);
        warmupPatched += 1;
      }
      // in_production also opens the campaign daily max-send to 40. Other states
      // leave daily_limit untouched (queue keeps draining at its current cap).
      if (dailyLimitTarget !== null) {
        await setDailyLimit(apiKey, row.email, dailyLimitTarget);
        dailyLimitPatched += 1;
      }
    } catch (error: unknown) {
      failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[account-lifecycle] Instantly PATCH failed for ${row.email} → ${status}: ${message}`,
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

  return {
    scanned: accounts.length,
    changed,
    warmupPatched,
    dailyLimitPatched,
    reasonsRefreshed,
    failed,
  };
}
