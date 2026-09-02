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
  resolveTransportForSend,
  SEND_TRANSPORT_SMTP,
  type SendTransport,
} from "./self-send/transport";
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
import { isSelfSendCapable } from "./self-send/transport-split";
import {
  deriveLifecycle,
  shouldAdoptSelfSendTransport,
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
  /** Rows Instantly no longer lists — newly flagged `absent_since` this run. */
  markedAbsent: number;
}

/**
 * Full snapshot of Instantly GET /accounts → Bronze (append-only history) +
 * Silver (upsert current health cols + name). Read-only against Instantly (spends
 * no quota). Fails loud on any Instantly error.
 */
export async function snapshotAccounts(apiKey: string): Promise<SnapshotSummary> {
  const accounts = await listAccounts(apiKey);
  const now = new Date();

  // An empty account list is not a legitimate state for this workspace, and the
  // absence sweep below would flag EVERY stored account as deleted. Fail loud
  // rather than let one bad upstream page wipe the fleet's inventory view.
  if (accounts.length === 0) {
    throw new Error(
      "[instantly-service] accounts-sync: Instantly returned zero accounts — refusing to run the absence sweep",
    );
  }

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
          // The account is live again (or still live) — clear any ghost flag.
          absentSince: null,
          updatedAt: now,
        },
      });
  }

  // Ghost sweep. Every account Instantly still lists was just upserted with
  // `updated_at = now`, so anything older is an account Instantly no longer has.
  // Keyed on the timestamp rather than a `NOT IN (…250 emails)` list on purpose:
  // a bind-parameter list grows with the fleet and eventually trips Postgres'
  // 65,534-parameter ceiling.
  //
  // Rows are flagged, never deleted — their sent events and lifecycle history
  // stay meaningful; they are simply excluded from inventory and capacity views.
  const absent = await db.execute(sql`
    UPDATE instantly_accounts
       SET absent_since = ${now}
     WHERE absent_since IS NULL
       AND updated_at < ${now}
    RETURNING email
  `);
  const markedAbsent = rowsOf<{ email: string }>(absent).length;

  if (markedAbsent > 0) {
    console.warn(
      `[instantly-service] accounts-sync: ${markedAbsent} account(s) no longer listed by Instantly — flagged absent`,
    );
  }

  return { synced: accounts.length, markedAbsent };
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
   * The GATE: true ⇔ the latest test's rows POOL to >= PRODUCTION_DELIVERY_PCT_BAR
   * (`Σinbox / Σseeds` across every ESP) — see {@link isDeliveryAtBar}. Same
   * sample as `deliveryPct` above, which is the rounded display form of it.
   */
  atBar: boolean;
  /**
   * When that test RAN (newest `tested_at` of its rows), ISO string. Feeds the
   * evidence-freshness half of the gate — a pass we can no longer vouch for is
   * not a pass. See DELIVERY_EVIDENCE_MAX_AGE_DAYS.
   */
  testedAt: string | null;
}

/**
 * Latest placement delivery per account. Returns ONE ROW PER (account, ESP) of the
 * account's latest test and pools them in JS via `isDeliveryAtBar`. The SQL keeps
 * the per-ESP grain deliberately: it is the raw silver shape, and the per-leg
 * split is what surfaced the Gmail-spam-vs-Outlook-fine finding the whole
 * deliverability model rests on — the GATE pools, the DATA does not.
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
      r.seed_total::int AS "seedTotal",
      r.tested_at AS "testedAt"
    FROM instantly_placement_results r
    JOIN latest l
      ON l.account_email = r.account_email AND l.test_id = r.test_id
  `);

  const espRowsByAccount = new Map<string, { inboxCount: number; seedTotal: number }[]>();
  const testedAtByAccount = new Map<string, string>();
  for (const r of rowsOf<{
    accountEmail: string;
    inboxCount: number;
    seedTotal: number;
    testedAt: string | Date | null;
  }>(result)) {
    const list = espRowsByAccount.get(r.accountEmail) ?? [];
    list.push({ inboxCount: Number(r.inboxCount), seedTotal: Number(r.seedTotal) });
    espRowsByAccount.set(r.accountEmail, list);

    if (r.testedAt !== null && r.testedAt !== undefined) {
      const iso = r.testedAt instanceof Date ? r.testedAt.toISOString() : String(r.testedAt);
      const seen = testedAtByAccount.get(r.accountEmail);
      // Rows of one test share a tested_at, but take the newest defensively.
      if (seen === undefined || iso > seen) testedAtByAccount.set(r.accountEmail, iso);
    }
  }

  const map = new Map<string, AccountDelivery>();
  for (const [email, espRows] of espRowsByAccount) {
    const inboxCount = espRows.reduce((s, r) => s + r.inboxCount, 0);
    const seedTotal = espRows.reduce((s, r) => s + r.seedTotal, 0);
    const deliveryPct = seedTotal > 0 ? Math.round((inboxCount * 100) / seedTotal) : null;
    map.set(email, {
      inboxCount,
      seedTotal,
      deliveryPct,
      atBar: isDeliveryAtBar(espRows),
      testedAt: testedAtByAccount.get(email) ?? null,
    });
  }
  return map;
}

export interface LifecycleView {
  status: LifecycleStatus | null;
  reason: string | null;
  updatedAt: string | null;
  /**
   * The account's resolved send-transport policy. Carried here so the hourly
   * limits sweep can tell whether Instantly is still the pipe without a second
   * query — it must NOT enforce Instantly-side limits on a mailbox we dispatch
   * ourselves. See `selectLifecycleLimitPatches`.
   */
  sendTransport: SendTransport;
}

/** Current lifecycle projection per account (for account-health + forecast). */
export async function fetchLifecycleByEmail(): Promise<Map<string, LifecycleView>> {
  const result = await db.execute(sql`
    SELECT email AS "email",
           lifecycle_status AS "status",
           lifecycle_reason AS "reason",
           lifecycle_updated_at AS "updatedAt",
           send_transport AS "sendTransport"
    FROM instantly_accounts
  `);
  const map = new Map<string, LifecycleView>();
  for (const r of rowsOf<{
    email: string;
    status: string | null;
    reason: string | null;
    updatedAt: string | Date | null;
    sendTransport: string | null;
  }>(result)) {
    map.set(r.email, {
      status: (r.status as LifecycleStatus | null) ?? null,
      reason: r.reason ?? null,
      updatedAt: r.updatedAt ? new Date(r.updatedAt).toISOString() : null,
      sendTransport: resolveTransportForSend(r.sendTransport),
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
 *
 * Each row also carries `infraProvider` — the infrastructure vendor that owns
 * the account's domain, read from `infra_domains` (the daily infra sync fills
 * it). It is the PRIMARY key of the send fill order (see `accountFillOrder`), so
 * the fleet drains one vendor before touching the next. A domain reported by two
 * vendors resolves deterministically to the one that fills earliest; a domain
 * with no inventory row at all yields null, which sorts LAST rather than first.
 *
 * `domainFillRank` is the SECOND key of that order: the domain's position within
 * its vendor, read from `instantly_domain_fill_order`. It is what lets a whole
 * domain go quiet — the vendor tier drains a vendor at a time, but a vendor's
 * mailboxes interleave domains, so without this key every domain of a vendor
 * stays mildly busy and none can be cancelled. Null (no row) sorts LAST within
 * the vendor, same reasoning as a null provider.
 */
/**
 * `sendTransport` is the account's send-transport POLICY ('instantly' | 'smtp').
 * It rides along so the send path can FREEZE it onto the campaign row without a
 * second query — the decision has to be taken at send time, since re-reading the
 * policy later would re-route a lead's followups mid-flight.
 */
export type PooledAccount = Account & {
  infraProvider: string | null;
  domainFillRank: number | null;
  sendTransport: string;
};

export async function fetchInProductionAccounts(
  featureSlug?: string | null,
): Promise<PooledAccount[]> {
  const slug = featureSlug ?? null;
  const result = await db.execute(sql`
    SELECT a.email AS "email",
           a.first_name AS "firstName",
           a.last_name AS "lastName",
           a.instantly_status AS "instantlyStatus",
           a.warmup_score AS "warmupScore",
           a.daily_limit AS "dailyLimit",
           a.provider_code AS "providerCode",
           a.timestamp_created AS "timestampCreated",
           a.send_transport AS "sendTransport",
           ip.provider AS "infraProvider",
           dfo.fill_rank AS "domainFillRank"
    FROM instantly_accounts a
    LEFT JOIN instantly_account_feature_policy p ON p.account_email = a.email
    LEFT JOIN LATERAL (
      SELECT d.provider
      FROM infra_domains d
      WHERE d.domain = split_part(a.email, '@', 2)
      ORDER BY CASE d.provider
                 WHEN 'gandi' THEN 0
                 WHEN 'mailforge' THEN 1
                 WHEN 'primeforge' THEN 2
                 WHEN 'instantly-dfy' THEN 3
                 ELSE 4
               END,
               d.provider
      LIMIT 1
    ) ip ON TRUE
    LEFT JOIN instantly_domain_fill_order dfo
      ON dfo.domain = split_part(a.email, '@', 2)
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
    sendTransport: string | null;
    infraProvider: string | null;
    domainFillRank: number | string | null;
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
    infraProvider: r.infraProvider,
    // node-postgres returns an int column as a JS number, but a `numeric`-typed
    // one as text; coerce so the sort compares numbers, never strings ("10" < "2").
    domainFillRank:
      r.domainFillRank === null || r.domainFillRank === undefined
        ? null
        : Number(r.domainFillRank),
    // Resolved rather than passed through, so an unrecognised or missing value
    // can only ever mean Instantly — the only way onto the self-send pipe is an
    // explicit, reversible UPDATE.
    sendTransport: resolveTransportForSend(r.sendTransport),
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
 *
 * ⚠️ DO NOT add an `instantly_status > 0` predicate here. It reads as an obvious
 * saving — why seed a mailbox Instantly has disabled? — and it is the wrong
 * direction: this test is the ONLY deliverability measurement those mailboxes
 * get, so excluding them strands them at a frozen score forever. It was added
 * and reverted on 2026-08-24.
 *
 * The premise behind it ("a disabled account cannot dispatch its seeds") is not
 * supported by the data: every one of the 13 disabled accounts carries a recent
 * test with `missing_count = 0`, i.e. every seed was delivered somewhere, so the
 * mail did go out. In practice these accounts also FLAP — `reactivate-accounts`
 * resumes them hourly and Instantly disables them again — so at the moment the
 * weekly test picks its senders they are routinely back in a sendable state.
 * That churn is ugly, but it is what keeps them measurable.
 *
 * The asymmetry that settles it: a wasted seed costs a slice of a flat monthly
 * subscription, while an unmeasurable mailbox can never leave `in_recovery`.
 * Prefer measuring.
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

/**
 * Identity this sweep presents to key-service when it asks which mailboxes we
 * hold a credential for. The sweep has no inbound request of its own, so it
 * names the route that drives it.
 */
const RECONCILE_CALLER = {
  method: "POST",
  path: "/internal/audit/accounts-sync",
} as const;

export interface ReconcileLifecycleSummary {
  scanned: number;
  changed: number;
  warmupPatched: number;
  dailyLimitPatched: number;
  /** Accounts whose STATUS was unchanged but whose stale `lifecycle_reason` was refreshed. */
  reasonsRefreshed: number;
  /** Accounts moved onto our own sender because Instantly had disabled them. */
  adoptedSelfSend: number;
  failed: number;
}

interface SilverAccountRow {
  email: string;
  instantlyStatus: number | null;
  warmupScore: number | null;
  dailyLimit: number | null;
  sendTransport: string | null;
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
  asOf: Date = new Date(),
): Promise<ReconcileLifecycleSummary> {
  const [accountsResult, domainPolicy, deliveryByEmail] = await Promise.all([
    db.execute(sql`
      SELECT email AS "email",
             instantly_status AS "instantlyStatus",
             warmup_score AS "warmupScore",
             daily_limit AS "dailyLimit",
             send_transport AS "sendTransport",
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
  let adoptedSelfSend = 0;
  let failed = 0;

  for (const row of accounts) {
    const currentStatus = (row.lifecycleStatus as LifecycleStatus | null) ?? null;
    const healthScore = Number(row.warmupScore ?? 0);
    const delivery = deliveryByEmail.get(row.email);
    const deliveryPctSnapshot = delivery?.deliveryPct ?? null;
    // Resolved rather than passed through, so an unrecognised stored value can
    // only ever mean Instantly — the only way onto the self-send pipe stays an
    // explicit, reversible UPDATE.
    let sendTransport = resolveTransportForSend(row.sendTransport);

    // RESCUE: Instantly disabled this mailbox for a reason that is a fact about
    // Instantly (its own IPs are Spamhaus-listed; a dead PROSPECT domain), not
    // about the mailbox. If we hold a credential, move it onto our own sender —
    // which makes `deriveLifecycle` skip both Instantly-owned gates below, so the
    // account is graded on DELIVERY alone. See shouldAdoptSelfSendTransport.
    const instantlyStatus = Number(row.instantlyStatus ?? 0);
    if (
      shouldAdoptSelfSendTransport({
        sendTransport,
        instantlyStatus,
        selfSendCapable:
          sendTransport !== SEND_TRANSPORT_SMTP && instantlyStatus <= 0
            ? await isSelfSendCapable(row.email, RECONCILE_CALLER)
            : false,
      })
    ) {
      await db
        .update(instantlyAccounts)
        .set({ sendTransport: SEND_TRANSPORT_SMTP, updatedAt: new Date() })
        .where(sql`${instantlyAccounts.email} = ${row.email}`);
      sendTransport = SEND_TRANSPORT_SMTP;
      adoptedSelfSend += 1;
      console.log(
        `[account-lifecycle] ${row.email}: Instantly disabled it (status ${instantlyStatus}) → send_transport=smtp`,
      );
    }

    const { status, reason } = deriveLifecycle({
      sendTransport,
      instantlyStatus,
      domain: emailDomain(row.email),
      healthScore,
      deliveryAtBar: delivery ? delivery.atBar : null,
      deliveryTestedAt: delivery?.testedAt ?? null,
      currentStatus,
      domainPolicy,
      asOf,
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

    // Both null on `smtp`: Instantly does not dispatch this mailbox, so its
    // warmup and daily_limit are no longer our enforcement points there — and
    // the account is often one Instantly disabled, so the PATCH would fail and
    // (because the persist is skipped on error) block the flip from landing.
    const warmupTarget = warmupDailyForStatus(status, sendTransport);
    const dailyLimitTarget = dailyLimitForStatus(status, sendTransport);

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
    adoptedSelfSend,
    failed,
  };
}
