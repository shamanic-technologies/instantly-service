/**
 * Per-account deliverability-health logic (pure — no IO). Powers GET
 * /internal/audit/account-health, the staff-only "Audit → Instantly" ops table
 * that lists every sending account with its identity, sending config, and
 * blocked state.
 *
 * ── Inbox-placement (Instantly V2 API, verified 2026-07-01) ──────────────────
 * The V2 API does NOT expose inbox placement as a per-account property. The
 * account object (GET /accounts) carries no inbox/spam/missing figures. Placement
 * exists ONLY as the output of an inbox-placement TEST, surfaced test-scoped via
 * `GET /inbox-placement-analytics` (one row per (test, sender, recipient):
 * `is_spam` boolean, `recipient_esp`, SPF/DKIM/DMARC — NO per-account
 * inbox/spam/missing percentage). That data is point-in-time, partial (only the
 * accounts a test sends from), and subscription-gated (Growth Inbox Placement, or
 * the endpoints 402).
 *
 * So `inboxPlacement` is NOT read off the account. It is derived from our OWN
 * Bronze/Silver/Gold placement history (see lib/placement-sync.ts): recurring
 * tests are captured to silver, and the LATEST test per account is blended into
 * this figure. `buildAccountHealth` receives that already-computed map and injects
 * it — the mapper never fabricates a number. An account with no placement history
 * (never in a test yet) gets `null`, never a silent 0%.
 */

import type { Account } from "./instantly-client";
import type { LifecycleStatus } from "./account-lifecycle";
import type { LifecycleView } from "./account-lifecycle-sync";
import type { QueueBreakdown } from "./queue-breakdown";
import { capForAccount } from "./account-lifecycle";

/**
 * What SEND SELECTION says about the fleet, injected into the ops row so the
 * table cannot disagree with the selector. Both members are optional: a caller
 * that passes neither gets `fillRank: null` everywhere and the cap evaluated at
 * `new Date()`, which is what every pre-existing caller wants.
 */
export interface SelectionView {
  /**
   * email → 1-based position in `accountFillOrder` over the in-production pool.
   * An account absent from the map is absent from the pool, so its rank is null.
   */
  fillRankByEmail?: Map<string, number>;
  /** Instant the age ramp is evaluated at — explicit so tests are deterministic. */
  asOf?: Date;
}

/**
 * Inbox placement for one account: ONE pooled score over its latest placement
 * test. Null only when the account has no test at all (or every row 0 seeds) —
 * a small test still yields a real score, sample size is not a gate.
 */
export interface InboxPlacement {
  /**
   * Pooled across every ESP of the test (`Σinbox / Σseeds`) — the exact number
   * `isDeliveryAtBar` gates on, so this percentage and `lifecycleReason` cannot
   * contradict each other.
   */
  inboxPct: number;
  spamPct: number;
  missingPct: number;
  /** ISO8601 timestamp of the placement test (newest across the legs). */
  testedAt: string;
}

/** One sending account's deliverability health (the locked contract row). */
export interface AccountHealth {
  email: string;
  /** Sending domain (part after `@`), or null when the email is malformed. */
  domain: string | null;
  /** Existing status representation: Instantly `status > 0` ⇒ "active". */
  status: string;
  /** Instantly Health Score `stat_warmup_score` (0-100), null if unknown. */
  warmupScore: number | null;
  /** Per-account daily MAX-SEND limit (cold-send cap), null if unknown. */
  dailyLimit: number | null;
  /**
   * The daily cap send SELECTION actually compares an account's load against:
   * `min(dailyLimit, rampCapForAge(timestamp_created, IN_PRODUCTION_DAILY_LIMIT))`
   * at `asOf`. Equal to `dailyLimit` for a mature (or undatable) mailbox; strictly
   * lower for one under `MATURE_AGE_DAYS`, whose real per-user quota is far below
   * the fleet cap for its first weeks.
   *
   * It exists because `dailyLimit` alone MISREPRESENTS a young mailbox on the ops
   * table: it renders 50 for an account the selector is holding at 23, so the
   * table and the selector disagree about the same account. Computed through the
   * SAME `capForAccount` the selector uses — never a second copy of the ramp.
   * Null only when the account carries no daily limit at all.
   */
  effectiveDailyCap: number | null;
  /**
   * 1-based position of this account in `accountFillOrder` over the in-production
   * pool — rank 1 is the mailbox a NEW sequence is offered first, and the fleet
   * saturates it before touching rank 2 (see `pickSequentialFillAccount`).
   *
   * This is the one number that explains the table: without it, an operator sorts
   * by sends and sees the mailboxes that DISPATCHED most (old followups, pinned to
   * their originating account) rather than the ones being ASSIGNED, and reads a
   * working waterfall as a broken one.
   *
   * Null when the account is not in that pool — blocked / not `in_production`, or
   * reserved to a feature slug (the pool read is the slug-less one, i.e. exactly
   * what an unreserved send draws from). Never fabricated: an account the selector
   * would not consider has no position in its order.
   */
  fillRank: number | null;
  /**
   * Per-account daily WARMUP send volume — Instantly `warmup.limit`, the number
   * of warm-up emails/day the account targets. A DISTINCT number from
   * `dailyLimit` (the max-send cap): a live account commonly runs warmup 10/day
   * while its send cap is 50/day. Null when Instantly reports no warmup config.
   */
  warmupLimit: number | null;
  /** True when the account is NOT send-eligible (lifecycle != in_production). */
  blocked: boolean;
  /**
   * Short reason string when blocked, null when send-eligible. Now the account's
   * lifecycle_status when blocked (in_recovery / deactivated_by_instantly /
   * deactivated_by_user), or "unclassified" when the lifecycle has not yet run.
   */
  blockReason: string | null;
  /** Auto-derived lifecycle state (null until reconcileLifecycle first runs). */
  lifecycleStatus: LifecycleStatus | null;
  /** Snapshot reason on the latest lifecycle transition (null until classified). */
  lifecycleReason: string | null;
  /** ISO8601 timestamp of the latest lifecycle transition (null until classified). */
  lifecycleUpdatedAt: string | null;
  /** Latest blended placement from our BSG history; null when never tested. */
  inboxPlacement: InboxPlacement | null;
  /**
   * Count of REAL (non-inferred) `email_sent` events observed today (UTC) from
   * this account, derived from our silver event log. The "N" in a "N/dailyLimit"
   * read. 0 when the account has not sent today (honest 0, never fabricated).
   */
  sentToday: number;
  /**
   * Count of REAL (non-inferred) `email_sent` events observed YESTERDAY — the
   * full previous UTC calendar day [prev-midnight, today-midnight) — from this
   * account, from our silver log. Same provenance as `sentToday`. 0 when the
   * account sent nothing yesterday (honest 0, never fabricated).
   */
  sentYesterday: number;
  /**
   * Emails queued to Instantly for this account but not yet sent — the count of
   * still-`provisioned` sequence-cost holds on active campaigns whose observed
   * sending account is this one (one campaign = one lead = one account). 0 when
   * nothing is queued to it. Campaigns pushed but not yet sending have no
   * observed account, so their imminent step-1 holds are not attributed to any
   * account (documented gap — the account is unknown until the first send).
   *
   * This is the queued-STEPS total, and it PARTITIONS exactly into the four
   * `queued*` date buckets below:
   *   `queueSize === queuedFirstUnsent + queuedNextToday + queuedNextTomorrow +
   *    queuedNextLater` (holds for every account, by construction — the buckets
   *    are derived from the same provisioned-step set this counts).
   */
  queueSize: number;
  /**
   * Queued-STEP BREAKDOWN — every remaining un-sent email across the account's
   * queued sequences, split by the projected send date of EACH step. These four
   * PARTITION `queueSize` (the queued-STEPS total, see above), NOT
   * `queuedSequences`.
   *
   * NOTE `queuedSequences` (a count of SEQUENCES / leads) is a DIFFERENT
   * granularity from these step buckets — both are intentional, kept side by
   * side. Each step's date is a PROJECTION: last-sent + the CHAINED real
   * per-step configured delays across every remaining step (not just the
   * immediate next). It is a nominal-cadence LOWER BOUND — Instantly's actual
   * dispatch slips later under throttling, and CHAINING COMPOUNDS that drift
   * (a step two hops out sums two nominal gaps, so its lower bound is softer
   * than the next step's). So `queuedNextToday` reads as "step DUE
   * today-or-overdue", not "will certainly send today". See
   * lib/queue-breakdown.ts. A queued step with no attributable account is
   * excluded here (same as `queueSize`), never fabricated.
   */
  queuedSequences: number;
  /** Q0-first — steps of sequences whose first email has not sent yet. */
  queuedFirstUnsent: number;
  /**
   * Q0-first as SEQUENCES — never-contacted leads on this account, i.e. how many
   * FIRST emails are actually due. This is the quantity SEND SELECTION counts for
   * today's load; `queuedFirstUnsent` counts all remaining steps of those same
   * sequences, so it over-states "today" by the whole future sequence.
   *
   * An ops surface rendering "queued today" should use
   * `queuedFirstUnsentSequences + queuedNextToday` against `dailyLimit` — that is
   * the number the selector decides on. Using the step total instead makes a
   * healthy account look saturated (102 vs a 45 cap) while the selector, seeing
   * ~32, correctly keeps assigning it leads.
   */
  queuedFirstUnsentSequences: number;
  /** Q0-next — step projected today (UTC) or overdue. */
  queuedNextToday: number;
  /**
   * The BACKLOG subset of `queuedNextToday` — steps whose nominal due date is
   * STRICTLY BEFORE today, i.e. owed on an earlier day and never dispatched.
   * `queuedOverdue <= queuedNextToday` always, and it is NOT part of the
   * four-bucket partition of `queueSize` (it double-counts steps already in
   * `queuedNextToday`, by design — do not add it to the sum).
   *
   * Separates "work due today" from "work we are behind on", which the merged
   * today-or-overdue bucket cannot. A rising `queuedOverdue` is the ops signal
   * that Instantly is dispatching slower than we are assigning.
   */
  queuedOverdue: number;
  /** Q1-next — step projected tomorrow (UTC). */
  queuedNextTomorrow: number;
  /** Q-next — step projected after tomorrow (UTC). */
  queuedNextLater: number;
  /**
   * Descriptive account type from Instantly's `provider_code` — how the mailbox
   * sends: "google" / "microsoft" / "imap". Null when Instantly reports no code.
   * NOTE: this is the connection provider, NOT the provisioning class
   * (DFY-prewarmed vs legacy shared-IP), which Instantly's account object does
   * not expose — that deeper classification is a separate follow-up (see #389).
   */
  accountType: string | null;
}

/**
 * Map Instantly's `provider_code` to a human account type. 1=Custom IMAP/SMTP,
 * 2=Google, 3/4=Microsoft. Any other / absent code (incl. 8) → null (never fabricated).
 *
 * Verified against developer.instantly.ai's account schema (provider_code enum
 * 1,2,3,4,8; its example account is provider_code 2 with a Gmail 550 status_message)
 * and observed prod: the pc2 fleet is the Google pre-warmed inboxes that throw the
 * Gmail "Daily user sending limit exceeded" error, while pc1 is the IMAP-imported
 * Mailforge fleet (the instantly-mail importer hard-codes provider_code 1 = Custom
 * IMAP/SMTP). The previous mapping (1→google, 2→microsoft, 3/4→imap) was inverted and
 * mislabeled every account (Google fleet shown as "microsoft", IMAP fleet as "google").
 */
export function mapProviderCode(code: number | undefined): string | null {
  switch (code) {
    case 1:
      return "imap";
    case 2:
      return "google";
    case 3:
    case 4:
      return "microsoft";
    default:
      return null;
  }
}

/** Existing status representation, mirroring the accounts-sync mapping. */
function statusLabel(status: number): string {
  return status > 0 ? "active" : "inactive";
}

/** Domain part of an email, or null when there is no `@domain`. */
function domainOf(email: string): string | null {
  const domain = email.split("@")[1];
  return domain && domain.length > 0 ? domain : null;
}

/**
 * Map raw Instantly accounts to the account-health contract rows. `blocked` /
 * `blockReason` derive from the account's silver LIFECYCLE (the SAME projection
 * the live send path reads: send-eligible ⇔ lifecycle_status == 'in_production'),
 * so the audit view can never disagree with who actually gets to send. An account
 * not yet classified (no lifecycle row) is reported blocked with reason
 * "unclassified" — never a fabricated in_production. `inboxPlacement` is injected
 * from the caller's placement map (our BSG history); an account absent from the
 * map gets null.
 */
export function buildAccountHealth(
  accounts: Account[],
  placementByEmail: Map<string, InboxPlacement> = new Map(),
  sentTodayByEmail: Map<string, number> = new Map(),
  queueSizeByEmail: Map<string, number> = new Map(),
  lifecycleByEmail: Map<string, LifecycleView> = new Map(),
  sentYesterdayByEmail: Map<string, number> = new Map(),
  queueBreakdownByEmail: Map<string, QueueBreakdown> = new Map(),
  selection: SelectionView = {},
): AccountHealth[] {
  // The age ramp is evaluated at ONE instant for the whole table, so two rows can
  // never be capped against different clocks. Defaulted here rather than inside
  // the row loop, which would read a slightly different `now` per account.
  const asOf = selection.asOf ?? new Date();
  const fillRankByEmail = selection.fillRankByEmail ?? new Map<string, number>();
  return accounts.map((a) => {
    const lifecycle = lifecycleByEmail.get(a.email) ?? null;
    const lifecycleStatus = lifecycle?.status ?? null;
    const blocked = lifecycleStatus !== "in_production";
    const blockReason = blocked ? (lifecycleStatus ?? "unclassified") : null;
    const breakdown = queueBreakdownByEmail.get(a.email) ?? null;
    return {
      email: a.email,
      domain: domainOf(a.email),
      status: statusLabel(a.status),
      warmupScore: a.stat_warmup_score ?? null,
      dailyLimit: a.daily_limit ?? null,
      // Null stays null: an account with no stated limit has no cap to report, and
      // `capForAccount`'s own fallback to the lifecycle base would invent one.
      effectiveDailyCap:
        a.daily_limit === undefined || a.daily_limit === null
          ? null
          : capForAccount(a, asOf),
      fillRank: fillRankByEmail.get(a.email) ?? null,
      warmupLimit: a.warmup?.limit ?? null,
      blocked,
      blockReason,
      lifecycleStatus,
      lifecycleReason: lifecycle?.reason ?? null,
      lifecycleUpdatedAt: lifecycle?.updatedAt ?? null,
      inboxPlacement: placementByEmail.get(a.email) ?? null,
      sentToday: sentTodayByEmail.get(a.email) ?? 0,
      sentYesterday: sentYesterdayByEmail.get(a.email) ?? 0,
      // Prefer the breakdown's step total so queueSize === sum of the four date
      // buckets by construction; fall back to the standalone queue map for
      // callers that pass no breakdown (accounts with no breakdown queue 0).
      queueSize: breakdown?.steps ?? queueSizeByEmail.get(a.email) ?? 0,
      queuedSequences: breakdown?.sequences ?? 0,
      queuedFirstUnsent: breakdown?.firstUnsent ?? 0,
      queuedFirstUnsentSequences: breakdown?.firstUnsentSequences ?? 0,
      queuedNextToday: breakdown?.nextToday ?? 0,
      queuedOverdue: breakdown?.nextOverdue ?? 0,
      queuedNextTomorrow: breakdown?.nextTomorrow ?? 0,
      queuedNextLater: breakdown?.nextLater ?? 0,
      accountType: mapProviderCode(a.provider_code),
    };
  });
}
