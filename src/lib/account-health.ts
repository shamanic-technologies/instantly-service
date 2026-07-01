/**
 * Per-account deliverability-health logic (pure — no IO). Powers GET
 * /internal/audit/account-health, the staff-only "Audit → Instantly" ops table
 * that lists every sending account with its identity, sending config, and
 * blocked state.
 *
 * ── Inbox-placement feasibility (Instantly V2 API, verified 2026-07-01) ──────
 * The V2 API does NOT expose inbox placement as a per-account property. The
 * account object (GET /accounts) carries no inbox/spam/missing figures. Placement
 * results exist ONLY as the output of a manually-run Inbox Placement Test,
 * surfaced test-scoped via `GET /inbox-placement-analytics` (one row per
 * (test, sender, recipient): `is_spam` boolean, `recipient_esp`, SPF/DKIM/DMARC —
 * NO per-account inbox/spam/missing percentage) and aggregated by
 * `POST /inbox-placement-analytics/stats-by-test-id`. That data is:
 *   - point-in-time (a manual test, not a live per-account signal),
 *   - partial (covers only the accounts chosen as senders for that test),
 *   - subscription-gated (Growth Inbox Placement, or the endpoints 402).
 * There is therefore no reliable "current placement for account X" feed to map
 * onto this contract's per-account `inboxPlacement`. Per the no-fabricate rule,
 * `inboxPlacement` is `null` for every account in v1. If Instantly later exposes
 * a standing per-account placement field (or we decide to attribute the latest
 * test's per-sender stats), populate it here from that REAL data only.
 */

import type { Account } from "./instantly-client";
import { classifyAccountBlock, type AccountBlockReason } from "./send-lead";

/** Inbox-placement breakdown for one account. Null until the API exposes it. */
export interface InboxPlacement {
  inboxPct: number;
  spamPct: number;
  missingPct: number;
  /** ISO8601 timestamp of the placement test. */
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
  /** Per-account daily send limit, null if unknown. */
  dailyLimit: number | null;
  /** True when the account is NOT send-eligible (see `classifyAccountBlock`). */
  blocked: boolean;
  /** Short reason string when blocked, null when send-eligible. */
  blockReason: AccountBlockReason | null;
  /** Null — the V2 API exposes no per-account placement (see file header). */
  inboxPlacement: InboxPlacement | null;
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
 * `blockReason` come from `classifyAccountBlock` — the SAME gate the live send
 * path (`filterHealthyAccounts`) uses, so the audit view can never disagree with
 * who actually gets to send. `inboxPlacement` is always null in v1 (see header).
 */
export function buildAccountHealth(accounts: Account[]): AccountHealth[] {
  return accounts.map((a) => {
    const blockReason = classifyAccountBlock(a);
    return {
      email: a.email,
      domain: domainOf(a.email),
      status: statusLabel(a.status),
      warmupScore: a.stat_warmup_score ?? null,
      dailyLimit: a.daily_limit ?? null,
      blocked: blockReason !== null,
      blockReason,
      inboxPlacement: null,
    };
  });
}
