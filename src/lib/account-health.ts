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
  /** Latest blended placement from our BSG history; null when never tested. */
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
 * who actually gets to send. `inboxPlacement` is injected from the caller's
 * placement map (our BSG history); an account absent from the map gets null.
 */
export function buildAccountHealth(
  accounts: Account[],
  placementByEmail: Map<string, InboxPlacement> = new Map(),
): AccountHealth[] {
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
      inboxPlacement: placementByEmail.get(a.email) ?? null,
    };
  });
}
