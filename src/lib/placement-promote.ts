/**
 * Inbox-placement promotion logic (pure — no IO). Turns raw
 * inbox-placement-analytics rows (bronze) into per-(test, account, ESP) silver
 * results, and blends the latest test's ESP rows into the single per-account
 * `inboxPlacement` figure the account-health contract exposes (gold).
 *
 * Counting (per sending account × recipient ESP within one test):
 *   - received  = an analytics row with a determined `is_spam` (record_type 2)
 *   - inbox     = received AND is_spam === false
 *   - spam      = received AND is_spam === true
 *   - seedTotal = distinct recipient inboxes targeted for that (account, ESP)
 *   - missing   = seedTotal − inbox − spam   (seed sent but never received)
 *   - *Pct      = round(count / seedTotal × 100)   (0 when seedTotal is 0)
 * Rows missing `sender_email` or `recipient_esp` cannot be attributed and are
 * skipped (they contribute to neither the numerator nor the denominator).
 */

import type { InboxPlacementAnalyticsRow } from "./instantly-client";
import type { InboxPlacement } from "./account-health";

/** One silver placement result (matches the `instantly_placement_results` columns). */
export interface SilverPlacementRow {
  testId: string;
  accountEmail: string;
  recipientEsp: number;
  testedAt: Date;
  seedTotal: number;
  inboxCount: number;
  spamCount: number;
  missingCount: number;
  inboxPct: number;
  spamPct: number;
  missingPct: number;
  spfPass: boolean | null;
  dkimPass: boolean | null;
  dmarcPass: boolean | null;
}

function pct(count: number, total: number): number {
  return total > 0 ? Math.round((count / total) * 100) : 0;
}

/** AND-fold a nullable boolean across rows: true iff all present values are true; null if none present. */
function andFold(values: Array<boolean | null>): boolean | null {
  const present = values.filter((v): v is boolean => v !== null && v !== undefined);
  if (present.length === 0) return null;
  return present.every((v) => v === true);
}

interface Group {
  accountEmail: string;
  recipientEsp: number;
  recipients: Set<string>;
  received: InboxPlacementAnalyticsRow[];
}

/**
 * Aggregate one test's raw analytics rows into per-(account, ESP) silver rows.
 * `testedAt` is the test's run timestamp (applied to every produced row).
 */
export function aggregatePlacementRows(
  rows: InboxPlacementAnalyticsRow[],
  testId: string,
  testedAt: Date,
): SilverPlacementRow[] {
  const groups = new Map<string, Group>();

  for (const r of rows) {
    const account = r.sender_email;
    const esp = r.recipient_esp;
    if (!account || esp === null || esp === undefined) continue;

    const key = JSON.stringify([account, esp]);
    let g = groups.get(key);
    if (!g) {
      g = { accountEmail: account, recipientEsp: esp, recipients: new Set(), received: [] };
      groups.set(key, g);
    }
    if (r.recipient_email) g.recipients.add(r.recipient_email);
    // A determined is_spam marks a received (record_type 2) result.
    if (r.is_spam !== null && r.is_spam !== undefined) g.received.push(r);
  }

  const out: SilverPlacementRow[] = [];
  for (const g of groups.values()) {
    const { accountEmail, recipientEsp } = g;

    const inboxCount = g.received.filter((r) => r.is_spam === false).length;
    const spamCount = g.received.filter((r) => r.is_spam === true).length;
    // seedTotal is at least the number of landed results (a received row implies
    // a real seed), so a sent-side row we never saw can't undercount it.
    const seedTotal = Math.max(g.recipients.size, inboxCount + spamCount);
    const missingCount = Math.max(0, seedTotal - inboxCount - spamCount);

    out.push({
      testId,
      accountEmail,
      recipientEsp,
      testedAt,
      seedTotal,
      inboxCount,
      spamCount,
      missingCount,
      inboxPct: pct(inboxCount, seedTotal),
      spamPct: pct(spamCount, seedTotal),
      missingPct: pct(missingCount, seedTotal),
      spfPass: andFold(g.received.map((r) => r.spf_pass)),
      dkimPass: andFold(g.received.map((r) => r.dkim_pass)),
      dmarcPass: andFold(g.received.map((r) => r.dmarc_pass)),
    });
  }
  return out;
}

/** The subset of silver columns the gold blend needs (from the latest test per account). */
export interface LatestEspRow {
  inboxCount: number;
  spamCount: number;
  missingCount: number;
  seedTotal: number;
  testedAt: Date;
}

/**
 * Blend one account's latest-test ESP rows into the single per-account
 * `inboxPlacement` figure. Sums counts across ESPs, recomputes the percentages
 * from the pooled totals, and takes the newest `testedAt`. Returns null when
 * there is no data or the pooled seed total is 0 (never a fabricated 0%).
 */
export function blendEspRows(rows: LatestEspRow[]): InboxPlacement | null {
  if (rows.length === 0) return null;
  const seedTotal = rows.reduce((s, r) => s + r.seedTotal, 0);
  if (seedTotal === 0) return null;

  const inbox = rows.reduce((s, r) => s + r.inboxCount, 0);
  const spam = rows.reduce((s, r) => s + r.spamCount, 0);
  const missing = rows.reduce((s, r) => s + r.missingCount, 0);
  const testedAt = rows.reduce(
    (max, r) => (r.testedAt > max ? r.testedAt : max),
    rows[0].testedAt,
  );

  return {
    inboxPct: pct(inbox, seedTotal),
    spamPct: pct(spam, seedTotal),
    missingPct: pct(missing, seedTotal),
    testedAt: testedAt.toISOString(),
  };
}

/**
 * Pick the daily placement-test batch: the ~1/7 of the testable pool that is
 * LEAST recently tested, so each account is tested ~once per week WITHOUT any
 * cursor/bucket state — the rotation is derived purely from each account's last
 * test date (silver `MAX(tested_at)`), self-balancing as the pool changes.
 * Never-tested accounts (`null`) sort FIRST (they need the test most to earn
 * promotion out of in_recovery). Ties break by email for determinism.
 *
 * `N = ceil(pool / weeklyDivisor)` guarantees full weekly coverage even with
 * rounding; a pool smaller than the divisor just gets tested more often. Pure —
 * no IO. The DAILY cron calling this gives each account a weekly cadence, keeping
 * the placement seed volume (~30-50 sends/account) to one day per week (best
 * practice — a mailbox's safe daily volume is ~50, already used by 40 send + 10
 * warmup, so a test can't run daily on top).
 */
export function selectDailyTestBatch(
  pool: string[],
  lastTestedByEmail: Map<string, Date | null>,
  weeklyDivisor = 7,
): string[] {
  if (pool.length === 0) return [];
  const n = Math.ceil(pool.length / weeklyDivisor);
  const sorted = [...pool].sort((a, b) => {
    const ta = lastTestedByEmail.get(a) ?? null;
    const tb = lastTestedByEmail.get(b) ?? null;
    if (ta === null && tb === null) return a < b ? -1 : a > b ? 1 : 0;
    if (ta === null) return -1; // never-tested first
    if (tb === null) return 1;
    const diff = ta.getTime() - tb.getTime();
    return diff !== 0 ? diff : a < b ? -1 : a > b ? 1 : 0;
  });
  return sorted.slice(0, n);
}
