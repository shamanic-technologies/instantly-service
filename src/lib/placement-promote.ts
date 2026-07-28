/**
 * Inbox-placement promotion logic (pure — no IO). Turns raw
 * inbox-placement-analytics rows (bronze) into per-(test, account, ESP) silver
 * results, and summarizes the latest test's ESP rows into the per-account
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
import type { EspPlacement, InboxPlacement } from "./account-health";
import { isGatedEspRow } from "./account-lifecycle";

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

/** The subset of silver columns the gold summary needs (from one test, per ESP). */
export interface LatestEspRow {
  recipientEsp: number;
  inboxCount: number;
  spamCount: number;
  missingCount: number;
  seedTotal: number;
  testedAt: Date;
}

/**
 * Summarize one test's ESP rows for an account into the `inboxPlacement` gold
 * figure. The headline percentages are the WORST gated ESP leg — the same leg the
 * lifecycle delivery gate reads (`isDeliveryAtBar`) — NOT a blend across ESPs.
 *
 * This used to blend (pooled inbox / pooled seeds), which produced an incoherent
 * ops row: an account inboxing 91% on Gmail and 100% on Outlook displayed "95%
 * inbox" next to `delivery_below_bar` on a 95% bar. Both numbers could not be true
 * at once. Reading the gate's own leg makes the displayed number and the lifecycle
 * reason agree by construction; `perEsp` shows where the drop is.
 *
 * When no leg is gradable (every leg under the seed floor) the headline falls back
 * to the worst leg overall — still a real measured leg, never a fabricated pooled
 * number. Returns null when there is no data or every leg has 0 seeds.
 */
export function summarizeEspRows(rows: LatestEspRow[]): InboxPlacement | null {
  const seeded = rows.filter((r) => r.seedTotal > 0);
  if (seeded.length === 0) return null;

  const perEsp: EspPlacement[] = seeded
    .map((r) => ({
      recipientEsp: r.recipientEsp,
      seedTotal: r.seedTotal,
      inboxPct: pct(r.inboxCount, r.seedTotal),
      spamPct: pct(r.spamCount, r.seedTotal),
      missingPct: pct(r.missingCount, r.seedTotal),
      gated: isGatedEspRow(r),
    }))
    .sort((a, b) => a.recipientEsp - b.recipientEsp);

  // The gate ignores under-seeded legs, so the headline must too — else the row
  // would again show a number the lifecycle reason contradicts.
  const candidates = perEsp.filter((e) => e.gated);
  const pool = candidates.length > 0 ? candidates : perEsp;
  const worst = pool.reduce((min, e) => (e.inboxPct < min.inboxPct ? e : min), pool[0]);

  const testedAt = seeded.reduce(
    (max, r) => (r.testedAt > max ? r.testedAt : max),
    seeded[0].testedAt,
  );

  return {
    inboxPct: worst.inboxPct,
    spamPct: worst.spamPct,
    missingPct: worst.missingPct,
    testedAt: testedAt.toISOString(),
    perEsp,
  };
}
