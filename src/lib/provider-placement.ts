/**
 * Where each receiving provider puts a sending mailbox's mail, and which emails
 * that mailbox may therefore send to a prospect that provider hosts (pure — no
 * IO). MS-SEED-PLACEMENT-1018, generalized by SENDER-HEALTH-1019.
 *
 * Three providers, measured apart because they filter apart:
 *   - `google`: our Google Workspace seed receivers (ESP 1).
 *   - `microsoft`: a Microsoft 365 BUSINESS tenant seed receiver (ESP 2).
 *   - `microsoft_consumer`: an Outlook.com seed receiver (ESP 4).
 *
 * ⚠️ MICROSOFT 365 AND OUTLOOK.COM ARE TWO FILTERS, NOT ONE. Prod 2026-10-19,
 * first two runs with both kinds of receiver: the Microsoft 365 tenant put 46 of
 * 46 seeds in Inbox (Primeforge 41/41, Gandi 5/5, all SCL 1), while Outlook.com
 * junked 160 of 186 (SCL 6), and 19 of the 46 senders it junked at 11:04 it put
 * in Inbox at 11:22 with the same body (zero went the other way). Pooled into one
 * "Microsoft" verdict, the consumer noise barred 16 production mailboxes from the
 * 703 Microsoft 365 domains we write to (2,782 prospect emails in 30 days),
 * while Outlook.com prospects were 4 emails of 24,118. Each kind now decides for
 * its own prospects only.
 *
 * Two rules, both per (mailbox, provider of the prospect):
 *   1. A FIRST email needs the mailbox measured Inbox there (>= 90 % of the seeds
 *      found, PRODUCTION_DELIVERY_PCT_BAR). Junk never; unmeasured only in cold
 *      start (no production mailbox measured Inbox at that provider yet).
 *      Deliverability practice: "seed-test a mailbox at each provider before
 *      sending there, never send where it is junked" (GlockApps, Mailreach,
 *      Instantly, Lemlist placement guides).
 *   2. NO email at all, follow-ups included, from a mailbox measured MOSTLY in
 *      spam there (more than half of at least MOSTLY_SPAM_MIN_FOUND_SEEDS seeds
 *      found). Its follow-up would land in spam anyway and every such landing
 *      costs the domain reputation; a follow-up is never moved to another sender
 *      (a thread keeps one sender), so it waits for the mailbox to measure better.
 *
 * Google first emails were already gated by the lifecycle (only `in_production`
 * mailboxes take new sequences, >= 90 % at Google); rule 1 now also covers a
 * first email queued BEFORE its mailbox was demoted (135 left from demoted
 * mailboxes 2026-10-16..19).
 */

import {
  DELIVERY_EVIDENCE_MAX_AGE_DAYS,
  PRODUCTION_DELIVERY_PCT_BAR,
} from "./account-lifecycle";

export type RecipientProvider = "google" | "microsoft" | "microsoft_consumer";

export const RECIPIENT_PROVIDERS: readonly RecipientProvider[] = ["google", "microsoft", "microsoft_consumer"];

/**
 * Evidence is pooled newest test first until it holds this many seeds: three
 * full Google receiver sets (MAX_SEED_RECEIVERS = 10), i.e. about three weekly
 * tests, still bounded by DELIVERY_EVIDENCE_MAX_AGE_DAYS.
 *
 * ⚠️ ONE TEST, EVEN A FULL ONE, MUST NOT OVERTURN THE HISTORY BEFORE IT.
 * At 10 seeds (SENDER-HEALTH-1019) a single 10/10 Sunday stood alone: on
 * 2026-10-25, 37 mailboxes at 0-70 % Inbox all October were promoted to
 * production on one good test and started 358 sequences in three days
 * (kevin.lourd@growthagency.news: 1/10, 0/10, 5/10, 4/6, 5/10, then 10/10).
 * Before that, at 1 seed, klourd@growthagency.agency was promoted on a
 * throttled 1/1 after five tests at 0-10 %. At 30 seeds the 90 % bar allows at
 * most 3 spam seeds over three tests: a mailbox earns production by staying
 * in the Inbox, not by one lucky run.
 */
export const PLACEMENT_EVIDENCE_MIN_SEEDS = 30;

/**
 * "Mostly spam" (rule 2) needs at least this many FOUND seeds: one Junk seed is
 * no proof, one full Google set mostly in Junk is.
 */
export const MOSTLY_SPAM_MIN_FOUND_SEEDS = 10;

/** One placement test of one mailbox at one provider. */
export interface PlacementTest {
  testId: string;
  testedAt: Date;
  seedTotal: number;
  inboxCount: number;
  spamCount: number;
}

export interface PooledPlacement {
  seedTotal: number;
  inboxCount: number;
  spamCount: number;
  /** When the NEWEST pooled test ran (the freshness of the evidence). */
  testedAt: Date;
  testIds: string[];
}

/**
 * The evidence a verdict reads: the newest test, then older ones still under
 * DELIVERY_EVIDENCE_MAX_AGE_DAYS, until PLACEMENT_EVIDENCE_MIN_SEEDS seeds are
 * held. The newest test is always included (a mailbox with only a small test
 * still grades: sample size is not a gate, it only stops a small test from
 * erasing a bigger history). Null when there is no test at all.
 */
export function poolPlacementTests(
  tests: readonly PlacementTest[],
  asOf: Date,
  minSeeds: number = PLACEMENT_EVIDENCE_MIN_SEEDS,
): PooledPlacement | null {
  const ordered = [...tests]
    .filter((t) => !Number.isNaN(t.testedAt.getTime()))
    .sort((a, b) => b.testedAt.getTime() - a.testedAt.getTime() || (a.testId < b.testId ? 1 : -1));
  if (ordered.length === 0) return null;
  const maxAgeMs = DELIVERY_EVIDENCE_MAX_AGE_DAYS * 86_400_000;
  const pooled: PooledPlacement = {
    seedTotal: 0,
    inboxCount: 0,
    spamCount: 0,
    testedAt: ordered[0].testedAt,
    testIds: [],
  };
  for (const [i, test] of ordered.entries()) {
    if (i > 0 && (pooled.seedTotal >= minSeeds || asOf.getTime() - test.testedAt.getTime() > maxAgeMs)) break;
    pooled.seedTotal += test.seedTotal;
    pooled.inboxCount += test.inboxCount;
    pooled.spamCount += test.spamCount;
    pooled.testIds.push(test.testId);
  }
  return pooled;
}

/**
 * `inbox`: at least PRODUCTION_DELIVERY_PCT_BAR % of the seeds the provider
 * delivered sat in its Inbox, on fresh evidence. `junk`: fresh evidence below.
 * `unmeasured`: no seed found, or evidence older than DELIVERY_EVIDENCE_MAX_AGE_DAYS.
 */
export type PlacementVerdict = "inbox" | "junk" | "unmeasured";

export interface ProviderPlacement {
  verdict: PlacementVerdict;
  /**
   * More than half of at least MOSTLY_SPAM_MIN_FOUND_SEEDS found seeds sat in
   * spam, on fresh evidence: the mailbox sends nothing to this provider.
   */
  mostlySpam: boolean;
}

/**
 * The verdict for one mailbox at one provider.
 *
 * ⚠️ OVER SEEDS FOUND, NOT SEEDS SENT. A seed nobody found yet is usually a read
 * that has not happened (a receiver token lapsed, a sync not yet run); scoring it
 * as spam would hold every prospect of the fleet on a reader outage. The
 * lifecycle gate keeps the stricter missing-counts-against reading.
 */
export function providerPlacement(evidence: PooledPlacement | null, asOf: Date): ProviderPlacement {
  const unmeasured: ProviderPlacement = { verdict: "unmeasured", mostlySpam: false };
  if (!evidence) return unmeasured;
  const ageDays = (asOf.getTime() - evidence.testedAt.getTime()) / 86_400_000;
  if (Number.isNaN(ageDays) || ageDays > DELIVERY_EVIDENCE_MAX_AGE_DAYS) return unmeasured;
  const found = evidence.inboxCount + evidence.spamCount;
  if (found <= 0) return unmeasured;
  return {
    verdict: evidence.inboxCount * 100 >= found * PRODUCTION_DELIVERY_PCT_BAR ? "inbox" : "junk",
    mostlySpam: found >= MOSTLY_SPAM_MIN_FOUND_SEEDS && evidence.spamCount * 2 > found,
  };
}

/**
 * No production mailbox is measured Inbox at this provider yet: nobody to
 * prefer, so an unmeasured mailbox may carry a first email there (holding every
 * such prospect until a test runs would stop mail that goes out today).
 */
export function isColdStart(placements: Iterable<ProviderPlacement | undefined>): boolean {
  for (const p of placements) if (p?.verdict === "inbox") return false;
  return true;
}

/** Rule 1: may this mailbox send a FIRST email to a prospect at this provider? */
export function mayWriteFirstEmail(placement: ProviderPlacement | undefined, coldStart: boolean): boolean {
  if (placement?.mostlySpam) return false;
  if (placement?.verdict === "inbox") return true;
  if (placement?.verdict === "junk") return false;
  return coldStart;
}

/** Rule 2: may this mailbox send ANY email (a follow-up) to a prospect at this provider? */
export function mayWriteFollowUp(placement: ProviderPlacement | undefined): boolean {
  return placement?.mostlySpam !== true;
}
