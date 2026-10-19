/**
 * Where Microsoft puts a sending mailbox's mail, and who may write to a
 * Microsoft-hosted prospect (pure — no IO). MS-SEED-PLACEMENT-1018.
 *
 * Our Google seeds scored alexander@plainsignalco.com 100% inbox while three
 * DefiniteJobs prospects at Microsoft (outlook.com, two Microsoft 365 tenants)
 * received its mail in Junk with SPF/DKIM/DMARC passing (prod 2026-10-18).
 * Google and Microsoft filter independently, so a Google score says nothing
 * about Microsoft. Each mailbox therefore carries a SEPARATE Microsoft verdict,
 * measured by our own Microsoft seed receivers, and it decides one thing only:
 * whether that mailbox may send a FIRST email to a prospect whose mail
 * Microsoft hosts.
 *
 * ⚠️ IT IS A ROUTING SIGNAL, NEVER A DEMOTION. The lifecycle delivery gate stays
 * on the Google receivers (`fetchLatestDeliveryByAccount` excludes ESP 2): a
 * mailbox Microsoft junks still lands in Inbox at Google, where most prospects
 * are, and demoting it would stop that mail for nothing. Pooling one or two
 * Microsoft seeds into the ~11-seed pooled score would demote on a single Junk.
 */

import {
  DELIVERY_EVIDENCE_MAX_AGE_DAYS,
  PRODUCTION_DELIVERY_PCT_BAR,
} from "./account-lifecycle";

/**
 * `inbox`: Microsoft put at least PRODUCTION_DELIVERY_PCT_BAR % of the seeds it
 * delivered in the Inbox (Focused or Other), on fresh evidence.
 * `junk`: fresh evidence below that bar.
 * `unmeasured`: no seed of the latest Microsoft test was found anywhere, or the
 * evidence is older than DELIVERY_EVIDENCE_MAX_AGE_DAYS.
 */
export type MicrosoftPlacementVerdict = "inbox" | "junk" | "unmeasured";

export interface MicrosoftPlacementEvidence {
  /** Seeds of the latest Microsoft test found in the Inbox (Focused + Other). */
  inboxCount: number;
  /** Seeds of the same test found in Junk. */
  spamCount: number;
  testedAt: Date | string | null;
}

/**
 * The verdict for one mailbox from its latest Microsoft test.
 *
 * ⚠️ OVER SEEDS FOUND, NOT SEEDS SENT. A seed nobody found yet says nothing about
 * Inbox vs Junk — it is usually a read that has not happened (a receiver whose
 * token lapsed, a sync not yet run). Scoring it as Junk would hold every
 * Microsoft prospect of the fleet on a reader outage. The lifecycle gate keeps
 * the stricter missing-counts-against reading; this one answers a narrower
 * question.
 */
export function microsoftPlacementVerdict(
  evidence: MicrosoftPlacementEvidence | null,
  asOf: Date,
): MicrosoftPlacementVerdict {
  if (!evidence || evidence.testedAt === null) return "unmeasured";
  const testedAt = new Date(evidence.testedAt);
  if (Number.isNaN(testedAt.getTime())) return "unmeasured";
  const ageDays = (asOf.getTime() - testedAt.getTime()) / 86_400_000;
  if (ageDays > DELIVERY_EVIDENCE_MAX_AGE_DAYS) return "unmeasured";
  const found = evidence.inboxCount + evidence.spamCount;
  if (found <= 0) return "unmeasured";
  return (evidence.inboxCount * 100) / found >= PRODUCTION_DELIVERY_PCT_BAR ? "inbox" : "junk";
}

/**
 * The fleet has no Microsoft evidence yet: not one production mailbox is
 * measured in Inbox at Microsoft.
 *
 * Cold-start rule (the choice and its sources are in the PR): deliverability
 * practice is "seed-test a mailbox at each provider before sending there, and
 * never send to a provider that junks you" (GlockApps, Mailreach, Instantly and
 * Lemlist placement guides all state it). So once ANY production mailbox is
 * measured in Inbox, only measured-Inbox mailboxes write to Microsoft prospects.
 * Before the first measurement exists there is nobody to prefer, and holding
 * every Microsoft prospect until a test runs would stop mail that goes out
 * today; then an unmeasured mailbox may carry it, exactly as before this rule.
 */
export function isMicrosoftColdStart(
  verdicts: Iterable<MicrosoftPlacementVerdict | undefined>,
): boolean {
  for (const verdict of verdicts) if (verdict === "inbox") return false;
  return true;
}

/**
 * May a mailbox with this verdict send a FIRST email to a Microsoft-hosted
 * prospect? Measured Inbox: yes. Measured Junk: never. Unmeasured: only while
 * the fleet is in cold start (see above).
 */
export function mayWriteToMicrosoftRecipient(
  verdict: MicrosoftPlacementVerdict | undefined,
  coldStart: boolean,
): boolean {
  if (verdict === "inbox") return true;
  if (verdict === "junk") return false;
  return coldStart;
}
