/**
 * Mirroring a lead's conversation into bronze AS IT HAPPENS, and reading it back
 * out of our own store.
 *
 * Instantly's cancel dialog says it plainly: cancelling a plan — or a single
 * inbox — permanently deletes every conversation those mailboxes sent and
 * received, lead replies included. Silver records THAT a lead replied; the words
 * they wrote live only in bronze `instantly_emails_raw`. So the day the plan is
 * cancelled, anything that reads a thread LIVE from Instantly goes blank at once
 * for every campaign this service has ever run, and at that point the words are
 * unrecoverable rather than merely unavailable.
 *
 * ⚠️ THE MIRROR HAD A HOLE EXACTLY WHERE THE COMMON CASE IS. Until now the only
 * thing that fetched a reply's BODY during normal operation was phase 3 of the
 * reconcile poll, which runs only for a campaign that DRIFTS on its counts. A
 * reply delivered by webhook updates our own event log immediately, so the
 * campaign does not drift, so its body was never fetched — the cleanly-delivered
 * reply, i.e. nearly all of them, was the one case never mirrored. Measured on
 * the fleet before the one-shot sweep: 359 inbound messages mirrored against 229
 * recorded reply events, newest mirrored inbound 2026-09-02 while replies kept
 * arriving through 2026-09-04.
 *
 * The fix is one fail-soft side effect on the ingestion choke point: when a real
 * event says an inbound message exists, copy that campaign's whole thread into
 * bronze. Both ingestion paths (webhook and reconcile poll) converge on
 * `promoteEvent`, so both are covered by one call.
 *
 * ⚠️ IT MIRRORS THE WHOLE THREAD, NOT JUST THE REPLY. This is a photocopy taken
 * before the original is destroyed: we cannot go back for more later, and what
 * a prospect wrote only makes sense beside what we said. Do NOT narrow it to
 * what some consumer asks for today.
 *
 * Bronze only. Nothing is promoted to silver and no event is synthesized — the
 * events already exist (they are what triggered this); this stores their words.
 * Spends no metered cost: a read against a flat subscription, same reasoning as
 * the Unibox backfill and the placement tests.
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { insertEmailsBatch } from "./bronze";
import { listEmails, type EmailRecord } from "./instantly-client";
import {
  resolveInstantlyApiKey,
  resolvePlatformInstantlyApiKey,
  type CallerInfo,
} from "./key-client";
import { REPLY_KINDS } from "./reply-kind";
import { isSelfSendCampaignId } from "./self-send/transport";

const CALLER: CallerInfo = { method: "POST", path: "/internal/mirror-emails" };

/** A reservation sentinel is an in-flight claim, not an Instantly campaign. */
const RESERVATION_PREFIX = "reserving:";

/**
 * The real events that mean an inbound message exists in the Unibox.
 *
 * Every reply kind is included, not only `reply_received`: Instantly emits the
 * qualification (`lead_interested`, `lead_out_of_office`, …) as its own event,
 * and a mirror keyed on one event type would miss a reply whose `reply_received`
 * we never received. Side effects fire only on the FIRST promotion of each
 * event, so the extra types cost at most a couple of reads per lead — and a read
 * against a flat subscription is not quota we are spending per page view.
 */
export const MIRRORED_INBOUND_EVENT_TYPES: ReadonlySet<string> = new Set<string>([
  "reply_received",
  "email_bounced",
  ...REPLY_KINDS,
]);

/** A campaign whose thread Instantly holds. */
export interface MirrorableCampaign {
  instantlyCampaignId: string;
  orgId: string | null;
  userId: string | null;
}

/**
 * True when Instantly is the one holding this campaign's mail.
 *
 * A `self:` sequence was dispatched by us and both halves are already in bronze;
 * a `reserving:` sentinel is not an Instantly id at all and `GET /campaigns/…`
 * 400s on it. Same rule every other Instantly-calling sweep follows.
 */
export function isInstantlyHeldCampaignId(instantlyCampaignId: string): boolean {
  return (
    !isSelfSendCampaignId(instantlyCampaignId) &&
    !instantlyCampaignId.startsWith(RESERVATION_PREFIX)
  );
}

/**
 * Copy a campaign's whole Instantly thread into bronze. Returns how many rows
 * were NEW (the insert conflicts on `instantly_email_id` and does nothing), so a
 * re-run honestly reports what it added.
 *
 * Throws on a key or Instantly failure — the CALLER decides whether that is
 * fatal. On the ingestion path it is not: the event itself is already recorded.
 */
export async function mirrorCampaignEmails(
  campaign: MirrorableCampaign,
): Promise<number> {
  // A platform send belongs to no org, so there is no org key to resolve — its
  // thread lives in the shared cold-email workspace and is just as destroyed by
  // a cancellation, so it is mirrored on the platform key rather than skipped.
  const key = campaign.orgId
    ? (
        await resolveInstantlyApiKey(
          campaign.orgId,
          campaign.userId ?? "system",
          CALLER,
        )
      ).key
    : await resolvePlatformInstantlyApiKey(CALLER);

  const records = await listEmails(key, {
    campaignId: campaign.instantlyCampaignId,
  });
  const stored = await insertEmailsBatch(
    campaign.instantlyCampaignId,
    campaign.orgId,
    records,
  );
  return stored.length;
}

/**
 * Mirror the thread when a real event says there is inbound mail to mirror.
 * No-op on any other event, on a sequence Instantly never carried, and on an
 * inferred event (which asserts a message nobody witnessed).
 *
 * Fail-soft and never throws: this runs inside `promoteEvent`, whose primary job
 * is recording the event, and on the webhook path a throw would become a 5xx
 * that Instantly counts toward disabling the whole subscription.
 */
export async function maybeMirrorCampaignEmails(
  campaign: MirrorableCampaign,
  eventType: string,
): Promise<void> {
  if (!MIRRORED_INBOUND_EVENT_TYPES.has(eventType)) return;
  if (!isInstantlyHeldCampaignId(campaign.instantlyCampaignId)) return;

  try {
    const stored = await mirrorCampaignEmails(campaign);
    console.log(
      `[instantly-service] mirror-emails: stored ${stored} new message(s) for campaign=${campaign.instantlyCampaignId} on ${eventType}`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] mirror-emails: could not mirror campaign=${campaign.instantlyCampaignId} on ${eventType} — ${message}; the event stands, the words may be missing`,
    );
  }
}

/** node-postgres resolves `db.execute` to a QueryResult object, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * Every mirrored message of one campaign, straight out of bronze.
 *
 * The stored payload IS the Instantly email record, so it drops into the same
 * `selectThreadMessages` the live path uses — one thread renderer, whichever
 * side the rows came from.
 */
export async function fetchMirroredEmailRecords(
  instantlyCampaignId: string,
): Promise<EmailRecord[]> {
  const result = await db.execute(sql`
    SELECT payload
    FROM instantly_emails_raw
    WHERE instantly_campaign_id = ${instantlyCampaignId}
    ORDER BY fetched_at ASC
  `);
  return rowsOf(result)
    .map((row) => row.payload)
    .filter((p): p is EmailRecord => typeof p === "object" && p !== null);
}

/**
 * True when our OWN event log says mail was exchanged on this sequence.
 *
 * This is what keeps three facts apart once the mirror is the only source. An
 * empty mirror is ambiguous on its own — it could mean the sequence has sent
 * nothing yet, or that we hold mail we never copied. Silver answers it: a real
 * (`inferred = false`) send or inbound event is evidence that messages exist, so
 * an empty mirror there is INCOMPLETE, not empty, and must never be returned as
 * an empty conversation. An inferred event asserts a message nobody witnessed
 * and is deliberately not evidence.
 */
export async function hasExchangedMailEvidence(
  instantlyCampaignId: string,
): Promise<boolean> {
  const result = await db.execute(sql`
    SELECT 1 AS present
    FROM instantly_events
    WHERE campaign_id = ${instantlyCampaignId}
      AND inferred = false
      AND event_type IN ('email_sent', 'reply_received', 'auto_reply_received', 'email_bounced')
    LIMIT 1
  `);
  return rowsOf(result).length > 0;
}
