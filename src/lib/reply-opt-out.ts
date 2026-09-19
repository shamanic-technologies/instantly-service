/**
 * A prospect who ASKS to stop, in the body of a reply.
 *
 * The unsubscribe link is not how people usually leave. They answer the email
 * and write "please remove me from your list" — and until now that sentence
 * went nowhere. Instantly classifies such a reply `lead_not_interested`, which
 * is accurate as a SENTIMENT and wrong as a CONSENT fact: this repo documents
 * `lead_not_interested` as the recyclable bucket, so the person who told us to
 * stop was filed as re-contactable in three months.
 *
 * Measured in prod 2026-09-17 over the mirrored Unibox: SEVEN leads had written
 * an explicit removal request, ZERO carried `lead_unsubscribed`, all seven read
 * `lead_not_interested`, and one of the seven still sat on an ACTIVE campaign.
 *
 * ⚠️ THIS IS NOT AN INFERENCE FROM SENTIMENT, and the distinction is what makes
 * it legitimate to write a consent record from a machine classification. The
 * consent module's own invariant bans deriving an opt-out from "a reply, a
 * bounce, a sentiment or a silence" — from the ABSENCE of engagement or from a
 * mood. Here the prospect states it in words, and those words are stored on the
 * record. A sentence somebody wrote is stronger evidence than a staff member's
 * recollection of a phone call, which the same module accepts without question.
 *
 * ⚠️ IT ROUTES THROUGH `recordLeadOptOut`, NOT through a direct
 * `lead_unsubscribed` promotion. Reuse is what buys the four things a bespoke
 * promotion would have had to re-implement: PERSON scope (every campaign this
 * org holds for the address, not only the one that received the reply), the
 * pause at the sender, withdrawal through the endpoint that already exists, and
 * the append-only consent log. A classifier can be wrong, so the ability to take
 * it back is not optional.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { htmlToText } from "./forward-positive-reply";
import { recordLeadOptOut, findStandingOptOut } from "./lead-optouts";
import { qualifyReply, type QualificationEventType } from "./self-send/qualify-reply";
import { isInstantlyHeldCampaignId, MIRRORED_INBOUND_EVENT_TYPES } from "./mirror-emails";

/** The single label that means "they asked us to stop". */
export const OPT_OUT_REPLY_KIND = "lead_opt_out_requested";

/**
 * How a recorded opt-out that came from a reply identifies itself.
 *
 * `channel` is an existing member of the consent vocabulary — the person did
 * tell us by email reply. `statedBy` names the classifier rather than a person,
 * so the audit never claims a human read it.
 */
export const REPLY_OPT_OUT_STATED_BY = "reply-classifier";

/** node-postgres resolves `db.execute` to a QueryResult object, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** The campaign fields this needs. A subset of the silver row, by design. */
export interface OptOutCandidateCampaign {
  instantlyCampaignId: string;
  leadEmail: string | null;
  orgId: string | null;
}

export interface MirroredInbound {
  instantlyEmailId: string | null;
  text: string;
}

/**
 * The prospect's most recent inbound message on this sequence, out of the
 * bronze mirror of Instantly's Unibox.
 *
 * `ue_type <> '1'` is the inbound filter the rest of this repo uses (type 1 is
 * outbound). Newest first because an opt-out request is the last thing someone
 * says, and because an older message is one a previous run already judged.
 */
export async function fetchLatestMirroredInbound(
  instantlyCampaignId: string,
): Promise<MirroredInbound | null> {
  const result = await db.execute(sql`
    SELECT instantly_email_id,
           payload->'body'->>'text' AS body_text,
           payload->'body'->>'html' AS body_html
    FROM instantly_emails_raw
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND payload->>'ue_type' <> '1'
    ORDER BY payload->>'timestamp_created' DESC
    LIMIT 1
  `);

  const [row] = rowsOf(result);
  if (!row) return null;

  const plain = typeof row.body_text === "string" ? row.body_text : "";
  const html = typeof row.body_html === "string" ? row.body_html : "";
  const text = (plain.trim() || htmlToText(html)).trim();
  if (!text) return null;

  return {
    instantlyEmailId: typeof row.instantly_email_id === "string" ? row.instantly_email_id : null,
    text,
  };
}

export interface RecordOptOutFromReplyInput {
  campaign: OptOutCandidateCampaign;
  /** What the prospect wrote. Quoted history is stripped by the classifier. */
  replyText: string;
  /** Already-known classification, when the caller has one. Skips the LLM call. */
  qualification?: QualificationEventType | null;
  /** Free-form provenance stored on the consent record. */
  evidence: Record<string, unknown>;
}

export type RecordOptOutFromReplyResult =
  | { recorded: true; campaignsAffected: number }
  | { recorded: false; reason: "not_an_opt_out" | "already_standing" | "no_org" | "unqualified" };

/**
 * Classify one reply and, when it asks us to stop, record the opt-out.
 *
 * FAIL LOUD on a classification error — the CALLERS swallow, because each of
 * them has already committed the fact that the reply arrived and one of them
 * runs inside Instantly's webhook (a 5xx there counts toward disabling the whole
 * subscription, which has cost this service a six-day outage). Keeping the throw
 * here means a sweep can still report the failure honestly rather than counting
 * it as "no opt-out found".
 *
 * An UNUSABLE classification records nothing and says so. Defaulting to "not an
 * opt-out" would be the same silent-fallback this repo bans everywhere else,
 * pointed at the one fact with a legal consequence.
 */
export async function recordOptOutFromReply(
  input: RecordOptOutFromReplyInput,
): Promise<RecordOptOutFromReplyResult> {
  const { campaign } = input;
  if (!campaign.orgId || !campaign.leadEmail) return { recorded: false, reason: "no_org" };

  const qualification =
    input.qualification !== undefined
      ? input.qualification
      : await qualifyReply(input.replyText, {
          instantlyCampaignId: campaign.instantlyCampaignId,
          leadEmail: campaign.leadEmail,
          source: "reply_opt_out",
        });

  if (qualification === null) return { recorded: false, reason: "unqualified" };
  if (qualification !== OPT_OUT_REPLY_KIND) return { recorded: false, reason: "not_an_opt_out" };

  const result = await recordLeadOptOut({
    orgId: campaign.orgId,
    leadEmail: campaign.leadEmail,
    channel: "email_reply",
    statedBy: REPLY_OPT_OUT_STATED_BY,
    notes: input.replyText.slice(0, 500),
    payload: { ...input.evidence, classification: qualification },
  });

  if (!result.recorded) return { recorded: false, reason: "already_standing" };

  console.log(
    `[instantly-service] reply-opt-out: recorded from a reply — lead=${campaign.leadEmail} campaign=${campaign.instantlyCampaignId} campaigns=${result.campaignsAffected} stopped=${result.campaignsStopped}`,
  );
  return { recorded: true, campaignsAffected: result.campaignsAffected };
}

/**
 * The `promoteEvent` side effect: read the prospect's latest mirrored reply and
 * record an opt-out if that is what it asks for.
 *
 * ⚠️ GATED ON THE MIRROR'S OWN EVENT SET, not on `reply_received` alone. A reply
 * webhook can arrive before Instantly's `/emails` holds the message, so the
 * mirror taken at that moment may not contain it — and side effects fire only on
 * the FIRST promotion of each event, so a single-event gate would get one
 * attempt and never retry. Instantly emits its own qualification as a separate
 * event moments later, by which point the body is there. Several attempts, and
 * `recordLeadOptOut` is idempotent per (org, lead), so the extra ones cost one
 * cheap read each.
 *
 * ⚠️ A `self:` SEQUENCE IS SKIPPED — the IMAP poller owns it, holds the message
 * text already, and has a classification in hand. Running here too would pay a
 * second model call to answer the same question.
 *
 * Fail-soft: this runs inside the webhook, so it must never throw.
 */
export async function maybeRecordOptOutFromReply(
  campaign: OptOutCandidateCampaign,
  eventType: string,
): Promise<void> {
  if (!MIRRORED_INBOUND_EVENT_TYPES.has(eventType)) return;
  if (!isInstantlyHeldCampaignId(campaign.instantlyCampaignId)) return;
  if (!campaign.orgId || !campaign.leadEmail) return;

  try {
    // Cheapest guard first: a person who already stands opted out needs no
    // second reading, and this saves the model call on every later event of a
    // sequence we have already stopped.
    const standing = await findStandingOptOut(campaign.orgId, campaign.leadEmail);
    if (standing) return;

    const inbound = await fetchLatestMirroredInbound(campaign.instantlyCampaignId);
    if (!inbound) return;

    await recordOptOutFromReply({
      campaign,
      replyText: inbound.text,
      evidence: {
        source: "mirrored_reply",
        eventType,
        instantlyCampaignId: campaign.instantlyCampaignId,
        instantlyEmailId: inbound.instantlyEmailId,
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] reply-opt-out: could not judge campaign=${campaign.instantlyCampaignId} on ${eventType} — ${message}; the reply stands, any opt-out in it is unrecorded`,
    );
  }
}
