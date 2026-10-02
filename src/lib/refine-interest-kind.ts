/**
 * Instantly says "interested"; our classifier can say WHICH kind of interest.
 *
 * Instantly's own verdict knows one positive kind, `lead_interested`. Our reply
 * vocabulary (lib/reply-kind) has two finer ones that change what the client is
 * told and how loudly: `lead_info_requested` ("send me more information") and
 * `lead_meeting_requested` ("let's talk Tuesday"). Prod 2026-10-01: Dr. Joe
 * (Doc Dinners) wrote "Send me more information on how it works."; our
 * classifier read `lead_info_requested` (confidence 1.0, twice), Instantly said
 * `lead_interested`, and Instantly's word drove every side effect: the client
 * was congratulated as if a call had been booked.
 *
 * So at the ingestion choke point (`promoteEvent`), BEFORE the event is written
 * and before any side effect reads it, Instantly's plain `lead_interested` is
 * replaced by the finer positive kind our classifier reads in the prospect's
 * latest mirrored reply. Like deal-progress resolution: write-time, never
 * read-time; bronze keeps Instantly's raw payload.
 *
 * ⚠️ ONLY WITHIN THE POSITIVE KINDS. A classifier that reads something else
 * (neutral, negative, an opt-out) does NOT overrule Instantly here: the opt-out
 * has its own consent path (lib/reply-opt-out) and a sentiment flip is a
 * different decision. Every stat is unchanged by construction: the three kinds
 * all project to `positive` (REPLY_KIND_CLASSIFICATION) and all count in
 * `repliesPositive`.
 *
 * ⚠️ A REDELIVERY ADOPTS THE FIRST ANSWER. The same webhook delivered twice,
 * or the reconcile poll re-reading the same `timestamp_last_interest_change`,
 * must dedupe on the silver unique index, which includes `event_type`. A
 * refinement that answered differently from what is already recorded would
 * insert a second event and re-fire every side effect (an answer to a dead
 * conversation). So ANY positive interest kind already recorded at the same
 * (campaign, lead, timestamp), `lead_interested` included (every row written
 * before this module existed), is adopted without asking the model.
 *
 * Instantly-held sequences only (self-send replies are classified by our own
 * poller already) and only Instantly's verdicts (`webhook`, `poll_*`); a kind a
 * person stated (`manual`) is never second-guessed. Fail-soft: any failure keeps
 * Instantly's verdict and says so; this runs inside Instantly's webhook.
 */

import { and, eq, inArray, sql } from "drizzle-orm";

import { db } from "../db";
import { instantlyEvents } from "../db/schema";
import { isInstantlyHeldCampaignId, maybeMirrorCampaignEmails, type MirrorableCampaign } from "./mirror-emails";
import { fetchLatestMirroredInbound, type MirroredInbound } from "./reply-opt-out";
import { qualifyReply, type QualificationEventType } from "./self-send/qualify-reply";

/** The kinds Instantly's plain "interested" may be refined into. */
export const REFINED_INTEREST_KINDS = ["lead_info_requested", "lead_meeting_requested"] as const;

/** Event sources that carry Instantly's OWN verdict. */
const INSTANTLY_VERDICT_SOURCES = new Set(["webhook", "poll_emails", "poll_leads"]);

export interface RefineInterestInput {
  eventType: string;
  source: string;
  inferred?: boolean;
  leadEmail: string | null;
  timestamp: Date;
}

export interface RefineInterestResult {
  eventType: string;
  /**
   * The classification read on the way, with the message it was read on, so the
   * opt-out hook does not pay a second model call for the same words.
   */
  reading: { qualification: QualificationEventType | null; inbound: MirroredInbound } | null;
}

/** Pure: is this event one Instantly's verdict may be refined on? */
export function isRefinableInterest(input: RefineInterestInput, instantlyCampaignId: string): boolean {
  return (
    input.eventType === "lead_interested" &&
    INSTANTLY_VERDICT_SOURCES.has(input.source) &&
    !input.inferred &&
    Boolean(input.leadEmail) &&
    isInstantlyHeldCampaignId(instantlyCampaignId)
  );
}

/** Pure: the kind to record, given Instantly's and our classifier's. */
export function refinedKind(instantlyKind: string, qualification: string | null): string {
  return (REFINED_INTEREST_KINDS as readonly string[]).includes(qualification ?? "")
    ? (qualification as string)
    : instantlyKind;
}

/** The interest kind already recorded for this exact delivery, if any. */
async function adoptedKind(instantlyCampaignId: string, leadEmail: string, timestamp: Date): Promise<string | null> {
  const rows = await db
    .select({ eventType: instantlyEvents.eventType })
    .from(instantlyEvents)
    .where(
      and(
        eq(instantlyEvents.campaignId, instantlyCampaignId),
        eq(instantlyEvents.leadEmail, leadEmail),
        eq(instantlyEvents.timestamp, timestamp),
        inArray(instantlyEvents.eventType, ["lead_interested", ...REFINED_INTEREST_KINDS]),
        sql`${instantlyEvents.inferred} = false`,
      ),
    )
    .limit(1);
  return rows[0]?.eventType ?? null;
}

export async function refineInstantlyInterest(
  campaign: MirrorableCampaign,
  input: RefineInterestInput,
): Promise<RefineInterestResult> {
  const keep: RefineInterestResult = { eventType: input.eventType, reading: null };
  if (!isRefinableInterest(input, campaign.instantlyCampaignId)) return keep;
  const leadEmail = input.leadEmail as string;

  try {
    const adopted = await adoptedKind(campaign.instantlyCampaignId, leadEmail, input.timestamp);
    if (adopted) return { eventType: adopted, reading: null };

    // The words first: a qualification can beat the mirror to the reply.
    let inbound = await fetchLatestMirroredInbound(campaign.instantlyCampaignId);
    if (!inbound) {
      await maybeMirrorCampaignEmails(campaign, input.eventType);
      inbound = await fetchLatestMirroredInbound(campaign.instantlyCampaignId);
    }
    if (!inbound) {
      console.warn(
        `[instantly-service] refine-interest: no readable reply for campaign=${campaign.instantlyCampaignId} lead=${leadEmail}; keeping Instantly's lead_interested`,
      );
      return keep;
    }

    const qualification = await qualifyReply(inbound.text, {
      subject: inbound.subject,
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail,
      source: "interest_refinement",
    });
    const eventType = refinedKind(input.eventType, qualification);
    if (eventType !== input.eventType) {
      console.log(
        `[instantly-service] refine-interest: campaign=${campaign.instantlyCampaignId} lead=${leadEmail} Instantly said lead_interested, the reply reads ${eventType}; recording ${eventType}`,
      );
    }
    return { eventType, reading: { qualification, inbound } };
  } catch (error: unknown) {
    console.warn(
      `[instantly-service] refine-interest: could not read campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${error instanceof Error ? error.message : String(error)}; keeping Instantly's lead_interested`,
    );
    return keep;
  }
}
