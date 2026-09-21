/**
 * A reply Instantly never gave a verdict on.
 *
 * Everything this service does with a reply hangs off the reply KIND, not off
 * `reply_received`: the forward to the agency inbox, the trigger of the campaign
 * bought to answer a buyer, the follow-up debt, and the call to the brand's rep
 * all gate on `isSalesInterestQualification` or on a sibling of it. Instantly
 * normally emits its own qualification seconds after the reply — measured over
 * 84 production pairs, 70 of them inside 60 seconds — and this service simply
 * trusts that verdict on the `instantly` transport.
 *
 * ⚠️ WHEN THAT SECOND EVENT NEVER ARRIVES, NOTHING HAPPENS AND NOTHING SAYS SO.
 * The reply is recorded, the campaign row reads `replied`, and every side effect
 * sits behind a gate that will never open. Measured 2026-09-21: of the 8 replies
 * in seven days, two carried no qualification at all — one a buyer asking for
 * pricing, the other a prospect who wrote a single word, "STOP!". The second had
 * been waiting 75 minutes with its opt-out unrecorded and its sequence running.
 *
 * So when Instantly stays silent, we classify the reply ourselves, with the
 * classifier this repo already owns and already trusts on the self-send
 * transport. Promoting the kind through `promoteEvent` is what re-opens every
 * gate at once — no side effect is re-implemented here.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { maybeMirrorCampaignEmails } from "./mirror-emails";
import { fetchLatestMirroredInbound } from "./reply-opt-out";
import { promoteEvent } from "./silver-promote";
import { QUALIFICATION_EVENT_TYPES, qualifyReply } from "./self-send/qualify-reply";

/**
 * How long Instantly gets to speak before we classify the reply ourselves.
 *
 * MEASURED, not chosen: over the 84 production pairs where Instantly did emit a
 * verdict, 70 arrived within 60 seconds, 74 within 15 minutes, and the remaining
 * 10 took longer (3 of them over an hour). Fifteen minutes therefore leaves
 * Instantly the window it actually uses while keeping a hot sales reply from
 * sitting unanswered for an hour.
 *
 * ⚠️ THE COST OF BEING EARLY IS REAL AND IS ACCEPTED. If Instantly speaks after
 * we have, its event is promoted too and the gold projection takes the latest —
 * but the SIDE EFFECTS fired on ours, so a rep may have been rung on our reading.
 * On the measured distribution that is ~7% of qualified replies, against a
 * failure mode where a buyer is never answered at all.
 */
export const QUALIFICATION_GRACE_MS = 15 * 60 * 1000;

/** How many replies one sweep classifies. Bounds the model spend per tick. */
export const QUALIFICATION_FALLBACK_BATCH = 20;

/** node-postgres resolves `db.execute` to a QueryResult object, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** One reply that has been waiting on a verdict nobody is going to send. */
export interface UnqualifiedReply {
  instantlyCampaignId: string;
  leadEmail: string;
  accountEmail: string | null;
  orgId: string | null;
  userId: string | null;
  repliedAt: Date;
}

/**
 * Replies with a real `reply_received` and no reply kind of any sort.
 *
 * ⚠️ THE `self:` AND `reserving:` SENTINELS ARE EXCLUDED, and for opposite
 * reasons. A `self:` sequence is the IMAP poller's: it reads the message itself
 * and classifies it in the same pass, so running here would pay a second model
 * call to answer a question already answered. A `reserving:` row is an in-flight
 * claim and not a sequence at all.
 *
 * ⚠️ IT ASKS FOR THE ABSENCE OF EVERY KIND, never of `lead_interested` alone.
 * A reply Instantly filed `lead_out_of_office` HAS a verdict, and re-reading it
 * would overrule better evidence (Instantly saw the real headers) with worse.
 *
 * Oldest first: a reply that has waited longest is the one most likely to still
 * be worth answering, and it is the one whose opt-out has gone unrecorded longest.
 */
export async function selectUnqualifiedReplies(
  limit: number,
  asOf: Date = new Date(),
): Promise<UnqualifiedReply[]> {
  const cutoff = new Date(asOf.getTime() - QUALIFICATION_GRACE_MS).toISOString();
  const kinds = sql.join(
    QUALIFICATION_EVENT_TYPES.map((k) => sql`${k}`),
    sql`, `,
  );

  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id,
           c.lead_email,
           c.account_email,
           c.org_id,
           c.user_id,
           r.replied_at
    FROM instantly_campaigns c
    JOIN LATERAL (
      SELECT min(e.timestamp) AS replied_at
      FROM instantly_events e
      WHERE e.campaign_id = c.instantly_campaign_id
        AND e.event_type = 'reply_received'
        AND e.inferred = false
    ) r ON r.replied_at IS NOT NULL
    WHERE c.instantly_campaign_id NOT LIKE 'self:%'
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND r.replied_at < ${cutoff}
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events q
        WHERE q.campaign_id = c.instantly_campaign_id
          AND q.event_type IN (${kinds})
      )
    ORDER BY r.replied_at ASC
    LIMIT ${limit}
  `);

  return rowsOf(result).flatMap((row) => {
    const instantlyCampaignId = String(row.instantly_campaign_id ?? "");
    const leadEmail = String(row.lead_email ?? "");
    if (!instantlyCampaignId || !leadEmail) return [];
    const repliedAt = new Date(String(row.replied_at ?? ""));
    if (Number.isNaN(repliedAt.getTime())) return [];
    return [
      {
        instantlyCampaignId,
        leadEmail,
        accountEmail: typeof row.account_email === "string" ? row.account_email : null,
        orgId: typeof row.org_id === "string" ? row.org_id : null,
        userId: typeof row.user_id === "string" ? row.user_id : null,
        repliedAt,
      },
    ];
  });
}

/** What one reply's classification attempt produced. */
export type QualifyOutcome =
  | { classified: true; eventType: string }
  | { classified: false; reason: "no_body" | "unqualified" };

/**
 * Classify one waiting reply and promote the kind.
 *
 * ⚠️ THE MIRROR IS RE-RUN FIRST, and that is the half that rescues the case
 * this exists for. `maybeMirrorCampaignEmails` fires on an inbound event, so a
 * reply webhook that beat Instantly's own `/emails` leaves an EMPTY mirror — and
 * the retry is itself gated on a later inbound event, which for these replies
 * never comes. So the missing verdict also costs us the words. Re-running it is
 * idempotent and is the retry that was never attempted.
 *
 * ⚠️ NO BODY MEANS NO CLASSIFICATION. Guessing a kind from an empty message is
 * the one thing worse than leaving the reply unqualified: every gate downstream
 * would open on a fabricated reading.
 */
export async function qualifyOneReply(reply: UnqualifiedReply): Promise<QualifyOutcome> {
  await maybeMirrorCampaignEmails(
    {
      instantlyCampaignId: reply.instantlyCampaignId,
      orgId: reply.orgId,
      userId: reply.userId,
    },
    "reply_received",
  );

  const inbound = await fetchLatestMirroredInbound(reply.instantlyCampaignId);
  // The bronze id is what attributes the promoted event to the message it was
  // read from; without it the kind would be an assertion with no provenance.
  if (!inbound || !inbound.instantlyEmailId) return { classified: false, reason: "no_body" };

  const kind = await qualifyReply(inbound.text, {
    instantlyCampaignId: reply.instantlyCampaignId,
    leadEmail: reply.leadEmail,
    source: "qualification_fallback",
  });
  if (kind === null) return { classified: false, reason: "unqualified" };

  await promoteEvent({
    eventType: kind,
    instantlyCampaignId: reply.instantlyCampaignId,
    leadEmail: reply.leadEmail,
    accountEmail: reply.accountEmail,
    step: null,
    variant: null,
    // The verdict is about the reply, so it carries the reply's own moment
    // rather than the sweep's — a kind timestamped now would sort ahead of a
    // later, better verdict from Instantly, and the gold projection takes the
    // latest.
    timestamp: reply.repliedAt,
    // ⚠️ `emails_backfill`, NOT `poll_emails`. Both read the Unibox mirror; this
    // column exists to say WHERE the judgement came from, and `poll_emails`
    // means Instantly's own verdict. Ours came from our classifier reading the
    // body, which is exactly what that value already documents.
    source: "emails_backfill",
    sourceRowId: inbound.instantlyEmailId,
    rawPayload: {
      classifiedBy: "reply-classifier",
      reason: "instantly_emitted_no_qualification",
    },
  });

  return { classified: true, eventType: kind };
}

export interface QualificationFallbackSummary {
  candidates: number;
  classified: number;
  noBody: number;
  unqualified: number;
  failed: number;
}

/**
 * One sweep.
 *
 * Fail-loud PER REPLY: a classifier outage on one message must not stop the
 * others, and a swallowed failure here would leave the reply looking handled.
 * The summary is only logged when it did something, so an idle fleet stays quiet.
 */
export async function runReplyQualificationFallback(
  opts: { limit?: number; asOf?: Date } = {},
): Promise<QualificationFallbackSummary> {
  const limit = opts.limit ?? QUALIFICATION_FALLBACK_BATCH;
  const candidates = await selectUnqualifiedReplies(limit, opts.asOf);

  const summary: QualificationFallbackSummary = {
    candidates: candidates.length,
    classified: 0,
    noBody: 0,
    unqualified: 0,
    failed: 0,
  };

  for (const reply of candidates) {
    try {
      const outcome = await qualifyOneReply(reply);
      if (outcome.classified) {
        summary.classified += 1;
        console.log(
          `[instantly-service] qualification-fallback: classified ${outcome.eventType} — lead=${reply.leadEmail} campaign=${reply.instantlyCampaignId} (Instantly emitted no verdict)`,
        );
      } else if (outcome.reason === "no_body") {
        summary.noBody += 1;
      } else {
        summary.unqualified += 1;
      }
    } catch (error: unknown) {
      summary.failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] qualification-fallback: FAILED lead=${reply.leadEmail} campaign=${reply.instantlyCampaignId}: ${message}`,
      );
    }
  }

  return summary;
}
