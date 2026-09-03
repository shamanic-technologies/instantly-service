/**
 * Qualifying inbound mail that reached bronze and never reached silver.
 *
 * `instantly_emails_raw` mirrors Instantly's Unibox — every message its
 * mailboxes sent or received, including what prospects wrote back. Silver
 * records only THAT a lead replied, and it learns that from a webhook. So the
 * two can legitimately disagree: a webhook Instantly never delivered (it
 * disables the subscription after repeated failures), an inbound the Unibox
 * backfill mirrored long after the fact, a mailbox Instantly can no longer read.
 * In every one of those cases the prospect's words sit in bronze while the
 * sequence keeps running.
 *
 * This sweep closes that gap for messages ALREADY mirrored. It reads nothing
 * from Instantly and sends nothing — it is a promotion of facts we already hold.
 *
 * ⚠️ THE CLASSIFICATION DECIDES WHETHER TO STOP, WHICH IS THE OPPOSITE ORDER
 * FROM THE LIVE POLLER, and the difference is forced by the evidence available.
 * The poller reads a real RFC 5322 message, so `isAutoReply` can read
 * `Auto-Submitted` and friends off the wire and rule the message out cheaply
 * BEFORE promoting `reply_received`. Instantly's stored payload carries no such
 * headers — only a subject, a body and its own `ue_type`. Here the reply KIND is
 * therefore the only discriminator available, so nothing is promoted until it is
 * known. Promoting `reply_received` first and correcting afterwards is not an
 * option: it stops the sequence and cancels the lead's remaining holds, and most
 * of what is in this backlog is out-of-office.
 *
 * Which makes the safe direction explicit: when in doubt, do NOT stop. An
 * autoresponder wrongly treated as a reply ends outreach at somebody who never
 * engaged and refunds spend that was correctly spent; a reply wrongly left
 * unqualified stays visible in bronze for a human to find.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { promoteEvent } from "./silver-promote";
import { qualifyReply } from "./self-send/qualify-reply";
import { stopLeadSequence } from "./stop-lead-sequence";
import { isSequenceStoppingReplyKind } from "./reply-kind";
import { isSelfSendCampaignId } from "./self-send/transport";
import type { CallerInfo } from "./key-client";

const CALLER: CallerInfo = {
  method: "POST",
  path: "/internal/audit/inbound-replies-backfill",
};

/** How much of a stored body to hand the classifier. */
const BODY_LIMIT = 4000;

/**
 * Subjects that announce an autoresponder in plain words, in the languages this
 * fleet actually receives.
 *
 * A cheap pre-filter, not a substitute for the classifier: it only ever routes a
 * message to `auto_reply_received`, which is the outcome that changes nothing
 * about the sequence. So a false positive costs an unqualified out-of-office
 * (already the common case) and never a wrongly-stopped sequence, while a false
 * negative just pays for one LLM call. Deliberately anchored at the START of the
 * subject: "Re: your automatic invoice reminder" is a human writing to us.
 */
const AUTORESPONDER_SUBJECT = new RegExp(
  "^\\s*(?:re\\s*:\\s*)*(?:" +
    [
      "auto(?:matic|mated)?[ -]?(?:reply|response|antwort)",
      "out of (?:the )?office",
      "away from (?:my|the) (?:desk|office)",
      "automatische antwort",
      "abwesenheits?notiz",
      "r[ée]ponse automatique",
      "absence du bureau",
      "risposta automatica",
      "fuori sede",
      "respuesta autom[áa]tica",
      "ausencia de la oficina",
      "automatisch antwoord",
      "autosvar",
      "frånvarande",
    ].join("|") +
    ")",
  "i",
);

/** True when the subject alone settles it: a machine answered. */
export function looksLikeAutoresponderSubject(subject: string | null): boolean {
  return subject !== null && AUTORESPONDER_SUBJECT.test(subject);
}

/** One inbound message that has never been promoted. */
export interface InboundCandidate {
  bronzeRowId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  orgId: string | null;
  accountEmail: string | null;
  subject: string | null;
  body: string;
  receivedAt: Date;
  /** Whether the sequence is still live — the urgent slice. */
  campaignActive: boolean;
}

export interface InboundBackfillSummary {
  /** Bronze inbound rows selected for this run. */
  candidates: number;
  /** Settled as a machine reply: `auto_reply_received`, sequence untouched. */
  autoReplies: number;
  /** Genuine human replies promoted as `reply_received` + their kind. */
  replies: number;
  /** Replies whose classification could not be trusted — nothing promoted. */
  unclassifiable: number;
  /** Sequences told to stop on the SENDER's side (Instantly pause / local stop). */
  sequencesStopped: number;
  /** Candidates that threw. Counted, logged, never fatal to the sweep. */
  failed: number;
  /** True when a `limit` cut the run short, so more remain. */
  truncated: boolean;
}

/**
 * Inbound rows whose sequence carries NO reply-ish silver event at all.
 *
 * Both event types are excluded, not just `reply_received`: a campaign already
 * carrying `auto_reply_received` has been classified — by Instantly's own
 * webhook, which saw the real headers — and re-deciding it here from a subject
 * and a body would be this service overruling better evidence with worse.
 *
 * `reserving:` sentinels cannot appear (they hold no emails) and `self:`
 * campaigns are excluded explicitly: Instantly never carried them, so it can
 * hold no inbound for them, and the row would be a mis-join if one existed.
 */
export async function loadInboundCandidates(
  limit: number | null,
): Promise<InboundCandidate[]> {
  const result = await db.execute(sql`
    SELECT m.id                        AS "bronzeRowId",
           c.instantly_campaign_id     AS "instantlyCampaignId",
           c.lead_email                AS "leadEmail",
           c.org_id                    AS "orgId",
           c.account_email             AS "accountEmail",
           m.payload->>'subject'       AS "subject",
           COALESCE(
             NULLIF(m.payload->'body'->>'text', ''),
             m.payload->'body'->>'html',
             ''
           )                           AS "body",
           COALESCE(
             (m.payload->>'timestamp_email')::timestamptz,
             m.fetched_at
           )                           AS "receivedAt",
           (c.status = 'active')       AS "campaignActive"
    FROM instantly_emails_raw m
    JOIN instantly_campaigns c
      ON c.instantly_campaign_id = m.instantly_campaign_id
    WHERE m.payload->>'ue_type' <> '1'
      AND c.instantly_campaign_id NOT LIKE 'self:%'
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type IN ('reply_received', 'auto_reply_received')
      )
    -- Live sequences first: those are the ones still emailing someone who has
    -- already written back, so a run cut short by a limit fixes those first.
    ORDER BY (c.status = 'active') DESC, "receivedAt" DESC
    ${limit === null ? sql`` : sql`LIMIT ${limit}`}
  `);

  return (result.rows as Record<string, unknown>[]).map((row) => ({
    bronzeRowId: String(row.bronzeRowId),
    instantlyCampaignId: String(row.instantlyCampaignId),
    leadEmail: String(row.leadEmail),
    orgId: (row.orgId as string | null) ?? null,
    accountEmail: (row.accountEmail as string | null) ?? null,
    subject: (row.subject as string | null) ?? null,
    body: String(row.body ?? ""),
    receivedAt: new Date(row.receivedAt as string | Date),
    campaignActive: row.campaignActive === true,
  }));
}

/**
 * Promote one candidate.
 *
 * Returns which bucket it landed in so the caller can total them honestly. Any
 * error propagates — the caller counts it per-candidate and moves on, because a
 * single unreadable message must not stop the sweep from rescuing the rest.
 */
export async function promoteCandidate(
  candidate: InboundCandidate,
): Promise<{ kind: "auto" | "reply" | "unclassifiable"; stopped: boolean }> {
  const kind = looksLikeAutoresponderSubject(candidate.subject)
    ? "lead_out_of_office"
    : await qualifyReply(candidate.body.slice(0, BODY_LIMIT));

  if (kind === null) {
    console.warn(
      `[instantly-service] inbound-replies-backfill: no usable classification for campaign=${candidate.instantlyCampaignId} lead=${candidate.leadEmail} — nothing promoted`,
    );
    return { kind: "unclassifiable", stopped: false };
  }

  const stops = isSequenceStoppingReplyKind(kind);

  // A machine answered. `auto_reply_received` is deliberately absent from
  // SEQUENCE_STOP_EVENTS, so this records what happened and changes nothing:
  // the prospect is back at their desk next week and has not engaged.
  if (!stops) {
    await promoteEvent({
      eventType: "auto_reply_received",
      instantlyCampaignId: candidate.instantlyCampaignId,
      leadEmail: candidate.leadEmail,
      accountEmail: candidate.accountEmail,
      step: null,
      variant: null,
      timestamp: candidate.receivedAt,
      source: "emails_backfill",
      sourceRowId: candidate.bronzeRowId,
    });
    // The kind carries the detail (`lead_out_of_office`) that the coarse
    // `auto_reply_received` does not, and it is equally non-stopping.
    await promoteEvent({
      eventType: kind,
      instantlyCampaignId: candidate.instantlyCampaignId,
      leadEmail: candidate.leadEmail,
      accountEmail: candidate.accountEmail,
      step: null,
      variant: null,
      timestamp: candidate.receivedAt,
      source: "emails_backfill",
      sourceRowId: candidate.bronzeRowId,
    });
    return { kind: "auto", stopped: false };
  }

  // A person wrote back. `reply_received` is a SEQUENCE_STOP_EVENT, so this one
  // call marks the row, cancels the lead's remaining provisioned holds and
  // refreshes gold — exactly what a live reply does.
  await promoteEvent({
    eventType: "reply_received",
    instantlyCampaignId: candidate.instantlyCampaignId,
    leadEmail: candidate.leadEmail,
    accountEmail: candidate.accountEmail,
    step: null,
    variant: null,
    timestamp: candidate.receivedAt,
    source: "emails_backfill",
    sourceRowId: candidate.bronzeRowId,
  });

  // Every reply gets its kind, positive or negative — the coarse
  // `reply_classification` and the gold sentiment both read this, and a
  // negative left untagged is a negative nobody can count.
  await promoteEvent({
    eventType: kind,
    instantlyCampaignId: candidate.instantlyCampaignId,
    leadEmail: candidate.leadEmail,
    accountEmail: candidate.accountEmail,
    step: null,
    variant: null,
    timestamp: candidate.receivedAt,
    source: "emails_backfill",
    sourceRowId: candidate.bronzeRowId,
  });

  // The call above did OUR half of the stop. Instantly is still dispatching:
  // its webhook never told us about this reply, so its own stop-on-reply never
  // fired either. Same second half as a manual qualification. A `self:` row
  // cannot occur here (excluded by the candidate query) but the guard keeps the
  // intent legible if that ever changes.
  let stopped = false;
  if (candidate.orgId && !isSelfSendCampaignId(candidate.instantlyCampaignId)) {
    stopped = await stopLeadSequence({
      orgId: candidate.orgId,
      instantlyCampaignId: candidate.instantlyCampaignId,
      leadEmail: candidate.leadEmail,
      reason: `inbound reply found in bronze, classified ${kind}`,
      caller: CALLER,
    });
  }

  return { kind: "reply", stopped };
}

/**
 * Sweep the backlog.
 *
 * Idempotent by construction: a promoted candidate gains a `reply_received` or
 * `auto_reply_received` event, which is exactly what the candidate query
 * excludes, so a second run selects it no more. Resumable for the same reason —
 * `limit` simply leaves the rest for the next run, live sequences first.
 *
 * `dryRun` reads the candidates and reports the plan without writing anything or
 * spending a single LLM call, so the row set can be checked against the database
 * before anything is promoted.
 */
export async function backfillInboundReplies(
  options: { dryRun?: boolean; limit?: number } = {},
): Promise<InboundBackfillSummary> {
  const dryRun = options.dryRun ?? true;
  const limit = options.limit ?? null;

  const candidates = await loadInboundCandidates(limit);

  const summary: InboundBackfillSummary = {
    candidates: candidates.length,
    autoReplies: 0,
    replies: 0,
    unclassifiable: 0,
    sequencesStopped: 0,
    failed: 0,
    truncated: limit !== null && candidates.length === limit,
  };

  if (dryRun) {
    const active = candidates.filter((c) => c.campaignActive).length;
    console.log(
      `[instantly-service] inbound-replies-backfill: DRY RUN — ${candidates.length} candidate(s), ${active} on a still-active sequence`,
    );
    return summary;
  }

  for (const candidate of candidates) {
    try {
      const outcome = await promoteCandidate(candidate);
      if (outcome.kind === "auto") summary.autoReplies += 1;
      else if (outcome.kind === "reply") summary.replies += 1;
      else summary.unclassifiable += 1;
      if (outcome.stopped) summary.sequencesStopped += 1;
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] inbound-replies-backfill: campaign=${candidate.instantlyCampaignId} failed: ${message}`,
      );
      summary.failed += 1;
    }
  }

  return summary;
}
