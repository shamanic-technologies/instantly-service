/**
 * Historical replies that asked us to stop, and were filed as "not interested".
 *
 * The live path (`lib/reply-opt-out`, fired from `promoteEvent`) only reaches a
 * reply promoted from here on. The mirrored Unibox already holds the backlog:
 * measured in prod 2026-09-17, SEVEN leads had written an explicit removal
 * request, ZERO carried `lead_unsubscribed`, all seven read
 * `lead_not_interested` — the RECYCLABLE bucket — and one still sat on an ACTIVE
 * campaign that would have kept emailing them.
 *
 * Reads nothing from Instantly and sends nothing: it re-reads words we already
 * stored and records the consent they state.
 *
 * ⚠️ NO REGEX PRE-FILTER, deliberately. It is tempting to narrow the candidate
 * set to bodies matching "unsubscribe|remove me|take me off" before paying for a
 * model call — and that is exactly the miss this sweep exists to close, because
 * a removal request is a sentence and not a keyword ("I'd rather you didn't
 * write again"). The classifier is `deepseek-flash` on a few hundred words; the
 * whole backlog is a few hundred calls, which is worth less than one missed
 * opt-out. Every other pre-filter in this repo routes toward a HARMLESS outcome;
 * this one would route toward a legally-consequential one.
 *
 * ⚠️ SAME CLASSIFIER AS THE LIVE PATH. `recordOptOutFromReply` is shared, so a
 * reply judged here and a reply judged by the webhook cannot drift.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import {
  fetchLatestMirroredInbound,
  recordOptOutFromReply,
  OPT_OUT_REPLY_KIND,
} from "./reply-opt-out";
import { qualifyReply } from "./self-send/qualify-reply";

export interface ReplyOptOutBackfillOptions {
  /** Default TRUE — read-only, reports the plan. */
  dryRun?: boolean;
  limit?: number;
}

export interface ReplyOptOutBackfillSummary {
  candidates: number;
  judged: number;
  optOuts: number;
  recorded: number;
  alreadyStanding: number;
  unqualified: number;
  failed: number;
  /** The leads an opt-out was found for, so a dry run names them. */
  leads: string[];
}

/** node-postgres resolves `db.execute` to a QueryResult object, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

interface Candidate {
  instantlyCampaignId: string;
  leadEmail: string;
  orgId: string;
}

/**
 * Sequences whose prospect wrote back and who are not already opted out.
 *
 * Ordered live-first: an `active` campaign is one still emailing somebody who
 * may have asked us to stop, which is the only part of this backlog that is
 * still doing harm.
 *
 * A `self:` sequence is excluded — its inbound lives in `imap_messages_raw` and
 * its replies were classified by the poller at read time, so there is nothing
 * mirrored here to re-judge.
 */
async function fetchCandidates(limit?: number): Promise<Candidate[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT ON (c.instantly_campaign_id)
           c.instantly_campaign_id,
           c.lead_email,
           c.org_id
    FROM instantly_campaigns c
    JOIN instantly_emails_raw m
      ON m.instantly_campaign_id = c.instantly_campaign_id
     AND m.payload->>'ue_type' <> '1'
    WHERE c.org_id IS NOT NULL
      AND c.lead_email IS NOT NULL
      AND c.instantly_campaign_id NOT LIKE 'self:%'
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND NOT EXISTS (
        SELECT 1
        FROM instantly_lead_optouts_raw o
        LEFT JOIN instantly_lead_optout_withdrawals w ON w.optout_id = o.id
        WHERE o.org_id = c.org_id
          AND o.lead_email = c.lead_email
          AND w.id IS NULL
      )
    ORDER BY c.instantly_campaign_id, (c.status = 'active') DESC
    ${limit ? sql`LIMIT ${limit}` : sql``}
  `);

  return rowsOf(result)
    .map((row) => ({
      instantlyCampaignId: String(row.instantly_campaign_id),
      leadEmail: String(row.lead_email),
      orgId: String(row.org_id),
    }))
    .filter((c) => c.instantlyCampaignId && c.leadEmail && c.orgId);
}

/**
 * Re-judge the mirrored backlog and record the opt-outs in it.
 *
 * Idempotent: a recorded opt-out leaves the candidate set (the `NOT EXISTS`
 * above), so a second run reports zero. Fail-loud per candidate — one
 * unclassifiable reply is counted and the sweep continues, because a sweep that
 * died on message 3 of 365 would leave the rest of the backlog unread.
 */
export async function backfillReplyOptOuts(
  options: ReplyOptOutBackfillOptions = {},
): Promise<ReplyOptOutBackfillSummary> {
  const dryRun = options.dryRun !== false;
  const candidates = await fetchCandidates(options.limit);

  const summary: ReplyOptOutBackfillSummary = {
    candidates: candidates.length,
    judged: 0,
    optOuts: 0,
    recorded: 0,
    alreadyStanding: 0,
    unqualified: 0,
    failed: 0,
    leads: [],
  };

  for (const candidate of candidates) {
    try {
      const inbound = await fetchLatestMirroredInbound(candidate.instantlyCampaignId);
      if (!inbound) continue;
      summary.judged += 1;

      if (dryRun) {
        // A dry run still CLASSIFIES — the plan a human reads before committing
        // is "which leads asked to stop", and a candidate count cannot answer it.
        // It simply records nothing.
        const qualification = await qualifyReply(inbound.text, {
          instantlyCampaignId: candidate.instantlyCampaignId,
          leadEmail: candidate.leadEmail,
          source: "reply_optout_backfill",
        });
        if (qualification === null) summary.unqualified += 1;
        else if (qualification === OPT_OUT_REPLY_KIND) {
          summary.optOuts += 1;
          summary.leads.push(candidate.leadEmail);
        }
        continue;
      }

      const outcome = await recordOptOutFromReply({
        campaign: candidate,
        replyText: inbound.text,
        evidence: {
          source: "reply_optout_backfill",
          instantlyCampaignId: candidate.instantlyCampaignId,
          instantlyEmailId: inbound.instantlyEmailId,
        },
      });

      if (outcome.recorded) {
        summary.optOuts += 1;
        summary.recorded += 1;
        summary.leads.push(candidate.leadEmail);
      } else if (outcome.reason === "already_standing") summary.alreadyStanding += 1;
      else if (outcome.reason === "unqualified") summary.unqualified += 1;
    } catch (error: unknown) {
      summary.failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.warn(
        `[instantly-service] reply-optout-backfill: campaign=${candidate.instantlyCampaignId} failed — ${message}`,
      );
    }
  }

  return summary;
}
