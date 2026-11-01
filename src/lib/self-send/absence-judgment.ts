/**
 * Every stored out-of-office is read ONCE for its return date, and a stated
 * return holds the person (OOO-RETURN-DATE-1101; rules in `absence.ts`).
 *
 * Two passes, both idempotent and fail-soft per row:
 *
 * 1. JUDGE: each `imap_messages_raw` row of kind `auto_reply` on one of our
 *    sequences with no `lead_absences` row is read by chat-service (same model
 *    and same caller run as the reply classifier: the org's `reply-judging` run
 *    of the day on the sequence's pipe), and the outcome is stored whatever it
 *    is (`return_date` / `no_date` / `stale` / `unreadable`): judged once. A
 *    failed call stores nothing; the next pass asks again. The dispatch reads
 *    `hold_until` per address (`loadPendingSequences`).
 *
 * 2. STATE: while a hold stands, lead-service's next follow-up date of the
 *    person at every brand of the sequence is moved to it, never earlier than
 *    what lead-service already holds and never over a stop. Marked on the row
 *    (`followup_stated_at` + what happened); a failure is retried next pass.
 *
 * The past is covered by the same passes: every autoresponder stored within
 * {@link ABSENCE_LOOKBACK_DAYS} is judged, so a sequence already in flight at
 * deploy time is held too.
 */

import { sql } from "drizzle-orm";
import { db } from "../../db";
import { errorMessage } from "../db-error.js";
import { platformComplete, type ChatCompleteParams } from "../chat-client";
import { replyJudgingRunForSequence } from "../reply-judging-run";
import {
  findLeadOnCampaignByEmail,
  readLeadFollowupDate,
  stateLeadFollowupDate,
} from "../lead-client";
import { stripQuotedHistory } from "./qualify-reply";
import {
  ABSENCE_SYSTEM_PROMPT,
  absenceJudgmentMessage,
  parseStatedReturn,
  resolveAbsence,
} from "./absence";

/** Autoresponders older than this are not read: no sequence of theirs is still waiting on them. */
export const ABSENCE_LOOKBACK_DAYS = 120;
/** Rows judged / stated in one pass; the rest wait for the next. */
export const ABSENCE_PASS_LIMIT = 50;

export interface AbsenceSweepSummary {
  candidates: number;
  judged: number;
  holds: number;
  judgmentFailed: number;
  followupsChecked: number;
  followupsStated: number;
  followupFailed: number;
}

interface UnjudgedAutoReply {
  imapMessageId: string;
  instantlyCampaignId: string;
  orgId: string | null;
  leadEmail: string;
  receivedAt: Date;
  subject: string | null;
  text: string;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  return (result as { rows?: Record<string, unknown>[] }).rows ?? [];
}

function toDate(value: unknown): Date {
  if (value instanceof Date) return value;
  const text = String(value);
  const iso = text.includes("T") ? text : text.replace(" ", "T");
  return new Date(/[Zz]|[+-]\d{2}:?\d{2}$/.test(iso) ? iso : `${iso}Z`);
}

export async function loadUnjudgedAutoReplies(limit = ABSENCE_PASS_LIMIT): Promise<UnjudgedAutoReply[]> {
  // `kind IN (...)` matches the partial index imap_messages_raw_replies_idx.
  const result = await db.execute(sql`
    SELECT m.id AS "imapMessageId",
           m.instantly_campaign_id AS "instantlyCampaignId",
           c.org_id AS "orgId",
           c.lead_email AS "leadEmail",
           COALESCE(m.received_at, m.polled_at) AS "receivedAt",
           m.payload->>'subject' AS "subject",
           COALESCE(m.payload->>'textSnippet', m.payload->>'text', '') AS "text"
    FROM imap_messages_raw m
    JOIN instantly_campaigns c ON c.instantly_campaign_id = m.instantly_campaign_id
    WHERE m.kind IN ('reply', 'auto_reply')
      AND m.kind = 'auto_reply'
      AND m.instantly_campaign_id IS NOT NULL
      AND COALESCE(m.received_at, m.polled_at) > (now() AT TIME ZONE 'UTC') - make_interval(days => ${ABSENCE_LOOKBACK_DAYS})
      AND NOT EXISTS (SELECT 1 FROM lead_absences a WHERE a.imap_message_id = m.id)
    ORDER BY COALESCE(m.received_at, m.polled_at) DESC
    LIMIT ${limit}
  `);
  return rowsOf(result).map((r) => ({
    imapMessageId: String(r.imapMessageId),
    instantlyCampaignId: String(r.instantlyCampaignId),
    orgId: r.orgId === null || r.orgId === undefined ? null : String(r.orgId),
    leadEmail: String(r.leadEmail).trim().toLowerCase(),
    receivedAt: toDate(r.receivedAt),
    subject: typeof r.subject === "string" ? r.subject : null,
    text: typeof r.text === "string" ? r.text : "",
  }));
}

/** The request sent to chat-service for one autoresponder. */
export function absenceJudgmentRequest(reply: Pick<UnjudgedAutoReply, "receivedAt" | "subject" | "text">): ChatCompleteParams {
  const own = stripQuotedHistory(reply.text);
  return {
    message: absenceJudgmentMessage({ receivedAt: reply.receivedAt, subject: reply.subject, text: own || reply.text }),
    systemPrompt: ABSENCE_SYSTEM_PROMPT,
    provider: "deepseek",
    model: "deepseek-flash",
    responseFormat: "json",
    temperature: 0,
    disableThinking: true,
  };
}

/** Judge one autoresponder and store the outcome. Throws when chat-service gives no answer (nothing stored). */
export async function judgeAbsence(reply: UnjudgedAutoReply): Promise<{ holdUntil: Date | null }> {
  const runId = await replyJudgingRunForSequence(reply.instantlyCampaignId, { fallbackOrgId: reply.orgId });
  const result = await platformComplete(absenceJudgmentRequest(reply), { runId });
  const stated = parseStatedReturn(result);
  const resolved = resolveAbsence(stated, reply.receivedAt);
  await db.execute(sql`
    INSERT INTO lead_absences (
      id, imap_message_id, instantly_campaign_id, org_id, lead_email, received_at,
      outcome, stated, first_day_back, hold_until, run_id
    ) VALUES (
      ${crypto.randomUUID()}, ${reply.imapMessageId}, ${reply.instantlyCampaignId}, ${reply.orgId},
      ${reply.leadEmail}, ${reply.receivedAt.toISOString()}::timestamptz AT TIME ZONE 'UTC',
      ${resolved.outcome},
      ${JSON.stringify({ answer: result.json ?? result.content ?? null, stated: stated ?? null })}::jsonb,
      ${resolved.firstDayBack}::date,
      ${resolved.holdUntil ? resolved.holdUntil.toISOString() : null}::timestamptz,
      ${runId}
    )
    ON CONFLICT (imap_message_id) DO NOTHING
  `);
  if (resolved.holdUntil) {
    console.log(
      `[instantly-service] absence: ${reply.leadEmail} is away, back ${resolved.firstDayBack}; nothing leaves before ${resolved.holdUntil.toISOString()} (sequence ${reply.instantlyCampaignId})`,
    );
  }
  return { holdUntil: resolved.holdUntil };
}

export interface UnstatedHold {
  id: string;
  orgId: string | null;
  campaignId: string | null;
  brandIds: string[];
  leadEmail: string;
  firstDayBack: string;
  holdUntil: Date;
}

async function loadUnstatedHolds(limit = ABSENCE_PASS_LIMIT): Promise<UnstatedHold[]> {
  const result = await db.execute(sql`
    SELECT a.id, a.org_id AS "orgId", c.campaign_id AS "campaignId",
           COALESCE(to_jsonb(c.brand_ids), '[]'::jsonb) AS "brandIds",
           a.lead_email AS "leadEmail", a.first_day_back::text AS "firstDayBack", a.hold_until AS "holdUntil"
    FROM lead_absences a
    JOIN instantly_campaigns c ON c.instantly_campaign_id = a.instantly_campaign_id
    WHERE a.hold_until > now() AND a.followup_stated_at IS NULL
    ORDER BY a.hold_until ASC
    LIMIT ${limit}
  `);
  return rowsOf(result).map((r) => ({
    id: String(r.id),
    orgId: r.orgId === null || r.orgId === undefined ? null : String(r.orgId),
    campaignId: r.campaignId === null || r.campaignId === undefined ? null : String(r.campaignId),
    brandIds: Array.isArray(r.brandIds) ? (r.brandIds as unknown[]).map(String) : [],
    leadEmail: String(r.leadEmail),
    firstDayBack: String(r.firstDayBack),
    holdUntil: toDate(r.holdUntil),
  }));
}

async function markFollowup(id: string, statement: string, done: boolean): Promise<void> {
  await db.execute(sql`
    UPDATE lead_absences
    SET followup_statement = ${statement.slice(0, 500)},
        followup_stated_at = ${done ? sql`now()` : sql`NULL`}
    WHERE id = ${id}
  `);
}

/**
 * Move lead-service's next follow-up date of the person to the hold, at every
 * brand of the sequence. Never earlier than what lead-service holds, never over
 * a stop. Returns what happened per brand.
 */
export async function stateHoldAsFollowupDate(hold: UnstatedHold): Promise<string> {
  if (!hold.orgId || !hold.campaignId) return "no_lead_row: platform send, no caller campaign";
  if (hold.brandIds.length === 0) return "no_brand";
  const row = await findLeadOnCampaignByEmail({ orgId: hold.orgId, campaignId: hold.campaignId, email: hold.leadEmail });
  if (!row || !row.id) return "no_lead_row";
  const outcomes: string[] = [];
  for (const brandId of hold.brandIds) {
    const current = await readLeadFollowupDate({ orgId: hold.orgId, leadRowId: row.id, brandId });
    if (current.nextFollowupAt === null) {
      outcomes.push(`${brandId}:kept_stopped`);
      continue;
    }
    if (new Date(current.nextFollowupAt).getTime() >= hold.holdUntil.getTime()) {
      outcomes.push(`${brandId}:already_later`);
      continue;
    }
    await stateLeadFollowupDate({
      orgId: hold.orgId,
      leadRowId: row.id,
      brandId,
      nextFollowupAt: hold.holdUntil.toISOString(),
      reason: `Out of office, back on ${hold.firstDayBack}`,
    });
    outcomes.push(`${brandId}:stated`);
  }
  return outcomes.join(",");
}

let inFlight: Promise<AbsenceSweepSummary> | null = null;

/** Judge the stored autoresponders not judged yet, then state the standing holds. Concurrent callers join one pass. */
export function judgeStoredAbsences(): Promise<AbsenceSweepSummary> {
  if (!inFlight) {
    inFlight = runSweep().finally(() => {
      inFlight = null;
    });
  }
  return inFlight;
}

async function runSweep(): Promise<AbsenceSweepSummary> {
  const summary: AbsenceSweepSummary = {
    candidates: 0,
    judged: 0,
    holds: 0,
    judgmentFailed: 0,
    followupsChecked: 0,
    followupsStated: 0,
    followupFailed: 0,
  };

  const unjudged = await loadUnjudgedAutoReplies();
  summary.candidates = unjudged.length;
  for (const reply of unjudged) {
    try {
      const { holdUntil } = await judgeAbsence(reply);
      summary.judged++;
      if (holdUntil) summary.holds++;
    } catch (error) {
      summary.judgmentFailed++;
      console.warn(
        `[instantly-service] absence: out-of-office of ${reply.leadEmail} (message ${reply.imapMessageId}) not judged, asked again next pass — ${errorMessage(error)}`,
      );
    }
  }

  for (const hold of await loadUnstatedHolds()) {
    summary.followupsChecked++;
    try {
      const statement = await stateHoldAsFollowupDate(hold);
      await markFollowup(hold.id, statement, true);
      if (statement.includes(":stated")) summary.followupsStated++;
    } catch (error) {
      summary.followupFailed++;
      const message = errorMessage(error);
      await markFollowup(hold.id, `failed: ${message}`, false);
      console.warn(
        `[instantly-service] absence: next follow-up date of ${hold.leadEmail} not moved to ${hold.holdUntil.toISOString()}, retried next pass — ${message}`,
      );
    }
  }

  if (summary.candidates > 0 || summary.followupsChecked > 0) {
    console.log(`[instantly-service] absence: pass ${JSON.stringify(summary)}`);
  }
  return summary;
}
