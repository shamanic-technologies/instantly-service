/**
 * Settle every permanent DSN by what it SAYS (BOUNCE-SPAM-BLOCK-1018): a dead
 * address is a bounce, a refused message is not. The rule is
 * `dsn-judgment.ts`; this is its IO.
 *
 * The poll (and the Gmail-mirror recovery) files a permanent DSN in bronze
 * (`imap_messages_raw`, kind `bounce`: it IS a delivery failure notice, and
 * the deliverability reads count refusals from those rows) and promotes
 * NOTHING. This sweep then, per row:
 *  1. asks chat-service's platform judgment once and PERSISTS it on the row
 *     (`payload.dsnJudgment`); a failed call leaves the row unjudged for the
 *     next run, and its send is decided by nothing until then;
 *  2. a send whose DSNs all say `message_refused` keeps or regains a clean
 *     record: every `email_bounced` event on it is retracted
 *     (`bounce-retraction.ts`), the person stays contactable;
 *  3. a DSN saying `address_dead` promotes `email_bounced` exactly as the poll
 *     used to (sequence stopped, holds cancelled, suppression downstream).
 *
 * Two modes, one code path: the routine run after every poll inside the
 * dispatch run (7-day window), and the fleet backfill over the whole history
 * (`POST /internal/self-send/dsn-judgments`, `sinceDays: null`). A dry run
 * JUDGES and persists the judgments (the spend is the measurement, and the
 * commit run then reuses them: the same numbers, never paid twice) but retracts
 * and promotes nothing.
 *
 * Spend: one platform judgment per permanent DSN tied to one of our sends,
 * input tokens only, billed by chat-service on a platform run made a child of
 * the send's own step run (`dsnJudgmentCaller`).
 */
import { errorMessage } from "../db-error.js";
import { sql } from "drizzle-orm";

import { db } from "../../db";
import { platformJudgment, type PlatformCaller } from "../chat-client";
import { promoteEvent } from "../silver-promote";
import { retractBouncedEvents } from "../bounce-retraction";
import {
  buildDsnQuestion,
  buildDsnState,
  DSN_ADDRESS_DEAD,
  DSN_MESSAGE_REFUSED,
  DSN_QUESTION_KEY,
  DSN_QUESTION_VERSION,
  dsnSendKey,
  planDsnSettlement,
  readDsnJudgment,
  type BounceEventRef,
  type DsnJudgment,
  type JudgedDsnRow,
} from "./dsn-judgment";

export const DSN_REFUSED_REASON = "message_refused_not_a_bounce";

/** The routine run's window: DSNs for one send arrive within minutes. */
export const ROUTINE_DSN_WINDOW_DAYS = 7;

/** Judgments asked at once during a backfill. */
const JUDGMENT_CONCURRENCY = 6;

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

interface DsnRow {
  id: string;
  accountEmail: string;
  instantlyCampaignId: string;
  step: number | null;
  leadEmail: string | null;
  from: string | null;
  subject: string | null;
  text: string;
  at: Date;
  judgment: DsnJudgment | null;
  /** The `email-send-step-N` run that sent the email this DSN answers, when one is on record. */
  sendRunId: string | null;
}

export interface DsnJudgmentSweepSummary {
  dryRun: boolean;
  sinceDays: number | null;
  rowsRead: number;
  judgedNow: number;
  judgmentFailed: number;
  judgmentInputTokens: number;
  /** Rows by persisted verdict (after this run's judgments). */
  addressDead: number;
  messageRefused: number;
  unjudged: number;
  /** Bounce events retracted (or that a dry run would retract). */
  bounceEventsRetracted: number;
  /** Distinct people whose bounce goes away. */
  peopleCleared: number;
  bouncesPromoted: number;
  /** Retracted events by the source that wrote them (self_send, webhook...). */
  retractedBySource: Record<string, number>;
  /** Refused DSN rows by the recipient's domain, top 20. */
  refusedByRecipientDomain: Record<string, number>;
  retractedLeads: string[];
}

async function loadDsnRows(sinceDays: number | null): Promise<DsnRow[]> {
  const window =
    sinceDays === null ? sql`` : sql`AND m.polled_at > now() - make_interval(days => ${sinceDays})`;
  const result = await db.execute(sql`
    SELECT m.id, m.account_email, m.instantly_campaign_id, m.step, c.lead_email,
           m.from_address, m.subject, COALESCE(m.payload->>'textSnippet', '') AS text,
           COALESCE(m.received_at, m.polled_at) AS at, m.payload->'dsnJudgment' AS judgment,
           sr.run_id AS send_run_id
    FROM imap_messages_raw m
    LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = m.instantly_campaign_id
    LEFT JOIN LATERAL (
      SELECT s.run_id FROM sequence_costs s
      WHERE s.instantly_campaign_id = m.instantly_campaign_id AND s.step = m.step AND s.run_id IS NOT NULL
      ORDER BY (s.status = 'actual') DESC, s.created_at DESC
      LIMIT 1
    ) sr ON true
    WHERE m.kind = 'bounce' AND m.instantly_campaign_id IS NOT NULL
      ${window}
    ORDER BY m.polled_at
  `);
  return rowsOf(result).map((r) => ({
    id: String(r.id),
    accountEmail: String(r.account_email),
    instantlyCampaignId: String(r.instantly_campaign_id),
    step: r.step == null ? null : Number(r.step),
    leadEmail: (r.lead_email as string | null) ?? null,
    from: (r.from_address as string | null) ?? null,
    subject: (r.subject as string | null) ?? null,
    text: String(r.text ?? ""),
    at: new Date(r.at as string | Date),
    judgment: currentJudgment(r.judgment),
    sendRunId: (r.send_run_id as string | null) ?? null,
  }));
}

/** A judgment asked under an older wording of the question counts as none. */
function currentJudgment(value: unknown): DsnJudgment | null {
  const judgment = value as DsnJudgment | null;
  return judgment && judgment.questionVersion === DSN_QUESTION_VERSION ? judgment : null;
}

async function loadBounceEvents(campaignIds: string[]): Promise<BounceEventRef[]> {
  if (campaignIds.length === 0) return [];
  const result = await db.execute(sql`
    SELECT id, campaign_id, lead_email, step, source_row_id, source
    FROM instantly_events
    WHERE event_type = 'email_bounced'
      AND campaign_id = ANY(${sql.param(campaignIds)}::text[])
  `);
  return rowsOf(result).map((r) => ({
    id: String(r.id),
    campaignId: String(r.campaign_id),
    leadEmail: (r.lead_email as string | null) ?? null,
    step: r.step == null ? null : Number(r.step),
    sourceRowId: (r.source_row_id as string | null) ?? null,
    source: String(r.source),
  }));
}

/**
 * Whose work reading a DSN is (PLATFORM-CALLER-RUN-1024): the run that SENT
 * the email it answers (the spend rides that send's `message_sent`); a past
 * send with no run of ours on record (83 of 587 rows in 30 d, all Instantly
 * transport) has nothing to ride, so it is declared support.
 */
export const DSN_NO_SEND_RUN_SUPPORT = {
  kind: "platform_llm",
  reason: "Reading a delivery failure notice for a past send that has no run of ours on record, so no outcome can carry it.",
} as const;

export function dsnJudgmentCaller(row: Pick<DsnRow, "sendRunId">): PlatformCaller {
  return row.sendRunId ? { runId: row.sendRunId } : { support: DSN_NO_SEND_RUN_SUPPORT };
}

async function judge(row: DsnRow): Promise<{ judgment: DsnJudgment; inputTokens: number }> {
  const result = await platformJudgment(
    {
      state: buildDsnState({ from: row.from, subject: row.subject, text: row.text }),
      questions: { [DSN_QUESTION_KEY]: buildDsnQuestion() },
    },
    dsnJudgmentCaller(row),
  );
  const answer = result.answers[DSN_QUESTION_KEY];
  if (!answer) throw new Error("judgment returned no answer for the dsn question");
  const judgment = readDsnJudgment(answer, result.model, new Date());
  await db.execute(sql`
    UPDATE imap_messages_raw
    SET payload = payload || ${JSON.stringify({ dsnJudgment: judgment })}::jsonb
    WHERE id = ${row.id}
  `);
  return { judgment, inputTokens: result.usage?.inputTokens ?? 0 };
}

async function judgeAll(rows: DsnRow[], summary: DsnJudgmentSweepSummary): Promise<void> {
  const queue = rows.filter((r) => r.judgment === null);
  const worker = async () => {
    for (let row = queue.shift(); row; row = queue.shift()) {
      try {
        const { judgment, inputTokens } = await judge(row);
        row.judgment = judgment;
        summary.judgedNow += 1;
        summary.judgmentInputTokens += inputTokens;
      } catch (error) {
        summary.judgmentFailed += 1;
        console.error(
          `[instantly-service] dsn-judgments: judgment FAILED for row=${row.id} campaign=${row.instantlyCampaignId}, left unjudged (no bounce promoted, retried next run): ${
            errorMessage(error)
          }`,
        );
      }
    }
  };
  await Promise.all(Array.from({ length: JUDGMENT_CONCURRENCY }, worker));
}

const domainOf = (email: string | null) => (email?.split("@")[1] ?? "(unknown)").toLowerCase();

export async function runDsnJudgmentSweep(
  options: { dryRun?: boolean; sinceDays?: number | null } = {},
): Promise<DsnJudgmentSweepSummary> {
  const dryRun = options.dryRun === true;
  const sinceDays = options.sinceDays === undefined ? ROUTINE_DSN_WINDOW_DAYS : options.sinceDays;
  const rows = await loadDsnRows(sinceDays);

  const summary: DsnJudgmentSweepSummary = {
    dryRun,
    sinceDays,
    rowsRead: rows.length,
    judgedNow: 0,
    judgmentFailed: 0,
    judgmentInputTokens: 0,
    addressDead: 0,
    messageRefused: 0,
    unjudged: 0,
    bounceEventsRetracted: 0,
    peopleCleared: 0,
    bouncesPromoted: 0,
    retractedBySource: {},
    refusedByRecipientDomain: {},
    retractedLeads: [],
  };
  if (rows.length === 0) return summary;

  await judgeAll(rows, summary);

  const judged: JudgedDsnRow[] = [];
  const unjudgedSendKeys = new Set<string>();
  const refusedDomains = new Map<string, number>();
  for (const row of rows) {
    if (!row.judgment) {
      summary.unjudged += 1;
      unjudgedSendKeys.add(dsnSendKey(row.instantlyCampaignId, row.step));
      continue;
    }
    if (row.judgment.verdict === DSN_ADDRESS_DEAD) summary.addressDead += 1;
    if (row.judgment.verdict === DSN_MESSAGE_REFUSED) {
      summary.messageRefused += 1;
      const d = domainOf(row.leadEmail);
      refusedDomains.set(d, (refusedDomains.get(d) ?? 0) + 1);
    }
    judged.push({
      id: row.id,
      instantlyCampaignId: row.instantlyCampaignId,
      step: row.step,
      verdict: row.judgment.verdict,
    });
  }
  summary.refusedByRecipientDomain = Object.fromEntries(
    [...refusedDomains.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20),
  );

  const events = await loadBounceEvents([...new Set(rows.map((r) => r.instantlyCampaignId))]);
  const plan = planDsnSettlement(judged, unjudgedSendKeys, events);

  summary.bounceEventsRetracted = plan.retract.length;
  for (const e of plan.retract) {
    summary.retractedBySource[e.source] = (summary.retractedBySource[e.source] ?? 0) + 1;
  }
  const people = new Set(plan.retract.map((e) => (e.leadEmail ?? "").toLowerCase()));
  summary.peopleCleared = people.size;
  summary.retractedLeads = [...people].sort();
  summary.bouncesPromoted = plan.promote.length;

  if (dryRun) return summary;

  await retractBouncedEvents(plan.retract, DSN_REFUSED_REASON, "retract:dsn-message-refused");

  const byId = new Map(rows.map((r) => [r.id, r]));
  for (const p of plan.promote) {
    const row = byId.get(p.id)!;
    if (!row.leadEmail) continue;
    await promoteEvent({
      eventType: "email_bounced",
      instantlyCampaignId: row.instantlyCampaignId,
      leadEmail: row.leadEmail,
      accountEmail: row.accountEmail,
      step: row.step,
      variant: null,
      timestamp: row.at,
      source: "self_send",
      sourceRowId: row.id,
    });
  }

  return summary;
}
