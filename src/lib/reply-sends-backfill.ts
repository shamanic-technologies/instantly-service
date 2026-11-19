/**
 * REPLY-BACKFILL-1119 (owner go 2026-11-19): the answers sent in a thread BEFORE
 * REPLY-SENT-UNIT-1118 (v0.84.36) went out with no `message_sent` on the ledger.
 * This declares the past ones through the SAME live code (lib/reply-send-outcomes:
 * `buildReplySendEntry`, `classifyReplySendAnswer`; lib/send-outcomes:
 * `postOutcomesBatch`), on the run that sent each one.
 *
 * Our send log (`smtp_dispatch_raw`, step 0 = an answer, see manual-reply-step)
 * never held the caller's run. The sender is found in workflow-service's record of
 * its script runs (`script_runs`: org, window, the thread it answered = its
 * `result.content.replyTo`, the address it named), handed to this module as a
 * read-only snapshot (`SenderRun[]`). A reply is matched by a POSITIVE key only:
 *  - same org, sent inside the run's window, the run made exactly ONE successful
 *    `instantly.reply.send` call, AND
 *  - the same thread: the run's `replyTo` is the reply's `In-Reply-To`, its named
 *    To (if any) is who the reply went to; or a NEW thread on both sides, the run
 *    naming the lead's address as To;
 *  - a lead the run declared it consumed for, if it declared any, is the reply's lead;
 *  - one run for one reply, both ways (an ambiguous pair is never declared).
 * Time proximity alone is never a match. No match = not declared, listed with why.
 *
 * Declared = the lead's `message_sent` on that run, dated when it left, nothing
 * consumed, no cost. The row is then kept in `reply_send_outcomes` (the live
 * table, with its mark) so a re-run and the live sweep both skip it.
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { buildReplySendEntry, classifyReplySendAnswer, type ReplySendMark } from "./reply-send-outcomes";
import { postOutcomesBatch } from "./send-outcomes";

/** runs-service's ledger starts here: a send before it is out of this pass. */
export const LEDGER_START = "2026-10-23T00:00:00.000Z";

/** One answer of our send log not recorded in `reply_send_outcomes`. */
export interface PastReply {
  dispatchId: string;
  orgId: string | null;
  leadId: string | null;
  instantlyCampaignId: string;
  leadEmail: string;
  /** ISO instant it left. */
  sentAt: string;
  sentBy: string | null;
  messageId: string | null;
  inReplyTo: string | null;
  newThread: boolean;
  /** The To the caller chose, when it chose one (else the lead). */
  chosenTo: string[];
  transport: "smtp" | "instantly";
}

/** One script run of workflow-service that made exactly one successful `instantly.reply.send` call. */
export interface SenderRun {
  scriptRunId: string;
  orgId: string;
  /** Its runs-service run: the `x-run-id` the answer was sent under. */
  runId: string;
  createdAt: string;
  finishedAt: string | null;
  okSendCalls: number;
  /** `result.content.replyTo`: the Message-Id of the message it answered. */
  replyTo: string | null;
  inThread: boolean | null;
  /** `result.content.recipients.to`. */
  to: string[];
  /** Leads it declared it consumed for (lead-service subjects). */
  leadIds: string[];
}

export type Unmatched =
  | "before_ledger_start"
  | "no_lead_id"
  | "no_org"
  | "hand_sent_no_sender_run"
  | "no_sender_run_on_record"
  | "sender_run_without_thread_key"
  | "ambiguous_runs"
  | "run_claimed_by_several_replies"
  | "run_already_declared_an_answer";

export type Verdict = { kind: "match"; run: SenderRun; key: "thread" | "new_thread" } | { kind: "unmatched"; reason: Unmatched };

const lower = (xs: string[]) => xs.map((x) => x.trim().toLowerCase());
const ms = (iso: string) => Date.parse(iso);

function inWindow(reply: PastReply, run: SenderRun): boolean {
  if (!run.finishedAt) return false;
  const t = ms(reply.sentAt);
  return ms(run.createdAt) <= t && t <= ms(run.finishedAt);
}

/** The positive key: same thread (or a new thread to the same address), never time alone. */
function threadKey(reply: PastReply, run: SenderRun): "thread" | "new_thread" | null {
  const to = lower(reply.chosenTo.length > 0 ? reply.chosenTo : [reply.leadEmail]);
  const runTo = lower(run.to);
  if (run.leadIds.length > 0 && (!reply.leadId || !run.leadIds.includes(reply.leadId))) return null;
  if (reply.inReplyTo && run.replyTo && run.replyTo === reply.inReplyTo) {
    if (runTo.length > 0 && !runTo.every((a) => to.includes(a))) return null;
    return "thread";
  }
  if (reply.newThread && !reply.inReplyTo && run.inThread === false && !run.replyTo) {
    if (runTo.length > 0 && runTo.every((a) => to.includes(a))) return "new_thread";
  }
  return null;
}

/**
 * Match every past reply to its sender run (pure). `declaredRunIds` = runs that already
 * carry a recorded answer in `reply_send_outcomes` (their one send is already declared).
 */
export function matchPastReplies(replies: PastReply[], runs: SenderRun[], declaredRunIds: Set<string>): Map<string, Verdict> {
  const out = new Map<string, Verdict>();
  const byOrg = new Map<string, SenderRun[]>();
  for (const r of runs) {
    if (r.okSendCalls !== 1) continue;
    const list = byOrg.get(r.orgId) ?? [];
    list.push(r);
    byOrg.set(r.orgId, list);
  }
  const tentative = new Map<string, { run: SenderRun; key: "thread" | "new_thread" }>();
  for (const reply of replies) {
    if (ms(reply.sentAt) < ms(LEDGER_START)) { out.set(reply.dispatchId, { kind: "unmatched", reason: "before_ledger_start" }); continue; }
    if (!reply.orgId) { out.set(reply.dispatchId, { kind: "unmatched", reason: "no_org" }); continue; }
    const windowed = (byOrg.get(reply.orgId) ?? []).filter((r) => inWindow(reply, r));
    const keyed = windowed.flatMap((run) => {
      const key = threadKey(reply, run);
      return key ? [{ run, key }] : [];
    });
    if (keyed.length > 1) { out.set(reply.dispatchId, { kind: "unmatched", reason: "ambiguous_runs" }); continue; }
    if (keyed.length === 0) {
      const reason: Unmatched = reply.sentBy === "human" ? "hand_sent_no_sender_run" : windowed.length > 0 ? "sender_run_without_thread_key" : "no_sender_run_on_record";
      out.set(reply.dispatchId, { kind: "unmatched", reason });
      continue;
    }
    if (!reply.leadId) { out.set(reply.dispatchId, { kind: "unmatched", reason: "no_lead_id" }); continue; }
    if (declaredRunIds.has(keyed[0].run.runId)) { out.set(reply.dispatchId, { kind: "unmatched", reason: "run_already_declared_an_answer" }); continue; }
    tentative.set(reply.dispatchId, keyed[0]);
  }
  const claims = new Map<string, number>();
  for (const { run } of tentative.values()) claims.set(run.runId, (claims.get(run.runId) ?? 0) + 1);
  for (const [id, { run, key }] of tentative) {
    out.set(id, (claims.get(run.runId) ?? 0) > 1 ? { kind: "unmatched", reason: "run_claimed_by_several_replies" } : { kind: "match", run, key });
  }
  return out;
}

/** Every answer of the send log that is not in `reply_send_outcomes` yet (oldest first). */
export async function readPastReplies(): Promise<PastReply[]> {
  const result = await db.execute(sql`
    SELECT d.id, c.org_id, c.lead_id, d.instantly_campaign_id, d.lead_email,
           to_char(d.dispatched_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS sent_at,
           d.payload->>'sentBy' AS sent_by, d.message_id, d.payload->>'inReplyTo' AS in_reply_to,
           d.payload->>'newThread' AS new_thread, d.payload->'to' AS chosen_to
    FROM smtp_dispatch_raw d
    LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = d.instantly_campaign_id
    WHERE d.step = 0 AND d.outcome = 'sent'
      AND NOT EXISTS (
        SELECT 1 FROM reply_send_outcomes r
        WHERE r.instantly_campaign_id = d.instantly_campaign_id AND r.message_id = COALESCE(d.message_id, 'dispatch:' || d.id)
      )
    ORDER BY d.dispatched_at
  `);
  return (result.rows as Array<Record<string, unknown>>).map((r) => {
    const chosen = r.chosen_to;
    return {
      dispatchId: String(r.id),
      orgId: (r.org_id as string | null) ?? null,
      leadId: (r.lead_id as string | null) ?? null,
      instantlyCampaignId: String(r.instantly_campaign_id),
      leadEmail: String(r.lead_email),
      sentAt: String(r.sent_at),
      sentBy: (r.sent_by as string | null) ?? null,
      messageId: (r.message_id as string | null) ?? null,
      inReplyTo: (r.in_reply_to as string | null) || null,
      newThread: r.new_thread === "true",
      chosenTo: Array.isArray(chosen) ? chosen.map(String) : typeof chosen === "string" ? [chosen] : [],
      transport: r.message_id ? "smtp" : "instantly",
    };
  });
}

async function readDeclaredRunIds(): Promise<Set<string>> {
  const result = await db.execute(sql`SELECT DISTINCT run_id FROM reply_send_outcomes WHERE run_id IS NOT NULL`);
  return new Set((result.rows as Array<{ run_id: string }>).map((r) => String(r.run_id)));
}

export interface BackfillReport {
  dryRun: boolean;
  read: number;
  matched: number;
  matchedBy: Record<string, number>;
  unmatched: Record<string, number>;
  perOrg: Record<string, Record<string, number>>;
  applied: { recorded: number; alreadyRecorded: number; runGone: number; refused: number; retry: number };
  declaredDispatchIds: string[];
  unmatchedList: Array<{ dispatchId: string; orgId: string | null; sentAt: string; sentBy: string | null; reason: Unmatched }>;
}

/** Keep the declared answer in the live table, with its mark, so nothing declares it again. */
async function keep(reply: PastReply, run: SenderRun, mark: ReplySendMark, error: string | null): Promise<void> {
  await db.execute(sql`
    INSERT INTO reply_send_outcomes (org_id, instantly_campaign_id, lead_email, message_id, transport, sent_by, run_id, sent_at, outcomes_declaration, outcomes_declared_at, outcomes_error)
    VALUES (${reply.orgId}, ${reply.instantlyCampaignId}, ${reply.leadEmail}, ${reply.messageId ?? `dispatch:${reply.dispatchId}`}, ${reply.transport},
            ${reply.sentBy ?? "unknown"}, ${run.runId}, ${reply.sentAt}::timestamptz AT TIME ZONE 'UTC', ${mark}, now(), ${error})
    ON CONFLICT (instantly_campaign_id, message_id) DO NOTHING
  `);
}

/**
 * Dry run by default: match and count, write nothing. `apply` declares up to `limit`
 * matched answers (oldest first, or only `onlyDispatchIds`), one call each.
 */
export async function backfillReplySends(opts: {
  senderRuns: SenderRun[];
  apply?: boolean;
  limit?: number;
  onlyDispatchIds?: string[];
}): Promise<BackfillReport> {
  const replies = await readPastReplies();
  const verdicts = matchPastReplies(replies, opts.senderRuns, await readDeclaredRunIds());
  const report: BackfillReport = {
    dryRun: !opts.apply,
    read: replies.length,
    matched: 0,
    matchedBy: {},
    unmatched: {},
    perOrg: {},
    applied: { recorded: 0, alreadyRecorded: 0, runGone: 0, refused: 0, retry: 0 },
    declaredDispatchIds: [],
    unmatchedList: [],
  };
  const toApply: Array<{ reply: PastReply; run: SenderRun }> = [];
  for (const reply of replies) {
    const v = verdicts.get(reply.dispatchId)!;
    const label = v.kind === "match" ? `matched_${v.key}` : v.reason;
    const org = (report.perOrg[reply.orgId ?? "none"] ??= {});
    org[label] = (org[label] ?? 0) + 1;
    if (v.kind === "match") {
      report.matched++;
      report.matchedBy[v.key] = (report.matchedBy[v.key] ?? 0) + 1;
      if (!opts.onlyDispatchIds || opts.onlyDispatchIds.includes(reply.dispatchId)) toApply.push({ reply, run: v.run });
    } else {
      report.unmatched[v.reason] = (report.unmatched[v.reason] ?? 0) + 1;
      report.unmatchedList.push({ dispatchId: reply.dispatchId, orgId: reply.orgId, sentAt: reply.sentAt, sentBy: reply.sentBy, reason: v.reason });
    }
  }
  if (!opts.apply) return report;

  for (const { reply, run } of toApply.slice(0, opts.limit ?? toApply.length)) {
    const answer = await postOutcomesBatch([buildReplySendEntry({ runId: run.runId, leadId: reply.leadId!, sentAt: reply.sentAt })]);
    const mark = classifyReplySendAnswer(answer.status, answer.body);
    if (mark === "retry") {
      console.error(`[instantly-service] reply-sends-backfill: runs-service ${answer.status} for run=${run.runId} (left for a re-run): ${answer.body.slice(0, 300)}`);
      report.applied.retry++;
      continue;
    }
    await keep(reply, run, mark, mark === "declared" ? null : answer.body.slice(0, 1000));
    if (mark === "declared") {
      const already = (() => {
        try {
          return (JSON.parse(answer.body) as { alreadyRecordedCount?: number }).alreadyRecordedCount ?? 0;
        } catch {
          return 0;
        }
      })();
      if (already > 0) report.applied.alreadyRecorded++;
      else report.applied.recorded++;
      report.declaredDispatchIds.push(reply.dispatchId);
    } else if (mark === "run_gone") report.applied.runGone++;
    else report.applied.refused++;
  }
  return report;
}
