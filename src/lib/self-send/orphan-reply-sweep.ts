/**
 * Turning a prospect's answer from ANOTHER address into the reply it is.
 *
 * The poll files every message that references none of our sends `unrelated`
 * (`inbound.ts`). Some of those are a prospect answering from another account,
 * or a colleague answering after a forward (prod 2026-09-24, Doc Dinners: Stacy
 * Blecher answered from drblecher@chsmetabolismdoc.com, the sequence went on and
 * the client never saw it). This sweep reads those rows from bronze, asks a Jev
 * judgment which lead they answer (`orphan-reply.ts`), PERSISTS the judgment on
 * the row (judged once, never re-asked) and, on a match, re-files the row as the
 * lead's reply and does everything a threaded reply does: `reply_received` (the
 * sequence stops, holds are cancelled), the Instantly-side stop, qualification,
 * opt-out (`actOnInboundReply`).
 *
 * Two modes, one code path:
 *   - routine: rows the poll flagged `orphanCandidate` (structural noise already
 *     out, body already stored), run right after every poll inside the dispatch
 *     run, so a recovered reply stops its sequence BEFORE the next selection;
 *   - backfill: every `unrelated` row in a window, narrowed in SQL then filtered
 *     by the same `orphanReplyExclusion`; a body we never stored is re-read from
 *     the mailbox over IMAP by Message-Id (`POST /internal/self-send/orphan-replies`).
 *
 * Spend: one platform judgment per message that shares a distinctive token with
 * a lead this mailbox wrote to; billed by chat-service on a platform run, input
 * tokens only. Nothing here declares a cost.
 */

import { sql } from "drizzle-orm";
import { simpleParser } from "mailparser";

import { db } from "../../db";
import { promoteEvent } from "../silver-promote";
import { platformJudgment } from "../chat-client";
import type { CallerInfo } from "../key-client";
import { htmlToText } from "../forward-positive-reply";
import { actOnInboundReply, emptyPollSummary, type KnownSend, type PollSummary } from "./imap-poller";
import { connectImapClient, createImapClient } from "./imap-client";
import { GMAIL_IMAP_PORT, loginFor, resolveMailboxCredential } from "./mailbox-credentials";
import { parseInstantlySequenceStep } from "./instantly-sends";
import { loadOwnMailDomains } from "./own-mail-domains";
import {
  buildOrphanReplyQuestion,
  buildOrphanReplyState,
  INSTANTLY_WARMUP_TAGS,
  ORPHAN_REPLY_QUESTION_KEY,
  orphanReplyExclusion,
  rankOrphanReplyLeads,
  readOrphanReplyVerdict,
  type OrphanReplyLead,
} from "./orphan-reply";
import type { InboundHeaders } from "./inbound";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/orphan-replies" };

/** How far back a lead may have been written to and still be answered. */
const LEAD_LOOKBACK_DAYS = 120;

/** The routine sweep's window: a flagged row older than this was already offered. */
const ROUTINE_WINDOW_DAYS = 7;

/** The widest backfill window. */
const MAX_BACKFILL_DAYS = 120;

/** Mailboxes re-read at once during a backfill (one session per mailbox). */
const BACKFILL_MAILBOX_CONCURRENCY = 4;

export interface OrphanReplyRecovery {
  rowId: string;
  accountEmail: string;
  from: string | null;
  subject: string | null;
  receivedAt: string;
  leadEmail: string;
  instantlyCampaignId: string;
  orgId: string | null;
  brandIds: string[];
  probability: number;
}

export interface OrphanReplySweepSummary {
  mode: "routine" | "backfill";
  dryRun: boolean;
  rowsRead: number;
  excluded: number;
  bodiesRefetched: number;
  bodiesUnavailable: number;
  noCandidateLead: number;
  judged: number;
  matched: number;
  none: number;
  lowConfidence: number;
  failed: number;
  judgmentInputTokens: number;
  recovered: OrphanReplyRecovery[];
  poll: PollSummary;
}

interface OrphanRow {
  id: string;
  accountEmail: string;
  messageId: string;
  fromAddress: string | null;
  subject: string | null;
  headers: InboundHeaders;
  text: string | null;
  receivedAt: Date;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

function str(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function toHeaders(value: unknown): InboundHeaders {
  if (!value || typeof value !== "object") return {};
  const out: Record<string, string> = {};
  for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
    out[key.toLowerCase()] = typeof raw === "string" ? raw : JSON.stringify(raw ?? "");
  }
  return out;
}

function toRow(r: Record<string, unknown>): OrphanRow {
  return {
    id: String(r.id),
    accountEmail: String(r.accountEmail),
    messageId: String(r.messageId),
    fromAddress: str(r.fromAddress),
    subject: str(r.subject),
    headers: toHeaders(r.headers),
    text: typeof r.text === "string" ? r.text : null,
    receivedAt: new Date((r.receivedAt ?? r.polledAt) as string),
  };
}

/** Rows the poll flagged and nobody has judged yet. */
async function loadRoutineRows(limit: number): Promise<OrphanRow[]> {
  const result = await db.execute(sql`
    SELECT id, account_email AS "accountEmail", message_id AS "messageId",
           from_address AS "fromAddress", subject, payload->'headers' AS "headers",
           payload->>'textSnippet' AS "text", received_at AS "receivedAt", polled_at AS "polledAt"
    FROM imap_messages_raw
    WHERE kind = 'unrelated'
      AND payload->>'orphanCandidate' = 'true'
      AND NOT (payload ? 'orphanJudgment')
      AND polled_at >= now() - make_interval(days => ${ROUTINE_WINDOW_DAYS})
    ORDER BY polled_at
    LIMIT ${limit}
  `);
  return rowsOf(result).map(toRow);
}

/**
 * Every unjudged `unrelated` row of the window, narrowed in SQL on the two
 * rules that remove ~99% of them (our own domains, Instantly's warmup tag) so
 * the full headers are only read for the rest. `orphanReplyExclusion` is still
 * applied to every row returned: the SQL only saves bytes, it decides nothing.
 */
async function loadBackfillRows(
  sinceDays: number,
  ownDomains: ReadonlySet<string>,
  limit: number,
): Promise<OrphanRow[]> {
  const domains = [...ownDomains];
  const tagPatterns = INSTANTLY_WARMUP_TAGS.map((tag) => `%${tag}%`);
  const result = await db.execute(sql`
    SELECT id, account_email AS "accountEmail", message_id AS "messageId",
           from_address AS "fromAddress", subject, payload->'headers' AS "headers",
           payload->>'textSnippet' AS "text", received_at AS "receivedAt", polled_at AS "polledAt"
    FROM imap_messages_raw
    WHERE kind = 'unrelated'
      AND NOT (payload ? 'orphanJudgment')
      AND polled_at >= now() - make_interval(days => ${sinceDays})
      AND lower(split_part(substring(coalesce(from_address, '') from '([^<>\\s"]+@[^<>\\s"]+)'), '@', 2))
          <> ALL(${sql.param(domains)}::text[])
      AND NOT (coalesce(subject, '') ILIKE ANY(${sql.param(tagPatterns)}::text[]))
    ORDER BY polled_at
    LIMIT ${limit}
  `);
  return rowsOf(result).map(toRow);
}

interface SentStep {
  instantlyCampaignId: string;
  step: number;
  sentAt: Date;
}

/**
 * The leads this mailbox wrote to in the lookback, with every step that went
 * out — from BOTH pipes, like `loadKnownSends`: our own dispatch log and what
 * Instantly sent from the same mailbox.
 */
async function loadMailboxLeads(
  accountEmail: string,
  since: Date,
): Promise<{ leads: OrphanReplyLead[]; steps: SentStep[]; campaigns: Map<string, { orgId: string | null; brandIds: string[] }> }> {
  const sentResult = await db.execute(sql`
    SELECT instantly_campaign_id AS "instantlyCampaignId", step::text AS "rawStep",
           dispatched_at AS "sentAt", 'smtp' AS "source"
    FROM smtp_dispatch_raw
    WHERE account_email = ${accountEmail} AND outcome = 'sent' AND step > 0
      AND dispatched_at >= ${since}
    UNION ALL
    SELECT instantly_campaign_id AS "instantlyCampaignId", payload->>'step' AS "rawStep",
           (payload->>'timestamp_email')::timestamptz AS "sentAt", 'instantly' AS "source"
    FROM instantly_emails_raw
    WHERE payload->>'eaccount' = ${accountEmail} AND payload->>'ue_type' = '1'
      AND instantly_campaign_id IS NOT NULL
      AND (payload->>'timestamp_email')::timestamptz >= ${since}
  `);

  const steps: SentStep[] = [];
  for (const r of rowsOf(sentResult)) {
    const step =
      r.source === "smtp" ? Number(r.rawStep) : parseInstantlySequenceStep(str(r.rawStep));
    if (step === null || !Number.isFinite(step) || step < 1) continue;
    steps.push({
      instantlyCampaignId: String(r.instantlyCampaignId),
      step,
      sentAt: new Date(r.sentAt as string),
    });
  }
  const campaignIds = [...new Set(steps.map((s) => s.instantlyCampaignId))];
  const campaigns = new Map<string, { orgId: string | null; brandIds: string[] }>();
  if (campaignIds.length === 0) return { leads: [], steps, campaigns };

  const leadResult = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId", c.lead_email AS "leadEmail",
           c.org_id AS "orgId", COALESCE(to_jsonb(c.brand_ids), '[]'::jsonb) AS "brandIds",
           l.first_name AS "firstName", l.last_name AS "lastName", l.company_name AS "companyName",
           s.subject AS "subject", left(s.body_html, 2000) AS "bodyHtml"
    FROM instantly_campaigns c
    LEFT JOIN LATERAL (
      SELECT first_name, last_name, company_name FROM instantly_leads
      WHERE instantly_campaign_id = c.instantly_campaign_id
      LIMIT 1
    ) l ON true
    LEFT JOIN sequence_steps s ON s.instantly_campaign_id = c.instantly_campaign_id AND s.step = 1
    WHERE c.instantly_campaign_id = ANY(${sql.param(campaignIds)}::text[])
  `);

  const leads: OrphanReplyLead[] = [];
  for (const r of rowsOf(leadResult)) {
    const id = String(r.instantlyCampaignId);
    const own = steps.filter((s) => s.instantlyCampaignId === id);
    if (own.length === 0) continue;
    const brandIds = Array.isArray(r.brandIds) ? (r.brandIds as unknown[]).map(String) : [];
    campaigns.set(id, { orgId: str(r.orgId), brandIds });
    const times = own.map((s) => s.sentAt.getTime());
    const excerpt = str(r.bodyHtml) ? htmlToText(String(r.bodyHtml)).replace(/\s+/g, " ").slice(0, 300) : null;
    leads.push({
      instantlyCampaignId: id,
      leadEmail: String(r.leadEmail),
      firstName: str(r.firstName),
      lastName: str(r.lastName),
      companyName: str(r.companyName),
      subject: str(r.subject),
      excerpt,
      lastStep: Math.max(...own.map((s) => s.step)),
      firstSentAt: new Date(Math.min(...times)),
      lastSentAt: new Date(Math.max(...times)),
      orgId: str(r.orgId),
    });
  }
  return { leads, steps, campaigns };
}

/** The leads as they stood when the message arrived: only steps sent before it. */
function leadsAsOf(leads: readonly OrphanReplyLead[], steps: readonly SentStep[], at: Date): OrphanReplyLead[] {
  const out: OrphanReplyLead[] = [];
  for (const lead of leads) {
    const before = steps.filter(
      (s) => s.instantlyCampaignId === lead.instantlyCampaignId && s.sentAt.getTime() <= at.getTime(),
    );
    if (before.length === 0) continue;
    const times = before.map((s) => s.sentAt.getTime());
    out.push({
      ...lead,
      lastStep: Math.max(...before.map((s) => s.step)),
      firstSentAt: new Date(Math.min(...times)),
      lastSentAt: new Date(Math.max(...times)),
    });
  }
  return out;
}

/**
 * Re-read the bodies we never stored, from the mailbox itself, by Message-Id.
 * One session per mailbox. A message the mailbox no longer holds stays without
 * a body; the judgment then reads the sender and subject only.
 */
async function refetchBodies(rows: OrphanRow[], summary: OrphanReplySweepSummary): Promise<void> {
  const byAccount = new Map<string, OrphanRow[]>();
  for (const row of rows) {
    if (row.text !== null) continue;
    const group = byAccount.get(row.accountEmail) ?? [];
    group.push(row);
    byAccount.set(row.accountEmail, group);
  }
  const groups = [...byAccount.entries()];
  let cursor = 0;

  const worker = async (): Promise<void> => {
    for (;;) {
      const entry = groups[cursor];
      cursor += 1;
      if (!entry) return;
      const [accountEmail, group] = entry;
      try {
        const credential = await resolveMailboxCredential(accountEmail, CALLER);
        const client = createImapClient(
          {
            host: credential.imapHost,
            port: GMAIL_IMAP_PORT,
            secure: true,
            auth: { user: loginFor(credential), pass: credential.appPassword },
            logger: false,
          },
          accountEmail,
        );
        await connectImapClient(client, loginFor(credential), credential.appPassword);
        try {
          const lock = await client.getMailboxLock("INBOX");
          try {
            for (const row of group) {
              const uids = await client.search({ header: { "message-id": row.messageId } }, { uid: true });
              const uid = Array.isArray(uids) ? uids[0] : undefined;
              const full = uid ? await client.fetchOne(String(uid), { source: true }, { uid: true }) : null;
              if (full && full.source) {
                const parsed = await simpleParser(full.source);
                row.text = parsed.text ?? (parsed.html ? htmlToText(String(parsed.html)) : "");
                summary.bodiesRefetched += 1;
              } else {
                summary.bodiesUnavailable += 1;
              }
            }
          } finally {
            lock.release();
          }
        } finally {
          await client.logout().catch(() => {
            // Torn down either way; a failed logout must not mask the read.
          });
        }
      } catch (error) {
        summary.bodiesUnavailable += group.length;
        console.error(
          `[instantly-service] orphan-replies: could not re-read bodies on account=${accountEmail}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(BACKFILL_MAILBOX_CONCURRENCY, groups.length) }, () => worker()),
  );
}

async function persistJudgment(
  row: OrphanRow,
  judgment: Record<string, unknown>,
  storeText: boolean,
): Promise<void> {
  const patch: Record<string, unknown> = { orphanJudgment: judgment };
  if (storeText && row.text !== null) patch.textSnippet = row.text.slice(0, 4000);
  await db.execute(sql`
    UPDATE imap_messages_raw
    SET payload = payload || ${JSON.stringify(patch)}::jsonb
    WHERE id = ${row.id}
  `);
}

export function emptyOrphanReplySweepSummary(
  mode: "routine" | "backfill",
  dryRun: boolean,
): OrphanReplySweepSummary {
  return {
    mode,
    dryRun,
    rowsRead: 0,
    excluded: 0,
    bodiesRefetched: 0,
    bodiesUnavailable: 0,
    noCandidateLead: 0,
    judged: 0,
    matched: 0,
    none: 0,
    lowConfidence: 0,
    failed: 0,
    judgmentInputTokens: 0,
    recovered: [],
    poll: emptyPollSummary(),
  };
}

/**
 * Judge the orphan rows and act on the matches.
 *
 * `dryRun` judges (that is the spend being measured) but writes and promotes
 * nothing. Fail-loud PER ROW: a failed judgment leaves the row unjudged, so the
 * next routine run offers it again; one bad row never stops the sweep.
 */
export async function runOrphanReplySweep(
  options: { backfill?: boolean; sinceDays?: number; dryRun?: boolean; limit?: number } = {},
): Promise<OrphanReplySweepSummary> {
  const backfill = options.backfill === true;
  const dryRun = options.dryRun === true;
  const limit = Math.max(1, Math.min(options.limit ?? 500, 20_000));
  const summary = emptyOrphanReplySweepSummary(backfill ? "backfill" : "routine", dryRun);

  const ownDomains = await loadOwnMailDomains();
  const sinceDays = Math.min(Math.max(options.sinceDays ?? 30, 1), MAX_BACKFILL_DAYS);
  const loaded = backfill
    ? await loadBackfillRows(sinceDays, ownDomains, limit)
    : await loadRoutineRows(limit);
  summary.rowsRead = loaded.length;

  // Header-level exclusion first, so a body is only re-read for what is left.
  const survivors: OrphanRow[] = [];
  for (const row of loaded) {
    const reason = orphanReplyExclusion(
      { fromAddress: row.fromAddress, subject: row.subject, headers: row.headers },
      ownDomains,
    );
    if (reason) {
      summary.excluded += 1;
      continue;
    }
    survivors.push(row);
  }
  if (backfill) await refetchBodies(survivors, summary);

  const mailboxes = new Map<string, Awaited<ReturnType<typeof loadMailboxLeads>>>();

  for (const row of survivors) {
    try {
      // The body may carry the warmup tag the subject did not.
      const reason = orphanReplyExclusion(
        { fromAddress: row.fromAddress, subject: row.subject, headers: row.headers, text: row.text },
        ownDomains,
      );
      if (reason) {
        summary.excluded += 1;
        if (!dryRun) {
          await persistJudgment(row, { outcome: "excluded", reason, judgedAt: new Date().toISOString() }, backfill);
        }
        continue;
      }

      let mailbox = mailboxes.get(row.accountEmail);
      if (!mailbox) {
        const since = new Date(Date.now() - (sinceDays + LEAD_LOOKBACK_DAYS) * 86_400_000);
        mailbox = await loadMailboxLeads(row.accountEmail, since);
        mailboxes.set(row.accountEmail, mailbox);
      }

      const ranked = rankOrphanReplyLeads(
        { fromAddress: row.fromAddress, subject: row.subject, text: row.text, receivedAt: row.receivedAt },
        leadsAsOf(mailbox.leads, mailbox.steps, row.receivedAt),
      );
      if (ranked.length === 0) {
        summary.noCandidateLead += 1;
        if (!dryRun) {
          await persistJudgment(row, { outcome: "no_candidate_lead", judgedAt: new Date().toISOString() }, backfill);
        }
        continue;
      }

      const result = await platformJudgment({
        state: buildOrphanReplyState({
          fromAddress: row.fromAddress,
          toAddress: row.accountEmail,
          subject: row.subject,
          receivedAt: row.receivedAt,
          text: row.text,
        }),
        questions: { [ORPHAN_REPLY_QUESTION_KEY]: buildOrphanReplyQuestion(ranked) },
      });
      summary.judged += 1;
      summary.judgmentInputTokens += result.usage?.inputTokens ?? 0;
      const answer = result.answers[ORPHAN_REPLY_QUESTION_KEY];
      if (!answer) throw new Error("judgment returned no answer for the orphan-reply question");
      const verdict = readOrphanReplyVerdict(answer, ranked);

      const judgment = {
        outcome: verdict.outcome,
        judgedAt: new Date().toISOString(),
        model: result.model,
        choice: answer.choice,
        probability: verdict.probability,
        confidence: answer.confidence,
        probabilities: answer.probabilities,
        candidates: ranked.map(({ lead, score }) => ({
          instantlyCampaignId: lead.instantlyCampaignId,
          leadEmail: lead.leadEmail,
          score,
        })),
        ...(verdict.outcome === "matched"
          ? { instantlyCampaignId: verdict.lead.instantlyCampaignId, leadEmail: verdict.lead.leadEmail }
          : {}),
      };

      if (verdict.outcome !== "matched") {
        if (verdict.outcome === "none") summary.none += 1;
        else summary.lowConfidence += 1;
        if (!dryRun) await persistJudgment(row, judgment, backfill);
        continue;
      }

      summary.matched += 1;
      const lead = verdict.lead;
      const campaign = mailbox.campaigns.get(lead.instantlyCampaignId);
      summary.recovered.push({
        rowId: row.id,
        accountEmail: row.accountEmail,
        from: row.fromAddress,
        subject: row.subject,
        receivedAt: row.receivedAt.toISOString(),
        leadEmail: lead.leadEmail,
        instantlyCampaignId: lead.instantlyCampaignId,
        orgId: lead.orgId,
        brandIds: campaign?.brandIds ?? [],
        probability: verdict.probability,
      });
      if (dryRun) continue;

      // Re-file the bronze row as the lead's reply. The thread read, the
      // Unibox projection and the forward all read `kind`/`instantly_campaign_id`,
      // so this is what puts the message under the lead's conversation.
      const patch: Record<string, unknown> = { orphanJudgment: judgment, reclassifiedFrom: "unrelated" };
      if (row.text !== null) patch.textSnippet = row.text.slice(0, 4000);
      await db.execute(sql`
        UPDATE imap_messages_raw
        SET kind = 'reply',
            instantly_campaign_id = ${lead.instantlyCampaignId},
            step = ${lead.lastStep},
            payload = payload || ${JSON.stringify(patch)}::jsonb
        WHERE id = ${row.id} AND kind = 'unrelated'
      `);

      await promoteEvent({
        eventType: "reply_received",
        instantlyCampaignId: lead.instantlyCampaignId,
        leadEmail: lead.leadEmail,
        accountEmail: row.accountEmail,
        step: lead.lastStep,
        variant: null,
        timestamp: row.receivedAt,
        source: "self_send",
        sourceRowId: row.id,
      });

      const send: KnownSend = {
        instantlyCampaignId: lead.instantlyCampaignId,
        leadEmail: lead.leadEmail,
        step: lead.lastStep,
        orgId: lead.orgId,
      };
      await actOnInboundReply({
        send,
        accountEmail: row.accountEmail,
        rowId: row.id,
        text: row.text ?? "",
        subject: row.subject,
        messageId: row.messageId,
        timestamp: row.receivedAt,
        summary: summary.poll,
      });
      console.log(
        `[instantly-service] orphan-replies: RECOVERED reply from=${row.fromAddress ?? "?"} account=${row.accountEmail} lead=${lead.leadEmail} campaign=${lead.instantlyCampaignId} p=${verdict.probability.toFixed(2)}`,
      );
    } catch (error) {
      summary.failed += 1;
      console.error(
        `[instantly-service] orphan-replies: row=${row.id} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  console.log(
    `[instantly-service] orphan-replies: done ${JSON.stringify(summary)}`,
  );
  return summary;
}
