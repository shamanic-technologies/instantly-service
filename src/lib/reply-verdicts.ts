/**
 * A reply's verdict at the grain of EACH REPLY.
 *
 * The reply kind used to exist only once per (campaign × lead): the latest kind
 * event on the thread. That grain let a newer reply hide behind an older
 * verdict — elena.staeheli@biopartner.ch's 09-28 referral sat behind her 09-24
 * out-of-office verdict and nobody saw it. With one current verdict per reply,
 * "which real replies have no verdict?" is a trivial question.
 *
 * Three layers (drizzle/0061):
 *   - BRONZE `reply_verdicts_raw` — every verdict ever produced, append-only:
 *     a person's statement, Instantly's qualification, our classifier's output.
 *     Verdicts mirrored from silver kind events carry the event id (unique), so
 *     re-running the projection writes nothing twice.
 *   - SILVER `replies` — one row per real inbound reply with its CURRENT verdict.
 *   - The per-(campaign × lead) value (`instantly_campaigns.reply_classification`,
 *     gold `reply_kind`, the stats' latest-sentiment) is UNCHANGED: it stays
 *     derived from the same kind events this layer ingests, so every current
 *     reader behaves exactly as before. The per-reply layer is a finer reading
 *     of the same stream plus the replies that stream never covered.
 *
 * ⚠️ A PROJECTION, NOT A HOT-PATH WRITE. Nothing in ingestion changed: the
 * webhook, the IMAP poller, the fallback and manual qualification keep
 * promoting kind events exactly as they did, and this module mirrors them into
 * bronze on an interval. So it can never slow or break a promotion, and a
 * deploy mid-tick loses nothing.
 *
 * WHICH REPLY A VERDICT IS ABOUT (attribution), strictly in this order:
 *   1. `exact` — the producer named it: a classifier verdict's `reply_ref`, a
 *      self-send event's `source_row_id` (the IMAP row), a fallback event's
 *      `source_row_id` (Instantly's email id).
 *   2. `latest_before` — otherwise the latest reply on the same thread received
 *      at or before the verdict (10-minute skew for clock drift). Instantly
 *      qualifies seconds to hours AFTER a reply, and a person states a kind
 *      after reading one, so the reply just before is the one it judges.
 *   3. none — a verdict older than every stored reply stays in bronze,
 *      attributed to nothing, and is not invented a target.
 *
 * CURRENT verdict per reply: a person's statement beats everything (and a
 * withdrawn one no longer counts), then the most recent, then the latest row.
 */

import { sql, type SQL } from "drizzle-orm";

import { db } from "../db";
import { REPLY_KINDS, REPLY_KIND_CLASSIFICATION } from "./reply-kind";
import { staffSenderSql } from "./staff-senders";
import { qualifyReply } from "./self-send/qualify-reply";
import { htmlToText } from "./forward-positive-reply";

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Clock-drift allowance when attributing a verdict to the reply before it. */
export const ATTRIBUTION_SKEW = "10 minutes";

const kindList = (): SQL =>
  sql.join(
    REPLY_KINDS.map((k) => sql`${k}`),
    sql`, `,
  );

/** `CASE kind WHEN … THEN 'positive' …` — the coarse projection, in SQL. */
function classificationCase(column: SQL): SQL {
  const whens = sql.join(
    Object.entries(REPLY_KIND_CLASSIFICATION).map(([k, v]) => sql`WHEN ${k} THEN ${v}`),
    sql` `,
  );
  return sql`CASE ${column} ${whens} END`;
}

/** 1. Mirror kind events into bronze. `sinceDays` null = the whole history. */
async function ingestEventVerdicts(sinceDays: number | null): Promise<number> {
  const since =
    sinceDays === null ? sql`` : sql`AND e.created_at > now() - make_interval(days => ${sinceDays})`;
  const result = await db.execute(sql`
    INSERT INTO reply_verdicts_raw
      (instantly_campaign_id, lead_email, kind, producer_type, producer, origin,
       source_event_id, source_row_id, raw, decided_at)
    SELECT e.campaign_id,
           e.lead_email,
           e.event_type,
           CASE WHEN e.source = 'manual' THEN 'human'
                WHEN e.source IN ('self_send', 'emails_backfill') THEN 'model'
                ELSE 'instantly' END,
           CASE WHEN e.source = 'manual' THEN 'manual'
                WHEN e.source IN ('self_send', 'emails_backfill') THEN 'deepseek-flash'
                ELSE 'instantly:' || e.source END,
           'event',
           e.id,
           e.source_row_id,
           jsonb_build_object('eventType', e.event_type, 'source', e.source, 'step', e.step),
           e.timestamp AT TIME ZONE 'UTC'
    FROM instantly_events e
    WHERE e.event_type IN (${kindList()})
      AND e.inferred = false
      AND e.campaign_id IS NOT NULL
      ${since}
    ON CONFLICT (source_event_id) DO NOTHING
  `);
  return (result as { rowCount?: number }).rowCount ?? 0;
}

/**
 * The inbound messages that are REAL replies: on a campaign thread, and not
 * written by us — neither one of our own people (staff-senders), nor one of our
 * own sending mailboxes, nor a mail server (postmaster / mailer-daemon).
 */
async function upsertReplies(): Promise<number> {
  const fromIe = sql`lower(r.payload->>'from_address_email')`;
  const imapFrom = sql`lower(coalesce(substring(m.from_address from '<([^<>]+)>'), m.from_address))`;
  const result = await db.execute(sql`
    INSERT INTO replies
      (id, source_table, source_row_id, provider_message_id, instantly_campaign_id,
       campaign_id, org_id, brand_ids, lead_email, from_email, transport, subject,
       received_at, synced_at)
    SELECT * FROM (
      SELECT 'ie:' || r.instantly_email_id, 'instantly_emails_raw', r.id, r.instantly_email_id,
             r.instantly_campaign_id, c.campaign_id, c.org_id, c.brand_ids, c.lead_email,
             ${fromIe}, 'instantly', r.payload->>'subject',
             COALESCE((r.payload->>'timestamp_email')::timestamptz, r.fetched_at AT TIME ZONE 'UTC'),
             now()
      FROM instantly_emails_raw r
      JOIN instantly_campaigns c ON c.instantly_campaign_id = r.instantly_campaign_id
      WHERE r.payload->>'ue_type' = '2'
        AND r.instantly_email_id IS NOT NULL
        -- A campaign row with no lead address cannot be read per lead.
        AND c.lead_email IS NOT NULL
        AND NOT ${staffSenderSql(sql`r.payload->>'from_address_email'`)}
        AND split_part(${fromIe}, '@', 1) NOT IN ('postmaster', 'mailer-daemon')
        AND NOT EXISTS (SELECT 1 FROM instantly_accounts a WHERE lower(a.email) = ${fromIe})

      UNION ALL

      SELECT 'imap:' || m.id, 'imap_messages_raw', m.id, m.message_id,
             m.instantly_campaign_id, c.campaign_id, c.org_id, c.brand_ids, c.lead_email,
             ${imapFrom}, COALESCE(c.send_transport, 'smtp'), m.subject,
             COALESCE(m.received_at, m.polled_at) AT TIME ZONE 'UTC',
             now()
      FROM imap_messages_raw m
      JOIN instantly_campaigns c ON c.instantly_campaign_id = m.instantly_campaign_id
      WHERE m.kind IN ('reply', 'auto_reply')
        AND c.lead_email IS NOT NULL
        -- The same message mirrored by Instantly is already a reply above.
        AND NOT EXISTS (
          SELECT 1 FROM instantly_emails_raw x
          WHERE x.instantly_campaign_id = m.instantly_campaign_id
            AND x.payload->>'message_id' = m.message_id
        )
    ) src
    ON CONFLICT (id) DO UPDATE SET
      campaign_id = excluded.campaign_id,
      org_id = excluded.org_id,
      brand_ids = excluded.brand_ids,
      lead_email = excluded.lead_email,
      transport = excluded.transport,
      synced_at = excluded.synced_at
  `);
  return (result as { rowCount?: number }).rowCount ?? 0;
}

/**
 * The attributed verdicts, as a SQL fragment (CTE body): one row per
 * (verdict, reply) with its attribution. A withdrawn human statement is out.
 */
function attributedVerdictsSql(): SQL {
  return sql`
    SELECT v.id AS verdict_id, x.reply_id, x.attribution, v.kind, v.producer_type,
           v.producer, v.confidence, v.decided_at, v.created_at
    FROM reply_verdicts_raw v
    LEFT JOIN instantly_events ev ON ev.id = v.source_event_id
    CROSS JOIN LATERAL (
      SELECT r.id AS reply_id, 'exact' AS attribution, 0 AS rank, r.received_at
      FROM replies r
      WHERE r.instantly_campaign_id = v.instantly_campaign_id
        AND (r.id = v.reply_ref
             OR (v.reply_ref IS NULL AND v.source_row_id IS NOT NULL
                 AND (r.source_row_id = v.source_row_id OR r.provider_message_id = v.source_row_id)))
      UNION ALL
      SELECT r.id, 'latest_before', 1, r.received_at
      FROM replies r
      WHERE r.instantly_campaign_id = v.instantly_campaign_id
        AND v.reply_ref IS NULL
        AND r.received_at <= v.decided_at + ${sql.raw(`interval '${ATTRIBUTION_SKEW}'`)}
      ORDER BY rank, received_at DESC
      LIMIT 1
    ) x
    WHERE ev.withdrawn_at IS NULL
  `;
}

/** 3. Recompute every reply's current verdict. */
async function recomputeCurrent(): Promise<void> {
  // A reply whose every verdict went away (a withdrawn statement) has none.
  await db.execute(sql`
    WITH attributed AS (${attributedVerdictsSql()})
    UPDATE replies r SET
      current_verdict_id = NULL, current_kind = NULL, current_classification = NULL,
      current_producer_type = NULL, current_producer = NULL, current_attribution = NULL,
      current_confidence = NULL, current_decided_at = NULL, verdict_count = 0
    WHERE r.current_verdict_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM attributed a WHERE a.reply_id = r.id)
  `);
  await db.execute(sql`
    WITH attributed AS (${attributedVerdictsSql()}),
    counts AS (SELECT reply_id, count(*)::int AS n FROM attributed GROUP BY reply_id),
    ranked AS (
      SELECT DISTINCT ON (reply_id) *
      FROM attributed
      ORDER BY reply_id, (producer_type = 'human') DESC, decided_at DESC, created_at DESC, verdict_id DESC
    )
    UPDATE replies r SET
      current_verdict_id = k.verdict_id,
      current_kind = k.kind,
      current_classification = ${classificationCase(sql`k.kind`)},
      current_producer_type = k.producer_type,
      current_producer = k.producer,
      current_attribution = k.attribution,
      current_confidence = k.confidence,
      current_decided_at = k.decided_at,
      verdict_count = c.n
    FROM ranked k JOIN counts c ON c.reply_id = k.reply_id
    WHERE r.id = k.reply_id
      AND (r.current_verdict_id IS DISTINCT FROM k.verdict_id OR r.verdict_count IS DISTINCT FROM c.n)
  `);
}

export interface ReplyVerdictSyncSummary {
  verdictsIngested: number;
  repliesUpserted: number;
}

/** One projection pass. `sinceDays` bounds the event scan; null = everything. */
export async function syncReplyVerdicts(
  opts: { sinceDays?: number | null } = {},
): Promise<ReplyVerdictSyncSummary> {
  const verdictsIngested = await ingestEventVerdicts(opts.sinceDays ?? null);
  const repliesUpserted = await upsertReplies();
  await recomputeCurrent();
  return { verdictsIngested, repliesUpserted };
}

// ─── Backfill: the replies no producer ever judged ───────────────────────────

export interface ReplyVerdictPlan {
  replies: number;
  withVerdict: number;
  carriedExact: number;
  carriedLatestBefore: number;
  withoutVerdict: number;
  /** Rough classifier cost, DeepSeek V4 Flash org price (input-dominated). */
  estimatedCostUsd: number;
}

/** Counts for the whole population — what a backfill would do, spending nothing. */
export async function planReplyVerdicts(sinceDays: number | null): Promise<ReplyVerdictPlan> {
  const window =
    sinceDays === null ? sql`TRUE` : sql`r.received_at > now() - make_interval(days => ${sinceDays})`;
  const row = rowsOf(
    await db.execute(sql`
      SELECT count(*)::int AS replies,
             count(*) FILTER (WHERE current_kind IS NOT NULL)::int AS with_verdict,
             count(*) FILTER (WHERE current_attribution = 'exact')::int AS exact,
             count(*) FILTER (WHERE current_attribution = 'latest_before')::int AS latest_before,
             count(*) FILTER (WHERE current_kind IS NULL)::int AS without
      FROM replies r WHERE ${window}
    `),
  )[0] ?? {};
  const without = Number(row.without ?? 0);
  // ~800 input tokens per call (prompt + reply) at ~$1.5 per 1M org price.
  return {
    replies: Number(row.replies ?? 0),
    withVerdict: Number(row.with_verdict ?? 0),
    carriedExact: Number(row.exact ?? 0),
    carriedLatestBefore: Number(row.latest_before ?? 0),
    withoutVerdict: without,
    estimatedCostUsd: Math.round(without * 800 * 1.5e-6 * 100) / 100,
  };
}

interface UnjudgedReply {
  id: string;
  instantlyCampaignId: string;
  leadEmail: string;
  subject: string | null;
  text: string;
}

async function selectUnjudgedReplies(sinceDays: number | null, limit: number | null): Promise<UnjudgedReply[]> {
  const window =
    sinceDays === null ? sql`TRUE` : sql`r.received_at > now() - make_interval(days => ${sinceDays})`;
  const rows = rowsOf(
    await db.execute(sql`
      SELECT r.id, r.instantly_campaign_id, r.lead_email, r.subject,
             COALESCE(NULLIF(ie.payload->'body'->>'text', ''), ie.payload->'body'->>'html',
                      m.payload->>'textSnippet', '') AS body
      FROM replies r
      LEFT JOIN instantly_emails_raw ie ON r.source_table = 'instantly_emails_raw' AND ie.id = r.source_row_id
      LEFT JOIN imap_messages_raw m ON r.source_table = 'imap_messages_raw' AND m.id = r.source_row_id
      WHERE r.current_kind IS NULL AND ${window}
      ORDER BY r.received_at
      ${limit === null ? sql`` : sql`LIMIT ${limit}`}
    `),
  );
  return rows.map((row) => ({
    id: String(row.id),
    instantlyCampaignId: String(row.instantly_campaign_id),
    leadEmail: String(row.lead_email),
    subject: typeof row.subject === "string" ? row.subject : null,
    text: htmlToText(String(row.body ?? "")),
  }));
}

export interface ReplyVerdictBackfillSummary {
  candidates: number;
  classified: number;
  unqualified: number;
  noBody: number;
  failed: number;
}

/**
 * Classify every stored reply no producer ever judged, recording the verdict in
 * BRONZE ONLY.
 *
 * ⚠️ NO SILVER EVENT IS PROMOTED, deliberately: an event fires the reply side
 * effects (forward, escalation, rep call, follow-up debt) and moves the
 * per-(campaign × lead) value. A historical reply must do neither — it would
 * re-contact people about conversations long closed, and silently change what
 * current readers see. Nothing is sent to anyone.
 */
export async function backfillReplyVerdicts(
  opts: { sinceDays?: number | null; limit?: number | null } = {},
): Promise<ReplyVerdictBackfillSummary> {
  await syncReplyVerdicts({ sinceDays: null });
  const candidates = await selectUnjudgedReplies(opts.sinceDays ?? null, opts.limit ?? null);
  const summary: ReplyVerdictBackfillSummary = {
    candidates: candidates.length,
    classified: 0,
    unqualified: 0,
    noBody: 0,
    failed: 0,
  };

  for (const reply of candidates) {
    if (!reply.text.trim()) {
      summary.noBody += 1;
      continue;
    }
    try {
      const kind = await qualifyReply(reply.text, {
        instantlyCampaignId: reply.instantlyCampaignId,
        leadEmail: reply.leadEmail,
        source: "reply_verdict_backfill",
        subject: reply.subject,
      });
      if (!kind) {
        summary.unqualified += 1;
        continue;
      }
      await db.execute(sql`
        INSERT INTO reply_verdicts_raw
          (instantly_campaign_id, lead_email, kind, producer_type, producer, origin, reply_ref, raw, decided_at)
        VALUES (${reply.instantlyCampaignId}, ${reply.leadEmail}, ${kind}, 'model', 'deepseek-flash',
                'backfill_classifier', ${reply.id}, ${JSON.stringify({ classification: kind })}::jsonb, now())
      `);
      summary.classified += 1;
    } catch (error: unknown) {
      summary.failed += 1;
      console.error(
        `[instantly-service] reply-verdict-backfill: FAILED reply=${reply.id}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  await recomputeCurrent();
  return summary;
}

// ─── The read another service rolls up from ──────────────────────────────────

export interface ReplyVerdictView {
  replyId: string;
  leadEmail: string;
  instantlyCampaignId: string;
  campaignId: string | null;
  brandIds: string[];
  transport: string;
  fromEmail: string | null;
  subject: string | null;
  receivedAt: string;
  verdict: {
    kind: string;
    classification: string | null;
    producerType: string;
    producer: string;
    attribution: string;
    confidence: number | null;
    decidedAt: string;
  } | null;
  verdictCount: number;
}

function iso(value: unknown): string {
  return (value instanceof Date ? value : new Date(String(value))).toISOString();
}

/**
 * Every real reply this org holds for these leads, with each reply's current
 * verdict, oldest first per lead. Org-scoped in the query itself; the address
 * matches case-insensitively.
 */
export async function readReplyVerdicts(input: {
  orgId: string;
  emails: string[];
  brandId?: string | null;
  campaignId?: string | null;
}): Promise<ReplyVerdictView[]> {
  const emails = [...new Set(input.emails.map((e) => e.trim().toLowerCase()).filter(Boolean))];
  if (emails.length === 0) return [];
  const list = sql.join(
    emails.map((e) => sql`${e}`),
    sql`, `,
  );
  const rows = rowsOf(
    await db.execute(sql`
      SELECT * FROM replies r
      WHERE r.org_id = ${input.orgId}
        AND lower(r.lead_email) IN (${list})
        ${input.campaignId ? sql`AND r.campaign_id = ${input.campaignId}` : sql``}
        ${input.brandId ? sql`AND ${input.brandId} = ANY(r.brand_ids)` : sql``}
      ORDER BY lower(r.lead_email), r.received_at
    `),
  );
  return rows.map((r) => ({
    replyId: String(r.id),
    leadEmail: String(r.lead_email),
    instantlyCampaignId: String(r.instantly_campaign_id),
    campaignId: (r.campaign_id as string | null) ?? null,
    brandIds: Array.isArray(r.brand_ids) ? (r.brand_ids as string[]) : [],
    transport: String(r.transport),
    fromEmail: (r.from_email as string | null) ?? null,
    subject: (r.subject as string | null) ?? null,
    receivedAt: iso(r.received_at),
    verdict:
      r.current_kind == null
        ? null
        : {
            kind: String(r.current_kind),
            classification: (r.current_classification as string | null) ?? null,
            producerType: String(r.current_producer_type),
            producer: String(r.current_producer),
            attribution: String(r.current_attribution),
            confidence: r.current_confidence == null ? null : Number(r.current_confidence),
            decidedAt: iso(r.current_decided_at),
          },
    verdictCount: Number(r.verdict_count ?? 0),
  }));
}
