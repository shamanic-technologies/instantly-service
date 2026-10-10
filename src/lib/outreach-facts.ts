/**
 * The outreach FACT FEED: every dated thing our outreach did or saw for a
 * person, served in one total order (`GET /internal/outreach-facts`) so
 * lead-service can COPY it by cursor into its own bronze, the same way it
 * copies crm-service's people facts. This service says WHAT HAPPENED, in its
 * own words; lead-service owns the label vocabulary and the timeline.
 *
 * Gold table `outreach_facts` (drizzle/0067), APPEND-ONLY:
 *
 *  - EVENT facts, one per real (`inferred = false`) silver event, emitted once
 *    (partial unique index on `subject_key = 'ievt:<event id>'`):
 *      `email_sent`    — every email of a sequence we sent, with its step and
 *                        whether it was the FIRST email or a FOLLOW-UP;
 *      `email_opened`, `link_clicked` (with the URL when our own tracker
 *                        recorded it), `email_bounced`, `unsubscribed`.
 *  - REPLY facts, one per real reply (silver `replies`), carrying its current
 *    verdict, the kind-derived distinctions, the Jev judgments and the
 *    escalation. When any of that changes, a NEW reply fact is appended naming
 *    the one it supersedes (`supersedes_seq`). Never an edit.
 *  - REPLY-SENT facts (`reply_sent`), one per email WE sent into a prospect's
 *    thread outside the sequence: every answer through `POST /orgs/replies`
 *    (a human's or the automation's, both transports: `smtp_dispatch_raw`
 *    step 0), an answer one of our people sent from their own client and
 *    CC'd the mailbox (IMAP `staff_reply`), and an answer typed straight into
 *    Instantly's Unibox (mirror `ue_type` 3 no dispatch row claims). Emitted
 *    once (partial unique index on `subject_key`, drizzle/0069), so a
 *    consumer holding a copy of the thread learns it moved.
 *  - `withdrawn` facts: the source is gone — a click later proven to be a link
 *    scanner (its event deleted), a bounce retracted as a delayed DSN, a
 *    recorded opt-out withdrawn by staff, a stated reply pruned once its real
 *    message was found. `supersedes_seq` names the withdrawn fact.
 *
 * Emission takes a GLOBAL advisory lock for its transaction, so `seq` values
 * commit in order and a reader paging by cursor never skips a fact.
 *
 * Not served: opens on our own transport (no pixel: the open there is inferred
 * from the click and the click is served), spam complaints (nothing captures
 * them), inferred events (projections, not observations).
 *
 * A projection, never a hot-path write: nothing in ingestion changed.
 */
import { createHash } from "node:crypto";

import { sql, type SQL } from "drizzle-orm";

import { db } from "../db";
import { escalationColumns, escalationJoin, toReplyVerdictView, type ReplyVerdictView } from "./reply-verdicts";
import { readReplyJudgments } from "./reply-judgments";

/** Any 64-bit constant unique to this lock. */
const FEED_LOCK_KEY = 726_400_117;

export const EVENT_FACT_TYPES = [
  "email_sent",
  "email_opened",
  "link_clicked",
  "email_bounced",
  "unsubscribed",
] as const;
export const OUTREACH_FACT_TYPES = [...EVENT_FACT_TYPES, "reply", "reply_sent", "withdrawn"] as const;
export type OutreachFactType = (typeof OUTREACH_FACT_TYPES)[number];

/** Silver event type → fact type. */
const EVENT_TYPE_TO_FACT: Record<string, (typeof EVENT_FACT_TYPES)[number]> = {
  email_sent: "email_sent",
  email_opened: "email_opened",
  email_link_clicked: "link_clicked",
  email_bounced: "email_bounced",
  lead_unsubscribed: "unsubscribed",
};

/** Event facts whose source can later disappear or be withdrawn. */
const WITHDRAWABLE_EVENT_FACTS = ["link_clicked", "email_bounced", "unsubscribed"] as const;

/**
 * A sequence send Instantly's `/emails` poll reported without a step is the
 * same email as a stepped webhook send when they are this close.
 */
const UNSTEPPED_DUPLICATE_SECONDS = 120;

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

function iso(value: unknown): string {
  return (value instanceof Date ? value : new Date(String(value))).toISOString();
}

const inList = (values: readonly string[]): SQL =>
  sql.join(
    values.map((v) => sql`${v}`),
    sql`, `,
  );

type Executor = { execute: (query: SQL) => Promise<unknown> };

// ─── Event facts ─────────────────────────────────────────────────────────────

/**
 * Emit every real event not emitted yet, oldest first. `sinceDays` bounds the
 * scan on the event's INSERTION time (a backdated event is still fresh there);
 * null = the whole history. Idempotent (unique subject).
 */
export function eventFactsInsertSql(sinceDays: number | null): SQL {
  const window =
    sinceDays === null ? sql`` : sql`AND e.created_at > now() - make_interval(days => ${sinceDays})`;
  const factType = sql`CASE e.event_type ${sql.join(
    Object.entries(EVENT_TYPE_TO_FACT).map(([k, v]) => sql`WHEN ${k} THEN ${v}`),
    sql` `,
  )} END`;
  const step = sql`NULLIF(e.step, 0)`;
  // A sequence send names its step (1 = the first email). Instantly's `/emails`
  // poll reports some without one: those are placed by ORDER on the thread.
  const position = sql`CASE
      WHEN e.step >= 1 THEN CASE WHEN e.step = 1 THEN 'first' ELSE 'followup' END
      WHEN EXISTS (
        SELECT 1 FROM instantly_events p
        WHERE p.campaign_id = e.campaign_id AND p.lead_email = e.lead_email
          AND p.event_type = 'email_sent' AND p.inferred = false
          AND p.timestamp < e.timestamp AND p.id <> e.id
      ) THEN 'followup'
      ELSE 'first' END`;
  const payload = sql`CASE e.event_type
      WHEN 'email_sent' THEN jsonb_build_object(
        'step', ${step}, 'position', ${position},
        'positionBasis', CASE WHEN e.step >= 1 THEN 'step' ELSE 'order' END,
        'accountEmail', e.account_email)
      WHEN 'email_link_clicked' THEN jsonb_build_object('step', ${step}, 'url', h.payload->>'url')
      WHEN 'lead_unsubscribed' THEN jsonb_build_object(
        'step', ${step}, 'via', CASE WHEN e.source = 'manual' THEN 'recorded' ELSE 'link' END)
      ELSE jsonb_build_object('step', ${step}) END`;
  return sql`
    INSERT INTO outreach_facts
      (subject_key, type, occurred_at, lead_email, org_id, campaign_id, instantly_campaign_id,
       brand_ids, transport, payload)
    SELECT 'ievt:' || e.id,
           ${factType},
           e.timestamp AT TIME ZONE 'UTC',
           COALESCE(c.lead_email, e.lead_email),
           c.org_id,
           c.campaign_id,
           e.campaign_id,
           c.brand_ids,
           COALESCE(c.send_transport, CASE WHEN e.source = 'self_send' THEN 'smtp' ELSE 'instantly' END),
           ${payload}
    FROM instantly_events e
    LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
    -- Our own tracker records the clicked URL; Instantly's click webhook carries none.
    LEFT JOIN tracking_hits_raw h
      ON e.event_type = 'email_link_clicked' AND e.source = 'self_send' AND h.id = e.source_row_id
    WHERE e.inferred = false
      AND e.event_type IN (${inList(Object.keys(EVENT_TYPE_TO_FACT))})
      AND e.campaign_id IS NOT NULL
      AND e.lead_email IS NOT NULL
      AND e.campaign_id NOT LIKE 'reserving:%'
      -- A withdrawn recorded opt-out that was never emitted stays out.
      AND e.withdrawn_at IS NULL
      -- An unstepped poll copy of a stepped webhook send is the same email.
      AND NOT (
        e.event_type = 'email_sent' AND COALESCE(e.step, 0) = 0
        AND EXISTS (
          SELECT 1 FROM instantly_events s
          WHERE s.campaign_id = e.campaign_id AND s.lead_email = e.lead_email
            AND s.event_type = 'email_sent' AND s.inferred = false AND s.step >= 1
            AND abs(extract(epoch FROM s.timestamp - e.timestamp)) < ${UNSTEPPED_DUPLICATE_SECONDS}
        )
      )
      ${window}
    ORDER BY e.timestamp, e.id
    ON CONFLICT (subject_key)
      WHERE type IN ('email_sent', 'email_opened', 'link_clicked', 'email_bounced', 'unsubscribed')
      DO NOTHING
  `;
}

/**
 * Withdraw the event facts whose source went away: the event was deleted (a
 * click re-classified as a link scanner, a bounce retracted as a delayed DSN)
 * or, for a recorded opt-out, withdrawn by staff. Once per fact.
 */
export function eventWithdrawalsInsertSql(): SQL {
  return sql`
    INSERT INTO outreach_facts
      (subject_key, type, supersedes_seq, occurred_at, lead_email, org_id, campaign_id,
       instantly_campaign_id, brand_ids, transport, payload)
    SELECT f.subject_key, 'withdrawn', f.seq, now(), f.lead_email, f.org_id, f.campaign_id,
           f.instantly_campaign_id, f.brand_ids, f.transport,
           jsonb_build_object(
             'withdrawnType', f.type,
             'reason', CASE WHEN e.id IS NULL THEN 'source_removed' ELSE 'statement_withdrawn' END)
    FROM outreach_facts f
    LEFT JOIN instantly_events e ON e.id = substr(f.subject_key, 6)
    WHERE f.type IN (${inList(WITHDRAWABLE_EVENT_FACTS)})
      AND (e.id IS NULL OR e.withdrawn_at IS NOT NULL)
      AND NOT EXISTS (SELECT 1 FROM outreach_facts w WHERE w.supersedes_seq = f.seq)
    ORDER BY f.seq
  `;
}

// ─── Reply-sent facts ────────────────────────────────────────────────────────

export const REPLY_SENT_VIA = ["replies_route", "staff_client", "provider_unibox"] as const;
export type ReplySentVia = (typeof REPLY_SENT_VIA)[number];

/**
 * Emit every email we sent into a prospect's thread outside the sequence, not
 * emitted yet, oldest first. Three stores hold one, each read whole through a
 * small index except the IMAP mirror (3 GB), read by `polled_at` over
 * `sinceDays` (null = the whole history):
 *  - `smtp_dispatch_raw` step 0 `sent`: every `POST /orgs/replies` answer on
 *    either transport, `sentBy` = `human` | `automation`;
 *  - `imap_messages_raw` `staff_reply`: one of our people answered from their
 *    own client and CC'd the mailbox;
 *  - `instantly_emails_raw` `ue_type` 3: typed into Instantly's Unibox. One our
 *    route sent is the dispatch row's (its `instantlyEmailId`), never twice.
 * Idempotent (unique subject).
 */
export function replySentFactsInsertSql(sinceDays: number | null): SQL {
  const imapWindow =
    sinceDays === null ? sql`` : sql`AND m.polled_at > now() - make_interval(days => ${sinceDays})`;
  return sql`
    INSERT INTO outreach_facts
      (subject_key, type, occurred_at, lead_email, org_id, campaign_id, instantly_campaign_id,
       brand_ids, transport, payload)
    SELECT x.subject_key, 'reply_sent', x.occurred_at, COALESCE(c.lead_email, x.lead_email), c.org_id,
           c.campaign_id, x.instantly_campaign_id, c.brand_ids,
           COALESCE(x.transport, c.send_transport, 'instantly'),
           jsonb_build_object('via', x.via, 'sentBy', x.sent_by, 'accountEmail', x.account_email,
                              'subject', x.subject)
    FROM (
      SELECT 'rsent:' || d.id AS subject_key, d.dispatched_at AT TIME ZONE 'UTC' AS occurred_at,
             d.lead_email, d.instantly_campaign_id,
             CASE WHEN d.payload->>'transport' = 'instantly' THEN 'instantly' ELSE 'smtp' END AS transport,
             'replies_route' AS via, d.payload->>'sentBy' AS sent_by, d.account_email,
             d.payload->>'subject' AS subject
      FROM smtp_dispatch_raw d
      WHERE d.step = 0 AND d.outcome = 'sent'

      UNION ALL

      SELECT 'rsent:imap:' || m.id, COALESCE(m.received_at, m.polled_at) AT TIME ZONE 'UTC',
             NULL, m.instantly_campaign_id, NULL,
             'staff_client', 'human', COALESCE(m.from_address, m.account_email), m.subject
      FROM imap_messages_raw m
      WHERE m.kind = 'staff_reply' AND m.instantly_campaign_id IS NOT NULL
        ${imapWindow}

      UNION ALL

      SELECT 'rsent:iem:' || r.instantly_email_id, (r.payload->>'timestamp_email')::timestamptz,
             r.payload->>'lead', r.instantly_campaign_id, 'instantly',
             'provider_unibox', 'human', r.payload->>'eaccount', r.payload->>'subject'
      FROM instantly_emails_raw r
      WHERE r.payload->>'ue_type' = '3'
        AND r.instantly_campaign_id IS NOT NULL
        AND r.payload->>'timestamp_email' IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM smtp_dispatch_raw d
          WHERE d.instantly_campaign_id = r.instantly_campaign_id AND d.step = 0
            AND d.payload->>'instantlyEmailId' = r.instantly_email_id
        )
    ) x
    LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = x.instantly_campaign_id
    WHERE x.instantly_campaign_id NOT LIKE 'reserving:%'
      AND COALESCE(c.lead_email, x.lead_email) IS NOT NULL
    ORDER BY x.occurred_at, x.subject_key
    ON CONFLICT (subject_key) WHERE type = 'reply_sent' DO NOTHING
  `;
}

// ─── Reply facts ─────────────────────────────────────────────────────────────

/**
 * What a reply fact says that, when it changes, is worth a correction. Volatile
 * bookkeeping (verdict count, decision time, confidence of the same answer) is
 * left out so a re-poll that restates the same verdict emits nothing.
 */
export function replyContentHash(view: ReplyVerdictView): string {
  const v = view.verdict;
  const stable = {
    leadEmail: view.leadEmail.toLowerCase(),
    campaignId: view.campaignId,
    brandIds: [...view.brandIds].sort(),
    transport: view.transport,
    fromEmail: view.fromEmail,
    subject: view.subject,
    receivedAt: view.receivedAt,
    verdict: v ? { kind: v.kind, classification: v.classification, producerType: v.producerType } : null,
    proposalType: view.judgments.proposalType?.value ?? null,
    question: view.judgments.question?.value ?? null,
    escalation: view.escalation,
  };
  return createHash("sha256").update(JSON.stringify(stable)).digest("hex").slice(0, 32);
}

/**
 * A reply is emitted the tick it lands, verdict or not: a consumer holding a
 * copy of the thread must learn it moved within minutes. Its verdict and
 * judgments arrive as a correction (a new fact superseding it).
 */
interface ReplyCandidate {
  view: ReplyVerdictView;
  orgId: string | null;
  hash: string;
}

async function loadReplyCandidates(): Promise<ReplyCandidate[]> {
  const rows = rowsOf(
    await db.execute(sql`
      SELECT r.*, ${escalationColumns()}
      FROM replies r
      ${escalationJoin()}
      ORDER BY r.received_at, r.id
    `),
  );
  const judgments = await readReplyJudgments(rows.map((r) => String(r.id)));
  return rows.map((row) => {
    const view = toReplyVerdictView(row, judgments.get(String(row.id)) ?? {});
    return { view, orgId: (row.org_id as string | null) ?? null, hash: replyContentHash(view) };
  });
}

interface LatestReplyFact {
  seq: number;
  type: string;
  hash: string | null;
  row: Record<string, unknown>;
}

async function latestReplyFacts(tx: Executor): Promise<Map<string, LatestReplyFact>> {
  const rows = rowsOf(
    await tx.execute(sql`
      SELECT DISTINCT ON (subject_key) *
      FROM outreach_facts
      WHERE subject_key LIKE 'reply:%'
      ORDER BY subject_key, seq DESC
    `),
  );
  const out = new Map<string, LatestReplyFact>();
  for (const row of rows) {
    out.set(String(row.subject_key), {
      seq: Number(row.seq),
      type: String(row.type),
      hash: (row.content_hash as string | null) ?? null,
      row,
    });
  }
  return out;
}

export interface ReplyFactPlan {
  emit: { candidate: ReplyCandidate; supersedesSeq: number | null }[];
  withdraw: LatestReplyFact[];
}

/** Pure: which reply facts to append, given what silver says and what was emitted. */
export function planReplyFacts(
  candidates: ReplyCandidate[],
  latest: Map<string, LatestReplyFact>,
): ReplyFactPlan {
  const plan: ReplyFactPlan = { emit: [], withdraw: [] };
  const present = new Set<string>();
  for (const candidate of candidates) {
    const key = `reply:${candidate.view.replyId}`;
    present.add(key);
    const prior = latest.get(key);
    if (prior && prior.type === "reply" && prior.hash === candidate.hash) continue;
    plan.emit.push({ candidate, supersedesSeq: prior && prior.type === "reply" ? prior.seq : null });
  }
  for (const [key, prior] of latest) {
    if (!present.has(key) && prior.type === "reply") plan.withdraw.push(prior);
  }
  return plan;
}

/** True once any reply fact was emitted (the first pass judges everything first). */
export async function feedHasReplyFacts(): Promise<boolean> {
  const row = rowsOf(
    await db.execute(sql`SELECT EXISTS (SELECT 1 FROM outreach_facts WHERE subject_key LIKE 'reply:%') AS any`),
  )[0];
  return row?.any === true;
}

// ─── One pass ────────────────────────────────────────────────────────────────

export interface OutreachFactsSyncSummary {
  eventFacts: number;
  withdrawnEvents: number;
  replyFacts: number;
  replyCorrections: number;
  withdrawnReplies: number;
  repliesSent: number;
}

/**
 * One emission pass. `sinceDays` bounds the event scan (null = everything); when
 * no event fact exists yet the pass is a full backfill whatever it was asked.
 */
export async function syncOutreachFacts(
  opts: { sinceDays?: number | null } = {},
): Promise<OutreachFactsSyncSummary> {
  // Read outside the lock: the reply side is small and a projection already.
  const candidates = await loadReplyCandidates();

  return db.transaction(async (tx) => {
    await tx.execute(sql`SELECT pg_advisory_xact_lock(${FEED_LOCK_KEY})`);

    const any = rowsOf(
      await tx.execute(sql`SELECT EXISTS (SELECT 1 FROM outreach_facts WHERE subject_key LIKE 'ievt:%') AS any`),
    )[0];
    const sinceDays = any?.any === true ? (opts.sinceDays ?? null) : null;

    const events = await tx.execute(eventFactsInsertSql(sinceDays));
    const withdrawnEvents = await tx.execute(eventWithdrawalsInsertSql());
    const repliesSent = await tx.execute(replySentFactsInsertSql(sinceDays));

    const plan = planReplyFacts(candidates, await latestReplyFacts(tx));
    let replyFacts = 0;
    let replyCorrections = 0;
    for (const { candidate, supersedesSeq } of plan.emit) {
      const v = candidate.view;
      await tx.execute(sql`
        INSERT INTO outreach_facts
          (subject_key, type, supersedes_seq, content_hash, occurred_at, lead_email, org_id,
           campaign_id, instantly_campaign_id, brand_ids, transport, payload)
        VALUES (${`reply:${v.replyId}`}, 'reply', ${supersedesSeq}, ${candidate.hash}, ${v.receivedAt},
                ${v.leadEmail}, ${candidate.orgId}, ${v.campaignId}, ${v.instantlyCampaignId},
                ${sql.param(v.brandIds)}::text[], ${v.transport}, ${JSON.stringify(v)}::jsonb)
      `);
      if (supersedesSeq === null) replyFacts += 1;
      else replyCorrections += 1;
    }
    for (const prior of plan.withdraw) {
      await tx.execute(sql`
        INSERT INTO outreach_facts
          (subject_key, type, supersedes_seq, occurred_at, lead_email, org_id, campaign_id,
           instantly_campaign_id, brand_ids, transport, payload)
        SELECT subject_key, 'withdrawn', seq, now(), lead_email, org_id, campaign_id,
               instantly_campaign_id, brand_ids, transport,
               jsonb_build_object('withdrawnType', 'reply', 'reason', 'source_removed')
        FROM outreach_facts WHERE seq = ${prior.seq}
      `);
    }

    return {
      eventFacts: (events as { rowCount?: number }).rowCount ?? 0,
      withdrawnEvents: (withdrawnEvents as { rowCount?: number }).rowCount ?? 0,
      replyFacts,
      replyCorrections,
      withdrawnReplies: plan.withdraw.length,
      repliesSent: (repliesSent as { rowCount?: number }).rowCount ?? 0,
    };
  });
}

// ─── The read ────────────────────────────────────────────────────────────────

export interface OutreachFact {
  seq: string;
  type: OutreachFactType;
  subjectKey: string;
  supersedesSeq: string | null;
  occurredAt: string;
  recordedAt: string;
  leadEmail: string;
  orgId: string | null;
  campaignId: string | null;
  instantlyCampaignId: string;
  brandIds: string[];
  transport: string | null;
  send: { step: number | null; position: "first" | "followup"; positionBasis: "step" | "order"; accountEmail: string | null } | null;
  open: { step: number | null } | null;
  click: { step: number | null; url: string | null } | null;
  bounce: { step: number | null } | null;
  unsubscribe: { step: number | null; via: "link" | "recorded" } | null;
  reply: ReplyVerdictView | null;
  replySent: { via: ReplySentVia; sentBy: "human" | "automation" | null; accountEmail: string | null; subject: string | null } | null;
  withdrawal: { withdrawnSeq: string; withdrawnType: string; reason: "source_removed" | "statement_withdrawn" } | null;
}

export function toOutreachFact(row: Record<string, unknown>): OutreachFact {
  const type = String(row.type) as OutreachFactType;
  const p = (row.payload ?? {}) as Record<string, unknown>;
  const step = p.step == null ? null : Number(p.step);
  return {
    seq: String(row.seq),
    type,
    subjectKey: String(row.subject_key),
    supersedesSeq: row.supersedes_seq == null ? null : String(row.supersedes_seq),
    occurredAt: iso(row.occurred_at),
    recordedAt: iso(row.recorded_at),
    leadEmail: String(row.lead_email),
    orgId: (row.org_id as string | null) ?? null,
    campaignId: (row.campaign_id as string | null) ?? null,
    instantlyCampaignId: String(row.instantly_campaign_id),
    brandIds: Array.isArray(row.brand_ids) ? (row.brand_ids as string[]) : [],
    transport: (row.transport as string | null) ?? null,
    send:
      type === "email_sent"
        ? {
            step,
            position: p.position as "first" | "followup",
            positionBasis: p.positionBasis as "step" | "order",
            accountEmail: (p.accountEmail as string | null) ?? null,
          }
        : null,
    open: type === "email_opened" ? { step } : null,
    click: type === "link_clicked" ? { step, url: (p.url as string | null) ?? null } : null,
    bounce: type === "email_bounced" ? { step } : null,
    unsubscribe: type === "unsubscribed" ? { step, via: p.via as "link" | "recorded" } : null,
    reply: type === "reply" ? (p as unknown as ReplyVerdictView) : null,
    replySent:
      type === "reply_sent"
        ? {
            via: p.via as ReplySentVia,
            sentBy: p.sentBy === "human" || p.sentBy === "automation" ? p.sentBy : null,
            accountEmail: (p.accountEmail as string | null) ?? null,
            subject: (p.subject as string | null) ?? null,
          }
        : null,
    withdrawal:
      type === "withdrawn"
        ? {
            withdrawnSeq: String(row.supersedes_seq),
            withdrawnType: String(p.withdrawnType),
            reason: p.reason as "source_removed" | "statement_withdrawn",
          }
        : null,
  };
}

/** The feed after `since` (exclusive), in feed order. */
export async function readOutreachFacts(args: {
  since: number;
  limit: number;
  orgId?: string;
  brandId?: string;
  email?: string;
}): Promise<{ facts: OutreachFact[]; nextCursor: string; hasMore: boolean }> {
  const rows = rowsOf(
    await db.execute(sql`
      SELECT * FROM outreach_facts f
      WHERE f.seq > ${args.since}
        ${args.orgId ? sql`AND f.org_id = ${args.orgId}` : sql``}
        ${args.brandId ? sql`AND ${args.brandId} = ANY(f.brand_ids)` : sql``}
        ${args.email ? sql`AND lower(f.lead_email) = ${args.email.trim().toLowerCase()}` : sql``}
      ORDER BY f.seq
      LIMIT ${args.limit + 1}
    `),
  );
  const hasMore = rows.length > args.limit;
  const facts = rows.slice(0, args.limit).map(toOutreachFact);
  return { facts, nextCursor: facts.length ? facts[facts.length - 1].seq : String(args.since), hasMore };
}
