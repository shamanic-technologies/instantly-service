/**
 * Every email of every sequence we run for a brand, with what happened to each
 * one — `GET /orgs/email-sequences?brandId=`.
 *
 * A client (brand owner) wants the list of emails we sent on their behalf: per
 * prospect, the first email and each follow-up, and whether it went out (and
 * when) or is still waiting for its slot. We PERFORMED the sends, so the status
 * is ours to state; content-generation-service holds the bodies and a consumer
 * joins the two by `leadId` (owner 2026-10-12, "permet à une brand de récupérer
 * la liste des emails envoyés").
 *
 * One row per SEQUENCE (per-lead campaign row), not per person: a person we
 * wrote to again after the re-contact window has two sequences, each with its
 * own steps.
 *
 * Per step, from our own records only:
 *   - `sent`    a REAL `email_sent` silver event exists for that step
 *               (inferred and withdrawn events excluded); `sentAt` = its instant.
 *   - `queued`  not sent yet and it still will: the step's send hold is
 *               `provisioned` (`sequence_costs` IS the send queue) on a sequence
 *               that is still active. It goes out in the prospect's business
 *               hours.
 *   - `stopped` it will never go out: the hold was cancelled (reply, bounce,
 *               opt-out, booked meeting, stop) or the sequence is no longer
 *               active.
 *
 * Step set = every step any of our three records knows (send holds, self-send
 * step rows, real send events). Historical holds written before migration 0027
 * carry no `instantly_campaign_id`; they match on `(campaign_id, lead_email)`,
 * the same fallback `matchesHoldCampaign` uses.
 *
 * Paged with a keyset on the per-lead campaign id (opaque cursor). Declares no
 * cost and sends nothing.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { isoOrNull, rowsOf } from "./engaged-leads";

export const EMAIL_SEQUENCES_DEFAULT_LIMIT = 500;
export const EMAIL_SEQUENCES_MAX_LIMIT = 2000;

export type SequenceStepStatus = "sent" | "queued" | "stopped";

export interface SequenceStep {
  /** 1-based. Step 1 is the first email, every later step a follow-up. */
  step: number;
  kind: "first_email" | "follow_up";
  status: SequenceStepStatus;
  /** When it went out (ISO 8601 UTC). Null unless `status` is `sent`. */
  sentAt: string | null;
}

export interface EmailSequence {
  leadId: string | null;
  leadEmail: string;
  campaignId: string | null;
  steps: SequenceStep[];
}

export interface EmailSequencesPage {
  sequences: EmailSequence[];
  nextCursor: string | null;
}

export class InvalidSequenceCursorError extends Error {
  constructor(cursor: string) {
    super(`invalid cursor: ${cursor}`);
    this.name = "InvalidSequenceCursorError";
  }
}

export function encodeSequenceCursor(sequenceKey: string): string {
  return Buffer.from(JSON.stringify([sequenceKey]), "utf8").toString("base64url");
}

/** Throws on a cursor this service did not mint — the route answers 400. */
export function decodeSequenceCursor(cursor: string): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    throw new InvalidSequenceCursorError(cursor);
  }
  if (!Array.isArray(parsed) || parsed.length !== 1 || typeof parsed[0] !== "string") {
    throw new InvalidSequenceCursorError(cursor);
  }
  return parsed[0];
}

export interface RawStepFacts {
  step: number;
  sentAt: string | null;
  /** At least one `provisioned` send hold for the step. */
  held: boolean;
}

/**
 * The customer-facing reading of one step. Pure, so the precedence is pinned:
 * a real send wins over everything; an inactive sequence sends nothing more;
 * otherwise a live hold (or no hold record at all on an active sequence) is
 * still queued, and a step whose holds were all cancelled is stopped.
 */
export function classifyStep(
  facts: RawStepFacts,
  sequenceActive: boolean,
): SequenceStep {
  const kind = facts.step <= 1 ? "first_email" : "follow_up";
  const sentAt = isoOrNull(facts.sentAt);
  if (sentAt !== null) return { step: facts.step, kind, status: "sent", sentAt };
  if (!sequenceActive) return { step: facts.step, kind, status: "stopped", sentAt: null };
  return {
    step: facts.step,
    kind,
    status: facts.held ? "queued" : "stopped",
    sentAt: null,
  };
}

interface SequenceRow {
  sequenceKey: string;
  leadId: string | null;
  leadEmail: string | null;
  campaignId: string | null;
  status: string;
  steps: unknown;
}

function toSteps(raw: unknown, sequenceActive: boolean, sequenceKey: string): SequenceStep[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .flatMap((s) => {
      const r = s as { step?: unknown; sentAt?: unknown; held?: unknown; anyHold?: unknown };
      const step = Number(r.step);
      if (!Number.isInteger(step) || step < 1) {
        // One unreadable step never fails the page: that step is left out and
        // reported by row. (The Instantly /emails sends once stored at step 0
        // were re-stepped in silver by migration 0070, so reaching here means
        // a record we cannot place.)
        console.error(
          `[instantly-service] email-sequences: step ${String(r.step)} on sequence ${sequenceKey} cannot be placed; left out`,
        );
        return [];
      }
      const held = r.held === true;
      // A step known only from a self-send step row (no hold, no send) on an
      // active sequence is waiting for its slot: treat it as held.
      const noHoldRecord = r.anyHold !== true;
      return [
        classifyStep(
          { step, sentAt: (r.sentAt as string | null) ?? null, held: held || noHoldRecord },
          sequenceActive,
        ),
      ];
    })
    .sort((a, b) => a.step - b.step);
}

/** Null when the row cannot be placed: it is reported and left out, never fails the page. */
export function toEmailSequence(row: SequenceRow): EmailSequence | null {
  if (!row.leadEmail) {
    console.error(
      `[instantly-service] email-sequences: sequence ${row.sequenceKey} has no single lead email; left out`,
    );
    return null;
  }
  return {
    leadId: row.leadId,
    leadEmail: row.leadEmail,
    campaignId: row.campaignId,
    steps: toSteps(row.steps, row.status === "active", row.sequenceKey),
  };
}

export async function fetchBrandEmailSequences(filters: {
  orgId: string;
  brandId: string;
  limit?: number;
  cursor?: string;
}): Promise<EmailSequencesPage> {
  const limit = filters.limit ?? EMAIL_SEQUENCES_DEFAULT_LIMIT;
  const after = filters.cursor !== undefined ? decodeSequenceCursor(filters.cursor) : null;

  const result = await db.execute(sql`
    WITH page AS (
      -- 23 campaign rows from 2026-02 predate the lead_email column: recover the
      -- address when the sequence holds exactly ONE lead (a legacy multi-lead
      -- campaign stays null and is reported, never attributed to one person).
      SELECT c.instantly_campaign_id, c.lead_id, c.campaign_id, c.status,
             COALESCE(
               c.lead_email,
               (SELECT CASE WHEN COUNT(DISTINCT lower(l.email)) = 1 THEN MIN(l.email) END
                  FROM instantly_leads l
                 WHERE l.instantly_campaign_id = c.instantly_campaign_id),
               (SELECT CASE WHEN COUNT(DISTINCT lower(e.lead_email)) = 1 THEN MIN(e.lead_email) END
                  FROM instantly_events e
                 WHERE e.campaign_id = c.instantly_campaign_id)
             ) AS lead_email
      FROM instantly_campaigns c
      WHERE c.org_id = ${filters.orgId}
        AND ${filters.brandId} = ANY(c.brand_ids)
        AND c.instantly_campaign_id NOT LIKE 'reserving:%'
        ${after !== null ? sql`AND c.instantly_campaign_id > ${after}` : sql``}
      ORDER BY c.instantly_campaign_id
      LIMIT ${limit + 1}
    ),
    holds AS (
      SELECT p.instantly_campaign_id, sc.step, sc.status
      FROM page p
      JOIN sequence_costs sc ON sc.instantly_campaign_id = p.instantly_campaign_id
      UNION ALL
      SELECT p.instantly_campaign_id, sc.step, sc.status
      FROM page p
      JOIN sequence_costs sc
        ON sc.instantly_campaign_id IS NULL
       AND sc.campaign_id = p.campaign_id
       AND sc.lead_email = p.lead_email
    ),
    hold_steps AS (
      SELECT instantly_campaign_id, step,
             BOOL_OR(status = 'provisioned') AS held
      FROM holds
      GROUP BY 1, 2
    ),
    sends AS (
      SELECT e.campaign_id AS instantly_campaign_id, e.step, MIN(e."timestamp") AS sent_at
      FROM instantly_events e
      JOIN page p ON p.instantly_campaign_id = e.campaign_id
      WHERE e.event_type = 'email_sent'
        AND e.inferred = false
        AND e.withdrawn_at IS NULL
        AND e.step IS NOT NULL
      GROUP BY 1, 2
    ),
    known_steps AS (
      SELECT instantly_campaign_id, step FROM hold_steps
      UNION
      SELECT instantly_campaign_id, step FROM sends
      UNION
      SELECT ss.instantly_campaign_id, ss.step
      FROM sequence_steps ss
      JOIN page p ON p.instantly_campaign_id = ss.instantly_campaign_id
    )
    SELECT p.instantly_campaign_id AS "sequenceKey",
           p.lead_id               AS "leadId",
           p.lead_email            AS "leadEmail",
           p.campaign_id           AS "campaignId",
           p.status                AS status,
           COALESCE(
             jsonb_agg(
               jsonb_build_object(
                 'step', k.step,
                 'sentAt', s.sent_at,
                 'held', COALESCE(h.held, false),
                 'anyHold', h.step IS NOT NULL
               ) ORDER BY k.step
             ) FILTER (WHERE k.step IS NOT NULL),
             '[]'::jsonb
           ) AS steps
    FROM page p
    LEFT JOIN known_steps k ON k.instantly_campaign_id = p.instantly_campaign_id
    LEFT JOIN hold_steps h ON h.instantly_campaign_id = k.instantly_campaign_id AND h.step = k.step
    LEFT JOIN sends s ON s.instantly_campaign_id = k.instantly_campaign_id AND s.step = k.step
    GROUP BY p.instantly_campaign_id, p.lead_id, p.lead_email, p.campaign_id, p.status
    ORDER BY p.instantly_campaign_id
  `);

  const rows = rowsOf(result) as unknown as SequenceRow[];
  const hasMore = rows.length > limit;
  const pageRows = hasMore ? rows.slice(0, limit) : rows;
  return {
    sequences: pageRows
      .map(toEmailSequence)
      .filter((s): s is EmailSequence => s !== null),
    nextCursor: hasMore ? encodeSequenceCursor(pageRows[pageRows.length - 1].sequenceKey) : null,
  };
}
