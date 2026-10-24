/**
 * LEDGER-HISTORY-1024: the sequences we sent, or that replied or clicked, while
 * our campaign row held no lead id (sends before lead ids were threaded through,
 * 2026-02..10) stood undeclared in the ledger (`no_lead_id`): never declared
 * under an invented subject. The EMAIL is the identity and lead-service owns the
 * id, so we ask it (`POST /internal/leads/ids/resolve`, by org + brands +
 * address; nothing is minted, several leads on one key = no answer) and record
 * the answer on our row (`lead_id`, only where it was NULL, stamped
 * `metadata.leadIdResolvedAt`). Then every `no_lead_id` mark is lifted so
 * the same declaration paths run again: the send backfill, the positive-reply
 * and website-visit sweeps. A row still unresolved is marked `no_lead_id`
 * again by those paths. Idempotent.
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { resolveLeadIds } from "./lead-client";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const RESOLVE_BATCH = 1000;

export interface LeadIdBackfillSummary {
  /** Campaign rows with no lead id that a `no_lead_id` mark points at. */
  read: number;
  resolved: number;
  unresolved: number;
  /** Marks lifted so the declaration paths run again (holds, conversations, visits). */
  holdMarksLifted: number;
  conversationMarksLifted: number;
  websiteVisitMarksLifted: number;
}

interface Row {
  id: string;
  orgId: string;
  brandIds: string[];
  email: string;
}

/**
 * Rows behind a `no_lead_id` mark: a hold's campaign row (by `instantly_campaign_id`,
 * or for a pre-0027 hold by caller campaign + address), or the row itself for a
 * conversation / website visit.
 */
async function loadRows(): Promise<Row[]> {
  const result = await db.execute(sql`
    SELECT c.id, c.org_id, c.brand_ids, c.lead_email FROM instantly_campaigns c
    WHERE c.lead_id IS NULL AND c.lead_email IS NOT NULL AND (
      c.conversation_declaration = 'no_lead_id' OR c.website_visit_declaration = 'no_lead_id'
      OR EXISTS (SELECT 1 FROM sequence_costs sc WHERE sc.outcomes_declaration = 'no_lead_id' AND sc.instantly_campaign_id = c.instantly_campaign_id)
      OR EXISTS (SELECT 1 FROM sequence_costs sc WHERE sc.outcomes_declaration = 'no_lead_id' AND sc.instantly_campaign_id IS NULL
        AND sc.campaign_id = c.campaign_id AND lower(sc.lead_email) = lower(c.lead_email))
    )
    ORDER BY c.id
  `);
  return (result.rows as Array<Record<string, unknown>>)
    .map((r) => ({
      id: String(r.id),
      orgId: String(r.org_id),
      brandIds: Array.isArray(r.brand_ids) ? (r.brand_ids as string[]) : [],
      email: String(r.lead_email),
    }))
    .filter((r) => UUID_RE.test(r.orgId));
}

export async function backfillMissingLeadIds(opts: { dryRun: boolean }): Promise<LeadIdBackfillSummary> {
  const rows = await loadRows();
  const summary: LeadIdBackfillSummary = {
    read: rows.length, resolved: 0, unresolved: 0, holdMarksLifted: 0, conversationMarksLifted: 0, websiteVisitMarksLifted: 0,
  };
  for (let i = 0; i < rows.length; i += RESOLVE_BATCH) {
    const batch = rows.slice(i, i + RESOLVE_BATCH);
    const answers = await resolveLeadIds(
      batch.map((r) => ({ orgId: r.orgId, email: r.email, ...(r.brandIds.length > 0 ? { brandIds: r.brandIds.slice(0, 50) } : {}) })),
    );
    for (let k = 0; k < batch.length; k++) {
      const leadId = answers[k];
      if (!leadId) {
        summary.unresolved++;
        continue;
      }
      summary.resolved++;
      if (opts.dryRun) continue;
      await db.execute(sql`
        UPDATE instantly_campaigns
        SET lead_id = ${leadId},
          metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('leadIdResolvedAt', now()::text)
        WHERE id = ${batch[k].id} AND lead_id IS NULL
      `);
    }
  }
  if (opts.dryRun) return summary;
  const holds = await db.execute(sql`
    UPDATE sequence_costs SET outcomes_declared_at = NULL, outcomes_declaration = NULL
    WHERE outcomes_declaration = 'no_lead_id'
  `);
  const conversations = await db.execute(sql`
    UPDATE instantly_campaigns SET conversation_declared_at = NULL, conversation_declaration = NULL
    WHERE conversation_declaration = 'no_lead_id' AND lead_id IS NOT NULL
  `);
  const visits = await db.execute(sql`
    UPDATE instantly_campaigns SET website_visit_declared_at = NULL, website_visit_declaration = NULL
    WHERE website_visit_declaration = 'no_lead_id' AND lead_id IS NOT NULL
  `);
  summary.holdMarksLifted = holds.rowCount ?? 0;
  summary.conversationMarksLifted = conversations.rowCount ?? 0;
  summary.websiteVisitMarksLifted = visits.rowCount ?? 0;
  console.log(`[lead-id-backfill] done ${JSON.stringify(summary)}`);
  return summary;
}
