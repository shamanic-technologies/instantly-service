/**
 * Moving a brand, with its whole history, from one org to another.
 *
 * The contract is the fleet's (`POST /internal/transfer-brand`, orchestrated by
 * brand-service): every row this service holds for the brand moves from
 * `sourceOrgId` to `targetOrgId`, and the brand id is rewritten to
 * `targetBrandId` when one is given. HISTORY moves, MONEY does not — this
 * service declares no cost and holds no balance, so nothing here touches one.
 *
 * WHAT "A ROW FOR THIS BRAND" MEANS HERE. Only two tables carry the brand
 * itself (`instantly_campaigns.brand_ids` and its gold projection
 * `instantly_lead_status_current.brand_ids`). Every other org-scoped table is
 * tied to the brand THROUGH a campaign: it carries the per-lead
 * `instantly_campaign_id` and a denormalised `org_id`. So the brand's campaign
 * set is resolved first and every child table follows it.
 *
 * ⚠️ ORDER: campaigns FIRST, children after. Writers that stamp `org_id` on a
 * new row (the webhook, the reconcile poll, the IMAP poller) read it off the
 * campaign row, so once the campaigns have moved any row written mid-transfer
 * already lands under the target. The reverse order would let those land under
 * the source in the gap between the two.
 *
 * ⚠️ THE CAMPAIGN SET SPANS BOTH ORGS. After the first statement the campaigns
 * sit under the target, so a set scoped to the source alone would be empty and
 * the children would never follow — on a re-run after a partial one, or for
 * stragglers written in between. Children are still only rewritten where their
 * own `org_id` is the SOURCE, which is what makes every statement idempotent:
 * a second call matches nothing and reports zero.
 *
 * No wrapping transaction, on purpose: Doc Dinners is ~21k campaigns and ~700k
 * child rows (268k of them in a 2.6 GB config mirror), and one transaction that
 * size would hold its locks across the whole move while the webhook keeps
 * writing. Each statement is atomic on its own, and idempotence is what makes a
 * partial run safe to finish by calling again.
 */
import { sql, type SQL } from "drizzle-orm";
import { db } from "../db";

export interface TransferBrandInput {
  sourceBrandId: string;
  sourceOrgId: string;
  targetOrgId: string;
  targetBrandId?: string;
}

export interface TransferredTable {
  tableName: string;
  count: number;
}

export interface TransferBrandResult {
  updatedTables: TransferredTable[];
  /** Rows of the brand that could NOT move, and why. Present only when non-empty. */
  skipped?: Array<TransferredTable & { reason: string }>;
}

/**
 * Tables tied to the brand through one of its campaigns, and carrying an
 * `org_id` of their own. Order within this list does not matter; all of them
 * run after the campaigns move.
 *
 * Tables that are tied to a campaign but carry NO org column
 * (`instantly_events`, `sequence_costs`, `sequence_steps`, `smtp_dispatch_raw`,
 * `imap_messages_raw`, `tracking_hits_raw`, `reply_classification_shadow`,
 * `instantly_events_retracted`) need nothing: they resolve their org by joining
 * to the campaign, which has already moved.
 */
export const CAMPAIGN_CHILD_TABLES = [
  "instantly_leads",
  "messages",
  "instantly_webhook_payloads_raw",
  "instantly_analytics_raw",
  "instantly_emails_raw",
  "instantly_leads_raw",
  "instantly_campaigns_config_raw",
  "instantly_manual_qualifications_raw",
  "instantly_manual_qualification_withdrawals",
  "scheduled_replies",
] as const;

function rowCount(result: unknown): number {
  return Number((result as { rowCount?: number | null }).rowCount ?? 0);
}

/**
 * The brand's campaigns, wherever they sit between the two orgs. A campaign is
 * the brand's only when the brand is its ONLY brand: a multi-brand campaign
 * belongs equally to a brand that is not moving, so it is reported as skipped
 * rather than moved or split (see `skipped` in the result).
 */
function brandCampaignIds(input: TransferBrandInput): SQL {
  const brands = input.targetBrandId
    ? sql`ARRAY[${input.sourceBrandId}, ${input.targetBrandId}]::text[]`
    : sql`ARRAY[${input.sourceBrandId}]::text[]`;
  return sql`
    SELECT instantly_campaign_id FROM instantly_campaigns
    WHERE org_id IN (${input.sourceOrgId}, ${input.targetOrgId})
      AND array_length(brand_ids, 1) = 1
      AND brand_ids[1] = ANY(${brands})
  `;
}

export async function transferBrand(input: TransferBrandInput): Promise<TransferBrandResult> {
  const { sourceBrandId, sourceOrgId, targetOrgId, targetBrandId } = input;
  const brandId = targetBrandId ?? sourceBrandId;
  const updatedTables: TransferredTable[] = [];

  // 1. The campaigns themselves — the only rows that carry the brand directly.
  const campaigns = await db.execute(sql`
    UPDATE instantly_campaigns
    SET org_id = ${targetOrgId},
        brand_ids = ARRAY[${brandId}]::text[],
        updated_at = now()
    WHERE org_id = ${sourceOrgId}
      AND array_length(brand_ids, 1) = 1
      AND brand_ids[1] = ${sourceBrandId}
  `);
  updatedTables.push({ tableName: "instantly_campaigns", count: rowCount(campaigns) });

  // 2. Everything tied to one of those campaigns and still stamped with the source org.
  for (const table of CAMPAIGN_CHILD_TABLES) {
    const result = await db.execute(sql`
      UPDATE ${sql.identifier(table)}
      SET org_id = ${targetOrgId}
      WHERE org_id = ${sourceOrgId}
        AND instantly_campaign_id IN (${brandCampaignIds(input)})
    `);
    updatedTables.push({ tableName: table, count: rowCount(result) });
  }

  // 3. The gold status row carries the brand too, so it takes both from its campaign.
  //    Unconditional (the April version ran it only when step 1 moved something,
  //    so a re-run after a partial one never finished the projection).
  const gold = await db.execute(sql`
    UPDATE instantly_lead_status_current g
    SET org_id = c.org_id,
        brand_ids = c.brand_ids,
        updated_at = now()
    FROM instantly_campaigns c
    WHERE g.instantly_campaign_id = c.instantly_campaign_id
      AND g.org_id = ${sourceOrgId}
      AND c.org_id = ${targetOrgId}
      AND g.instantly_campaign_id IN (${brandCampaignIds(input)})
  `);
  updatedTables.push({ tableName: "instantly_lead_status_current", count: rowCount(gold) });

  // 4. Recorded opt-outs are COPIED, not moved. An opt-out is a consent statement
  //    about a PERSON to an ORG ("stop emailing me"), not a row of the brand:
  //    the source org may still run other brands that must keep honouring it,
  //    while the new org would otherwise be free to email somebody who asked
  //    us to stop. Both orgs refusing is the only safe direction. Only STANDING
  //    statements (no withdrawal) are copied — a withdrawn one refuses nothing.
  //    The copy carries the original's id in its payload, which is what makes a
  //    re-run a no-op.
  const optouts = await db.execute(sql`
    INSERT INTO instantly_lead_optouts_raw
      (id, org_id, lead_email, channel, stated_by, notes, payload, stated_at)
    SELECT gen_random_uuid()::text, ${targetOrgId}, o.lead_email, o.channel, o.stated_by, o.notes,
           o.payload || jsonb_build_object(
             'transferredFromOptoutId', o.id,
             'transferredFromOrgId', o.org_id
           ),
           o.stated_at
    FROM instantly_lead_optouts_raw o
    WHERE o.org_id = ${sourceOrgId}
      AND NOT EXISTS (SELECT 1 FROM instantly_lead_optout_withdrawals w WHERE w.optout_id = o.id)
      AND lower(o.lead_email) IN (
        SELECT lower(c.lead_email) FROM instantly_campaigns c
        WHERE c.instantly_campaign_id IN (${brandCampaignIds(input)})
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_lead_optouts_raw t
        WHERE t.org_id = ${targetOrgId}
          AND t.payload->>'transferredFromOptoutId' = o.id
      )
  `);
  updatedTables.push({ tableName: "instantly_lead_optouts_raw (copied)", count: rowCount(optouts) });

  const result: TransferBrandResult = { updatedTables };

  // A campaign stating this brand AND another cannot move without taking the
  // other brand's history with it. Say so instead of silently leaving it behind.
  const multi = await db.execute(sql`
    SELECT count(*)::int AS n FROM instantly_campaigns
    WHERE org_id = ${sourceOrgId}
      AND array_length(brand_ids, 1) > 1
      AND ${sourceBrandId} = ANY(brand_ids)
  `);
  const multiCount = Number((multi.rows?.[0] as { n?: number } | undefined)?.n ?? 0);
  if (multiCount > 0) {
    result.skipped = [
      {
        tableName: "instantly_campaigns",
        count: multiCount,
        reason: "multi-brand campaign: it also belongs to a brand that is not being transferred",
      },
    ];
    console.warn(
      `[transfer-brand] ${multiCount} multi-brand campaign(s) of brand ${sourceBrandId} left under org ${sourceOrgId}`,
    );
  }

  console.log(
    `[transfer-brand] brand ${sourceBrandId} ${sourceOrgId} -> ${targetOrgId}` +
      (targetBrandId ? ` (as ${targetBrandId})` : "") +
      `: ${JSON.stringify(updatedTables)}`,
  );
  return result;
}
