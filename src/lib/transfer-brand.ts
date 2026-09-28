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
 *
 * ⚠️ THE CAMPAIGN SET IS RESOLVED ONCE, INTO A CONSTANT ARRAY, AND NO CHILD
 * STATEMENT JOINS ANYTHING. The first version embedded the set as a subquery in
 * every statement (`instantly_campaign_id IN (SELECT … brand_ids[1] = …)`). The
 * planner cannot estimate that brand expression, so it assumed ONE campaign and
 * picked a nested loop whose inner side was a sequential scan of the child
 * table — once per campaign. On `messages` (no index on the campaign id) that
 * was 21,484 scans of 190k rows: 25+ minutes for a transfer with NOTHING left to
 * move, past the orchestrator's budget on every retry (prod, 2026-09-28).
 * Handed a constant `= ANY($ids::text[])` instead, the planner knows the array's
 * size and Postgres hashes it, so each child table is read ONCE whatever the
 * statistics say (measured on the same data: 0.1-2s per table). A join can
 * only come back by putting a subquery back; the unit test forbids it.
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
 * The brand's campaigns, wherever they sit between the two orgs, as a plain
 * list. A campaign is the brand's only when the brand is its ONLY brand: a
 * multi-brand campaign belongs equally to a brand that is not moving, so it is
 * reported as skipped rather than moved or split (see `skipped` in the result).
 *
 * Read ONCE, after the campaigns move, and handed to every later statement as a
 * single array parameter (see the header for why a subquery is not an option).
 * `sql.param` keeps it ONE parameter: a bare array in a drizzle template expands
 * to one placeholder per element, which stops working past 65,535 campaigns.
 */
async function resolveBrandCampaignIds(input: TransferBrandInput): Promise<string[]> {
  const brands = input.targetBrandId
    ? [input.sourceBrandId, input.targetBrandId]
    : [input.sourceBrandId];
  const result = await db.execute(sql`
    SELECT COALESCE(array_agg(instantly_campaign_id), '{}'::text[]) AS ids
    FROM instantly_campaigns
    WHERE org_id IN (${input.sourceOrgId}, ${input.targetOrgId})
      AND array_length(brand_ids, 1) = 1
      AND brand_ids[1] = ANY(${sql.param(brands)}::text[])
      AND instantly_campaign_id IS NOT NULL
  `);
  const row = result.rows?.[0] as { ids?: unknown } | undefined;
  if (!Array.isArray(row?.ids)) {
    throw new Error(
      `[transfer-brand] resolving the campaigns of brand ${input.sourceBrandId} returned no id list: ${JSON.stringify(row)}`,
    );
  }
  return row.ids as string[];
}

/** One constant `text[]` parameter holding the brand's campaign ids. */
function anyCampaign(column: SQL, campaignIds: string[]): SQL {
  return sql`${column} = ANY(${sql.param(campaignIds)}::text[])`;
}

export async function transferBrand(input: TransferBrandInput): Promise<TransferBrandResult> {
  const { sourceBrandId, sourceOrgId, targetOrgId, targetBrandId } = input;
  const brandId = targetBrandId ?? sourceBrandId;
  const updatedTables: TransferredTable[] = [];
  const startedAt = Date.now();

  // Every statement is logged when it starts and when it finishes, with its
  // duration: the call this replaced ran for 25+ minutes on ONE statement with
  // nothing in the log to say which, so a stall here must name itself.
  async function timed<T>(label: string, run: () => Promise<T>): Promise<T> {
    const t0 = Date.now();
    console.log(`[transfer-brand] brand ${sourceBrandId}: ${label} started`);
    const value = await run();
    const detail = typeof value === "number" ? `${value} row(s)` : Array.isArray(value) ? `${value.length} id(s)` : "done";
    console.log(`[transfer-brand] brand ${sourceBrandId}: ${label} ${detail} in ${Date.now() - t0}ms`);
    return value;
  }

  // 1. The campaigns themselves — the only rows that carry the brand directly.
  const campaigns = await timed("instantly_campaigns", async () =>
    rowCount(
      await db.execute(sql`
        UPDATE instantly_campaigns
        SET org_id = ${targetOrgId},
            brand_ids = ARRAY[${brandId}]::text[],
            updated_at = now()
        WHERE org_id = ${sourceOrgId}
          AND array_length(brand_ids, 1) = 1
          AND brand_ids[1] = ${sourceBrandId}
      `),
    ),
  );
  updatedTables.push({ tableName: "instantly_campaigns", count: campaigns });

  // 2. The brand's campaign set, once. Every statement below reads this list and
  //    nothing else; with no campaign of the brand in either org there is nothing
  //    tied to it here, so each table reports zero without being scanned.
  const campaignIds = await timed("resolve campaign ids", () => resolveBrandCampaignIds(input));
  const hasCampaigns = campaignIds.length > 0;

  // 3. Everything tied to one of those campaigns and still stamped with the source org.
  for (const table of CAMPAIGN_CHILD_TABLES) {
    const count = hasCampaigns
      ? await timed(table, async () =>
          rowCount(
            await db.execute(sql`
              UPDATE ${sql.identifier(table)}
              SET org_id = ${targetOrgId}
              WHERE org_id = ${sourceOrgId}
                AND ${anyCampaign(sql`instantly_campaign_id`, campaignIds)}
            `),
          ),
        )
      : 0;
    updatedTables.push({ tableName: table, count });
  }

  // 4. The gold status row carries the brand too, so it takes both from its campaign.
  //    Runs whether or not step 1 moved anything (the April version ran it only
  //    when step 1 moved something, so a re-run after a partial one never finished
  //    the projection). Both sides of the join are filtered by the constant list,
  //    and both have an index on the join key, so no plan can repeat a scan per
  //    campaign.
  const gold = hasCampaigns
    ? await timed("instantly_lead_status_current", async () =>
        rowCount(
          await db.execute(sql`
            UPDATE instantly_lead_status_current g
            SET org_id = c.org_id,
                brand_ids = c.brand_ids,
                updated_at = now()
            FROM instantly_campaigns c
            WHERE g.instantly_campaign_id = c.instantly_campaign_id
              AND g.org_id = ${sourceOrgId}
              AND c.org_id = ${targetOrgId}
              AND ${anyCampaign(sql`g.instantly_campaign_id`, campaignIds)}
              AND ${anyCampaign(sql`c.instantly_campaign_id`, campaignIds)}
          `),
        ),
      )
    : 0;
  updatedTables.push({ tableName: "instantly_lead_status_current", count: gold });

  // 5. Recorded opt-outs are COPIED, not moved. An opt-out is a consent statement
  //    about a PERSON to an ORG ("stop emailing me"), not a row of the brand:
  //    the source org may still run other brands that must keep honouring it,
  //    while the new org would otherwise be free to email somebody who asked
  //    us to stop. Both orgs refusing is the only safe direction. Only STANDING
  //    statements (no withdrawal) are copied — a withdrawn one refuses nothing.
  //    The copy carries the original's id in its payload, which is what makes a
  //    re-run a no-op.
  const optouts = hasCampaigns
    ? await timed("instantly_lead_optouts_raw (copied)", async () =>
        rowCount(
          await db.execute(sql`
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
                WHERE ${anyCampaign(sql`c.instantly_campaign_id`, campaignIds)}
              )
              AND NOT EXISTS (
                SELECT 1 FROM instantly_lead_optouts_raw t
                WHERE t.org_id = ${targetOrgId}
                  AND t.payload->>'transferredFromOptoutId' = o.id
              )
          `),
        ),
      )
    : 0;
  updatedTables.push({ tableName: "instantly_lead_optouts_raw (copied)", count: optouts });

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
      ` in ${Date.now() - startedAt}ms: ${JSON.stringify(updatedTables)}`,
  );
  return result;
}
