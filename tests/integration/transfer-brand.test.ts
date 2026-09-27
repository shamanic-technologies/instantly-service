/**
 * DB-backed proof of the brand transfer, one assertion per table: after a
 * transfer nothing of the brand remains under the source org, the other brand
 * of the same org is untouched, and a second call is a no-op.
 *
 * Builds its own pool rather than importing `src/db`, which forces
 * `sslmode=verify-full` and therefore cannot reach a throwaway local Postgres.
 * Skipped when no database is configured (CI runs without one).
 */
import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

const { pool } = vi.hoisted(() => {
  const url = process.env.INSTANTLY_SERVICE_DATABASE_URL;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { Pool: P } = require("pg");
  return { pool: url ? new P({ connectionString: url }) : null };
});
vi.mock("../../src/db", async () => {
  const { drizzle: d } = await import("drizzle-orm/node-postgres");
  return { db: pool ? d(pool) : null };
});

const { transferBrand, CAMPAIGN_CHILD_TABLES } = await import("../../src/lib/transfer-brand");

const SRC = crypto.randomUUID();
const DST = crypto.randomUUID();
const BRAND = crypto.randomUUID();
const OTHER = crypto.randomUUID();
const NEW_BRAND = crypto.randomUUID();
const tag = crypto.randomUUID().slice(0, 8);
const MOVING = [`t-${tag}-a`, `t-${tag}-b`];
const STAYING = `t-${tag}-other`;
const MULTI = `t-${tag}-multi`;
const ALL = [...MOVING, STAYING, MULTI];

const q = (text: string, params: unknown[] = []) => pool!.query(text, params);
const emailOf = (ic: string) => `${ic}@lead.test`;

async function seedCampaign(ic: string, brands: string[]) {
  const e = emailOf(ic);
  await q(
    `INSERT INTO instantly_campaigns (id, instantly_campaign_id, name, brand_ids, org_id, lead_email, campaign_id)
     VALUES (gen_random_uuid()::text, $1, $1, $2, $3, $4, $5)`,
    [ic, brands, SRC, e, crypto.randomUUID()],
  );
  await q(`INSERT INTO instantly_leads (id, instantly_campaign_id, email, org_id) VALUES (gen_random_uuid()::text, $1, $2, $3)`, [ic, e, SRC]);
  await q(
    `INSERT INTO messages (id, source_table, source_row_id, direction, kind, transport, account_email, thread_id, outcome, occurred_at, instantly_campaign_id, org_id)
     VALUES (gen_random_uuid()::text, 'smtp_dispatch_raw', $1, 'out', 'outreach', 'smtp', 'm@x.test', $2, 'sent', now(), $2, $3)`,
    [crypto.randomUUID(), ic, SRC],
  );
  await q(
    `INSERT INTO instantly_lead_status_current (org_id, instantly_campaign_id, lead_email, brand_ids) VALUES ($1, $2, $3, $4)`,
    [SRC, ic, e, brands],
  );
  for (const t of ["instantly_webhook_payloads_raw", "instantly_analytics_raw", "instantly_campaigns_config_raw"]) {
    await q(`INSERT INTO ${t} (id, instantly_campaign_id, payload, org_id) VALUES (gen_random_uuid()::text, $1, '{}', $2)`, [ic, SRC]);
  }
  await q(
    `INSERT INTO instantly_emails_raw (id, instantly_email_id, instantly_campaign_id, payload, org_id) VALUES (gen_random_uuid()::text, $1, $2, '{}', $3)`,
    [crypto.randomUUID(), ic, SRC],
  );
  await q(
    `INSERT INTO instantly_leads_raw (id, instantly_campaign_id, lead_email, payload, org_id) VALUES (gen_random_uuid()::text, $1, $2, '{}', $3)`,
    [ic, e, SRC],
  );
  const mq = await q(
    `INSERT INTO instantly_manual_qualifications_raw (id, org_id, campaign_id, instantly_campaign_id, lead_email, status, reply_kind, qualified_by, payload)
     VALUES (gen_random_uuid()::text, $1, 'c', $2, $3, 'lead_interested', 'lead_interested', 'u', '{}') RETURNING id`,
    [SRC, ic, e],
  );
  await q(
    `INSERT INTO instantly_manual_qualification_withdrawals (id, qualification_id, org_id, campaign_id, instantly_campaign_id, lead_email, withdrawn_by)
     VALUES (gen_random_uuid()::text, $1, $2, 'c', $3, $4, 'u')`,
    [mq.rows[0].id, SRC, ic, e],
  );
  await q(
    `INSERT INTO scheduled_replies (id, org_id, user_id, campaign_id, instantly_campaign_id, lead_email, body_html, scheduled_for)
     VALUES (gen_random_uuid()::text, $1, 'u', 'c', $2, $3, '<p>x</p>', now())`,
    [SRC, ic, e],
  );
}

async function seedOptout(email: string, withdrawn: boolean) {
  const o = await q(
    `INSERT INTO instantly_lead_optouts_raw (id, org_id, lead_email, channel, stated_by, payload) VALUES (gen_random_uuid()::text, $1, $2, 'sms', 'u', '{}') RETURNING id`,
    [SRC, email],
  );
  if (withdrawn) {
    await q(
      `INSERT INTO instantly_lead_optout_withdrawals (id, optout_id, org_id, lead_email, withdrawn_by) VALUES (gen_random_uuid()::text, $1, $2, $3, 'u')`,
      [o.rows[0].id, SRC, email],
    );
  }
}

async function countUnder(table: string, org: string, ids: string[]) {
  const col = "instantly_campaign_id";
  const r = await q(`SELECT count(*)::int AS n FROM ${table} WHERE org_id = $1 AND ${col} = ANY($2)`, [org, ids]);
  return r.rows[0].n as number;
}

const TABLES = ["instantly_campaigns", ...CAMPAIGN_CHILD_TABLES, "instantly_lead_status_current"];

describe.skipIf(SKIP)("transfer-brand (DB-backed)", () => {
  beforeAll(async () => {
    await seedCampaign(MOVING[0], [BRAND]);
    await seedCampaign(MOVING[1], [BRAND]);
    await seedCampaign(STAYING, [OTHER]);
    await seedCampaign(MULTI, [BRAND, OTHER]);
    await seedOptout(emailOf(MOVING[0]).toUpperCase(), false); // standing, casing differs
    await seedOptout(emailOf(MOVING[1]), true); // withdrawn — refuses nothing, not copied
    await seedOptout(emailOf(STAYING), false); // another brand's lead — not copied
  });

  afterAll(async () => {
    if (!pool) return;
    for (const t of TABLES) await q(`DELETE FROM ${t} WHERE instantly_campaign_id = ANY($1)`, [ALL]);
    await q(`DELETE FROM instantly_lead_optout_withdrawals WHERE org_id IN ($1, $2)`, [SRC, DST]);
    await q(`DELETE FROM instantly_lead_optouts_raw WHERE org_id IN ($1, $2)`, [SRC, DST]);
    await pool.end();
  });

  it("moves every table's rows of the brand, leaves the other brand, then is a no-op", async () => {
    const first = await transferBrand({ sourceBrandId: BRAND, sourceOrgId: SRC, targetOrgId: DST, targetBrandId: NEW_BRAND });

    for (const table of TABLES) {
      expect(await countUnder(table, SRC, MOVING), `${table} still under source`).toBe(0);
      expect(await countUnder(table, DST, MOVING), `${table} not under target`).toBeGreaterThan(0);
      expect(await countUnder(table, SRC, [STAYING]), `${table} moved the other brand`).toBeGreaterThan(0);
      const reported = first.updatedTables.find((t) => t.tableName === table)!.count;
      expect(reported, `${table} reported count`).toBe(await countUnder(table, DST, MOVING));
    }

    // Brand id rewritten on both tables that carry it.
    const brands = await q(
      `SELECT brand_ids FROM instantly_campaigns WHERE instantly_campaign_id = ANY($1)
       UNION ALL SELECT brand_ids FROM instantly_lead_status_current WHERE instantly_campaign_id = ANY($1)`,
      [MOVING],
    );
    for (const r of brands.rows) expect(r.brand_ids).toEqual([NEW_BRAND]);

    // Multi-brand campaign stays, and is reported.
    expect(await countUnder("instantly_campaigns", SRC, [MULTI])).toBe(1);
    expect(first.skipped?.[0]).toMatchObject({ tableName: "instantly_campaigns", count: 1 });

    // Opt-outs: the standing one of a moved lead is copied, the source keeps it.
    const copied = await q(`SELECT lead_email, payload FROM instantly_lead_optouts_raw WHERE org_id = $1`, [DST]);
    expect(copied.rows.map((r) => r.lead_email.toLowerCase())).toEqual([emailOf(MOVING[0])]);
    expect(copied.rows[0].payload.transferredFromOrgId).toBe(SRC);
    const kept = await q(`SELECT count(*)::int AS n FROM instantly_lead_optouts_raw WHERE org_id = $1`, [SRC]);
    expect(kept.rows[0].n).toBe(3);

    // Re-run: nothing left to do, anywhere.
    const second = await transferBrand({ sourceBrandId: BRAND, sourceOrgId: SRC, targetOrgId: DST, targetBrandId: NEW_BRAND });
    for (const t of second.updatedTables) expect(t.count, `${t.tableName} on re-run`).toBe(0);
  });

  it("finishes a partial run: children left behind by an earlier call still follow", async () => {
    // Simulate the April version: campaigns moved, a child row left on the source.
    await q(`UPDATE instantly_emails_raw SET org_id = $1 WHERE instantly_campaign_id = $2`, [SRC, MOVING[0]]);
    const r = await transferBrand({ sourceBrandId: BRAND, sourceOrgId: SRC, targetOrgId: DST, targetBrandId: NEW_BRAND });
    expect(r.updatedTables.find((t) => t.tableName === "instantly_emails_raw")!.count).toBe(1);
    expect(await countUnder("instantly_emails_raw", SRC, MOVING)).toBe(0);
  });
});
