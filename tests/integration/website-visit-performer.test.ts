/**
 * DB-backed proof of OUTCOME-PERFORMER-1106: migration 0106 applies; a website visit declared before its performer was
 * stated is read by the completion pass, planned under its script, marked once runs-service proves it stored it, and
 * never read again. Skipped when no database is configured (CI runs without one):
 * `INSTANTLY_SERVICE_DATABASE_URL=postgres://kevinlourd@localhost:5432/<throwaway> npx vitest run <file>`.
 */
import path from "node:path";
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

const { completeWebsiteVisitPerformers } = await import("../../src/lib/website-visit-outcomes");

const ORG = crypto.randomUUID();
const LEAD = crypto.randomUUID();
const RUN = crypto.randomUUID();
const IC = `perf-${crypto.randomUUID()}`;
const ROW = crypto.randomUUID();
const SLUG = "sales-cold-email-outreach-rampart";
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);

describe.skipIf(SKIP)("website visit performer completion (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    await q(
      `INSERT INTO instantly_campaigns (id, instantly_campaign_id, name, brand_ids, org_id, lead_email, lead_id, workflow_slug,
         website_visit_run_id, website_visit_declared_at, website_visit_declaration)
       VALUES ($1, $2, $2, ARRAY['b-1'], $3, 'p@x.test', $4, $5, $6, now(), 'declared')`,
      [ROW, IC, ORG, LEAD, SLUG, RUN],
    );
    await q(
      `INSERT INTO instantly_events (id, event_type, campaign_id, lead_email, "timestamp", step, source, inferred)
       VALUES (gen_random_uuid()::text, 'email_link_clicked', $1, 'P@x.test', '2026-06-12 09:14:03', 1, 'webhook', false)`,
      [IC],
    );
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM instantly_events WHERE campaign_id = $1`, [IC]);
    await q(`DELETE FROM instantly_campaigns WHERE id = $1`, [ROW]);
    await pool.end();
  });

  it("dry run plans the past visit under its script, writes nothing", async () => {
    const s = await completeWebsiteVisitPerformers({ dryRun: true, limit: 10_000 });
    expect(s.byPerformer[`script:${SLUG}`]).toBeGreaterThanOrEqual(1);
    const r = await q(`SELECT website_visit_performer FROM instantly_campaigns WHERE id = $1`, [ROW]);
    expect(r.rows[0].website_visit_performer).toBeNull();
  });

  it("states it once on runs-service's proof, dated at the click, then never reads it again", async () => {
    const fetchMock = vi.fn(async () => new Response('{"recordedCount":0,"alreadyRecordedCount":0,"performerStatedCount":1}', { status: 201 }));
    vi.stubGlobal("fetch", fetchMock);
    await completeWebsiteVisitPerformers({ dryRun: false, limit: 10_000 });
    const mine = fetchMock.mock.calls.find((c) => String(c[0]).includes(RUN));
    expect(JSON.parse((mine![1] as RequestInit).body as string)).toEqual({
      produced: [{ outcomeKey: "website_visit", subjectService: "lead-service", subjectId: LEAD, performer: { kind: "script", workflowSlug: SLUG } }],
      consumed: [],
      occurredAt: "2026-06-12T09:14:03.000Z",
    });
    const r = await q(`SELECT website_visit_performer, website_visit_performer_at FROM instantly_campaigns WHERE id = $1`, [ROW]);
    expect(r.rows[0].website_visit_performer).toBe("stated");
    expect(r.rows[0].website_visit_performer_at).not.toBeNull();
    fetchMock.mockClear();
    await completeWebsiteVisitPerformers({ dryRun: false, limit: 10_000 });
    expect(fetchMock.mock.calls.some((c) => String(c[0]).includes(RUN))).toBe(false);
    vi.unstubAllGlobals();
  });
});
