/**
 * DB-backed proof of the one-time follow-up cut (FOLLOWUP-JIT-1107): a lead with
 * steps 2 and 3 queued ends with both cancelled and their held costs cancelled
 * at runs-service, step 1 (sent or not) untouched, one audit row; a re-run
 * changes nothing; the dry run prints per brand leads, steps and dollars; the
 * follow-up door no longer sees a queued sequence once step 1 has left; the
 * restore of cut follow-ups never takes these back.
 *
 * Own pool, applies every migration. Skipped when no database is configured:
 * `INSTANTLY_SERVICE_DATABASE_URL=postgres://postgres@localhost:5432/x npx vitest run <file>`.
 */
import path from "node:path";
import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

const { pool, runs } = vi.hoisted(() => {
  const url = process.env.INSTANTLY_SERVICE_DATABASE_URL;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { Pool: P } = require("pg");
  return {
    pool: url ? new P({ connectionString: url }) : null,
    runs: { getRunWithCosts: vi.fn(), updateCostStatus: vi.fn() },
  };
});
vi.mock("../../src/db", async () => {
  const { drizzle: d } = await import("drizzle-orm/node-postgres");
  return { db: pool ? d(pool) : null };
});
vi.mock("../../src/lib/runs-client", async (orig) => ({
  ...(await orig<typeof import("../../src/lib/runs-client")>()),
  ...runs,
}));
const announce = vi.fn().mockResolvedValue(undefined);
vi.mock("../../src/lib/evidence-changed", () => ({ announceEvidenceChanged: (...a: unknown[]) => announce(...a) }));

const { cutQueuedFollowups } = await import("../../src/lib/followup-jit-cut");
const { findQueuedBrandSequence } = await import("../../src/lib/follow-up-send");
const { loadCutFollowupCandidates } = await import("../../src/lib/restore-cut-followups");

const SUFFIX = crypto.randomUUID().slice(0, 8);
const ORG = crypto.randomUUID();
const USER = crypto.randomUUID();
const BRAND = crypto.randomUUID();
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);

/** Each step's two costs at runs-service: 1.25 + 1.25 cents, provisioned until cancelled. */
const costState = new Map<string, "provisioned" | "cancelled">();

async function sequence(lead: string, steps: Array<{ step: number; status: string }>) {
  const icid = `self:${crypto.randomUUID()}`;
  await q(
    `INSERT INTO instantly_campaigns (id, campaign_id, lead_email, instantly_campaign_id, name, status, org_id, user_id, brand_ids, run_id, created_at, updated_at)
     VALUES ($1, $2, $3, $4, 'n', 'active', $5, $6, $7, $8, now() - interval '1 day', now())`,
    [crypto.randomUUID(), `camp-${SUFFIX}`, lead, icid, ORG, USER, [BRAND], `run-${SUFFIX}`],
  );
  for (const s of steps) {
    await q(
      `INSERT INTO sequence_steps (id, instantly_campaign_id, step, subject, body_html) VALUES ($1, $2, $3, 'hi', '<p>hello</p>')`,
      [crypto.randomUUID(), icid, s.step],
    );
    const runId = `steprun-${crypto.randomUUID()}`;
    const costId = `acct-${runId}`;
    const domainCostId = `dom-${runId}`;
    costState.set(costId, s.status === "provisioned" ? "provisioned" : "cancelled");
    costState.set(domainCostId, s.status === "provisioned" ? "provisioned" : "cancelled");
    await q(
      `INSERT INTO sequence_costs (id, campaign_id, instantly_campaign_id, lead_email, step, run_id, cost_id, domain_cost_id, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [crypto.randomUUID(), `camp-${SUFFIX}`, icid, lead, s.step, runId, costId, domainCostId, s.status],
    );
  }
  return icid;
}
const holds = async (icid: string) =>
  (await q(`SELECT step, status FROM sequence_costs WHERE instantly_campaign_id = $1 ORDER BY step`, [icid])).rows;

describe.skipIf(SKIP)("one-time follow-up cut (DB-backed)", () => {
  let sentFirst: string;
  let queuedFirst: string;

  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });

    const cost = (id: string, runId: string) => ({
      id, runId, costName: id.startsWith("acct") ? "instantly-account-email-sent" : "instantly-domain-email-sent",
      costSource: "platform", quantity: "1", unitCostInUsdCents: "1.25", totalCostInUsdCents: "1.25",
      status: costState.get(id), createdAt: new Date().toISOString(),
    });
    runs.getRunWithCosts.mockImplementation(async (runId: string) => ({
      id: runId, costs: [cost(`acct-${runId}`, runId), cost(`dom-${runId}`, runId)],
    }));
    runs.updateCostStatus.mockImplementation(async (runId: string, costId: string, status: "cancelled") => {
      costState.set(costId, status);
      return cost(costId, runId);
    });

    // Step 1 already sent (actual), steps 2 and 3 queued.
    sentFirst = await sequence(`sent@${SUFFIX}.test`, [
      { step: 1, status: "actual" },
      { step: 2, status: "provisioned" },
      { step: 3, status: "provisioned" },
    ]);
    await q(
      `INSERT INTO instantly_events (id, campaign_id, lead_email, event_type, step, timestamp, inferred, raw_payload)
       VALUES ($1, $2, $3, 'email_sent', 1, now() - interval '1 day', false, '{}'::jsonb)`,
      [crypto.randomUUID(), sentFirst, `sent@${SUFFIX}.test`],
    );
    // First email not sent yet: step 1 stays queued, step 2 is cut.
    queuedFirst = await sequence(`queued@${SUFFIX}.test`, [
      { step: 1, status: "provisioned" },
      { step: 2, status: "provisioned" },
    ]);
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM followup_jit_cuts WHERE org_id = $1`, [ORG]);
    await q(`DELETE FROM instantly_events WHERE lead_email LIKE $1`, [`%@${SUFFIX}.test`]);
    await q(`DELETE FROM instantly_lead_status_current WHERE lead_email LIKE $1`, [`%@${SUFFIX}.test`]).catch(() => {});
    await q(`DELETE FROM sequence_steps WHERE instantly_campaign_id = ANY($1)`, [[sentFirst, queuedFirst]]);
    await q(`DELETE FROM sequence_costs WHERE lead_email LIKE $1`, [`%@${SUFFIX}.test`]);
    await q(`DELETE FROM instantly_campaigns WHERE org_id = $1`, [ORG]);
    await pool.end();
  });

  it("dry run prints per brand: leads, steps, dollars that would go back, and touches nothing", async () => {
    const plan = await cutQueuedFollowups({ dryRun: true, orgId: ORG });
    expect(plan).toMatchObject({ dryRun: true, leads: 2, steps: 3, usdCents: 7.5, cut: 0, failed: 0, unreadHolds: 0 });
    expect(plan.byBrand).toEqual([{ orgId: ORG, brandIds: [BRAND], leads: 2, steps: 3, usdCents: 7.5 }]);
    expect(runs.updateCostStatus).not.toHaveBeenCalled();
    expect(await holds(sentFirst)).toEqual([
      { step: 1, status: "actual" }, { step: 2, status: "provisioned" }, { step: 3, status: "provisioned" },
    ]);
  });

  it("cancels steps 2 and 3 and their held costs, step 1 untouched, one audit row per lead", async () => {
    const done = await cutQueuedFollowups({ dryRun: false, orgId: ORG });
    expect(done).toMatchObject({ dryRun: false, leads: 2, steps: 3, cut: 2, failed: 0, usdCents: 7.5 });

    expect(await holds(sentFirst)).toEqual([
      { step: 1, status: "actual" }, { step: 2, status: "cancelled" }, { step: 3, status: "cancelled" },
    ]);
    expect(await holds(queuedFirst)).toEqual([
      { step: 1, status: "provisioned" }, { step: 2, status: "cancelled" },
    ]);
    // Six costs cancelled at runs-service (two per step), none of a step 1.
    expect(runs.updateCostStatus).toHaveBeenCalledTimes(6);
    for (const [, costId, status] of runs.updateCostStatus.mock.calls) expect(status).toBe("cancelled");
    expect([...costState.entries()].filter(([, s]) => s === "provisioned")).toHaveLength(2); // queuedFirst step 1

    const audit = (await q(
      `SELECT lead_email, steps, holds_cancelled, released_usd_cents::float AS cents FROM followup_jit_cuts WHERE org_id = $1 ORDER BY lead_email`,
      [ORG],
    )).rows;
    expect(audit).toEqual([
      { lead_email: `queued@${SUFFIX}.test`, steps: [2], holds_cancelled: 1, cents: 2.5 },
      { lead_email: `sent@${SUFFIX}.test`, steps: [2, 3], holds_cancelled: 2, cents: 5 },
    ]);
    expect(announce).toHaveBeenCalledWith(ORG, expect.arrayContaining([`sent@${SUFFIX}.test`, `queued@${SUFFIX}.test`]), "followup-jit-cut");
  });

  it("a re-run changes nothing", async () => {
    runs.updateCostStatus.mockClear();
    const again = await cutQueuedFollowups({ dryRun: false, orgId: ORG });
    expect(again).toMatchObject({ leads: 0, steps: 0, cut: 0, failed: 0, usdCents: 0, byBrand: [] });
    expect(runs.updateCostStatus).not.toHaveBeenCalled();
    const audit = (await q(`SELECT holds_cancelled FROM followup_jit_cuts WHERE org_id = $1 ORDER BY lead_email`, [ORG])).rows;
    expect(audit).toEqual([{ holds_cancelled: 1 }, { holds_cancelled: 2 }]);
  });

  it("the lead's next follow-up is no longer refused once its first email has left; an unsent first email still is", async () => {
    expect(await findQueuedBrandSequence(ORG, `sent@${SUFFIX}.test`, [BRAND], null)).toBeNull();
    expect(await findQueuedBrandSequence(ORG, `queued@${SUFFIX}.test`, [BRAND], null)).toMatchObject({ instantlyCampaignId: queuedFirst });
  });

  it("the restore of cut follow-ups never puts these steps back", async () => {
    expect(await loadCutFollowupCandidates({ instantlyCampaignIds: [sentFirst, queuedFirst] })).toEqual([]);
    // Control: without the audit row, the sent lead's cut steps WOULD be a restore candidate.
    await q(`DELETE FROM followup_jit_cuts WHERE instantly_campaign_id = $1`, [sentFirst]);
    const control = await loadCutFollowupCandidates({ instantlyCampaignIds: [sentFirst] });
    expect(control.map((c) => c.steps)).toEqual([[2, 3]]);
  });
});
