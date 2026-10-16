/**
 * DB-backed proof of the interrupted-send sweeps (STALE-RESERVATION-1016): a `reserving:`
 * row past the delay with no trace of a send is released (the lead servable again); a
 * fresh one is untouched; one whose email may have left (stamp, Instantly creation
 * trace) is KEPT, marked once, never re-sent; an orphan step run's provisioned costs
 * are cancelled while a queued hold's are not.
 *
 * Own pool, applies every migration. Skipped when no database is configured:
 * `INSTANTLY_SERVICE_DATABASE_URL=postgres://postgres@localhost:5432/x npx vitest run <file>`.
 */
import path from "node:path";
import { describe, it, expect, beforeAll, afterAll, vi, beforeEach } from "vitest";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

const { pool, runs } = vi.hoisted(() => {
  const url = process.env.INSTANTLY_SERVICE_DATABASE_URL;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { Pool: P } = require("pg");
  return {
    pool: url ? new P({ connectionString: url }) : null,
    runs: {
      listRunEvents: vi.fn(),
      listRuns: vi.fn(),
      getRunWithCosts: vi.fn(),
      updateCostStatus: vi.fn(),
      updateRun: vi.fn(),
    },
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

const { releaseStaleReservations, settleOrphanStepRuns } = await import("../../src/lib/stale-reservations");

const SUFFIX = crypto.randomUUID().slice(0, 8);
const ORG = crypto.randomUUID();
const USER = crypto.randomUUID();
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);

async function reservation(lead: string, opts: { ageMin: number; runId: string; metadata?: object }) {
  const id = crypto.randomUUID();
  await q(
    `INSERT INTO instantly_campaigns (id, campaign_id, lead_email, instantly_campaign_id, name, status, org_id, user_id, brand_ids, run_id, metadata, created_at, updated_at)
     VALUES ($1, $2, $3, $4, 'n', 'active', $5, $6, '{}', $7, $8, now() - make_interval(mins => $9), now() - make_interval(mins => $9))`,
    [id, `camp-${SUFFIX}`, lead, `reserving:${crypto.randomUUID()}`, ORG, USER, opts.runId, opts.metadata ? JSON.stringify(opts.metadata) : null, opts.ageMin],
  );
  return id;
}
const row = async (id: string) => (await q(`SELECT instantly_campaign_id, metadata FROM instantly_campaigns WHERE id = $1`, [id])).rows[0];

describe.skipIf(SKIP)("interrupted-send sweeps (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
  });
  beforeEach(() => {
    for (const fn of Object.values(runs)) fn.mockReset();
    announce.mockClear();
  });
  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM sequence_costs WHERE lead_email LIKE $1`, [`%@${SUFFIX}.test`]);
    await q(`DELETE FROM instantly_campaigns WHERE org_id = $1`, [ORG]);
    await pool.end();
  });

  it("releases a stale no-send reservation, leaves a fresh one, keeps one whose email may have left", async () => {
    const stale = await reservation(`drungar@${SUFFIX}.test`, { ageMin: 60, runId: "run-stale" });
    const fresh = await reservation(`fresh@${SUFFIX}.test`, { ageMin: 1, runId: "run-fresh" });
    const stamped = await reservation(`stamped@${SUFFIX}.test`, {
      ageMin: 60,
      runId: "run-stamped",
      metadata: { instantlyCreateStartedAt: "2026-10-16T14:49:30Z" },
    });
    const traced = await reservation(`traced@${SUFFIX}.test`, { ageMin: 60, runId: "run-traced" });
    const gone = await reservation(`jordan@${SUFFIX}.test`, { ageMin: 60 * 24 * 120, runId: "run-purged" });
    runs.listRunEvents.mockImplementation(async (runId: string) => {
      if (runId === "run-purged") return null;
      if (runId === "run-traced") {
        return [{ event: "send-campaign-created", detail: "instantlyCampaignId=0fd04c25-real, added=1", createdAt: "2026-10-16T14:49:30Z" }];
      }
      return [{ event: "send-start", detail: null, createdAt: "2026-10-16T14:49:25Z" }];
    });

    const summary = await releaseStaleReservations({ limit: 1000 });

    expect(await row(stale)).toBeUndefined();
    expect(await row(gone)).toBeUndefined();
    expect((await row(fresh)).instantly_campaign_id).toMatch(/^reserving:/);
    expect((await row(stamped)).metadata.staleReservationKept.reason).toMatch(/Instantly campaign/);
    expect((await row(traced)).metadata.staleReservationKept.reason).toMatch(/send-campaign-created/);
    expect(runs.listRunEvents).not.toHaveBeenCalledWith("run-fresh", expect.anything());
    expect(announce).toHaveBeenCalledWith(ORG, [`drungar@${SUFFIX}.test`], "stale_reservation_released");
    expect(announce).toHaveBeenCalledTimes(2);
    expect(summary).toMatchObject({ released: 2, kept: 2 });

    // A kept row is reported once, never re-examined.
    runs.listRunEvents.mockClear();
    const again = await releaseStaleReservations({ limit: 1000 });
    expect(again.kept).toBe(0);
    expect(runs.listRunEvents).not.toHaveBeenCalledWith("run-stamped", expect.anything());
  });

  it("a runs-service failure releases nothing (retried next tick)", async () => {
    const stale = await reservation(`flaky@${SUFFIX}.test`, { ageMin: 60, runId: "run-flaky" });
    runs.listRunEvents.mockImplementation(async (runId: string) => {
      if (runId === "run-flaky") throw new Error("runs-service GET /v1/runs/run-flaky/events failed: 503 - down");
      return [];
    });
    await releaseStaleReservations({ limit: 1000 });
    expect((await row(stale)).instantly_campaign_id).toMatch(/^reserving:/);
    await q(`DELETE FROM instantly_campaigns WHERE id = $1`, [stale]);
  });

  it("cancels the provisioned costs of an orphan step run, never those of a queued hold", async () => {
    await q(
      `INSERT INTO instantly_campaigns (id, campaign_id, lead_email, instantly_campaign_id, name, status, org_id, user_id, brand_ids)
       VALUES ($1, $2, $3, $4, 'n', 'active', $5, $6, '{}')`,
      [crypto.randomUUID(), `camp2-${SUFFIX}`, `queued@${SUFFIX}.test`, `self:${crypto.randomUUID()}`, ORG, USER],
    );
    await q(
      `INSERT INTO sequence_costs (id, lead_email, step, run_id, cost_id, domain_cost_id, status) VALUES ($1, $2, 1, 'step-queued', 'cost-q1', 'cost-q2', 'provisioned')`,
      [crypto.randomUUID(), `queued@${SUFFIX}.test`],
    );
    const old = "2026-10-16T14:49:26.000Z";
    runs.listRuns.mockImplementation(async (_f: unknown, identity: { orgId: string }) =>
      identity.orgId !== ORG
        ? []
        : [
            { id: "step-orphan", taskName: "email-send-step-1", status: "running", startedAt: old, ownProvisionedCostInUsdCents: "5.68", parentRunId: "p", serviceName: "instantly-service" },
            { id: "step-queued", taskName: "email-send-step-1", status: "running", startedAt: old, ownProvisionedCostInUsdCents: "5.68", parentRunId: "p", serviceName: "instantly-service" },
            { id: "verify", taskName: "email-verification", status: "running", startedAt: old, ownProvisionedCostInUsdCents: "1", parentRunId: "p", serviceName: "instantly-service" },
          ],
    );
    const costs = (prefix: string) => [
      { id: `${prefix}1`, status: "provisioned" },
      { id: `${prefix}2`, status: "provisioned" },
    ];
    runs.getRunWithCosts.mockImplementation(async (id: string) => ({
      id,
      status: "running",
      taskName: "email-send-step-1",
      parentRunId: "p",
      startedAt: old,
      costs: id === "step-orphan" ? costs("cost-o") : costs("cost-q"),
    }));
    runs.updateCostStatus.mockResolvedValue({});
    runs.updateRun.mockResolvedValue({});

    const summary = await settleOrphanStepRuns({ asOf: new Date("2026-10-16T16:00:00Z") });

    expect(runs.getRunWithCosts).not.toHaveBeenCalledWith("verify", expect.anything());
    expect(runs.updateCostStatus.mock.calls.map((c) => c[1]).sort()).toEqual(["cost-o1", "cost-o2"]);
    expect(runs.updateCostStatus).toHaveBeenCalledWith("step-orphan", "cost-o1", "cancelled", expect.objectContaining({ orgId: ORG, userId: USER, runId: "step-orphan" }));
    expect(runs.updateRun).toHaveBeenCalledTimes(1);
    expect(runs.updateRun).toHaveBeenCalledWith("step-orphan", "failed", expect.anything(), expect.any(String));
    expect(summary).toMatchObject({ settled: 1, costsCancelled: 2 });
  });
});
