/**
 * DB-backed proof of the outcome-trigger fire store (CLICK-FIRES-WEBSITE-VISIT-1015): migration 0079 applies,
 * a person's fire is written ONCE, a due row is taken once under a lease, and a settled row never moves again.
 *
 * Builds its own pool rather than importing `src/db` (which forces `sslmode=verify-full`), and applies every
 * migration itself (idempotent on a migrated database). Skipped when no database is configured (CI runs
 * without one): `INSTANTLY_SERVICE_DATABASE_URL=postgres://postgres@localhost:55447/x npx vitest run <file>`.
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

const { drizzleOutcomeFireStore: store } = await import("../../src/lib/website-visit-trigger");

const ORG = `org-${crypto.randomUUID()}`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);
const NOW = new Date("2026-10-15T12:00:00.000Z");
const fire = (campaignId: string, leadEmail = "p@x.test") => ({
  outcomeKey: "website_visit",
  orgId: ORG,
  campaignId,
  instantlyCampaignId: `self:${campaignId}`,
  leadEmail,
  leadId: "lead-1",
  sourceEventId: "silver-1",
  occurredAt: new Date("2026-10-15T11:58:00.000Z"),
});

describe.skipIf(SKIP)("outcome_trigger_fires (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM outcome_trigger_fires WHERE org_id = $1`, [ORG]);
    await pool.end();
  });

  it("writes a person's fire once per (outcome, org, campaign, address)", async () => {
    const first = await store.insertOnce(fire("c-1"), NOW);
    expect(first).toMatchObject({ orgId: ORG, campaignId: "c-1", leadEmail: "p@x.test", attempts: 0 });
    expect(first!.occurredAt.toISOString()).toBe("2026-10-15T11:58:00.000Z");
    expect(await store.insertOnce(fire("c-1"), NOW)).toBeNull();
    expect(await store.insertOnce(fire("c-2"), NOW)).not.toBeNull();
    const r = await q(`SELECT count(*)::int AS n, min(status) AS status FROM outcome_trigger_fires WHERE org_id = $1`, [ORG]);
    expect(r.rows[0]).toEqual({ n: 2, status: "pending" });
  });

  it("takes a due row once under a lease, never a row not due yet", async () => {
    await q(`DELETE FROM outcome_trigger_fires WHERE org_id = $1`, [ORG]);
    await store.insertOnce(fire("due"), new Date(NOW.getTime() - 1000));
    await store.insertOnce(fire("later"), new Date(NOW.getTime() + 60_000));

    const lease = new Date(NOW.getTime() + 5 * 60_000);
    const taken = (await store.claimDue(NOW, lease, 100)).filter((f) => f.orgId === ORG);
    expect(taken.map((f) => f.campaignId)).toEqual(["due"]);
    expect((await store.claimDue(NOW, lease, 100)).filter((f) => f.orgId === ORG)).toEqual([]);
    const r = await q(`SELECT next_attempt_at FROM outcome_trigger_fires WHERE org_id = $1 AND campaign_id = 'due'`, [ORG]);
    expect(new Date(r.rows[0].next_attempt_at).toISOString()).toBe(lease.toISOString());
  });

  it("settles: a retry moves attempts and the next attempt; a delivered row never moves again", async () => {
    await q(`DELETE FROM outcome_trigger_fires WHERE org_id = $1`, [ORG]);
    const f = (await store.insertOnce(fire("s"), NOW))!;

    await store.settle(f.id, { status: "pending", attempts: 1, nextAttemptAt: new Date(NOW.getTime() + 60_000), error: "503" }, NOW);
    let r = await q(`SELECT status, attempts, last_error FROM outcome_trigger_fires WHERE id = $1`, [f.id]);
    expect(r.rows[0]).toEqual({ status: "pending", attempts: 1, last_error: "503" });

    await store.settle(
      f.id,
      { status: "delivered", triggerId: "website_visited", brandId: "b", offerId: "o", idempotencyKey: "k", campaignEventId: "evt-1", replayed: true },
      NOW,
    );
    await store.settle(f.id, { status: "skipped", skipReason: "no_offer", detail: "late" }, NOW);
    r = await q(
      `SELECT status, trigger_id, idempotency_key, campaign_event_id, replayed, skip_reason, delivered_at FROM outcome_trigger_fires WHERE id = $1`,
      [f.id],
    );
    expect(r.rows[0]).toMatchObject({
      status: "delivered",
      trigger_id: "website_visited",
      idempotency_key: "k",
      campaign_event_id: "evt-1",
      replayed: true,
      skip_reason: null,
    });
    expect(r.rows[0].delivered_at).not.toBeNull();
  });
});
