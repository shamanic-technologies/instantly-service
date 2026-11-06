/**
 * A step that LEFT is never due again, whatever the cost ledger says
 * (2026-11-06 21:35 UTC, Doc Dinners: two prospects got their first email twice,
 * 83 min apart: the hold's provisioned→actual write had failed after the real
 * send, the ledger read "never sent", and a deploy had cleared the in-process
 * recentlySent guard).
 *
 * Skipped when no database is configured (CI runs without one):
 * `INSTANTLY_SERVICE_DATABASE_URL=postgres://postgres@localhost:5432/x npx vitest run <file>`.
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
  const { drizzle } = await import("drizzle-orm/node-postgres");
  return { db: pool ? drizzle(pool) : null };
});

const CAMPAIGN = `self:pending-sent-${crypto.randomUUID()}`;
const LEAD = "prospect@lead.test";

describe.skipIf(SKIP)("loadPendingSequences (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    await pool!.query(
      `INSERT INTO instantly_campaigns (id, instantly_campaign_id, name, org_id, brand_ids, status, delivery_status, account_email, send_transport, lead_email)
       VALUES ($1, $2, 'c', 'org-1', ARRAY['b'], 'active', 'sent', 'amy@a.test', 'smtp', $3)`,
      [crypto.randomUUID(), CAMPAIGN, LEAD],
    );
    for (const step of [1, 2, 3]) {
      await pool!.query(
        `INSERT INTO sequence_costs (id, lead_email, step, run_id, status, instantly_campaign_id) VALUES ($1, $2, $3, 'run', 'provisioned', $4)`,
        [crypto.randomUUID(), LEAD, step, CAMPAIGN],
      );
    }
  });

  afterAll(async () => {
    if (!pool) return;
    await pool.query(`DELETE FROM smtp_sent_messages WHERE instantly_campaign_id = $1`, [CAMPAIGN]);
    await pool.query(`DELETE FROM sequence_costs WHERE instantly_campaign_id = $1`, [CAMPAIGN]);
    await pool.query(`DELETE FROM instantly_campaigns WHERE instantly_campaign_id = $1`, [CAMPAIGN]);
    await pool.end();
  });

  it("a ledger that never saw the send still never sends step 1 twice", async () => {
    const { loadPendingSequences } = await import("../../src/lib/self-send/dispatch-worker");
    const [before] = await loadPendingSequences(CAMPAIGN);
    expect(before).toMatchObject({ provisionedSteps: [1, 2, 3], lastSentStep: null });

    const { recordSentMessage } = await import("../../src/lib/self-send/send-record");
    const sentAt = new Date("2026-11-06T20:12:43Z");
    await recordSentMessage({
      dispatchId: crypto.randomUUID(),
      instantlyCampaignId: CAMPAIGN,
      leadEmail: LEAD,
      accountEmail: "amy@a.test",
      step: 1,
      kind: "sequence_step",
      messageId: "<s1@a.test>",
      rawMessage: Buffer.from("Subject: hi\r\n\r\nbody"),
      messageInputs: {},
      decision: { schema: "send-decision/v1", selectedAt: sentAt },
      sentAt,
    });
    // The hold of step 1 is still `provisioned` (its actual write failed).
    const [after] = await loadPendingSequences(CAMPAIGN);
    expect(after).toMatchObject({ provisionedSteps: [2, 3], lastSentStep: 1 });
    expect(after!.lastSentAt).not.toBeNull();
    const { nextDueStep } = await import("../../src/lib/self-send/dispatch");
    expect(nextDueStep(after!, new Date("2026-11-06T21:35:00Z"))?.step ?? null).not.toBe(1);
  });
});
