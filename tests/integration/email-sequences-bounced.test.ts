/**
 * DB-backed: a send the receiving server accepted then refused (a DSN read in
 * the mailbox, promoted as a real `email_bounced`) reads `bounced` on the
 * client's GET /orgs/email-sequences, never `sent`. Replays the 2026-10-15
 * DefiniteJobs send (Gmail accepted 19:12:23, GoDaddy's DSN 19:12:26).
 * Skipped without INSTANTLY_SERVICE_DATABASE_URL (see domain-blocklist.test.ts).
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

const { fetchBrandEmailSequences } = await import("../../src/lib/brand-email-sequences");

const ORG = `org-${crypto.randomUUID()}`;
const BRAND = crypto.randomUUID();
const SEQ = `self:${crypto.randomUUID()}`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);

describe.skipIf(SKIP)("email-sequences: a DSN refusal is not a delivery (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    await q(
      `INSERT INTO instantly_campaigns (id, instantly_campaign_id, name, org_id, brand_ids, lead_email, campaign_id, status)
       VALUES ($1, $2, 'x', $3, ARRAY[$4], 'team@definitejobsgroup.co.uk', 'camp-1', 'active')`,
      [crypto.randomUUID(), SEQ, ORG, BRAND],
    );
    await q(
      `INSERT INTO instantly_events (id, event_type, campaign_id, lead_email, step, "timestamp", source)
       VALUES ($1, 'email_sent', $3, 'team@definitejobsgroup.co.uk', 1, '2026-10-15 19:12:23.342', 'self_send'),
              ($2, 'email_bounced', $3, 'team@definitejobsgroup.co.uk', 1, '2026-10-15 19:12:26', 'self_send')`,
      [crypto.randomUUID(), crypto.randomUUID(), SEQ],
    );
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM instantly_events WHERE campaign_id = $1`, [SEQ]);
    await q(`DELETE FROM instantly_campaigns WHERE org_id = $1`, [ORG]);
    await pool.end();
  });

  it("serves the first email as bounced with both instants", async () => {
    const page = await fetchBrandEmailSequences({ orgId: ORG, brandId: BRAND });
    expect(page.sequences[0]!.steps[0]).toEqual({
      step: 1,
      kind: "first_email",
      status: "bounced",
      sentAt: "2026-10-15T19:12:23.342Z",
      bouncedAt: "2026-10-15T19:12:26.000Z",
    });
  });
});
