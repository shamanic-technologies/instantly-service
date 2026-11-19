/**
 * DB-backed proof of SES event filing (SES-COLD-1119): migration 0113 applies;
 * a permanent bounce of a send stored under SES's rewritten Message-Id lands as
 * an `imap_messages_raw` bounce row ON that send (campaign + step) for the DSN
 * judgment sweep; an SNS redelivery files nothing twice; a seed's bounce lands on
 * the sending mailbox with no campaign; the pipe decision reads ses_sending_domains.
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

const CAMPAIGN = `self:ses-events-${crypto.randomUUID()}`;
const SES_ID = `0100019a-${crypto.randomUUID()}-000000`;
const SNS_1 = `sns-${crypto.randomUUID()}`;
const SNS_2 = `sns-${crypto.randomUUID()}`;
const DOMAIN = `ses-test-${crypto.randomUUID().slice(0, 8)}.org`;

describe.skipIf(SKIP)("SES events filing (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    await pool!.query(
      `INSERT INTO smtp_dispatch_raw (id, instantly_campaign_id, lead_email, account_email, step, outcome, message_id, response, payload)
       VALUES ($1, $2, 'lead@x.test', 'k.lourd@growthservice.org', 2, 'sent', $3, $4, '{}'::jsonb)`,
      [crypto.randomUUID(), CAMPAIGN, `<${SES_ID}@email.amazonses.com>`, `250 Ok ${SES_ID}`],
    );
  });

  afterAll(async () => {
    if (!pool) return;
    await pool.query(`DELETE FROM imap_messages_raw WHERE message_id IN ($1, $2)`, [`<ses-event-${SNS_1}@amazonses.com>`, `<ses-event-${SNS_2}@amazonses.com>`]);
    await pool.query(`DELETE FROM ses_events_raw WHERE sns_message_id IN ($1, $2)`, [SNS_1, SNS_2]);
    await pool.query(`DELETE FROM smtp_dispatch_raw WHERE instantly_campaign_id = $1`, [CAMPAIGN]);
    await pool.query(`DELETE FROM ses_sending_domains WHERE domain = $1`, [DOMAIN]);
    await pool.end();
  });

  it("a permanent bounce lands as a bounce row on its send, once", async () => {
    const { fileSesNotification } = await import("../../src/lib/self-send/ses-events");
    const event = {
      eventType: "Bounce",
      mail: { messageId: SES_ID, source: "Kevin Lourd <k.lourd@growthservice.org>", destination: ["lead@x.test"] },
      bounce: { bounceType: "Permanent", bounceSubType: "General", timestamp: "2026-11-19T10:00:00.000Z", bouncedRecipients: [{ emailAddress: "lead@x.test", status: "5.1.1", diagnosticCode: "smtp; 550 5.1.1 user unknown" }] },
    };
    const first = await fileSesNotification(SNS_1, event);
    expect(first).toEqual({ filedAs: "bounce", duplicate: false, instantlyCampaignId: CAMPAIGN });
    const again = await fileSesNotification(SNS_1, event);
    expect(again.duplicate).toBe(true);
    const { rows } = await pool!.query(
      `SELECT account_email, kind, instantly_campaign_id, step, payload->>'textSnippet' AS words FROM imap_messages_raw WHERE message_id = $1`,
      [`<ses-event-${SNS_1}@amazonses.com>`],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ account_email: "k.lourd@growthservice.org", kind: "bounce", instantly_campaign_id: CAMPAIGN, step: 2 });
    expect(rows[0].words).toMatch(/550 5\.1\.1 user unknown/);
  });

  it("a seed's bounce (no sequence) lands on the sending mailbox with no campaign", async () => {
    const { fileSesNotification } = await import("../../src/lib/self-send/ses-events");
    const filed = await fileSesNotification(SNS_2, {
      eventType: "Bounce",
      mail: { messageId: `0100019a-${crypto.randomUUID()}-000000`, source: "bria@growthservice.org" },
      bounce: { bounceType: "Permanent", bounceSubType: "General", bouncedRecipients: [{ emailAddress: "bounce@simulator.amazonses.com" }] },
    });
    expect(filed).toEqual({ filedAs: "bounce", duplicate: false, instantlyCampaignId: null });
    const { rows } = await pool!.query(`SELECT account_email, instantly_campaign_id FROM imap_messages_raw WHERE message_id = $1`, [`<ses-event-${SNS_2}@amazonses.com>`]);
    expect(rows).toEqual([{ account_email: "bria@growthservice.org", instantly_campaign_id: null }]);
  });

  it("the pipe decision reads ses_sending_domains", async () => {
    const { loadLivePipes, clearLivePipesCache, pipeForSender } = await import("../../src/lib/self-send/outbound");
    await pool!.query(`INSERT INTO ses_sending_domains (domain, live_since) VALUES ($1, now())`, [DOMAIN]);
    clearLivePipesCache();
    const live = await loadLivePipes();
    expect(pipeForSender(`a@${DOMAIN}`, live, false)).toBe("ses");
    expect(pipeForSender(`a@${DOMAIN}`, live, true)).toBe("smtp");
  });
});
