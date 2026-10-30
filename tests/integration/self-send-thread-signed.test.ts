/**
 * DB-backed proof of UNIBOX-SIGNED-BODY-1030: the thread read joins the send
 * record, the thread's pinned signature and the sending account, and serves
 * every email we dispatched as it left (recorded html, else re-rendered), while
 * an inbound reply passes through untouched.
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

const CAMPAIGN = `self:signed-thread-${crypto.randomUUID()}`;
const ACCOUNT = `elizabeth-${crypto.randomUUID().slice(0, 8)}@capebinaryio.test`;
const LEAD = "stefan@vertigis.test";
const RECORDED_HTML = "<p>Step two body</p><p>--</p><p>Elizabeth Thomas<br>Distribute.you | Marketing Agency</p>";

describe.skipIf(SKIP)("self-send thread serves the signed body (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });

    await pool!.query(
      `INSERT INTO instantly_campaigns (id, instantly_campaign_id, name, brand_ids, lead_email, status, signature_html)
       VALUES (gen_random_uuid()::text, $1, 'signed-thread', ARRAY[]::text[], $2, 'active', $3)`,
      [CAMPAIGN, LEAD, "<p>{full_name}<br>Acme</p>"],
    );
    await pool!.query(
      `INSERT INTO instantly_accounts (id, email, first_name, last_name) VALUES (gen_random_uuid()::text, $1, 'Elizabeth', 'Thomas')`,
      [ACCOUNT],
    );
    await pool!.query(
      `INSERT INTO sequence_steps (id, instantly_campaign_id, step, subject, body_html)
       VALUES (gen_random_uuid()::text, $1, 1, 'Hello', '<p>Step one body</p>'), (gen_random_uuid()::text, $1, 2, 'Hello', '<p>Step two body</p>')`,
      [CAMPAIGN],
    );
    await pool!.query(
      `INSERT INTO smtp_dispatch_raw (id, instantly_campaign_id, lead_email, account_email, step, outcome, dispatched_at, payload)
       VALUES ('d1-' || $1, $1, $2, $3, 1, 'sent', '2026-10-20T10:00:00Z', '{}'::jsonb),
              ('d2-' || $1, $1, $2, $3, 2, 'sent', '2026-10-29T10:00:00Z', '{}'::jsonb)`,
      [CAMPAIGN, LEAD, ACCOUNT],
    );
    await pool!.query(
      `INSERT INTO smtp_sent_messages (id, dispatch_id, instantly_campaign_id, lead_email, account_email, step, kind,
         message_id, raw_message, raw_sha256, raw_bytes, message_inputs, decision, sent_at)
       VALUES (gen_random_uuid()::text, 'd2-' || $1, $1, $2, $3, 2, 'sequence_step', '<m2@x>', '\\x00', 'x', 1,
         jsonb_build_object('built', jsonb_build_object('html', $4::text)), '{}'::jsonb, '2026-10-29T10:00:00Z')`,
      [CAMPAIGN, LEAD, ACCOUNT, RECORDED_HTML],
    );
    await pool!.query(
      `INSERT INTO imap_messages_raw (id, account_email, message_id, from_address, subject, kind, instantly_campaign_id, received_at, payload)
       VALUES (gen_random_uuid()::text, $1, '<r1@x>', $2, 'Re: Hello', 'reply', $3, '2026-10-30T10:00:00Z', '{"textSnippet":"Sounds good"}'::jsonb)`,
      [ACCOUNT, LEAD, CAMPAIGN],
    );
  });

  afterAll(async () => {
    if (!pool) return;
    for (const t of ["smtp_sent_messages", "smtp_dispatch_raw", "sequence_steps", "imap_messages_raw", "instantly_campaigns"]) {
      await pool.query(`DELETE FROM ${t} WHERE instantly_campaign_id = $1`, [CAMPAIGN]);
    }
    await pool.query(`DELETE FROM instantly_accounts WHERE email = $1`, [ACCOUNT]);
    await pool.end();
  });

  it("fetchSelfSendThread: re-rendered step 1 (pinned signature filled), recorded step 2, inbound untouched", async () => {
    const { fetchSelfSendThread } = await import("../../src/lib/self-send/thread");
    const thread = await fetchSelfSendThread(CAMPAIGN);
    expect(thread.map((m) => m.bodyText)).toEqual([
      "Step one body\n\n--\n\nElizabeth Thomas\nAcme",
      "Step two body\n\n--\n\nElizabeth Thomas\nDistribute.you | Marketing Agency",
      "Sounds good",
    ]);
  });

  it("fetchOwnDispatchedMessages serves the same signed bodies", async () => {
    const { fetchOwnDispatchedMessages } = await import("../../src/lib/self-send/thread");
    const own = await fetchOwnDispatchedMessages(CAMPAIGN);
    expect(own.map((m) => m.message.bodyText)).toEqual([
      "Step one body\n\n--\n\nElizabeth Thomas\nAcme",
      "Step two body\n\n--\n\nElizabeth Thomas\nDistribute.you | Marketing Agency",
    ]);
  });
});
