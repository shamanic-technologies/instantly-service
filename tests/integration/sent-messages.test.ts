/**
 * DB-backed proof of the exact-message record (FACTORY-RECORD-SEND-1028):
 * migration 0097 applies; the stored bytes come back byte-equal (every byte
 * value, through `bytea`), with their sha256; the staff read filters by lead;
 * retention prunes only rows past 180 days.
 *
 * Skipped when no database is configured (CI runs without one):
 * `INSTANTLY_SERVICE_DATABASE_URL=postgres://postgres@localhost:5432/x npx vitest run <file>`.
 */
import path from "node:path";
import { createHash } from "node:crypto";
import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

const { pool } = vi.hoisted(() => {
  const url = process.env.INSTANTLY_SERVICE_DATABASE_URL;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { Pool: P } = require("pg");
  return { pool: url ? new P({ connectionString: url }) : null };
});

// src/db forces TLS; this suite talks to the same database through its own pool.
vi.mock("../../src/db", async () => {
  const { drizzle } = await import("drizzle-orm/node-postgres");
  return { db: pool ? drizzle(pool) : null };
});

const CAMPAIGN = `self:sent-record-${crypto.randomUUID()}`;

describe.skipIf(SKIP)("smtp_sent_messages (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
  });

  afterAll(async () => {
    if (!pool) return;
    await pool.query(`DELETE FROM smtp_sent_messages WHERE instantly_campaign_id = $1`, [CAMPAIGN]);
    await pool.end();
  });

  it("stores and serves the exact bytes, every byte value included", async () => {
    const { recordSentMessage, loadSentMessages } = await import("../../src/lib/self-send/send-record");
    const raw = Buffer.concat([
      Buffer.from("Message-ID: <a@x.test>\r\nSubject: =?UTF-8?Q?Caf=C3=A9?=\r\n\r\n"),
      Buffer.from(Array.from({ length: 256 }, (_, i) => i)),
      Buffer.from("\r\n"),
    ]);
    const sentAt = new Date();

    await expect(
      recordSentMessage({
        dispatchId: crypto.randomUUID(),
        instantlyCampaignId: CAMPAIGN,
        leadEmail: "Prospect@Lead.test",
        accountEmail: "amy@a.test",
        step: 2,
        kind: "sequence_step",
        messageId: "<a@x.test>",
        rawMessage: raw,
        messageInputs: { subject: "Café" },
        decision: { schema: "send-decision/v1", selectedAt: sentAt },
        sentAt,
      }),
    ).resolves.toBe(true);

    const [row] = await loadSentMessages({ leadEmail: "prospect@lead.test", limit: 5 });
    expect(row).toMatchObject({ instantlyCampaignId: CAMPAIGN, step: 2, messageId: "<a@x.test>", rawBytes: raw.length });
    const back = Buffer.from(row!.rawMessageBase64, "base64");
    expect(back.equals(raw)).toBe(true);
    expect(row!.rawSha256).toBe(createHash("sha256").update(raw).digest("hex"));
    expect(row!.decision).toEqual({ schema: "send-decision/v1", selectedAt: sentAt.toISOString() });
  });

  it("prunes only what is past retention", async () => {
    const { pruneSentMessages, loadSentMessages, SENT_MESSAGE_RETENTION_DAYS } = await import(
      "../../src/lib/self-send/send-record"
    );
    const later = new Date(Date.now() + (SENT_MESSAGE_RETENTION_DAYS - 1) * 24 * 60 * 60 * 1000);
    await pruneSentMessages(later);
    expect(await loadSentMessages({ instantlyCampaignId: CAMPAIGN, limit: 5 })).toHaveLength(1);

    const past = new Date(Date.now() + (SENT_MESSAGE_RETENTION_DAYS + 1) * 24 * 60 * 60 * 1000);
    expect(await pruneSentMessages(past)).toBeGreaterThanOrEqual(1);
    expect(await loadSentMessages({ instantlyCampaignId: CAMPAIGN, limit: 5 })).toHaveLength(0);
  });
});
