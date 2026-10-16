/**
 * DB-backed: automated SURBL removal requests are stored bronze (attempt) +
 * silver (event) + gold (daily series), and a request that reached the form
 * waits a week; a failure before the form is retried the next day.
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

const { fileDueRemovals } = await import("../../src/lib/blocklist-removal");
const { loadDomainBlocklistRead } = await import("../../src/lib/domain-blocklist-read");

const DOMAIN = `rm-${crypto.randomUUID().slice(0, 8)}.test`;
const SEQ = `self:${crypto.randomUUID()}`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);
const CONTACT = {
  fullName: "Kevin Lourd", email: "kevin@distribute.you", phone: "+33 6 00 00 00 00",
  organization: "Blooming Generation SASU", address: "285 rue de l'Eglise, 46140 Douelle, France", website: "https://distribute.you",
};

function surbl(response: string) {
  return vi.fn(async () => ({ response, code: Number(response.slice(0, 3)) }));
}

describe.skipIf(SKIP)("automated removal requests (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    await q(`INSERT INTO domain_blocklist_status (domain, list, listed, answer, labels, listed_since, last_checked_at, last_answered_at, first_answered_at, last_outcome)
             VALUES ($1, 'surbl', true, '127.0.0.64', '{ABUSE}', '2026-10-16T06:00:00Z', now(), now(), '2026-10-16T06:00:00Z', 'listed')`, [DOMAIN]);
    await q(`INSERT INTO sequence_steps (id, instantly_campaign_id, step, subject, body_html, delay_days) VALUES ($1, $2, 1, 'Quick idea', '<p>Hi there</p>', 0)`, [crypto.randomUUID(), SEQ]);
    await q(`INSERT INTO smtp_dispatch_raw (id, instantly_campaign_id, lead_email, account_email, step, outcome, message_id, payload, dispatched_at)
             VALUES ($1, $2, 'p@x.com', $3, 1, 'sent', '<m1@x>', '{}'::jsonb, now() - interval '1 day')`, [crypto.randomUUID(), SEQ, `raquel@${DOMAIN}`]);
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM domain_blocklist_removal_attempts WHERE domain = $1`, [DOMAIN]);
    await q(`DELETE FROM domain_blocklist_events WHERE domain = $1`, [DOMAIN]);
    await q(`DELETE FROM domain_blocklist_status WHERE domain = $1`, [DOMAIN]);
    await q(`DELETE FROM smtp_dispatch_raw WHERE instantly_campaign_id = $1`, [SEQ]);
    await q(`DELETE FROM sequence_steps WHERE instantly_campaign_id = $1`, [SEQ]);
    await pool.end();
  });

  const attempts = async () => (await q(`SELECT outcome FROM domain_blocklist_removal_attempts WHERE domain = $1 ORDER BY attempted_at`, [DOMAIN])).rows.map((r) => r.outcome);
  const events = async () => (await q(`SELECT event_type FROM domain_blocklist_events WHERE domain = $1 ORDER BY occurred_at`, [DOMAIN])).rows.map((r) => r.event_type);

  it("day 1, the owner's phone is not set: one failure stored, SURBL never contacted", async () => {
    const f = surbl("250 2.0.0 Ok: queued");
    const out = await fileDueRemovals({ now: new Date("2026-10-16T07:00:00Z"), loadContact: async () => { throw new Error("not set on kevinlourd.com (Identity page): phone"); }, send: f });
    expect(out).toMatchObject({ due: 1, failed: 1, sent: 0 });
    expect(f).not.toHaveBeenCalled();
    expect(await attempts()).toEqual(["failed"]);
    expect(await events()).toEqual(["removal_failed"]);
  });

  it("day 2, phone set: the request is sent, stored with what we posted", async () => {
    const f = surbl("250 2.0.0 Ok: queued as 4ABC");
    const out = await fileDueRemovals({ now: new Date("2026-10-17T07:00:00Z"), loadContact: async () => CONTACT, send: f });
    expect(out).toMatchObject({ due: 1, sent: 1 });
    const row = (await q(`SELECT request, http_status, response_excerpt FROM domain_blocklist_removal_attempts WHERE domain = $1 AND outcome = 'sent'`, [DOMAIN])).rows[0];
    expect(row.request.fp).toBe(DOMAIN);
    expect(row.request.headers).toMatch(/From: .*raquel@/);
    expect(row.request.headers).toMatch(/To: recipient@redacted\.invalid/);
    expect(row.response_excerpt).toMatch(/queued/);
    expect(row.request.emailTo).toBe("whitelist@surbl.org");
    expect(await events()).toEqual(["removal_failed", "removal_requested"]);
  });

  it("day 5: still listed, inside the week, SURBL is not asked again", async () => {
    const f = surbl("250 2.0.0 Ok: queued");
    const out = await fileDueRemovals({ now: new Date("2026-10-20T07:00:00Z"), loadContact: async () => CONTACT, send: f });
    expect(out).toMatchObject({ due: 0, skippedCoolingDown: 1 });
    expect(f).not.toHaveBeenCalled();
  });

  it("day 9: a week later it is asked again; the staff series counts each day", async () => {
    const out = await fileDueRemovals({ now: new Date("2026-10-24T07:00:00Z"), loadContact: async () => CONTACT, send: surbl("250 2.0.0 Ok: queued") });
    expect(out.sent).toBe(1);
    const read = await loadDomainBlocklistRead(10, new Date("2026-10-24T12:00:00Z"));
    const byDay = Object.fromEntries(read.series.removalRequests.map((d) => [d.day, d]));
    expect(byDay["2026-10-16"]).toMatchObject({ failed: 1, requested: 0 });
    expect(byDay["2026-10-17"]).toMatchObject({ failed: 0, requested: 1 });
    expect(byDay["2026-10-24"]).toMatchObject({ requested: 1 });
  });
});
