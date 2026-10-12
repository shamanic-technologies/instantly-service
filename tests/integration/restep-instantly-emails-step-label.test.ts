/**
 * DB-backed proof of migration 0070: Instantly /emails events stored at step 0
 * (the old parser read the first digit of `0_<n>_0`) land at their real step,
 * exactly as the fixed parser would have promoted them, and no send is counted
 * twice. Runs the migration's own SQL against seeded rows, twice (idempotent).
 *
 * Needs a migrated database in INSTANTLY_SERVICE_DATABASE_URL (no sslmode);
 * skipped otherwise (CI runs without one).
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import fs from "fs";
import path from "path";
import { Pool } from "pg";

const URL = process.env.INSTANTLY_SERVICE_DATABASE_URL;
const SKIP = !URL;
const pool = URL ? new Pool({ connectionString: URL }) : null;

const MIGRATION = fs.readFileSync(
  path.join(__dirname, "..", "..", "drizzle", "0070_restep_instantly_emails_step_label.sql"),
  "utf-8",
);

async function runMigration() {
  const client = await pool!.connect();
  try {
    await client.query("BEGIN");
    for (const stmt of MIGRATION.split("--> statement-breakpoint")) {
      if (stmt.trim()) await client.query(stmt);
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

const tag = crypto.randomUUID().slice(0, 8);
const CAMP = `restep-${tag}`;
const lead = (n: string) => `${n}-${tag}@lead.test`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);

async function bronze(label: string): Promise<string> {
  const id = crypto.randomUUID();
  await q(
    `INSERT INTO instantly_emails_raw (id, instantly_campaign_id, instantly_email_id, payload) VALUES ($1, $2, $1, $3::jsonb)`,
    [id, CAMP, JSON.stringify({ step: label, ue_type: 1 })],
  );
  return id;
}

async function event(e: {
  type?: string;
  lead: string;
  step: number;
  ts: string;
  source: string;
  sourceRowId?: string | null;
  inferred?: boolean;
  inferredFrom?: string | null;
  account?: string | null;
}): Promise<string> {
  const id = crypto.randomUUID();
  await q(
    `INSERT INTO instantly_events (id, event_type, campaign_id, lead_email, account_email, "timestamp", step, source, source_row_id, inferred, inferred_from_event_id, inferred_rule)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
    [
      id,
      e.type ?? "email_sent",
      CAMP,
      e.lead,
      e.account ?? null,
      e.ts,
      e.step,
      e.source,
      e.sourceRowId ?? null,
      e.inferred ?? false,
      e.inferredFrom ?? null,
      e.inferred ? "test_rule" : null,
    ],
  );
  return id;
}

async function row(id: string) {
  const { rows } = await q(`SELECT *, "timestamp"::text AS ts FROM instantly_events WHERE id = $1`, [id]);
  return rows[0] ?? null;
}

async function realSends() {
  const { rows } = await q(
    `SELECT lead_email, step FROM instantly_events
     WHERE campaign_id = $1 AND event_type = 'email_sent' AND inferred = false
     ORDER BY lead_email, step`,
    [CAMP],
  );
  return rows.map((r) => `${r.lead_email}#${r.step}`);
}

const ids: Record<string, string> = {};

describe.skipIf(SKIP)("migration 0070: re-step Instantly /emails events stored at step 0", () => {
  beforeAll(async () => {
    // A: third email, nothing at step 3 yet -> moves to step 3.
    ids.aPoll = await event({ lead: lead("a"), step: 0, ts: "2026-05-01T10:00:00Z", source: "poll_emails", sourceRowId: await bronze("0_2_0"), account: "box@x.test" });

    // B: second email, an INFERRED row holds step 2 (with a cascade child
    // pointing at it) -> the inferred row becomes real in place, step-0 row gone.
    ids.bPoll = await event({ lead: lead("b"), step: 0, ts: "2026-05-02T10:00:00Z", source: "poll_emails", sourceRowId: await bronze("0_1_0"), account: "box@x.test" });
    ids.bTwin = await event({ lead: lead("b"), step: 2, ts: "2026-05-09T10:00:00Z", source: "inferred", inferred: true, inferredFrom: crypto.randomUUID() });
    ids.bChild = await event({ lead: lead("b"), step: 1, ts: "2026-05-09T10:00:00Z", source: "inferred", inferred: true, inferredFrom: ids.bTwin });

    // C: first email, the webhook already recorded the same send 2 s later ->
    // the step-0 duplicate goes, the webhook row keeps Instantly's earlier time.
    ids.cPoll = await event({ lead: lead("c"), step: 0, ts: "2026-05-03T10:00:00Z", source: "poll_emails", sourceRowId: await bronze("0_0_0") });
    ids.cTwin = await event({ lead: lead("c"), step: 1, ts: "2026-05-03T10:00:02Z", source: "webhook" });

    // D: a reply to the 2nd email stored at step 0, with the send it implied
    // inferred at step 0 too; the real step-2 send exists.
    ids.dSent = await event({ lead: lead("d"), step: 2, ts: "2026-05-04T09:00:00Z", source: "webhook" });
    ids.dReply = await event({ type: "reply_received", lead: lead("d"), step: 0, ts: "2026-05-05T10:00:00Z", source: "poll_emails", sourceRowId: await bronze("0_1_0") });
    ids.dImplied = await event({ lead: lead("d"), step: 0, ts: "2026-05-05T10:00:00Z", source: "inferred", inferred: true, inferredFrom: ids.dReply });

    // Untouched: a step-0 open from the leads poll (no label), a webhook step 0.
    ids.open = await event({ type: "email_opened", lead: lead("e"), step: 0, ts: "2026-05-06T10:00:00Z", source: "poll_leads" });

    expect(await realSends()).toEqual([
      `${lead("a")}#0`,
      `${lead("b")}#0`,
      `${lead("c")}#0`,
      `${lead("c")}#1`,
      `${lead("d")}#2`,
    ]);

    await runMigration();
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM instantly_events_retracted WHERE event_id = ANY($1)`, [Object.values(ids)]);
    await q(`DELETE FROM instantly_events WHERE campaign_id = $1`, [CAMP]);
    await q(`DELETE FROM instantly_emails_raw WHERE instantly_campaign_id = $1`, [CAMP]);
    await pool.end();
  });

  it("puts every send at its real step and counts each send once", async () => {
    expect(await realSends()).toEqual([
      `${lead("a")}#3`,
      `${lead("b")}#2`,
      `${lead("c")}#1`,
      `${lead("d")}#2`,
    ]);
  });

  it("moves a send with nothing at its real step", async () => {
    const a = await row(ids.aPoll);
    expect(a.step).toBe(3);
    expect(a.source).toBe("poll_emails");
  });

  it("upgrades the inferred row in place (id kept for its cascade child) and drops the step-0 row", async () => {
    expect(await row(ids.bPoll)).toBeNull();
    const twin = await row(ids.bTwin);
    expect(twin).toMatchObject({ step: 2, inferred: false, source: "poll_emails", account_email: "box@x.test", inferred_from_event_id: null, inferred_rule: null });
    expect(twin.ts).toBe("2026-05-02 10:00:00");
    expect((await row(ids.bChild)).inferred_from_event_id).toBe(ids.bTwin);
  });

  it("drops the duplicate of a webhook send and keeps the earlier send time", async () => {
    expect(await row(ids.cPoll)).toBeNull();
    const twin = await row(ids.cTwin);
    expect(twin).toMatchObject({ step: 1, source: "webhook", inferred: false });
    expect(twin.ts).toBe("2026-05-03 10:00:00");
  });

  it("re-steps a reply and follows it with the send it implied", async () => {
    expect((await row(ids.dReply)).step).toBe(2);
    expect(await row(ids.dImplied)).toBeNull(); // the real step-2 send already holds it
  });

  it("leaves step-0 events with no Instantly label alone", async () => {
    expect((await row(ids.open)).step).toBe(0);
  });

  it("archives every touched row whole before changing it", async () => {
    const { rows } = await q(
      `SELECT event_id, action, event->>'step' AS step FROM instantly_events_retracted WHERE event_id = ANY($1) ORDER BY action, event_id`,
      [Object.values(ids)],
    );
    const byId = Object.fromEntries(rows.map((r) => [r.event_id, r]));
    expect(byId[ids.aPoll]).toMatchObject({ action: "restepped", step: "0" });
    expect(byId[ids.bPoll]).toMatchObject({ action: "retracted", step: "0" });
    expect(byId[ids.bTwin]).toMatchObject({ action: "restepped", step: "2" });
    expect(byId[ids.cPoll]).toMatchObject({ action: "retracted", step: "0" });
    expect(byId[ids.cTwin]).toMatchObject({ action: "restepped", step: "1" });
    expect(byId[ids.dReply]).toMatchObject({ action: "restepped", step: "0" });
    expect(byId[ids.dImplied]).toMatchObject({ action: "retracted", step: "0" });
    expect(byId[ids.open]).toBeUndefined();
  });

  it("is a no-op the second time", async () => {
    const before = await q(`SELECT * FROM instantly_events WHERE campaign_id = $1 ORDER BY id`, [CAMP]);
    await runMigration();
    const after = await q(`SELECT * FROM instantly_events WHERE campaign_id = $1 ORDER BY id`, [CAMP]);
    expect(after.rows).toEqual(before.rows);
  });
});
