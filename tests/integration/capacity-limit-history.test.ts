/**
 * DB-backed proof of CAPACITY-FROZEN-LIMIT-1021: capacity history reads the limit
 * in force each day from the LATEST of an Instantly snapshot and a lifecycle
 * flip's `daily_limit_after` (migration 0088). The snapshots stopped on
 * 2026-10-12; before this, a limit changed by a flip never reached the chart.
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
  return { pool: url ? new P({ connectionString: url, options: "-c TimeZone=UTC" }) : null };
});
vi.mock("../../src/db", async () => {
  const { drizzle: d } = await import("drizzle-orm/node-postgres");
  return { db: pool ? d(pool) : null };
});

const { fetchCapacityHistoryByMailbox } = await import("../../src/lib/capacity-history");

const SUFFIX = crypto.randomUUID().slice(0, 8);
const EMAIL = `kevin@cap-${SUFFIX}.test`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);
const DAY = 86_400_000;
const midnight = Date.parse(`${new Date().toISOString().slice(0, 10)}T00:00:00Z`);
const at = (daysAgo: number, hour: number) => new Date(midnight - daysAgo * DAY + hour * 3_600_000);

describe.skipIf(SKIP)("capacity history limit as-of a day (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    await q(`INSERT INTO instantly_accounts (id, email, daily_limit) VALUES ($1, $2, 50)`, [crypto.randomUUID(), EMAIL]);
    // In production from 4 days ago; the last Instantly snapshot (3 days ago) says 20.
    await q(
      `INSERT INTO instantly_account_lifecycle_events (id, account_email, from_status, to_status, reason, daily_limit, created_at)
       VALUES ($1, $2, 'in_recovery', 'in_production', 'passed', 20, $3)`,
      [crypto.randomUUID(), EMAIL, at(4, 1)],
    );
    await q(
      `INSERT INTO instantly_accounts_raw (id, account_email, daily_limit, fetched_at, payload)
       VALUES ($1, $2, 20, $3, '{}'::jsonb)`,
      [crypto.randomUUID(), EMAIL, at(3, 1)],
    );
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM instantly_account_lifecycle_events WHERE account_email = $1`, [EMAIL]);
    await q(`DELETE FROM instantly_accounts_raw WHERE account_email = $1`, [EMAIL]);
    await q(`DELETE FROM instantly_accounts WHERE email = $1`, [EMAIL]);
    await pool.end();
  });

  const limits = async () =>
    (await fetchCapacityHistoryByMailbox(5)).map((p) => p.mailboxes.get(EMAIL)?.operatorLimit ?? null);

  it("before any flip carries a limit, every day reads the snapshot (the frozen 20)", async () => {
    // 4 days ago: in production, no snapshot yet = limit unknown (0, stated).
    expect(await limits()).toEqual([0, 20, 20, 20, 20]);
    const [first] = await fetchCapacityHistoryByMailbox(5);
    expect(first!.mailboxes.get(EMAIL)!.limitUnknownAddresses).toEqual([EMAIL]);
  });

  it("a flip that puts 50 in force TODAY shows the same day, the past keeps its snapshot", async () => {
    await q(
      `INSERT INTO instantly_account_lifecycle_events (id, account_email, from_status, to_status, reason, daily_limit, daily_limit_after, created_at)
       VALUES ($1, $2, 'in_production', 'in_production', 'passed', 20, 50, $3)`,
      [crypto.randomUUID(), EMAIL, new Date(Math.min(Date.now(), at(0, 0).getTime() + 60_000))],
    );
    expect(await limits()).toEqual([0, 20, 20, 20, 50]);
  });
});
