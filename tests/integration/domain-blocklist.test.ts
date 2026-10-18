/**
 * DB-backed proof of the sending-domain blocklist check (DOMAIN-BLOCKLIST-1016):
 * migration 0080 applies; a new listing writes ONE `listed` event, ONE removal
 * link and NO staff email (owner 2026-10-18); the same listing the next day
 * writes nothing; a refused query keeps the state; a fresh clean answer delists.
 *
 * Builds its own pool (like outcome-trigger-fires.test.ts). Skipped when no
 * database is configured (CI runs without one):
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
  const { drizzle: d } = await import("drizzle-orm/node-postgres");
  return { db: pool ? d(pool) : null };
});

const { runBlocklistCheck } = await import("../../src/lib/domain-blocklist-sync");
const { loadDomainBlocklistRead } = await import("../../src/lib/domain-blocklist-read");
type Lookup = import("../../src/lib/domain-blocklist-sync").Lookup;
type Def = import("../../src/lib/domain-blocklist").BlocklistDefinition;

const SUFFIX = crypto.randomUUID().slice(0, 8);
const LISTED = `listed-${SUFFIX}.test`;
const CLEAN = `clean-${SUFFIX}.test`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);

/** SURBL lists LISTED (when `listed`), every list answers its canary; `refuse` = SURBL refuses everything. */
function world(opts: { listed: boolean; refuse?: boolean }) {
  return async (list: Def): Promise<Lookup> => async (name) => {
    if (list.key === "surbl" && opts.refuse) return { kind: "answer", addresses: ["127.0.0.1"] };
    if (name === `${list.canary}.${list.zone}`) {
      const canary: Record<string, string> = { surbl: "127.0.0.254", spamhaus_dbl: "127.0.1.2", uribl: "127.0.0.14" };
      return { kind: "answer", addresses: [canary[list.key]!] };
    }
    if (list.key === "surbl" && opts.listed && name === `${LISTED}.${list.zone}`) {
      return { kind: "answer", addresses: ["127.0.0.64"] };
    }
    return { kind: "nxdomain" };
  };
}

describe.skipIf(SKIP)("domain blocklist check (DB-backed)", () => {
  const reconcile = vi.fn().mockResolvedValue({ changed: 0 });

  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    for (const email of [`raquel@${LISTED}`, `ruth@${LISTED}`, `amy@${CLEAN}`]) {
      await q(`INSERT INTO instantly_accounts (id, email) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [crypto.randomUUID(), email]);
    }
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM domain_blocklist_events WHERE domain IN ($1, $2)`, [LISTED, CLEAN]);
    await q(`DELETE FROM domain_blocklist_status WHERE domain IN ($1, $2)`, [LISTED, CLEAN]);
    await q(`DELETE FROM instantly_accounts WHERE email LIKE $1 OR email LIKE $2`, [`%@${LISTED}`, `%@${CLEAN}`]);
    await pool.end();
  });

  const events = async () =>
    (await q(`SELECT event_type, alert_status, mailboxes FROM domain_blocklist_events WHERE domain = $1 ORDER BY occurred_at, event_type`, [LISTED])).rows;

  it("day 1, a new listing: one listed event + one removal link, no staff email, lifecycle reconciled", async () => {
    const summary = await runBlocklistCheck({ asOf: new Date("2026-10-16T06:00:00Z"), lookupFactory: world({ listed: true }), reconcile });
    expect(summary.listed).toBe(1);
    expect(reconcile).toHaveBeenCalledTimes(1);
    const rows = await events();
    expect(rows.map((r) => r.event_type)).toEqual(["listed", "removal_link_issued"]);
    expect(rows[0].alert_status).toBeNull();
    expect([...rows[0].mailboxes].sort()).toEqual([`raquel@${LISTED}`, `ruth@${LISTED}`]);

  });

  it("day 2, the same listing: no new event, no reconcile", async () => {
    const summary = await runBlocklistCheck({ asOf: new Date("2026-10-17T06:00:00Z"), lookupFactory: world({ listed: true }), reconcile });
    expect(summary.listed).toBe(0);
    expect(reconcile).toHaveBeenCalledTimes(1);
    expect(await events()).toHaveLength(2);
    const status = (await q(`SELECT listed, listed_since FROM domain_blocklist_status WHERE domain = $1 AND list = 'surbl'`, [LISTED])).rows[0];
    expect(status.listed).toBe(true);
    expect(new Date(status.listed_since).toISOString()).toBe("2026-10-16T06:00:00.000Z");
  });

  it("day 3, SURBL refuses our query: the domain stays listed (never clean unchecked)", async () => {
    const summary = await runBlocklistCheck({ asOf: new Date("2026-10-18T06:00:00Z"), lookupFactory: world({ listed: false, refuse: true }), reconcile });
    expect(summary.lists.find((l) => l.list === "surbl")).toMatchObject({ answered: false });
    expect(summary.delisted).toBe(0);
    const status = (await q(`SELECT listed, last_outcome FROM domain_blocklist_status WHERE domain = $1 AND list = 'surbl'`, [LISTED])).rows[0];
    expect(status).toEqual({ listed: true, last_outcome: "unanswered" });
  });

  it("day 4, a fresh clean answer: one delisted event, lifecycle reconciled, the staff read shows it", async () => {
    const summary = await runBlocklistCheck({ asOf: new Date("2026-10-19T06:00:00Z"), lookupFactory: world({ listed: false }), reconcile });
    expect(summary.delisted).toBe(1);
    expect(reconcile).toHaveBeenCalledTimes(2);
    expect((await events()).map((r) => r.event_type)).toContain("delisted");

    const read = await loadDomainBlocklistRead(10, new Date("2026-10-19T12:00:00Z"));
    const dom = read.domains.find((d) => d.domain === LISTED)!;
    expect(dom.listed).toBe(false);
    expect(dom.lists.find((l) => l.list === "surbl")!.state).toBe("clean");
    const series = read.series.domainsListed.filter((d) => d.total !== null && d.total > 0).map((d) => d.day);
    expect(series).toEqual(expect.arrayContaining(["2026-10-16", "2026-10-17", "2026-10-18", "2026-10-19"]));
    expect(read.series.removalRequests.find((d) => d.day === "2026-10-16")!.linksIssued).toBeGreaterThanOrEqual(1);
  });
});
