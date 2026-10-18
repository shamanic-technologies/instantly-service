/**
 * DB-backed proof of the CLICK-HUMANS-1018 rules, each pinned BOTH ways against
 * real SQL (migration 0084 applied): a known scanner hit is flagged, a real
 * browser hit is kept.
 *
 *  - `datacenter_network`: an AWS address is flagged; a residential address is
 *    kept; an Amazon employee clicking from Amazon's network is kept.
 *  - `click_burst`: three clicks within a second are flagged; a double-click is kept.
 *  - `many_user_agents`: three browsers in a day are flagged; desktop + phone kept.
 *  - Instantly-recorded clicks: a three-click burst is retracted from silver
 *    (archived), a double-click and a lone click stay.
 *
 * Builds its own pool. Skipped when no database is configured (CI runs without one):
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

const { loadNetworkEvidence } = await import("../../src/lib/self-send/click-promotion");
const { retractVendorBurstClicks } = await import("../../src/lib/vendor-click-burst");
const { syncDatacenterNetworks, DATACENTER_OPERATORS } = await import("../../src/lib/self-send/datacenter-networks");

const SUFFIX = crypto.randomUUID().slice(0, 8);
const CAMPAIGN = `self:rules-${SUFFIX}`;
const VENDOR_CAMPAIGN = `inst-rules-${SUFFIX}`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);
const T0 = new Date("2026-10-15T14:00:00.000Z").getTime();
const at = (seconds: number) => new Date(T0 + seconds * 1000).toISOString();

const MAC = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36";
const IPHONE = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Mobile/15E148 Safari/604.1";
const WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36";

async function hit(id: string, lead: string, seconds: number, opts: { ip?: string; ua?: string } = {}) {
  await q(
    `INSERT INTO tracking_hits_raw (id, kind, instantly_campaign_id, lead_email, step, method, user_agent, client_ip, payload, received_at)
     VALUES ($1, 'click', $2, $3, 1, 'GET', $4, $5, '{}'::jsonb, $6)`,
    [`${id}-${SUFFIX}`, CAMPAIGN, lead, opts.ua ?? MAC, opts.ip ?? null, at(seconds)],
  );
  return `${id}-${SUFFIX}`;
}

async function vendorClick(id: string, lead: string, seconds: number) {
  await q(
    `INSERT INTO instantly_events (id, event_type, campaign_id, lead_email, "timestamp", step, source)
     VALUES ($1, 'email_link_clicked', $2, $3, $4, 1, 'webhook')`,
    [`${id}-${SUFFIX}`, VENDOR_CAMPAIGN, lead, at(seconds)],
  );
  return `${id}-${SUFFIX}`;
}

describe.skipIf(SKIP)("click human/scanner rules (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    // One AWS prefix, as the list worker stores it.
    await q(
      `INSERT INTO datacenter_networks (asn, prefix, operator, operator_domains, fetched_at)
       VALUES (16509, '52.73.0.0/16', 'Amazon Web Services', ARRAY['amazon.com'], now())
       ON CONFLICT DO NOTHING`,
    );
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM tracking_hits_raw WHERE instantly_campaign_id = $1`, [CAMPAIGN]);
    await q(`DELETE FROM instantly_events WHERE campaign_id = $1`, [VENDOR_CAMPAIGN]);
    await q(`DELETE FROM instantly_events_retracted WHERE event_id LIKE $1`, [`%-${SUFFIX}`]);
    await pool.end();
  });

  it("datacenter_network: an AWS address is flagged, a residential one and Amazon's own staff are kept", async () => {
    const scanner = await hit("aws", "rmedina@limamemorial.org", 0, { ip: "52.73.134.153" });
    const mapped = await hit("aws-mapped", "jwoo@luriechildrens.org", 100, { ip: "::ffff:52.73.9.9" });
    const person = await hit("isp", "amatoclinic@ntelos.net", 200, { ip: "206.248.238.142" });
    const staff = await hit("amzn", "buyer@amazon.com", 300, { ip: "52.73.1.1" });
    const evidence = await loadNetworkEvidence([scanner, mapped, person, staff]);
    expect(evidence.get(scanner)?.fromDatacenterNetwork).toBe(true);
    expect(evidence.get(mapped)?.fromDatacenterNetwork).toBe(true);
    expect(evidence.get(person)?.fromDatacenterNetwork).toBe(false);
    expect(evidence.get(staff)?.fromDatacenterNetwork).toBe(false);
  });

  it("the list refresh stores IPv4 + IPv6 prefixes (host bits cleared) and KEEPS an operator whose read failed", async () => {
    const asns = DATACENTER_OPERATORS.flatMap((o) => o.asns);
    const fakeFetch = (async (url: string) => {
      const asn = Number(/AS(\d+)/.exec(url)![1]);
      if (asn === 16509) return new Response("{}", { status: 503 });
      return new Response(
        JSON.stringify({ data: { prefixes: [{ prefix: `10.${asns.indexOf(asn)}.0.1/16` }, { prefix: `2600:${asn % 9999}::/32` }] } }),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const summary = await syncDatacenterNetworks(fakeFetch);
    expect(summary.failed.map((f) => f.asn)).toEqual([16509]);
    expect(summary.asnsSynced).toBe(asns.length - 1);

    const kept = await q(`SELECT prefix::text FROM datacenter_networks WHERE asn = 16509`);
    expect(kept.rows.map((r: { prefix: string }) => r.prefix)).toEqual(["52.73.0.0/16"]);
    const azure = await q(`SELECT prefix::text FROM datacenter_networks WHERE asn = 8075 ORDER BY 1`);
    expect(azure.rows.map((r: { prefix: string }) => r.prefix)).toEqual([`10.${asns.indexOf(8075)}.0.0/16`, "2600:8075::/32"]);
  });

  it("click_burst: three clicks inside a second are flagged, a double-click is kept", async () => {
    const a = await hit("b1", "sweep@example.org", 1000, { ip: "1.2.3.4" });
    await hit("b2", "sweep@example.org", 1000.3, { ip: "1.2.3.4" });
    await hit("b3", "sweep@example.org", 1000.6, { ip: "1.2.3.4" });
    const d1 = await hit("d1", "double@example.org", 2000, { ip: "5.6.7.8" });
    await hit("d2", "double@example.org", 2000.4, { ip: "5.6.7.8" });
    const evidence = await loadNetworkEvidence([a, d1]);
    expect(evidence.get(a)?.inClickBurst).toBe(true);
    expect(evidence.get(d1)?.inClickBurst).toBe(false);
  });

  it("many_user_agents: three browsers in a day are flagged, desktop + phone are kept", async () => {
    const r1 = await hit("u1", "connolly@example.org", 5000, { ua: WIN });
    await hit("u2", "connolly@example.org", 8000, { ua: MAC });
    await hit("u3", "connolly@example.org", 12000, { ua: IPHONE });
    const p1 = await hit("p1", "reader@example.org", 5000, { ua: MAC });
    await hit("p2", "reader@example.org", 9000, { ua: IPHONE });
    const evidence = await loadNetworkEvidence([r1, p1]);
    expect(evidence.get(r1)?.clickedWithManyUserAgents).toBe(true);
    expect(evidence.get(p1)?.clickedWithManyUserAgents).toBe(false);
  });

  it("Instantly-recorded clicks: a three-click burst is retracted and archived, a double-click and a lone click stay", async () => {
    const burst = [
      await vendorClick("v1", "scanned@example.com", 0),
      await vendorClick("v2", "scanned@example.com", 0.2),
      await vendorClick("v3", "scanned@example.com", 0.5),
    ];
    const kept = [
      await vendorClick("k1", "double@example.com", 0),
      await vendorClick("k2", "double@example.com", 0.5),
      await vendorClick("k3", "lone@example.com", 0),
    ];

    const plan = await retractVendorBurstClicks({ dryRun: true });
    expect(plan).toMatchObject({ burstClicks: 3, leadsTouched: 1, leadsDemoted: 1 });

    await retractVendorBurstClicks({ dryRun: false });
    const left = await q(`SELECT id FROM instantly_events WHERE campaign_id = $1 ORDER BY id`, [VENDOR_CAMPAIGN]);
    expect(left.rows.map((r: { id: string }) => r.id).sort()).toEqual([...kept].sort());
    const archived = await q(
      `SELECT event_id, reason FROM instantly_events_retracted WHERE event_id = ANY($1::text[])`,
      [burst],
    );
    expect(archived.rows).toHaveLength(3);
    expect(archived.rows.every((r: { reason: string }) => r.reason === "scanner:click_burst")).toBe(true);

    // Idempotent: nothing left to retract.
    expect(await retractVendorBurstClicks({ dryRun: true })).toMatchObject({ burstClicks: 0 });
  });
});
