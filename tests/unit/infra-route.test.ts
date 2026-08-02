import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockSync = vi.fn();
vi.mock("../../src/lib/infra-sync", () => ({
  syncProviderInfra: (...args: unknown[]) => mockSync(...args),
}));

const mockLoadDomains = vi.fn();
const mockLoadRates = vi.fn();
vi.mock("../../src/lib/infra-gold", () => ({
  loadInventoryDomains: (...args: unknown[]) => mockLoadDomains(...args),
  loadEffectiveRates: (...args: unknown[]) => mockLoadRates(...args),
}));

import { clearStatsCache } from "../../src/lib/stats-cache";

function inventoryDomain(overrides: Record<string, unknown> = {}) {
  return {
    provider: "instantly-dfy",
    domain: "resilientnirvana.com",
    role: "prewarm",
    status: "active",
    expiresAt: null,
    autorenew: null,
    deletionScheduled: false,
    cancelledAt: null,
    absentSince: null,
    priceCents: null,
    priceCurrency: null,
    mailboxCount: 5,
    instantlyAccountCount: 5,
    inProductionCount: 4,
    sentLast30d: 100,
    ...overrides,
  };
}

const DFY_RATE_ROWS = [
  { provider: "instantly-dfy", scope: "domain-year", item: "", unitCents: 1500, currency: "USD", source: "rate-card", note: null },
  { provider: "instantly-dfy", scope: "mailbox-month", item: "", unitCents: 1000, currency: "USD", source: "rate-card", note: null },
  { provider: "instantly", scope: "plan-month", item: "hypergrowth", unitCents: 9700, currency: "USD", source: "rate-card", note: null },
];

async function makeApp() {
  const router = (await import("../../src/routes/infra")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/infra", router);
  return app;
}

describe("POST /internal/infra/sync", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clearStatsCache();
    vi.spyOn(console, "log").mockImplementation(() => {});
  });

  it("returns 202 immediately rather than holding the request for the vendor sweep", async () => {
    let resolveJob: () => void;
    mockSync.mockImplementation(() => new Promise<void>((r) => { resolveJob = r; }));

    const app = await makeApp();
    const t0 = Date.now();
    const res = await request(app).post("/internal/infra/sync");
    const elapsed = Date.now() - t0;

    expect(res.status).toBe(202);
    expect(res.body.accepted).toBe(true);
    expect(elapsed).toBeLessThan(100);

    resolveJob!();
  });

  it("returns a uuid runId so the background run is traceable in the logs", async () => {
    mockSync.mockResolvedValue({ providersSucceeded: 6 });

    const app = await makeApp();
    const res = await request(app).post("/internal/infra/sync");

    expect(res.body.runId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
  });

  it("passes the caller context key-service requires for a platform decrypt", async () => {
    mockSync.mockResolvedValue({ providersSucceeded: 6 });

    const app = await makeApp();
    await request(app).post("/internal/infra/sync");
    await new Promise((r) => setImmediate(r));

    expect(mockSync).toHaveBeenCalledWith({
      method: "POST",
      path: "/internal/infra/sync",
    });
  });

  it("logs a background failure loudly instead of crashing the process", async () => {
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mockSync.mockRejectedValue(new Error("every provider failed"));

    const app = await makeApp();
    const res = await request(app).post("/internal/infra/sync");
    expect(res.status).toBe(202);

    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    const matched = errSpy.mock.calls.some((args) =>
      String(args[0] ?? "").includes("infra-sync run="),
    );
    expect(matched).toBe(true);
    errSpy.mockRestore();
  });
});

describe("infra gold reads", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Without this, a cached payload from a previous test is served without
    // touching the loaders and every assertion below reads stale data.
    clearStatsCache();
    vi.spyOn(console, "error").mockImplementation(() => {});
    mockLoadRates.mockResolvedValue(DFY_RATE_ROWS);
  });

  it("GET /domains reports the cost, its provenance, and the cost per email", async () => {
    mockLoadDomains.mockResolvedValue([inventoryDomain()]);

    const app = await makeApp();
    const res = await request(app).get("/internal/infra/domains");

    expect(res.status).toBe(200);
    const row = res.body.domains[0];
    // $15/yr → 125¢/mo, plus 5 mailboxes × $10/mo.
    expect(row.monthlyCostCents).toBe(5125);
    expect(row.costSource).toBe("rate-card");
    expect(row.costPerEmailCents).toBe(51.25);
  });

  it("GET /domains shows vendor mailboxes and Instantly accounts side by side", async () => {
    mockLoadDomains.mockResolvedValue([
      inventoryDomain({ provider: "gandi", mailboxCount: 1, instantlyAccountCount: 27 }),
    ]);

    const app = await makeApp();
    const res = await request(app).get("/internal/infra/domains");

    const row = res.body.domains[0];
    expect(row.vendorMailboxes).toBe(1);
    expect(row.instantlyAccounts).toBe(27);
    // Gandi has no rate row and reports no per-domain price here.
    expect(row.monthlyCostCents).toBeNull();
    expect(row.costSource).toBeNull();
  });

  it("GET /waste flags an idle paid domain and stays report-only", async () => {
    mockLoadDomains.mockResolvedValue([
      inventoryDomain({ domain: "outcaged.com", instantlyAccountCount: 0, sentLast30d: 0 }),
    ]);

    const app = await makeApp();
    const res = await request(app).get("/internal/infra/waste");

    expect(res.status).toBe(200);
    expect(res.body.findingCount).toBe(1);
    expect(res.body.findings[0].reason).toBe("paid_no_sending_accounts");
    // Nothing in the payload proposes or performs an action.
    expect(JSON.stringify(res.body)).not.toMatch(/autorenew_off|delete|cancel_now|action/i);
  });

  it("GET /spend keeps currencies apart and names what it cannot price", async () => {
    mockLoadDomains.mockResolvedValue([
      inventoryDomain(),
      inventoryDomain({ domain: "agileconsultco.com", provider: "primeforge", sentLast30d: 40 }),
      inventoryDomain({
        domain: "growthagency.dev",
        provider: "gandi",
        priceCents: 3838,
        priceCurrency: "EUR",
        sentLast30d: 60,
      }),
    ]);

    const app = await makeApp();
    const res = await request(app).get("/internal/infra/spend");

    expect(res.status).toBe(200);
    expect(res.body.monthlyByCurrency.map((c: { currency: string }) => c.currency).sort()).toEqual([
      "EUR",
      "USD",
    ]);
    expect(res.body.unpricedProviders).toEqual(["primeforge"]);
    expect(res.body.planSubscriptions[0].item).toBe("hypergrowth");
  });

  it("GET /spend counts sends per domain, not per inventory row", async () => {
    // The same domain reported by its registrar AND its mail host.
    mockLoadDomains.mockResolvedValue([
      inventoryDomain({ domain: "dual.com", provider: "gandi", sentLast30d: 100 }),
      inventoryDomain({ domain: "dual.com", provider: "mailforge", sentLast30d: 100 }),
    ]);

    const app = await makeApp();
    const res = await request(app).get("/internal/infra/spend");

    expect(res.body.sentLast30d).toBe(100);
  });

  it("fails loud when the inventory cannot be read — no empty-but-successful page", async () => {
    mockLoadDomains.mockRejectedValue(new Error("neon down"));

    const app = await makeApp();
    const res = await request(app).get("/internal/infra/domains");

    expect(res.status).toBe(500);
  });

  it("does not cache a failed load — a later request retries", async () => {
    mockLoadDomains.mockRejectedValueOnce(new Error("transient"));
    mockLoadDomains.mockResolvedValue([inventoryDomain()]);

    const app = await makeApp();
    expect((await request(app).get("/internal/infra/spend")).status).toBe(500);
    expect((await request(app).get("/internal/infra/spend")).status).toBe(200);
  });
});
