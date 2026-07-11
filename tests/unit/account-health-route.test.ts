import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn(async () => ({ rows: [] }));
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockExecute(...a) },
}));
vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  sequenceCosts: {},
}));

const mockListAccounts = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listAccounts: (...args: unknown[]) => mockListAccounts(...args),
}));

const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformInstantlyApiKey: (...args: unknown[]) =>
    mockResolvePlatformKey(...args),
}));

async function makeApp() {
  const router = (await import("../../src/routes/audit")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/audit", router);
  return app;
}

describe("GET /internal/audit/account-health", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolvePlatformKey.mockResolvedValue("test-key");
    mockExecute.mockResolvedValue({ rows: [] });
  });

  it("returns the locked shape — all scalar fields present + typed, inboxPlacement null", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "a@good.com", status: 1, stat_warmup_score: 100, daily_limit: 30 },
      { email: "b@distribute.you", status: 1, stat_warmup_score: 100, daily_limit: 40 },
      { email: "c@good.com", status: 0, stat_warmup_score: 100, daily_limit: 20 },
    ]);
    // Route reads placement, sentToday, sentYesterday, queueSize, lifecycle via
    // db.execute (in that Promise.all order). Seed the first four empty,
    // lifecycle last.
    mockExecute
      .mockResolvedValueOnce({ rows: [] }) // placement
      .mockResolvedValueOnce({ rows: [] }) // sentToday
      .mockResolvedValueOnce({ rows: [] }) // sentYesterday
      .mockResolvedValueOnce({ rows: [] }) // queueSize
      .mockResolvedValueOnce({
        rows: [
          { email: "a@good.com", status: "in_production", reason: "passed", updatedAt: "2026-07-05T00:00:00.000Z" },
          { email: "b@distribute.you", status: "deactivated_by_user", reason: "brand_domain", updatedAt: "2026-07-05T00:00:00.000Z" },
          { email: "c@good.com", status: "deactivated_by_instantly", reason: "deactivated_by_instantly", updatedAt: "2026-07-05T00:00:00.000Z" },
        ],
      });

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/account-health");

    expect(res.status).toBe(200);
    const b = res.body;
    expect(typeof b.asOf).toBe("string");
    expect(Number.isNaN(Date.parse(b.asOf))).toBe(false);
    expect(Array.isArray(b.accounts)).toBe(true);
    expect(b.accounts).toHaveLength(3);

    for (const a of b.accounts) {
      expect(typeof a.email).toBe("string");
      expect(a.domain === null || typeof a.domain === "string").toBe(true);
      expect(typeof a.status).toBe("string");
      expect(a.warmupScore === null || typeof a.warmupScore === "number").toBe(true);
      expect(a.dailyLimit === null || typeof a.dailyLimit === "number").toBe(true);
      expect(a.warmupLimit === null || typeof a.warmupLimit === "number").toBe(true);
      expect(typeof a.blocked).toBe("boolean");
      expect(a.blockReason === null || typeof a.blockReason === "string").toBe(true);
      expect(a.inboxPlacement).toBeNull();
      // New per-account throughput fields — present + typed, honest defaults.
      expect(typeof a.sentToday).toBe("number");
      expect(typeof a.sentYesterday).toBe("number");
      expect(typeof a.queueSize).toBe("number");
      expect(a.accountType === null || typeof a.accountType === "string").toBe(true);
      expect(a.sentToday).toBe(0);
      expect(a.sentYesterday).toBe(0);
      expect(a.queueSize).toBe(0);
    }

    const byEmail = Object.fromEntries(b.accounts.map((a: any) => [a.email, a]));
    expect(byEmail["a@good.com"]).toMatchObject({
      domain: "good.com",
      status: "active",
      blocked: false,
      blockReason: null,
      lifecycleStatus: "in_production",
      lifecycleReason: "passed",
    });
    expect(byEmail["b@distribute.you"]).toMatchObject({
      blocked: true,
      blockReason: "deactivated_by_user",
      lifecycleStatus: "deactivated_by_user",
    });
    expect(byEmail["c@good.com"]).toMatchObject({
      status: "inactive",
      blocked: true,
      blockReason: "deactivated_by_instantly",
      lifecycleStatus: "deactivated_by_instantly",
    });
  });

  it("accounts is [] (still present) when the workspace has no accounts", async () => {
    mockListAccounts.mockResolvedValue([]);
    const app = await makeApp();
    const res = await request(app).get("/internal/audit/account-health");
    expect(res.status).toBe(200);
    expect(res.body.accounts).toEqual([]);
    expect(typeof res.body.asOf).toBe("string");
  });

  it("fails loud (500) when the platform key cannot be resolved — no fabricated list", async () => {
    mockResolvePlatformKey.mockRejectedValue(
      new Error("key-service GET /keys/platform/instantly/decrypt failed: 404"),
    );
    mockListAccounts.mockResolvedValue([]);

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/account-health");

    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/key-service/);
    expect(mockListAccounts).not.toHaveBeenCalled();
  });

  it("fails loud (500) when the account source throws — no silent fallback", async () => {
    mockListAccounts.mockRejectedValue(new Error("instantly boom"));

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/account-health");

    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/instantly boom/);
  });
});
