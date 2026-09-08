import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  sequenceCosts: {},
}));

const mockListAccounts = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listAccounts: (...args: unknown[]) => mockListAccounts(...args),
}));

class KeyServiceError extends Error {
  constructor(public readonly statusCode: number, message: string) {
    super(message);
    this.name = "KeyServiceError";
  }
}
const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformInstantlyApiKey: (...args: unknown[]) =>
    mockResolvePlatformKey(...args),
  KeyServiceError,
}));

// The alias map. Capacity is summed per real MAILBOX, so the route needs it —
// mocked at its own boundary rather than through key-service + the vendor's
// pagination, which is not what these cases are about.
const mockMailboxLogins = vi.fn(async () => new Map<string, string>());
vi.mock("../../src/lib/self-send/mailbox-credentials", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  loadMailboxLogins: () => mockMailboxLogins(),
}));

import { clearStatsCache } from "../../src/lib/stats-cache";

async function makeApp() {
  const router = (await import("../../src/routes/audit")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/audit", router);
  return app;
}

describe("GET /internal/audit/sending-forecast", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // The route wraps its work in the 60s stats cache under a fixed platform
    // key; without clearing, the first test's payload would be served to every
    // later test (cross-test cache hit skips the mocked DB → wrong assertions).
    clearStatsCache();
    mockResolvePlatformKey.mockResolvedValue("test-key");
    mockMailboxLogins.mockResolvedValue(new Map());
    mockExecute.mockResolvedValue({ rows: [] });
  });

  it("returns the locked shape with all fields present + typed", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "a@good.com", status: 1, stat_warmup_score: 100, daily_limit: 30 },
      { email: "b@good.com", status: 1, stat_warmup_score: 100, daily_limit: 20 },
      { email: "c@distribute.you", status: 1, stat_warmup_score: 100, daily_limit: 40 },
    ]);
    // Route reads lifecycle + the ramp's volume (Promise.all) then pending leads.
    // Seed in that order: a + b are in_production, c is deactivated_by_user.
    mockExecute
      .mockResolvedValueOnce({
        rows: [
          { email: "a@good.com", status: "in_production", reason: "passed", updatedAt: "2026-07-05T00:00:00.000Z" },
          { email: "b@good.com", status: "in_production", reason: "passed", updatedAt: "2026-07-05T00:00:00.000Z" },
          { email: "c@distribute.you", status: "deactivated_by_user", reason: "brand_domain", updatedAt: "2026-07-05T00:00:00.000Z" },
        ],
      })
      // Measured volume: `a` sustains 20/day so it may attempt 30, bounded by its
      // stated 30; `b` sustains 4 so it may attempt 6, well under its stated 20.
      .mockResolvedValueOnce({
        rows: [
          { accountEmail: "a@good.com", day: "2026-09-01", n: 20 },
          { accountEmail: "a@good.com", day: "2026-09-02", n: 20 },
          { accountEmail: "b@good.com", day: "2026-09-01", n: 4 },
          { accountEmail: "b@good.com", day: "2026-09-02", n: 4 },
        ],
      })
      // One never-contacted lead with 2 pending steps.
      .mockResolvedValueOnce({
        rows: [{ provisionedSteps: [1, 2], lastSentStep: null, lastSentAt: null }],
      });

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/sending-forecast");

    expect(res.status).toBe(200);
    const b = res.body;
    expect(typeof b.asOf).toBe("string");
    expect(Number.isNaN(Date.parse(b.asOf))).toBe(false);
    // ⚠️ The RAMPED capacity, not the sum of the stated limits. `a` is at volume
    // so its own 30 binds; `b` has been sending 4/day so it carries 6, not 20.
    // Reporting 50 here would be the theoretical ceiling — what the fleet could
    // send if every mailbox were already at full volume, which it is not.
    expect(b.dailyCapacity).toBe(36);
    expect(b.healthyAccountCount).toBe(2);
    expect(b.totalAccountCount).toBe(3);
    expect(b.blockedDomainCount).toBe(1);
    expect(Array.isArray(b.days)).toBe(true);
    for (const d of b.days) {
      expect(typeof d.date).toBe("string");
      expect(typeof d.scheduledCount).toBe("number");
    }
    // 2 steps projected → total scheduled == 2 (bounded, none dropped).
    const total = b.days.reduce((s: number, d: any) => s + d.scheduledCount, 0);
    expect(total).toBe(2);
  });

  it("days is [] when nothing is scheduled (still present, non-null)", async () => {
    mockListAccounts.mockResolvedValue([]);
    mockExecute.mockResolvedValue({ rows: [] });

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/sending-forecast");

    expect(res.status).toBe(200);
    expect(res.body.days).toEqual([]);
    expect(res.body.dailyCapacity).toBe(0);
    expect(res.body.totalAccountCount).toBe(0);
  });

  it("fails loud (500) when key-service has no platform key (404) — no silent zero", async () => {
    mockResolvePlatformKey.mockRejectedValue(
      new KeyServiceError(404, "Platform key not configured"),
    );
    mockListAccounts.mockResolvedValue([]);
    mockExecute.mockResolvedValue({ rows: [] });

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/sending-forecast");

    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/Platform key not configured/);
    expect(mockListAccounts).not.toHaveBeenCalled();
  });

  it("fails loud (500) when the account source throws — no fabricated capacity", async () => {
    mockListAccounts.mockRejectedValue(new Error("instantly boom"));
    mockExecute.mockResolvedValue({ rows: [] });

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/sending-forecast");

    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/instantly boom/);
  });

  it("dedups concurrent requests to ONE loader run (collapses the retry storm)", async () => {
    // email-gateway retries the SAME request on its ~10s AbortSignal timeout and
    // the abort does not cancel the server query, so N identical heavy
    // aggregations used to pile on the pool. The in-flight cache must collapse a
    // burst to a single loader run (one listAccounts + one DB round).
    mockListAccounts.mockResolvedValue([
      { email: "a@good.com", status: 1, stat_warmup_score: 100, daily_limit: 30 },
    ]);
    mockExecute.mockResolvedValue({
      rows: [{ email: "a@good.com", status: "in_production", reason: "passed", updatedAt: null }],
    });

    const app = await makeApp();
    const [r1, r2, r3] = await Promise.all([
      request(app).get("/internal/audit/sending-forecast"),
      request(app).get("/internal/audit/sending-forecast"),
      request(app).get("/internal/audit/sending-forecast"),
    ]);

    expect(r1.status).toBe(200);
    expect(r2.status).toBe(200);
    expect(r3.status).toBe(200);
    // All three served by ONE loader execution, not three.
    expect(mockListAccounts).toHaveBeenCalledTimes(1);
    // Same cached payload for every caller.
    expect(r2.body).toEqual(r1.body);
    expect(r3.body).toEqual(r1.body);
  });

  it("does NOT cache a failed loader (a later request re-runs — no poisoned cache)", async () => {
    mockListAccounts.mockRejectedValueOnce(new Error("transient")).mockResolvedValueOnce([]);
    mockExecute.mockResolvedValue({ rows: [] });

    const app = await makeApp();
    const first = await request(app).get("/internal/audit/sending-forecast");
    expect(first.status).toBe(500);

    // The rejection cached nothing → the retry runs the loader again and succeeds.
    const second = await request(app).get("/internal/audit/sending-forecast");
    expect(second.status).toBe(200);
    expect(mockListAccounts).toHaveBeenCalledTimes(2);
  });
});
