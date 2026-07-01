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
    mockResolvePlatformKey.mockResolvedValue("test-key");
  });

  it("returns the locked shape with all fields present + typed", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "a@good.com", status: 1, stat_warmup_score: 100, daily_limit: 30 },
      { email: "b@good.com", status: 1, stat_warmup_score: 100, daily_limit: 20 },
      { email: "c@distribute.you", status: 1, stat_warmup_score: 100, daily_limit: 40 }, // blocked
    ]);
    // One never-contacted lead with 2 pending steps.
    mockExecute.mockResolvedValue({
      rows: [
        { provisionedSteps: [1, 2], lastSentStep: null, lastSentAt: null },
      ],
    });

    const app = await makeApp();
    const res = await request(app).get("/internal/audit/sending-forecast");

    expect(res.status).toBe(200);
    const b = res.body;
    expect(typeof b.asOf).toBe("string");
    expect(Number.isNaN(Date.parse(b.asOf))).toBe(false);
    expect(b.dailyCapacity).toBe(50); // 30 + 20 healthy only
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
});
