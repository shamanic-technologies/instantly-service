import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

vi.mock("../../src/db", () => ({
  db: { execute: vi.fn().mockResolvedValue({ rows: [] }) },
}));
vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  sequenceCosts: {},
}));

const mockGetAccountRaw = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listAccounts: vi.fn(),
  getAccountRaw: (...args: unknown[]) => mockGetAccountRaw(...args),
}));

const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformInstantlyApiKey: (...args: unknown[]) =>
    mockResolvePlatformKey(...args),
  KeyServiceError: class extends Error {},
}));

async function makeApp() {
  const router = (await import("../../src/routes/audit")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/audit", router);
  return app;
}

describe("GET /internal/audit/account-detail", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolvePlatformKey.mockResolvedValue("test-key");
  });

  it("returns the FULL raw Instantly account object under {account}", async () => {
    const raw = {
      email: "amy@dfy.com",
      status: 1,
      enable_slow_ramp: false,
      daily_limit: 50,
      provider_code: 1,
      warmup: { limit: 10, increment: "1", advanced: { weekday_only: true } },
      timestamp_created: "2026-05-01T00:00:00.000Z",
    };
    mockGetAccountRaw.mockResolvedValue(raw);

    const app = await makeApp();
    const res = await request(app)
      .get("/internal/audit/account-detail")
      .query({ email: "amy@dfy.com" });

    expect(res.status).toBe(200);
    expect(res.body.account).toEqual(raw);
    expect(mockGetAccountRaw).toHaveBeenCalledWith("test-key", "amy@dfy.com");
  });

  it("400 when email query param is missing", async () => {
    const app = await makeApp();
    const res = await request(app).get("/internal/audit/account-detail");

    expect(res.status).toBe(400);
    expect(mockGetAccountRaw).not.toHaveBeenCalled();
  });

  it("fails loud (500) when Instantly throws — no fabricated object", async () => {
    mockGetAccountRaw.mockRejectedValue(new Error("instantly 404 account not found"));

    const app = await makeApp();
    const res = await request(app)
      .get("/internal/audit/account-detail")
      .query({ email: "gone@dfy.com" });

    expect(res.status).toBe(500);
    expect(res.body.account).toBeUndefined();
  });
});
