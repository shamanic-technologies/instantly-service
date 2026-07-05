import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

vi.mock("../../src/db", () => ({
  db: { execute: vi.fn() },
}));
vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  sequenceCosts: {},
  instantlyAccounts: {},
}));

const mockListAccounts = vi.fn();
const mockSetWarmupDailyLimit = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listAccounts: (...args: unknown[]) => mockListAccounts(...args),
  setWarmupDailyLimit: (...args: unknown[]) => mockSetWarmupDailyLimit(...args),
}));

const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformInstantlyApiKey: (...args: unknown[]) =>
    mockResolvePlatformKey(...args),
}));

const mockFetchManuallyBlacklisted = vi.fn(async () => new Set<string>());
const mockSetAccountManualBlacklist = vi.fn(async () => undefined);
vi.mock("../../src/lib/account-blacklist", () => ({
  BLACKLIST_WARMUP_DAILY_LIMIT: 50,
  ALLOWED_WARMUP_DAILY_LIMIT: 10,
  fetchManuallyBlacklistedEmails: () => mockFetchManuallyBlacklisted(),
  setAccountManualBlacklist: (...args: unknown[]) =>
    mockSetAccountManualBlacklist(...args),
}));

async function makeApp() {
  const router = (await import("../../src/routes/audit")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/audit", router);
  return app;
}

describe("POST /internal/audit/account-blacklist", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolvePlatformKey.mockResolvedValue("test-key");
    mockSetWarmupDailyLimit.mockResolvedValue({});
  });

  it("blacklisted:true → warmup 50, persists flag, returns the locked contract", async () => {
    const app = await makeApp();
    const res = await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "rest@x.com", blacklisted: true });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      email: "rest@x.com",
      manuallyBlacklisted: true,
      warmupDailyLimit: 50,
    });
    expect(mockSetWarmupDailyLimit).toHaveBeenCalledWith("test-key", "rest@x.com", 50);
    expect(mockSetAccountManualBlacklist).toHaveBeenCalledWith("rest@x.com", true);
  });

  it("blacklisted:false → warmup 10, clears flag, returns the locked contract", async () => {
    const app = await makeApp();
    const res = await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "back@x.com", blacklisted: false });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      email: "back@x.com",
      manuallyBlacklisted: false,
      warmupDailyLimit: 10,
    });
    expect(mockSetWarmupDailyLimit).toHaveBeenCalledWith("test-key", "back@x.com", 10);
    expect(mockSetAccountManualBlacklist).toHaveBeenCalledWith("back@x.com", false);
  });

  it("PATCHes Instantly warmup FIRST, then persists (ordering invariant)", async () => {
    const order: string[] = [];
    mockSetWarmupDailyLimit.mockImplementation(async () => {
      order.push("warmup");
    });
    mockSetAccountManualBlacklist.mockImplementation(async () => {
      order.push("persist");
    });

    const app = await makeApp();
    await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "e@x.com", blacklisted: true });

    expect(order).toEqual(["warmup", "persist"]);
  });

  it("fails loud (500) and does NOT persist the flag when the Instantly PATCH fails", async () => {
    mockSetWarmupDailyLimit.mockRejectedValue(new Error("instantly warmup 400"));

    const app = await makeApp();
    const res = await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "e@x.com", blacklisted: true });

    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/instantly warmup 400/);
    expect(mockSetAccountManualBlacklist).not.toHaveBeenCalled();
  });

  it("fails loud (500) when the platform key cannot be resolved — no Instantly PATCH", async () => {
    mockResolvePlatformKey.mockRejectedValue(new Error("key-service 404"));

    const app = await makeApp();
    const res = await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "e@x.com", blacklisted: true });

    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/key-service/);
    expect(mockSetWarmupDailyLimit).not.toHaveBeenCalled();
    expect(mockSetAccountManualBlacklist).not.toHaveBeenCalled();
  });

  it("400 on an invalid body (missing blacklisted / bad type)", async () => {
    const app = await makeApp();

    const r1 = await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "e@x.com" });
    expect(r1.status).toBe(400);

    const r2 = await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "", blacklisted: true });
    expect(r2.status).toBe(400);

    const r3 = await request(app)
      .post("/internal/audit/account-blacklist")
      .send({ email: "e@x.com", blacklisted: "yes" });
    expect(r3.status).toBe(400);

    expect(mockSetWarmupDailyLimit).not.toHaveBeenCalled();
  });
});
