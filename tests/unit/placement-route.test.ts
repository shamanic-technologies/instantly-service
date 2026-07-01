import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockExecute(...a) },
}));
vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  sequenceCosts: {},
  instantlyPlacementTestsRaw: {},
  instantlyPlacementAnalyticsRaw: {},
  instantlyPlacementResults: {},
}));

const mockListAccounts = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listAccounts: (...a: unknown[]) => mockListAccounts(...a),
}));

const mockResolveKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformInstantlyApiKey: (...a: unknown[]) => mockResolveKey(...a),
}));

async function makeApp() {
  const router = (await import("../../src/routes/audit")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/audit", router);
  return app;
}

describe("placement audit routes", () => {
  const OLD = process.env.PLACEMENT_TESTS_ENABLED;
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolveKey.mockResolvedValue("test-key");
  });
  afterEach(() => {
    process.env.PLACEMENT_TESTS_ENABLED = OLD;
  });

  describe("GET /internal/audit/account-health/history", () => {
    it("400 when email query param is missing", async () => {
      const app = await makeApp();
      const res = await request(app).get("/internal/audit/account-health/history");
      expect(res.status).toBe(400);
      expect(res.body.error).toMatch(/email/);
    });

    it("returns blended per-test history (newest first) for the account", async () => {
      // Two tests for a@x.com: t2 newer (Gmail spam), t1 older (Gmail inbox).
      mockExecute.mockResolvedValue({
        rows: [
          { test_id: "t2", recipient_esp: 1, tested_at: "2026-07-01T00:00:00.000Z", seed_total: 4, inbox_count: 0, spam_count: 4, missing_count: 0 },
          { test_id: "t1", recipient_esp: 1, tested_at: "2026-06-25T00:00:00.000Z", seed_total: 4, inbox_count: 4, spam_count: 0, missing_count: 0 },
        ],
      });
      const app = await makeApp();
      const res = await request(app).get("/internal/audit/account-health/history?email=a@x.com");
      expect(res.status).toBe(200);
      expect(res.body.email).toBe("a@x.com");
      expect(res.body.history).toHaveLength(2);
      expect(res.body.history[0]).toMatchObject({ testId: "t2", spamPct: 100, inboxPct: 0 });
      expect(res.body.history[1]).toMatchObject({ testId: "t1", inboxPct: 100 });
    });
  });

  describe("POST /internal/audit/placement-test/sync", () => {
    it("202 accepted with a runId (background)", async () => {
      const app = await makeApp();
      const res = await request(app).post("/internal/audit/placement-test/sync");
      expect(res.status).toBe(202);
      expect(res.body.accepted).toBe(true);
      expect(typeof res.body.runId).toBe("string");
    });
  });

  describe("POST /internal/audit/placement-test/ensure", () => {
    it("409 when scheduling is disabled (default off)", async () => {
      delete process.env.PLACEMENT_TESTS_ENABLED;
      const app = await makeApp();
      const res = await request(app).post("/internal/audit/placement-test/ensure");
      expect(res.status).toBe(409);
      expect(res.body.error).toMatch(/PLACEMENT_TESTS_ENABLED/);
      expect(mockResolveKey).not.toHaveBeenCalled();
    });

    it("409 when PLACEMENT_TESTS_ENABLED is any value other than 'true'", async () => {
      process.env.PLACEMENT_TESTS_ENABLED = "1";
      const app = await makeApp();
      const res = await request(app).post("/internal/audit/placement-test/ensure");
      expect(res.status).toBe(409);
    });
  });
});
