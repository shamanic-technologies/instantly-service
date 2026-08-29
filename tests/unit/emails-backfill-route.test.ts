import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

vi.mock("../../src/db", () => ({ db: { execute: vi.fn(), select: vi.fn() } }));
vi.mock("../../src/db/schema", () => ({ instantlyCampaigns: {}, sequenceCosts: {} }));

const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformInstantlyApiKey: (...a: unknown[]) => mockResolvePlatformKey(...a),
  KeyServiceError: class extends Error {},
}));

const mockBackfillEmails = vi.fn();
vi.mock("../../src/lib/emails-backfill", () => ({
  backfillEmails: (...a: unknown[]) => mockBackfillEmails(...a),
}));

async function makeApp() {
  const router = (await import("../../src/routes/audit")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/audit", router);
  return app;
}

/** Let the route's fire-and-forget background work settle. */
const settle = () => new Promise((r) => setImmediate(r));

describe("POST /internal/audit/emails-backfill", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolvePlatformKey.mockResolvedValue("platform-key");
    mockBackfillEmails.mockResolvedValue({
      pages: 1,
      emailsRead: 0,
      emailsStored: 0,
      inboundRead: 0,
      campaignlessRead: 0,
      exhausted: true,
    });
  });

  it("answers 202 with a run id and sweeps in the background", async () => {
    const app = await makeApp();
    const res = await request(app).post("/internal/audit/emails-backfill").send({});

    expect(res.status).toBe(202);
    expect(res.body.accepted).toBe(true);
    expect(typeof res.body.runId).toBe("string");

    await settle();
    expect(mockBackfillEmails).toHaveBeenCalledWith("platform-key", { maxPages: undefined });
  });

  it("passes a positive maxPages through to bound the walk", async () => {
    const app = await makeApp();
    await request(app).post("/internal/audit/emails-backfill").send({ maxPages: 5 });

    await settle();
    expect(mockBackfillEmails).toHaveBeenCalledWith("platform-key", { maxPages: 5 });
  });

  it("ignores a non-positive maxPages rather than sweeping zero pages", async () => {
    const app = await makeApp();
    await request(app).post("/internal/audit/emails-backfill").send({ maxPages: 0 });

    await settle();
    expect(mockBackfillEmails).toHaveBeenCalledWith("platform-key", { maxPages: undefined });
  });

  it("still answers 202 when the sweep fails — the failure is logged, not swallowed silently", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mockBackfillEmails.mockRejectedValue(new Error("Instantly 429"));

    const app = await makeApp();
    const res = await request(app).post("/internal/audit/emails-backfill").send({});
    expect(res.status).toBe(202);

    await settle();
    await settle();
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("Instantly 429"));
    errorSpy.mockRestore();
  });
});
