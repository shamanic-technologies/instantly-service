import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: vi.fn() }) }),
    update: () => ({ set: () => ({ where: () => Promise.resolve([{}]) }) }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  sequenceCosts: {},
}));

const mockReconcileAll = vi.fn();
vi.mock("../../src/lib/reconcile", () => ({
  reconcileAll: (...args: unknown[]) => mockReconcileAll(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: vi.fn(),
  updateCampaignStatus: vi.fn(),
}));

vi.mock("../../src/lib/campaign-error-handler", () => ({
  handleCampaignError: vi.fn(),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: vi.fn(),
}));

vi.mock("../../src/lib/trace-event", () => ({
  traceEvent: vi.fn().mockResolvedValue(undefined),
}));

async function makeApp() {
  const router = (await import("../../src/routes/campaigns")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal/campaigns", router);
  return app;
}

describe("POST /internal/campaigns/reconcile (async dispatch)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns 202 within 100ms even when reconcileAll takes longer", async () => {
    let resolveJob: () => void;
    mockReconcileAll.mockImplementation(
      () => new Promise<void>((r) => { resolveJob = r; }),
    );

    const app = await makeApp();
    const t0 = Date.now();
    const res = await request(app).post("/internal/campaigns/reconcile");
    const elapsed = Date.now() - t0;

    expect(res.status).toBe(202);
    expect(elapsed).toBeLessThan(100);

    resolveJob!();
  });

  it("response body has runId (uuid) and startedAt (ISO)", async () => {
    mockReconcileAll.mockResolvedValue({});

    const app = await makeApp();
    const res = await request(app).post("/internal/campaigns/reconcile");

    expect(res.status).toBe(202);
    expect(res.body.runId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
    expect(new Date(res.body.startedAt).toString()).not.toBe("Invalid Date");
  });

  it("invokes reconcileAll exactly once per request", async () => {
    mockReconcileAll.mockResolvedValue({});

    const app = await makeApp();
    await request(app).post("/internal/campaigns/reconcile");

    // Wait microtask so the .catch chain settles.
    await new Promise((r) => setImmediate(r));
    expect(mockReconcileAll).toHaveBeenCalledTimes(1);
  });

  it("background error is logged but does not crash the process", async () => {
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mockReconcileAll.mockRejectedValue(new Error("boom"));

    const app = await makeApp();
    const res = await request(app).post("/internal/campaigns/reconcile");
    expect(res.status).toBe(202);

    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    const matched = errSpy.mock.calls.some((args) =>
      String(args[0] ?? "").includes("reconcile run="),
    );
    expect(matched).toBe(true);
    errSpy.mockRestore();
  });
});
