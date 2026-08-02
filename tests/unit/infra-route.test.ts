import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockSync = vi.fn();
vi.mock("../../src/lib/infra-sync", () => ({
  syncProviderInfra: (...args: unknown[]) => mockSync(...args),
}));

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
