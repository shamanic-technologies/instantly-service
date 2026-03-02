import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

process.env.RUNS_SERVICE_URL = "http://localhost:3006";
process.env.RUNS_SERVICE_API_KEY = "test-runs-key";

describe("runs-client", () => {
  beforeEach(() => {
    mockFetch.mockClear();
  });

  it("should export createRun function", async () => {
    const { createRun } = await import("../../src/lib/runs-client");
    expect(typeof createRun).toBe("function");
  });

  it("should export updateRun function", async () => {
    const { updateRun } = await import("../../src/lib/runs-client");
    expect(typeof updateRun).toBe("function");
  });

  it("should export addCosts function", async () => {
    const { addCosts } = await import("../../src/lib/runs-client");
    expect(typeof addCosts).toBe("function");
  });

  it("createRun should hardcode appId as instantly-service", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ id: "run-1", status: "running" }),
    });

    const { createRun } = await import("../../src/lib/runs-client");
    await createRun({
      orgId: "org-1",
      serviceName: "instantly-service",
      taskName: "test-task",
    });

    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.appId).toBe("instantly-service");
    expect(body.orgId).toBe("org-1");
    expect(body.serviceName).toBe("instantly-service");
  });

  it("addCosts should include costSource on each item", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ costs: [{ id: "cost-1" }] }),
    });

    const { addCosts } = await import("../../src/lib/runs-client");
    await addCosts("run-1", [
      { costName: "instantly-email-send", quantity: 1, costSource: "platform", status: "actual" },
    ]);

    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain("/v1/runs/run-1/costs");
    const body = JSON.parse(options.body);
    expect(body.items[0].costSource).toBe("platform");
    expect(body.items[0].costName).toBe("instantly-email-send");
  });
});
