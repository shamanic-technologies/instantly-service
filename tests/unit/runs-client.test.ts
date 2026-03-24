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

  it("createRun should send identity headers and body without orgId/userId/parentRunId", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ id: "run-1", status: "running" }),
    });

    const { createRun } = await import("../../src/lib/runs-client");
    await createRun(
      { serviceName: "instantly-service", taskName: "test-task" },
      { orgId: "org-1", userId: "user-1", runId: "parent-run-1" },
    );

    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.serviceName).toBe("instantly-service");
    expect(body.orgId).toBeUndefined();
    expect(body.userId).toBeUndefined();
    expect(body.parentRunId).toBeUndefined();
    expect(options.headers["x-org-id"]).toBe("org-1");
    expect(options.headers["x-user-id"]).toBe("user-1");
    expect(options.headers["x-run-id"]).toBe("parent-run-1");
  });

  it("addCosts should include costSource on each item and forward identity headers", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ costs: [{ id: "cost-1" }] }),
    });

    const { addCosts } = await import("../../src/lib/runs-client");
    await addCosts("run-1", [
      { costName: "instantly-email-send", quantity: 1, costSource: "platform", status: "actual" },
    ], { orgId: "org-1", userId: "user-1", runId: "run-1" });

    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain("/v1/runs/run-1/costs");
    const body = JSON.parse(options.body);
    expect(body.items[0].costSource).toBe("platform");
    expect(body.items[0].costName).toBe("instantly-email-send");
    expect(options.headers["x-org-id"]).toBe("org-1");
    expect(options.headers["x-user-id"]).toBe("user-1");
    expect(options.headers["x-run-id"]).toBe("run-1");
  });

  it("should forward tracking headers when present in identity context", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ id: "run-1", status: "running" }),
    });

    const { createRun } = await import("../../src/lib/runs-client");
    await createRun(
      { serviceName: "instantly-service", taskName: "test-task" },
      {
        orgId: "org-1",
        userId: "user-1",
        runId: "parent-run-1",
        tracking: {
          campaignId: "camp-1",
          brandId: "brand-1",
          workflowName: "wf-1",
          featureSlug: "cold-outreach",
        },
      },
    );

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers["x-campaign-id"]).toBe("camp-1");
    expect(options.headers["x-brand-id"]).toBe("brand-1");
    expect(options.headers["x-workflow-name"]).toBe("wf-1");
    expect(options.headers["x-feature-slug"]).toBe("cold-outreach");
  });

  it("should not include tracking headers when not present in identity context", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ id: "run-1", status: "running" }),
    });

    const { createRun } = await import("../../src/lib/runs-client");
    await createRun(
      { serviceName: "instantly-service", taskName: "test-task" },
      { orgId: "org-1", userId: "user-1" },
    );

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers["x-campaign-id"]).toBeUndefined();
    expect(options.headers["x-brand-id"]).toBeUndefined();
    expect(options.headers["x-workflow-name"]).toBeUndefined();
    expect(options.headers["x-feature-slug"]).toBeUndefined();
  });
});
