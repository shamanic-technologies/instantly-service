import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { authorizeCreditSpend, COST_ESTIMATES } from "../../src/lib/billing-client";

describe("billing-client", () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockReset();
    vi.stubGlobal("fetch", mockFetch);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  const identity = {
    orgId: "org-1",
    userId: "user-1",
    runId: "run-1",
    campaignId: "camp-1",
    brandId: "brand-1",
    workflowName: "wf-1",
  };

  it("should return sufficient: true when balance is enough", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: true, balance_cents: 500 }),
    });

    const result = await authorizeCreditSpend(10, "test-cost", identity);

    expect(result).toEqual({ sufficient: true, balance_cents: 500 });
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("/v1/credits/authorize"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ required_cents: 10, description: "test-cost" }),
      }),
    );
  });

  it("should return sufficient: false when balance is insufficient", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: false, balance_cents: 3 }),
    });

    const result = await authorizeCreditSpend(10, "test-cost", identity);

    expect(result).toEqual({ sufficient: false, balance_cents: 3 });
  });

  it("should forward all identity headers", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: true, balance_cents: 100 }),
    });

    await authorizeCreditSpend(5, "desc", identity);

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers).toMatchObject({
      "x-org-id": "org-1",
      "x-user-id": "user-1",
      "x-run-id": "run-1",
      "x-campaign-id": "camp-1",
      "x-brand-id": "brand-1",
      "x-workflow-name": "wf-1",
    });
  });

  it("should omit optional headers when not provided", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: true, balance_cents: 100 }),
    });

    await authorizeCreditSpend(5, "desc", {
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers).not.toHaveProperty("x-campaign-id");
    expect(options.headers).not.toHaveProperty("x-brand-id");
    expect(options.headers).not.toHaveProperty("x-workflow-name");
  });

  it("should throw on non-ok response", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 500,
      text: () => Promise.resolve("Internal Server Error"),
    });

    await expect(authorizeCreditSpend(10, "test", identity)).rejects.toThrow(
      "billing-service POST /v1/credits/authorize failed: 500",
    );
  });

  it("should export cost estimates", () => {
    expect(COST_ESTIMATES["instantly-email-send"]).toBeGreaterThan(0);
    expect(COST_ESTIMATES["instantly-campaign-create"]).toBeGreaterThan(0);
    expect(COST_ESTIMATES["instantly-lead-add"]).toBeGreaterThan(0);
  });
});
