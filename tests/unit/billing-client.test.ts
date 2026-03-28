import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { authorizeCreditSpend } from "../../src/lib/billing-client";

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
    workflowSlug: "wf-1",
    featureSlug: "cold-outreach",
  };

  const items = [{ costName: "instantly-email-send", quantity: 3 }];

  it("should send items array and return authorization result", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: true, balance_cents: 500, required_cents: 15 }),
    });

    const result = await authorizeCreditSpend(items, "test-cost", identity);

    expect(result).toEqual({ sufficient: true, balance_cents: 500, required_cents: 15 });
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("/v1/credits/authorize"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ items, description: "test-cost" }),
      }),
    );
  });

  it("should return sufficient: false when balance is insufficient", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: false, balance_cents: 3, required_cents: 15 }),
    });

    const result = await authorizeCreditSpend(items, "test-cost", identity);

    expect(result).toEqual({ sufficient: false, balance_cents: 3, required_cents: 15 });
  });

  it("should forward all identity headers", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: true, balance_cents: 100, required_cents: 5 }),
    });

    await authorizeCreditSpend(items, "desc", identity);

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers).toMatchObject({
      "x-org-id": "org-1",
      "x-user-id": "user-1",
      "x-run-id": "run-1",
      "x-campaign-id": "camp-1",
      "x-brand-id": "brand-1",
      "x-workflow-slug": "wf-1",
      "x-feature-slug": "cold-outreach",
    });
  });

  it("should omit optional headers when not provided", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: true, balance_cents: 100, required_cents: 5 }),
    });

    await authorizeCreditSpend(items, "desc", {
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers).not.toHaveProperty("x-campaign-id");
    expect(options.headers).not.toHaveProperty("x-brand-id");
    expect(options.headers).not.toHaveProperty("x-workflow-slug");
    expect(options.headers).not.toHaveProperty("x-feature-slug");
  });

  it("should throw on non-ok response", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 500,
      text: () => Promise.resolve("Internal Server Error"),
    });

    await expect(authorizeCreditSpend(items, "test", identity)).rejects.toThrow(
      "billing-service POST /v1/credits/authorize failed: 500",
    );
  });

  it("should support multiple cost items", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ sufficient: true, balance_cents: 1000, required_cents: 20 }),
    });

    const multiItems = [
      { costName: "instantly-email-send", quantity: 3 },
      { costName: "instantly-campaign-create", quantity: 1 },
    ];

    await authorizeCreditSpend(multiItems, "multi", identity);

    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.items).toHaveLength(2);
    expect(body.items[0]).toEqual({ costName: "instantly-email-send", quantity: 3 });
    expect(body.items[1]).toEqual({ costName: "instantly-campaign-create", quantity: 1 });
  });
});
