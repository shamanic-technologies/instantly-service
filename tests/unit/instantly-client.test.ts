import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

describe("instantly-client", () => {
  beforeEach(() => {
    mockFetch.mockClear();
  });

  it("should export createCampaign function", async () => {
    const { createCampaign } = await import("../../src/lib/instantly-client");
    expect(typeof createCampaign).toBe("function");
  });

  it("should export getCampaign function", async () => {
    const { getCampaign } = await import("../../src/lib/instantly-client");
    expect(typeof getCampaign).toBe("function");
  });

  it("should export addLeads function", async () => {
    const { addLeads } = await import("../../src/lib/instantly-client");
    expect(typeof addLeads).toBe("function");
  });

  it("should export listAccounts function", async () => {
    const { listAccounts } = await import("../../src/lib/instantly-client");
    expect(typeof listAccounts).toBe("function");
  });

  it("should export getCampaignAnalytics function", async () => {
    const { getCampaignAnalytics } = await import("../../src/lib/instantly-client");
    expect(typeof getCampaignAnalytics).toBe("function");
  });
});
