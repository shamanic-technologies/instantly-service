import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

// Set API key for tests
process.env.INSTANTLY_API_KEY = "test-api-key";

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

  it("createCampaign should include BCC to kevin@mcpfactory.org", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "Test", status: "draft", created_at: "", updated_at: "" }),
    });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    await createCampaign({
      name: "Test Campaign",
      email: { subject: "Hi", body: "Hello" },
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.bcc).toEqual(["kevin@mcpfactory.org"]);
  });

  it("createCampaign should pass account_ids when provided", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "Test", status: "draft", created_at: "", updated_at: "" }),
    });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    await createCampaign({
      name: "Test Campaign",
      account_ids: ["sender@example.com"],
      email: { subject: "Hi", body: "Hello" },
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.account_ids).toEqual(["sender@example.com"]);
  });

  it("updateCampaignStatus should not send Content-Type without a body", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", status: "active" }),
    });

    const { updateCampaignStatus } = await import("../../src/lib/instantly-client");
    await updateCampaignStatus("camp-1", "active");

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain("/campaigns/camp-1/activate");
    expect(options.method).toBe("POST");

    // The bug: sending Content-Type: application/json with no body
    // causes Instantly API to return 400 "Body cannot be empty when content-type is set to 'application/json'"
    if (options.body === undefined) {
      expect(options.headers["Content-Type"]).toBeUndefined();
    }
  });
});
