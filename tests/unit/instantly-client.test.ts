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

  it("createCampaign should build multi-step sequences from steps array", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "Test", status: "draft", created_at: "", updated_at: "" }),
    });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    await createCampaign({
      name: "Test Campaign",
      steps: [
        { subject: "Hello", bodyHtml: "<p>First</p>", daysSinceLastStep: 0 },
        { subject: "Hello", bodyHtml: "<p>Follow up</p>", daysSinceLastStep: 3 },
      ],
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);

    // Should have sequences array with steps
    expect(body.sequences).toHaveLength(1);
    expect(body.sequences[0].steps).toHaveLength(2);
    expect(body.sequences[0].steps[0]).toEqual({
      type: "email",
      delay: 0,
      variants: [{ subject: "Hello", body: "<p>First</p>" }],
    });
    expect(body.sequences[0].steps[1]).toEqual({
      type: "email",
      delay: 3,
      variants: [{ subject: "Hello", body: "<p>Follow up</p>" }],
    });

    // Should NOT include account_ids or bcc (V2 ignores them in create)
    expect(body.account_ids).toBeUndefined();
    expect(body.bcc).toBeUndefined();
  });

  it("updateCampaign should PATCH campaign with email_list, bcc_list, and stop_on_reply", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "Test", status: "draft" }),
    });

    const { updateCampaign } = await import("../../src/lib/instantly-client");
    await updateCampaign("camp-1", {
      email_list: ["sender@example.com"],
      bcc_list: ["kevin@mcpfactory.org"],
      stop_on_reply: true,
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain("/campaigns/camp-1");
    expect(options.method).toBe("PATCH");
    const body = JSON.parse(options.body);
    expect(body.email_list).toEqual(["sender@example.com"]);
    expect(body.bcc_list).toEqual(["kevin@mcpfactory.org"]);
    expect(body.stop_on_reply).toBe(true);
  });

  it("addLeads should use 'campaign' field (not 'campaign_id') per V2 API", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "lead-1", email: "test@example.com" }),
    });

    const { addLeads } = await import("../../src/lib/instantly-client");
    await addLeads({
      campaign_id: "camp-123",
      leads: [{ email: "test@example.com", first_name: "Test" }],
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.campaign).toBe("camp-123");
    expect(body.campaign_id).toBeUndefined();
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

    if (options.body === undefined) {
      expect(options.headers["Content-Type"]).toBeUndefined();
    }
  });
});
