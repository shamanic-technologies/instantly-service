import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

const TEST_API_KEY = "test-api-key";

describe("instantly-client", () => {
  beforeEach(() => {
    mockFetch.mockClear();
    vi.resetModules();
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
    await createCampaign(TEST_API_KEY, {
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
    // Instantly delay = "days after THIS step before NEXT step"
    // Step 1 delay = step 2's daysSinceLastStep (3)
    expect(body.sequences[0].steps[0]).toEqual({
      type: "email",
      delay: 3,
      variants: [{ subject: "Hello", body: "<p>First</p>" }],
    });
    // Last step delay = 0 (no next step)
    expect(body.sequences[0].steps[1]).toEqual({
      type: "email",
      delay: 0,
      variants: [{ subject: "Hello", body: "<p>Follow up</p>" }],
    });

    // Should NOT include account_ids or bcc (V2 ignores them in create)
    expect(body.account_ids).toBeUndefined();
    expect(body.bcc).toBeUndefined();

    // Should use the provided API key
    expect(options.headers.Authorization).toBe(`Bearer ${TEST_API_KEY}`);
  });

  it("createCampaign should correctly shift delays for 3-step sequences", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "Test", status: "draft", created_at: "", updated_at: "" }),
    });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    await createCampaign(TEST_API_KEY, {
      name: "Test Campaign",
      steps: [
        { subject: "Hello", bodyHtml: "<p>First</p>", daysSinceLastStep: 0 },
        { subject: "Hello", bodyHtml: "<p>Follow up 1</p>", daysSinceLastStep: 3 },
        { subject: "Hello", bodyHtml: "<p>Follow up 2</p>", daysSinceLastStep: 7 },
      ],
    });

    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    const steps = body.sequences[0].steps;

    // delay[0] = step 2's daysSinceLastStep = 3
    expect(steps[0].delay).toBe(3);
    // delay[1] = step 3's daysSinceLastStep = 7
    expect(steps[1].delay).toBe(7);
    // delay[2] = 0 (last step)
    expect(steps[2].delay).toBe(0);
  });

  it("createCampaign single-step should have delay 0", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "Test", status: "draft", created_at: "", updated_at: "" }),
    });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    await createCampaign(TEST_API_KEY, {
      name: "Test Campaign",
      steps: [
        { subject: "Hello", bodyHtml: "<p>Only email</p>", daysSinceLastStep: 0 },
      ],
    });

    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.sequences[0].steps[0].delay).toBe(0);
  });

  it("updateCampaign should PATCH campaign with email_list, bcc_list, and stop_on_reply", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "Test", status: "draft" }),
    });

    const { updateCampaign } = await import("../../src/lib/instantly-client");
    await updateCampaign(TEST_API_KEY, "camp-1", {
      email_list: ["sender@example.com"],
      bcc_list: ["bcc@test.com"],
      stop_on_reply: true,
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain("/campaigns/camp-1");
    expect(options.method).toBe("PATCH");
    const body = JSON.parse(options.body);
    expect(body.email_list).toEqual(["sender@example.com"]);
    expect(body.bcc_list).toEqual(["bcc@test.com"]);
    expect(body.stop_on_reply).toBe(true);
  });

  it("addLeads should use 'campaign' field (not 'campaign_id') per V2 API", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "lead-1", email: "test@example.com" }),
    });

    const { addLeads } = await import("../../src/lib/instantly-client");
    await addLeads(TEST_API_KEY, {
      campaign_id: "camp-123",
      leads: [{ email: "test@example.com", first_name: "Test" }],
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [, options] = mockFetch.mock.calls[0];
    const body = JSON.parse(options.body);
    expect(body.campaign).toBe("camp-123");
    expect(body.campaign_id).toBeUndefined();
  });

  it("should include response body in error when all retries fail with 429/500", async () => {
    // All 3 attempts return 429 with a body
    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        text: () => Promise.resolve('{"error":"rate_limit","message":"Too many requests"}'),
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 500,
        text: () => Promise.resolve('{"error":"internal","message":"Campaign limit reached"}'),
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 500,
        text: () => Promise.resolve('{"error":"internal","message":"Campaign limit reached"}'),
      });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    await expect(
      createCampaign(TEST_API_KEY, {
        name: "Test Campaign",
        steps: [{ subject: "Hello", bodyHtml: "<p>Hi</p>", daysSinceLastStep: 0 }],
      }),
    ).rejects.toThrow(/Campaign limit reached/);
  });

  it("updateCampaignStatus should not send Content-Type without a body", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "camp-1", status: "active" }),
    });

    const { updateCampaignStatus } = await import("../../src/lib/instantly-client");
    await updateCampaignStatus(TEST_API_KEY, "camp-1", "active");

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain("/campaigns/camp-1/activate");
    expect(options.method).toBe("POST");

    if (options.body === undefined) {
      expect(options.headers["Content-Type"]).toBeUndefined();
    }
  });

  it("listEmails serializes /emails requests via ≥3s throttle gate", async () => {
    vi.useFakeTimers();
    const setTimeoutSpy = vi.spyOn(global, "setTimeout");
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ items: [] }),
    });

    const { listEmails } = await import("../../src/lib/instantly-client");

    const p1 = listEmails(TEST_API_KEY, { campaignId: "c1" });
    await vi.runAllTimersAsync();
    await p1;

    const p2 = listEmails(TEST_API_KEY, { campaignId: "c2" });
    await vi.runAllTimersAsync();
    await p2;

    const delays = setTimeoutSpy.mock.calls
      .map(([, ms]) => Number(ms))
      .filter((d) => !Number.isNaN(d));
    expect(delays.some((d) => d >= 3000)).toBe(true);

    setTimeoutSpy.mockRestore();
    vi.useRealTimers();
  });

  it("non-/emails paths are not throttled", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve([]),
    });

    const { getCampaignAnalytics } = await import("../../src/lib/instantly-client");

    const t0 = Date.now();
    await getCampaignAnalytics(TEST_API_KEY, "c1");
    await getCampaignAnalytics(TEST_API_KEY, "c2");
    expect(Date.now() - t0).toBeLessThan(500);
  });

  it("happy-path 200 emits no console.log/warn (no log spam)", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve([{ id: "c1" }]),
    });

    const { getCampaignAnalytics } = await import("../../src/lib/instantly-client");
    await getCampaignAnalytics(TEST_API_KEY, "c1");

    const noisy = (s: ReturnType<typeof vi.spyOn>) =>
      s.mock.calls.some((args) => String(args[0] ?? "").includes("[instantly-api]"));
    expect(noisy(logSpy)).toBe(false);
    expect(noisy(warnSpy)).toBe(false);

    logSpy.mockRestore();
    warnSpy.mockRestore();
  });

  it("429 retries emit no per-attempt console.warn", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockFetch
      .mockResolvedValueOnce({ ok: false, status: 429, text: () => Promise.resolve("rate") })
      .mockResolvedValueOnce({ ok: false, status: 429, text: () => Promise.resolve("rate") })
      .mockResolvedValueOnce({ ok: false, status: 429, text: () => Promise.resolve("rate") });

    const { getCampaignAnalytics } = await import("../../src/lib/instantly-client");
    await expect(getCampaignAnalytics(TEST_API_KEY, "c1")).rejects.toThrow(/429/);

    const apiWarns = warnSpy.mock.calls.filter((args) =>
      String(args[0] ?? "").includes("[instantly-api]"),
    );
    expect(apiWarns).toHaveLength(0);

    warnSpy.mockRestore();
  }, 30000);

  it("non-retryable 4xx still emits console.error", async () => {
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mockFetch.mockResolvedValue({
      ok: false,
      status: 400,
      text: () => Promise.resolve("bad"),
    });

    const { getCampaignAnalytics } = await import("../../src/lib/instantly-client");
    await expect(getCampaignAnalytics(TEST_API_KEY, "c1")).rejects.toThrow(/400/);

    const apiErrors = errSpy.mock.calls.filter((args) =>
      String(args[0] ?? "").includes("[instantly-api]"),
    );
    expect(apiErrors.length).toBeGreaterThanOrEqual(1);

    errSpy.mockRestore();
  }, 30000);
});
