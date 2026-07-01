import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

const TEST_API_KEY = "test-api-key";

describe("instantly-client", () => {
  beforeEach(() => {
    mockFetch.mockClear();
    vi.resetModules();
  });

  // Defensive cleanup: tests that use fake timers must NOT leak fake-timer
  // state into the next test. If a fake-timer test fails before its manual
  // useRealTimers() call, every subsequent test runs with fake setTimeout
  // and `await sleep(...)` hangs forever.
  afterEach(() => {
    vi.useRealTimers();
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

  it("createCampaign schedules business hours on weekdays, default tz when none given", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true, status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "T", status: "draft", created_at: "", updated_at: "" }),
    });
    const { createCampaign } = await import("../../src/lib/instantly-client");
    await createCampaign(TEST_API_KEY, { name: "T", steps: [{ subject: "s", bodyHtml: "<p>b</p>", daysSinceLastStep: 0 }] });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    const sched = body.campaign_schedule.schedules[0];
    expect(sched.timing).toEqual({ from: "08:00", to: "17:00" });
    expect(sched.days).toEqual({ "0": false, "1": true, "2": true, "3": true, "4": true, "5": true, "6": false });
    expect(sched.timezone).toBe("America/Chicago");
  });

  it("createCampaign uses the supplied recipient timezone", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true, status: 200,
      json: () => Promise.resolve({ id: "camp-1", name: "T", status: "draft", created_at: "", updated_at: "" }),
    });
    const { createCampaign } = await import("../../src/lib/instantly-client");
    await createCampaign(TEST_API_KEY, {
      name: "T",
      steps: [{ subject: "s", bodyHtml: "<p>b</p>", daysSinceLastStep: 0 }],
      timezone: "America/New_York",
    });

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.campaign_schedule.schedules[0].timezone).toBe("America/New_York");
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
        headers: { get: () => null },
        text: () => Promise.resolve('{"error":"rate_limit","message":"Too many requests"}'),
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 500,
        headers: { get: () => null },
        text: () => Promise.resolve('{"error":"internal","message":"Campaign limit reached"}'),
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 500,
        headers: { get: () => null },
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

  it("429 with Retry-After header waits at least that many seconds before retrying", async () => {
    vi.useFakeTimers();
    const setTimeoutSpy = vi.spyOn(global, "setTimeout");

    // First attempt 429 with Retry-After: 5, second attempt succeeds
    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        headers: { get: (name: string) => (name === "Retry-After" ? "5" : null) },
        text: () => Promise.resolve('{"error":"rate_limit"}'),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ id: "camp-1" }),
      });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    const p = createCampaign(TEST_API_KEY, {
      name: "Test",
      steps: [{ subject: "Hi", bodyHtml: "<p>Hi</p>", daysSinceLastStep: 0 }],
    });
    await vi.runAllTimersAsync();
    await p;

    const delays = setTimeoutSpy.mock.calls
      .map(([, ms]) => Number(ms))
      .filter((d) => !Number.isNaN(d));
    // Retry-After of 5s = 5000ms. The 429 retry MUST wait at least that long.
    expect(delays.some((d) => d >= 5000)).toBe(true);

    setTimeoutSpy.mockRestore();
    vi.useRealTimers();
  });

  it("429 without Retry-After falls back to exponential backoff", async () => {
    vi.useFakeTimers();
    const setTimeoutSpy = vi.spyOn(global, "setTimeout");

    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        headers: { get: () => null },
        text: () => Promise.resolve('{"error":"rate_limit"}'),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ id: "camp-1" }),
      });

    const { createCampaign } = await import("../../src/lib/instantly-client");
    const p = createCampaign(TEST_API_KEY, {
      name: "Test",
      steps: [{ subject: "Hi", bodyHtml: "<p>Hi</p>", daysSinceLastStep: 0 }],
    });
    await vi.runAllTimersAsync();
    await p;

    const delays = setTimeoutSpy.mock.calls
      .map(([, ms]) => Number(ms))
      .filter((d) => !Number.isNaN(d));
    // Exponential backoff on first retry: 1000 + jitter (0-1000) = 1000-2000ms
    expect(delays.some((d) => d >= 1000 && d < 2000)).toBe(true);

    setTimeoutSpy.mockRestore();
    vi.useRealTimers();
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

  it("non-/emails paths use the general (fast) throttle slot, not the slow /emails one", async () => {
    // General slot is ~110ms — two sequential calls finish well under 500ms.
    // (vs. the /emails slot at 3100ms which would gate the second call.)
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

  it("non-/emails paths: parallel batch is paced through the general throttle (≥110ms gaps)", async () => {
    // The retry-stuck cron submits a batch of ~10 calls via Promise.all.
    // Each call must serialize through `throttle(generalSlot)` so the
    // workspace stays under Instantly's general 600 req/min cap.
    vi.useFakeTimers();
    const setTimeoutSpy = vi.spyOn(global, "setTimeout");
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "c" }),
    });

    const { getCampaign } = await import("../../src/lib/instantly-client");

    const N = 5;
    const calls = Array.from({ length: N }, (_, i) => getCampaign(TEST_API_KEY, `c${i}`));
    await vi.runAllTimersAsync();
    await Promise.all(calls);

    // With 5 concurrent calls hitting the general throttle (110ms interval),
    // the waits accumulate: 0, 110, 220, 330, 440 — so calls 2..5 each issue
    // their own setTimeout with a positive delay. At least N-1 such waits.
    const positiveDelays = setTimeoutSpy.mock.calls
      .map(([, ms]) => Number(ms))
      .filter((d) => Number.isFinite(d) && d > 0);
    expect(positiveDelays.length).toBeGreaterThanOrEqual(N - 1);

    // And we never accidentally hit the slow /emails throttle (≥3000ms).
    const emailsThrottleDelays = positiveDelays.filter((d) => d >= 3000);
    expect(emailsThrottleDelays).toHaveLength(0);

    // The max wait should be bounded by ~N * 110ms — well under any /emails
    // gate. Generous upper bound to absorb test scheduling noise.
    expect(Math.max(...positiveDelays)).toBeLessThan(N * 200);

    setTimeoutSpy.mockRestore();
    vi.useRealTimers();
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

  // ─── listAccounts pagination ──────────────────────────────────────────────
  //
  // Historic bug 2026-05-28: `listAccounts` was a single un-paginated GET,
  // so it only ever returned Instantly's default page (10 items). With 156
  // active accounts in the workspace, 146 were invisible to
  // `pickRandomAccount` — sends + retry-stuck redispatches saturated the
  // first 10 accounts while the rest sat idle. These tests pin the
  // pagination contract so the bug cannot reappear.

  it("listAccounts: single page (no next_starting_after) returns items, 1 fetch", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () =>
        Promise.resolve({
          items: [
            { email: "a@x.com", status: 1, warmup_status: 1 },
            { email: "b@x.com", status: 1, warmup_status: 1 },
          ],
        }),
    });

    const { listAccounts } = await import("../../src/lib/instantly-client");
    const accounts = await listAccounts(TEST_API_KEY);

    expect(accounts).toHaveLength(2);
    expect(accounts.map((a) => a.email)).toEqual(["a@x.com", "b@x.com"]);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("listAccounts: paginates via next_starting_after across 3 pages, concatenates in order", async () => {
    mockFetch
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            items: Array.from({ length: 100 }, (_, i) => ({
              email: `p1-${i}@x.com`,
              status: 1,
              warmup_status: 1,
            })),
            next_starting_after: "cursor-1",
          }),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            items: Array.from({ length: 100 }, (_, i) => ({
              email: `p2-${i}@x.com`,
              status: 1,
              warmup_status: 1,
            })),
            next_starting_after: "cursor-2",
          }),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            items: [{ email: "p3-0@x.com", status: 1, warmup_status: 1 }],
          }),
      });

    const { listAccounts } = await import("../../src/lib/instantly-client");
    const accounts = await listAccounts(TEST_API_KEY);

    expect(accounts).toHaveLength(201);
    expect(accounts[0].email).toBe("p1-0@x.com");
    expect(accounts[100].email).toBe("p2-0@x.com");
    expect(accounts[200].email).toBe("p3-0@x.com");
    expect(mockFetch).toHaveBeenCalledTimes(3);

    // Second/third fetch URLs must carry the cursor from the prior response.
    const url2 = String(mockFetch.mock.calls[1][0]);
    const url3 = String(mockFetch.mock.calls[2][0]);
    expect(url2).toContain("starting_after=cursor-1");
    expect(url3).toContain("starting_after=cursor-2");
  });

  it("listAccounts: sends limit=100 on every page (Instantly's max — probed empirically)", async () => {
    mockFetch
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            items: [{ email: "a@x.com", status: 1, warmup_status: 1 }],
            next_starting_after: "c1",
          }),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            items: [{ email: "b@x.com", status: 1, warmup_status: 1 }],
          }),
      });

    const { listAccounts } = await import("../../src/lib/instantly-client");
    await listAccounts(TEST_API_KEY);

    for (const call of mockFetch.mock.calls) {
      expect(String(call[0])).toContain("limit=100");
    }
  });

  it("listAccounts: empty response returns [] with one fetch (no infinite loop)", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ items: [] }),
    });

    const { listAccounts } = await import("../../src/lib/instantly-client");
    const accounts = await listAccounts(TEST_API_KEY);

    expect(accounts).toEqual([]);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("listAccounts: stops when next_starting_after is empty even if items still present", async () => {
    // Defensive: if Instantly ever drops the cursor on the last page but
    // still returns items, we MUST stop. No cursor = no next page.
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () =>
        Promise.resolve({
          items: Array.from({ length: 100 }, (_, i) => ({
            email: `acct-${i}@x.com`,
            status: 1,
            warmup_status: 1,
          })),
        }),
    });

    const { listAccounts } = await import("../../src/lib/instantly-client");
    const accounts = await listAccounts(TEST_API_KEY);

    expect(accounts).toHaveLength(100);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("listAllCampaignAnalytics: one non-paginated GET /campaigns/analytics (no id), returns the whole array", async () => {
    // Instantly returns ALL campaigns' analytics as a single flat array when id
    // is omitted — this endpoint has no cursor, so exactly ONE call is made.
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () =>
        Promise.resolve([
          { campaign_id: "a", campaign_status: 1, emails_sent_count: 3 },
          { campaign_id: "b", campaign_status: 2, emails_sent_count: 1 },
        ]),
    });

    const { listAllCampaignAnalytics } = await import("../../src/lib/instantly-client");
    const rows = await listAllCampaignAnalytics(TEST_API_KEY);

    expect(rows).toHaveLength(2);
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url] = mockFetch.mock.calls[0];
    expect(url).toContain("/campaigns/analytics");
    expect(url).not.toContain("id=");
  });

  it("listAllCampaignSequenceLengths: paginates /campaigns and sums steps across sequences", async () => {
    mockFetch
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            items: [
              { id: "a", status: 1, sequences: [{ steps: [{}, {}, {}] }] }, // 3 steps
              { id: "b", status: 2, sequences: [{ steps: [{}] }, { steps: [{}, {}] }] }, // 1 + 2 = 3
            ],
            next_starting_after: "cursor-1",
          }),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            items: [{ id: "c", status: 1, sequences: [] }], // 0 steps
          }),
      });

    const { listAllCampaignSequenceLengths } = await import("../../src/lib/instantly-client");
    const campaigns = await listAllCampaignSequenceLengths(TEST_API_KEY);

    expect(mockFetch).toHaveBeenCalledTimes(2); // followed the cursor
    expect(campaigns).toEqual([
      { id: "a", status: 1, stepCount: 3 },
      { id: "b", status: 2, stepCount: 3 },
      { id: "c", status: 1, stepCount: 0 },
    ]);
  });
});
