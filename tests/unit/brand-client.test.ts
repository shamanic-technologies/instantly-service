import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { getCurrentGoal, getCurrentGoals } from "../../src/lib/brand-client";

describe("brand-client", () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockReset();
    vi.stubGlobal("fetch", mockFetch);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns currentGoal from a 2xx runtime-context response", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ currentGoal: "signup", brand: {}, brandProfile: {} }),
    });

    const goal = await getCurrentGoal("brand-1");

    expect(goal).toBe("signup");
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toContain("/internal/brands/brand-1/runtime-context");
    expect(opts.method).toBe("GET");
    expect(opts.headers).toHaveProperty("x-api-key");
  });

  it("throws (fail-loud) on a non-2xx response", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 404,
      text: () => Promise.resolve("Brand not found"),
    });

    await expect(getCurrentGoal("missing")).rejects.toThrow(/404/);
  });

  it("getCurrentGoals fetches every brand and preserves order", async () => {
    mockFetch
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ currentGoal: "purchase" }) })
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ currentGoal: "signup" }) });

    const goals = await getCurrentGoals(["a", "b"]);

    expect(goals).toEqual(["purchase", "signup"]);
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it("getCurrentGoals rejects if ANY lookup fails", async () => {
    mockFetch
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ currentGoal: "signup" }) })
      .mockResolvedValueOnce({ ok: false, status: 500, text: () => Promise.resolve("boom") });

    await expect(getCurrentGoals(["a", "b"])).rejects.toThrow(/500/);
  });
});
