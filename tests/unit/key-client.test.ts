import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

process.env.KEY_SERVICE_URL = "http://localhost:3001";
process.env.KEY_SERVICE_API_KEY = "test-key-service-key";

describe("key-client", () => {
  beforeEach(() => {
    mockFetch.mockClear();
  });

  it("should export resolveInstantlyApiKey function", async () => {
    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    expect(typeof resolveInstantlyApiKey).toBe("function");
  });

  it("resolveInstantlyApiKey should call unified decrypt endpoint with identity headers", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "resolved-key-123", keySource: "platform" }),
    });

    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    const result = await resolveInstantlyApiKey("org_123", "user_456", {
      method: "POST",
      path: "/send",
    });

    expect(result).toEqual({ key: "resolved-key-123", keySource: "platform" });
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe("http://localhost:3001/keys/instantly/decrypt");
    expect(options.method).toBe("GET");
    expect(options.headers["x-api-key"]).toBe("test-key-service-key");
    expect(options.headers["x-org-id"]).toBe("org_123");
    expect(options.headers["x-user-id"]).toBe("user_456");
    expect(options.headers["X-Caller-Service"]).toBe("instantly");
    expect(options.headers["X-Caller-Method"]).toBe("POST");
    expect(options.headers["X-Caller-Path"]).toBe("/send");
  });

  it("resolveInstantlyApiKey should return org keySource when org key is used", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "org-key-789", keySource: "org" }),
    });

    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    const result = await resolveInstantlyApiKey("org_abc", "user_def", {
      method: "POST",
      path: "/campaigns",
    });

    expect(result).toEqual({ key: "org-key-789", keySource: "org" });
  });

  it("resolveInstantlyApiKey should throw KeyServiceError on 404", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      text: () => Promise.resolve("Key not configured"),
    });

    const { resolveInstantlyApiKey, KeyServiceError } = await import("../../src/lib/key-client");
    try {
      await resolveInstantlyApiKey("org_no_key", "user_1", {
        method: "POST",
        path: "/send",
      });
      expect.fail("Should have thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(KeyServiceError);
      expect((error as InstanceType<typeof KeyServiceError>).statusCode).toBe(404);
      expect((error as Error).message).toContain("key-service GET");
    }
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("resolveInstantlyApiKey should throw on fetch failure", async () => {
    mockFetch.mockRejectedValueOnce(new Error("ECONNREFUSED"));

    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    await expect(
      resolveInstantlyApiKey("org_1", "user_1", {
        method: "POST",
        path: "/send",
      })
    ).rejects.toThrow("ECONNREFUSED");
  });

  it("resolveInstantlyApiKey should pass system userId for cron-like operations", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "cron-key", keySource: "platform" }),
    });

    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    await resolveInstantlyApiKey("org_123", "system", {
      method: "POST",
      path: "/campaigns/check-status",
    });

    const [, options] = mockFetch.mock.calls[0];
    expect(options.headers["x-user-id"]).toBe("system");
  });
});
