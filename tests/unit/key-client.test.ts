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

  it("should export decryptAppKey function", async () => {
    const { decryptAppKey } = await import("../../src/lib/key-client");
    expect(typeof decryptAppKey).toBe("function");
  });

  it("should export decryptByokKey function", async () => {
    const { decryptByokKey } = await import("../../src/lib/key-client");
    expect(typeof decryptByokKey).toBe("function");
  });

  it("should export resolveInstantlyApiKey function", async () => {
    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    expect(typeof resolveInstantlyApiKey).toBe("function");
  });

  it("decryptAppKey should call correct URL with caller headers", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "decrypted-key-123" }),
    });

    const { decryptAppKey } = await import("../../src/lib/key-client");
    const key = await decryptAppKey("instantly", "instantly-service", {
      method: "POST",
      path: "/send",
    });

    expect(key).toBe("decrypted-key-123");
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe(
      "http://localhost:3001/internal/app-keys/instantly/decrypt?appId=instantly-service"
    );
    expect(options.method).toBe("GET");
    expect(options.headers["x-api-key"]).toBe("test-key-service-key");
    expect(options.headers["X-Caller-Service"]).toBe("instantly");
    expect(options.headers["X-Caller-Method"]).toBe("POST");
    expect(options.headers["X-Caller-Path"]).toBe("/send");
  });

  it("decryptAppKey should throw KeyServiceError on non-ok response", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      text: () => Promise.resolve("Key not configured"),
    });

    const { decryptAppKey, KeyServiceError } = await import("../../src/lib/key-client");
    try {
      await decryptAppKey("instantly", "instantly-service", {
        method: "POST",
        path: "/send",
      });
      expect.fail("Should have thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(KeyServiceError);
      expect((error as InstanceType<typeof KeyServiceError>).statusCode).toBe(404);
      expect((error as Error).message).toContain("key-service GET");
    }
  });

  it("decryptAppKey should throw on fetch failure", async () => {
    mockFetch.mockRejectedValueOnce(new Error("ECONNREFUSED"));

    const { decryptAppKey } = await import("../../src/lib/key-client");
    await expect(
      decryptAppKey("instantly", "instantly-service", {
        method: "POST",
        path: "/send",
      })
    ).rejects.toThrow("ECONNREFUSED");
  });

  it("decryptAppKey should encode provider and appId in URL", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "my provider", key: "key-456" }),
    });

    const { decryptAppKey } = await import("../../src/lib/key-client");
    await decryptAppKey("my provider", "my app", {
      method: "GET",
      path: "/test",
    });

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(
      "http://localhost:3001/internal/app-keys/my%20provider/decrypt?appId=my%20app"
    );
  });

  // ─── BYOK tests ───────────────────────────────────────────────────────────

  it("decryptByokKey should call BYOK decrypt URL with clerkOrgId", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "byok-key-789" }),
    });

    const { decryptByokKey } = await import("../../src/lib/key-client");
    const key = await decryptByokKey("instantly", "org_abc123", {
      method: "POST",
      path: "/send",
    });

    expect(key).toBe("byok-key-789");
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe(
      "http://localhost:3001/internal/keys/instantly/decrypt?clerkOrgId=org_abc123"
    );
    expect(options.headers["X-Caller-Service"]).toBe("instantly");
    expect(options.headers["X-Caller-Method"]).toBe("POST");
    expect(options.headers["X-Caller-Path"]).toBe("/send");
  });

  it("decryptByokKey should throw KeyServiceError with 404 when key not configured", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      text: () => Promise.resolve("Key not configured"),
    });

    const { decryptByokKey, KeyServiceError } = await import("../../src/lib/key-client");
    try {
      await decryptByokKey("instantly", "org_missing", {
        method: "POST",
        path: "/send",
      });
      expect.fail("Should have thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(KeyServiceError);
      expect((error as InstanceType<typeof KeyServiceError>).statusCode).toBe(404);
    }
  });

  it("decryptByokKey should encode provider and clerkOrgId in URL", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "my provider", key: "key-enc" }),
    });

    const { decryptByokKey } = await import("../../src/lib/key-client");
    await decryptByokKey("my provider", "org with spaces", {
      method: "GET",
      path: "/test",
    });

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(
      "http://localhost:3001/internal/keys/my%20provider/decrypt?clerkOrgId=org%20with%20spaces"
    );
  });

  // ─── resolveInstantlyApiKey tests ─────────────────────────────────────────

  it("resolveInstantlyApiKey should use BYOK key when clerkOrgId is provided", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "byok-org-key" }),
    });

    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    const key = await resolveInstantlyApiKey("org_123", {
      method: "POST",
      path: "/send",
    });

    expect(key).toBe("byok-org-key");
    const [url] = mockFetch.mock.calls[0];
    expect(url).toContain("/internal/keys/instantly/decrypt?clerkOrgId=org_123");
  });

  it("resolveInstantlyApiKey should use app key when clerkOrgId is null", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "shared-app-key" }),
    });

    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    const key = await resolveInstantlyApiKey(null, {
      method: "POST",
      path: "/accounts/sync",
    });

    expect(key).toBe("shared-app-key");
    const [url] = mockFetch.mock.calls[0];
    expect(url).toContain("/internal/app-keys/instantly/decrypt?appId=instantly-service");
  });

  it("resolveInstantlyApiKey should use app key when clerkOrgId is undefined", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ provider: "instantly", key: "shared-app-key" }),
    });

    const { resolveInstantlyApiKey } = await import("../../src/lib/key-client");
    const key = await resolveInstantlyApiKey(undefined, {
      method: "GET",
      path: "/accounts/warmup-analytics",
    });

    expect(key).toBe("shared-app-key");
    const [url] = mockFetch.mock.calls[0];
    expect(url).toContain("/internal/app-keys/instantly/decrypt");
  });

  it("resolveInstantlyApiKey should NOT fall back to app key when BYOK returns 404", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      text: () => Promise.resolve("Key not configured"),
    });

    const { resolveInstantlyApiKey, KeyServiceError } = await import("../../src/lib/key-client");
    try {
      await resolveInstantlyApiKey("org_no_key", {
        method: "POST",
        path: "/send",
      });
      expect.fail("Should have thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(KeyServiceError);
      expect((error as InstanceType<typeof KeyServiceError>).statusCode).toBe(404);
    }
    // Should only have made 1 fetch call (BYOK), NOT 2 (no app key fallback)
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });
});
