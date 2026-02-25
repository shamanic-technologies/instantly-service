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

  it("decryptAppKey should throw on non-ok response", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      text: () => Promise.resolve("Key not configured"),
    });

    const { decryptAppKey } = await import("../../src/lib/key-client");
    await expect(
      decryptAppKey("instantly", "instantly-service", {
        method: "POST",
        path: "/send",
      })
    ).rejects.toThrow("key-service GET");
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
});
