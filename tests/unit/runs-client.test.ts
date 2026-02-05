import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

describe("runs-client", () => {
  beforeEach(() => {
    mockFetch.mockClear();
  });

  it("should export ensureOrganization function", async () => {
    const { ensureOrganization } = await import("../../src/lib/runs-client");
    expect(typeof ensureOrganization).toBe("function");
  });

  it("should export createRun function", async () => {
    const { createRun } = await import("../../src/lib/runs-client");
    expect(typeof createRun).toBe("function");
  });

  it("should export updateRun function", async () => {
    const { updateRun } = await import("../../src/lib/runs-client");
    expect(typeof updateRun).toBe("function");
  });

  it("should export addCosts function", async () => {
    const { addCosts } = await import("../../src/lib/runs-client");
    expect(typeof addCosts).toBe("function");
  });
});
