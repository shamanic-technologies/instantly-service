import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Set env vars before import
process.env.FEATURES_SERVICE_URL = "http://features:3000";
process.env.FEATURES_SERVICE_API_KEY = "feat-key";
process.env.WORKFLOW_SERVICE_URL = "http://workflows:3000";
process.env.WORKFLOW_SERVICE_API_KEY = "wf-key";

import {
  resolveFeatureDynastySlugs,
  resolveWorkflowDynastySlugs,
  fetchFeatureDynasties,
  fetchWorkflowDynasties,
  buildSlugToDynastyMap,
} from "../../src/lib/dynasty-client";

const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

describe("dynasty-client", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("resolveFeatureDynastySlugs", () => {
    it("should return slugs from features-service", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ slugs: ["feat-alpha", "feat-alpha-v2", "feat-alpha-v3"] }),
      });

      const result = await resolveFeatureDynastySlugs("feat-alpha");

      expect(result).toEqual(["feat-alpha", "feat-alpha-v2", "feat-alpha-v3"]);
      expect(mockFetch).toHaveBeenCalledWith(
        "http://features:3000/features/dynasty/slugs?dynastySlug=feat-alpha",
        expect.objectContaining({
          headers: expect.objectContaining({ "X-API-Key": "feat-key" }),
        }),
      );
    });

    it("should return empty array on non-ok response", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });

      const result = await resolveFeatureDynastySlugs("unknown");

      expect(result).toEqual([]);
    });

    it("should forward extra headers", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ slugs: ["feat-1"] }),
      });

      await resolveFeatureDynastySlugs("feat-1", { "x-org-id": "org-1" });

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({ "x-org-id": "org-1", "X-API-Key": "feat-key" }),
        }),
      );
    });
  });

  describe("resolveWorkflowDynastySlugs", () => {
    it("should return slugs from workflow-service", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ slugs: ["cold-email", "cold-email-v2"] }),
      });

      const result = await resolveWorkflowDynastySlugs("cold-email");

      expect(result).toEqual(["cold-email", "cold-email-v2"]);
      expect(mockFetch).toHaveBeenCalledWith(
        "http://workflows:3000/workflows/dynasty/slugs?dynastySlug=cold-email",
        expect.objectContaining({
          headers: expect.objectContaining({ "X-API-Key": "wf-key" }),
        }),
      );
    });

    it("should return empty array on non-ok response", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });

      const result = await resolveWorkflowDynastySlugs("unknown");

      expect(result).toEqual([]);
    });
  });

  describe("fetchFeatureDynasties", () => {
    it("should return all dynasties", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          dynasties: [
            { dynastySlug: "feat-alpha", slugs: ["feat-alpha", "feat-alpha-v2"] },
            { dynastySlug: "feat-beta", slugs: ["feat-beta"] },
          ],
        }),
      });

      const result = await fetchFeatureDynasties();

      expect(result).toHaveLength(2);
      expect(mockFetch).toHaveBeenCalledWith(
        "http://features:3000/features/dynasties",
        expect.any(Object),
      );
    });

    it("should return empty array on failure", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 503 });

      const result = await fetchFeatureDynasties();

      expect(result).toEqual([]);
    });
  });

  describe("fetchWorkflowDynasties", () => {
    it("should return all dynasties", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          dynasties: [
            { dynastySlug: "cold-email", slugs: ["cold-email", "cold-email-v2"] },
          ],
        }),
      });

      const result = await fetchWorkflowDynasties();

      expect(result).toHaveLength(1);
    });
  });

  describe("buildSlugToDynastyMap", () => {
    it("should build reverse map from dynasties", () => {
      const dynasties = [
        { dynastySlug: "cold-email", slugs: ["cold-email", "cold-email-v2", "cold-email-v3"] },
        { dynastySlug: "warm-intro", slugs: ["warm-intro", "warm-intro-v2"] },
      ];

      const map = buildSlugToDynastyMap(dynasties);

      expect(map.get("cold-email")).toBe("cold-email");
      expect(map.get("cold-email-v2")).toBe("cold-email");
      expect(map.get("cold-email-v3")).toBe("cold-email");
      expect(map.get("warm-intro")).toBe("warm-intro");
      expect(map.get("warm-intro-v2")).toBe("warm-intro");
      expect(map.get("unknown")).toBeUndefined();
    });

    it("should handle empty dynasties", () => {
      const map = buildSlugToDynastyMap([]);

      expect(map.size).toBe(0);
    });
  });
});
