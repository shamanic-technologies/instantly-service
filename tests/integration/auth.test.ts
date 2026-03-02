import { describe, it, expect } from "vitest";
import request from "supertest";
import { createTestApp, getAuthHeaders } from "../helpers/test-app";

describe("Auth Integration", () => {
  const app = createTestApp();

  describe("Protected routes", () => {
    it("should return 401 without API key", async () => {
      const response = await request(app).get("/campaigns/test-id");

      expect(response.status).toBe(401);
      expect(response.body.error).toBe("Unauthorized");
    });

    it("should return 401 with wrong API key", async () => {
      const response = await request(app)
        .get("/campaigns/test-id")
        .set("X-API-Key", "wrong-key");

      expect(response.status).toBe(401);
    });

    it("should allow access with correct API key and identity headers", async () => {
      process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

      const response = await request(app)
        .get("/campaigns/test-id")
        .set(getAuthHeaders());

      // Should get 404 (not found) not 401 (unauthorized) or 400 (missing headers)
      expect(response.status).not.toBe(401);
      expect(response.status).not.toBe(400);
    });
  });

  describe("Identity headers", () => {
    it("should return 400 when x-org-id is missing", async () => {
      process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

      const response = await request(app)
        .get("/campaigns/test-id")
        .set("X-API-Key", "test-api-key")
        .set("x-user-id", "test-user");

      expect(response.status).toBe(400);
      expect(response.body.error).toContain("x-org-id");
    });

    it("should return 400 when x-user-id is missing", async () => {
      process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

      const response = await request(app)
        .get("/campaigns/test-id")
        .set("X-API-Key", "test-api-key")
        .set("x-org-id", "test-org");

      expect(response.status).toBe(400);
      expect(response.body.error).toContain("x-org-id");
    });

    it("should not require identity headers on /health", async () => {
      const response = await request(app).get("/health");

      expect(response.status).toBe(200);
    });

    it("should not require identity headers on /webhooks", async () => {
      const response = await request(app)
        .post("/webhooks/instantly")
        .send({ event_type: "test" });

      // Should not be 401 (auth) — webhooks are public
      // May be 400 from webhook payload validation, which is expected
      expect(response.status).not.toBe(401);
    });
  });
});
