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

    it("should allow access with correct API key", async () => {
      process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

      const response = await request(app)
        .get("/campaigns/test-id")
        .set(getAuthHeaders());

      // Should get 404 (not found) not 401 (unauthorized)
      expect(response.status).not.toBe(401);
    });
  });
});
