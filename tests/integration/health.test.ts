import { describe, it, expect } from "vitest";
import request from "supertest";
import { createTestApp } from "../helpers/test-app";

describe("Health Integration", () => {
  const app = createTestApp();

  describe("GET /", () => {
    it("should return service info", async () => {
      const response = await request(app).get("/");

      expect(response.status).toBe(200);
      expect(response.body.service).toBe("instantly-service");
      expect(response.body.version).toBe("1.0.0");
    });
  });

  describe("GET /health", () => {
    it("should return health status", async () => {
      const response = await request(app).get("/health");

      expect(response.status).toBe(200);
      expect(response.body.status).toBe("ok");
      expect(response.body.service).toBe("instantly-service");
    });
  });
});
