import express from "express";
import healthRoutes from "../../src/routes/health";
import campaignsRoutes from "../../src/routes/campaigns";
import leadsRoutes from "../../src/routes/leads";
import accountsRoutes from "../../src/routes/accounts";
import analyticsRoutes from "../../src/routes/analytics";
import webhooksRoutes from "../../src/routes/webhooks";
import { serviceAuth } from "../../src/middleware/serviceAuth";

export function createTestApp() {
  const app = express();
  app.use(express.json());

  // Public routes
  app.use(healthRoutes);
  app.use("/webhooks", webhooksRoutes);

  // Protected routes
  app.use("/campaigns", serviceAuth, campaignsRoutes);
  app.use("/campaigns", serviceAuth, leadsRoutes);
  app.use("/accounts", serviceAuth, accountsRoutes);
  app.use(serviceAuth, analyticsRoutes);

  return app;
}

export function getAuthHeaders() {
  return {
    "X-API-Key": process.env.INSTANTLY_SERVICE_API_KEY || "test-api-key",
  };
}
