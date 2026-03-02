import express from "express";
import healthRoutes from "../../src/routes/health";
import campaignsRoutes from "../../src/routes/campaigns";
import leadsRoutes from "../../src/routes/leads";
import accountsRoutes from "../../src/routes/accounts";
import analyticsRoutes from "../../src/routes/analytics";
import webhooksRoutes from "../../src/routes/webhooks";
import sendRoutes from "../../src/routes/send";
import statusRoutes from "../../src/routes/status";
import { serviceAuth } from "../../src/middleware/serviceAuth";
import { identityHeaders } from "../../src/middleware/identityHeaders";

export function createTestApp() {
  const app = express();
  app.use(express.json());

  // Public routes
  app.use(healthRoutes);
  app.use("/webhooks", webhooksRoutes);

  // Protected routes (require X-API-Key + x-org-id + x-user-id)
  app.use("/send", serviceAuth, identityHeaders, sendRoutes);
  app.use("/status", serviceAuth, identityHeaders, statusRoutes);
  app.use("/campaigns", serviceAuth, identityHeaders, campaignsRoutes);
  app.use("/campaigns", serviceAuth, identityHeaders, leadsRoutes);
  app.use("/accounts", serviceAuth, identityHeaders, accountsRoutes);
  app.use(serviceAuth, identityHeaders, analyticsRoutes);

  return app;
}

export function getAuthHeaders() {
  return {
    "X-API-Key": process.env.INSTANTLY_SERVICE_API_KEY || "test-api-key",
    "x-org-id": "test-org",
    "x-user-id": "test-user",
  };
}
