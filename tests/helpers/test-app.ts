import express from "express";
import healthRoutes from "../../src/routes/health";
import campaignsRoutes from "../../src/routes/campaigns";
import leadsRoutes from "../../src/routes/leads";
import accountsRoutes from "../../src/routes/accounts";
import analyticsRoutes from "../../src/routes/analytics";
import analyticsPublicRoutes from "../../src/routes/analytics-public";
import webhooksRoutes from "../../src/routes/webhooks";
import sendRoutes from "../../src/routes/send";
import statusRoutes from "../../src/routes/status";
import transferBrandRoutes from "../../src/routes/transfer-brand";
import { serviceAuth } from "../../src/middleware/serviceAuth";
import { requireOrgId } from "../../src/middleware/requireOrgId";

export function createTestApp() {
  const app = express();
  app.use(express.json());

  // Public routes
  app.use(healthRoutes);
  app.use("/webhooks", webhooksRoutes);

  // Protected public routes (x-api-key only)
  app.use("/public", serviceAuth, analyticsPublicRoutes);

  // Internal routes (x-api-key only)
  app.use("/internal/campaigns", serviceAuth, campaignsRoutes);
  app.use("/internal/accounts", serviceAuth, accountsRoutes);
  app.use("/internal/transfer-brand", serviceAuth, transferBrandRoutes);

  // Org-scoped routes (x-api-key + x-org-id required)
  app.use("/orgs/send", serviceAuth, requireOrgId, sendRoutes);
  app.use("/orgs/status", serviceAuth, requireOrgId, statusRoutes);
  app.use("/orgs/campaigns", serviceAuth, requireOrgId, campaignsRoutes);
  app.use("/orgs/campaigns", serviceAuth, requireOrgId, leadsRoutes);
  app.use("/orgs/accounts", serviceAuth, requireOrgId, accountsRoutes);
  app.use("/orgs", serviceAuth, requireOrgId, analyticsRoutes);

  return app;
}

export function getAuthHeaders() {
  return {
    "X-API-Key": process.env.INSTANTLY_SERVICE_API_KEY || "test-api-key",
    "x-org-id": "test-org",
    "x-user-id": "test-user",
    "x-run-id": "test-run",
  };
}
