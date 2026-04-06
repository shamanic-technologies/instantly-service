import express from "express";
import cors from "cors";
import * as dotenv from "dotenv";
import { readFileSync, existsSync } from "fs";
import path from "path";

dotenv.config();

import healthRoutes from "./routes/health";
import campaignsRoutes from "./routes/campaigns";
import leadsRoutes from "./routes/leads";
import accountsRoutes from "./routes/accounts";
import analyticsRoutes from "./routes/analytics";
import analyticsPublicRoutes from "./routes/analytics-public";
import webhooksRoutes from "./routes/webhooks";
import sendRoutes from "./routes/send";
import statusRoutes from "./routes/status";
import { serviceAuth } from "./middleware/serviceAuth";
import { requireOrgId } from "./middleware/requireOrgId";

const app = express();

app.use(cors());
app.use(express.json());

// OpenAPI spec endpoint
const openapiPath = path.join(__dirname, "..", "openapi.json");
app.get("/openapi.json", (_req, res) => {
  if (existsSync(openapiPath)) {
    res.json(JSON.parse(readFileSync(openapiPath, "utf-8")));
  } else {
    res
      .status(404)
      .json({ error: "OpenAPI spec not generated. Run: npm run generate:openapi" });
  }
});

// ─── Public routes (no auth) ────────────────────────────────────────────────
app.use(healthRoutes);
app.use("/webhooks", webhooksRoutes);

// ─── Protected public routes (x-api-key only) ──────────────────────────────
app.use("/public", serviceAuth, analyticsPublicRoutes);

// ─── Internal routes (x-api-key only, no org context) ───────────────────────
app.use("/internal/campaigns", serviceAuth, campaignsRoutes);  // check-status
app.use("/internal/accounts", serviceAuth, accountsRoutes);    // list all accounts

// ─── Org-scoped routes (x-api-key + x-org-id required, rest optional) ───────
app.use("/orgs/send", serviceAuth, requireOrgId, sendRoutes);
app.use("/orgs/status", serviceAuth, requireOrgId, statusRoutes);
app.use("/orgs/campaigns", serviceAuth, requireOrgId, campaignsRoutes);
app.use("/orgs/campaigns", serviceAuth, requireOrgId, leadsRoutes);
app.use("/orgs/accounts", serviceAuth, requireOrgId, accountsRoutes);
app.use("/orgs", serviceAuth, requireOrgId, analyticsRoutes);

const PORT = process.env.PORT || 3011;

async function deployEmailTemplates(): Promise<void> {
  try {
    const { deployTemplates } = await import("./lib/email-client");
    await deployTemplates(
      {
        appId: "instantly-service",
        templates: [
          {
            name: "campaign-error",
            subject: "[Instantly] Campaign error: {{campaignId}}",
            htmlBody: [
              "<h2>Campaign Error Detected</h2>",
              "<p><strong>Campaign ID:</strong> {{campaignId}}</p>",
              "<p><strong>Lead Email:</strong> {{leadEmail}}</p>",
              "<p><strong>Instantly Campaign ID:</strong> {{instantlyCampaignId}}</p>",
              "<p><strong>Error:</strong></p>",
              "<pre>{{errorReason}}</pre>",
            ].join("\n"),
          },
        ],
      },
      { orgId: "system", userId: "system", runId: "system" },
    );
    console.log("[startup] Email templates deployed");
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`[startup] Failed to deploy email templates (non-fatal): ${message}`);
  }
}

async function start() {
  const { runMigrations } = await import("./db/migrate");
  await runMigrations();
  await deployEmailTemplates();
  app.listen(PORT, () => {
    console.log(`instantly-service running on port ${PORT}`);
  });
}

start().catch((err) => {
  console.error("[startup] Fatal error:", err);
  process.exit(1);
});

export { app };
