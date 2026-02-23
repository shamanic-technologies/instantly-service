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
import webhooksRoutes from "./routes/webhooks";
import sendRoutes from "./routes/send";
import { serviceAuth } from "./middleware/serviceAuth";

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

// Public routes (no auth)
app.use(healthRoutes);
app.use("/webhooks", webhooksRoutes);

// Protected routes (require X-API-Key)
app.use("/send", serviceAuth, sendRoutes);
app.use("/campaigns", serviceAuth, campaignsRoutes);
app.use("/campaigns", serviceAuth, leadsRoutes);
app.use("/accounts", serviceAuth, accountsRoutes);
app.use("/", serviceAuth, analyticsRoutes);

const PORT = process.env.PORT || 3011;

async function deployEmailTemplates(): Promise<void> {
  try {
    const { deployTemplates } = await import("./lib/email-client");
    await deployTemplates({
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
    });
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
