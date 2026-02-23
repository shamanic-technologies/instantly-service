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

async function start() {
  const { runMigrations } = await import("./db/migrate");
  await runMigrations();
  app.listen(PORT, () => {
    console.log(`instantly-service running on port ${PORT}`);
  });
}

start().catch((err) => {
  console.error("[startup] Fatal error:", err);
  process.exit(1);
});

export { app };
