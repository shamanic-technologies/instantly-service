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
import transferBrandRoutes from "./routes/transfer-brand";
import manualQualificationsRoutes from "./routes/manual-qualifications";
import auditRoutes from "./routes/audit";
import { serviceAuth } from "./middleware/serviceAuth";
import { requireOrgId } from "./middleware/requireOrgId";

const app = express();

app.use(cors());
app.use(express.json({ limit: "10mb" }));

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
app.use("/internal/campaigns", serviceAuth, campaignsRoutes);  // reconcile + retry-stuck triggers
app.use("/internal/accounts", serviceAuth, accountsRoutes);    // list all accounts
app.use("/internal/transfer-brand", serviceAuth, transferBrandRoutes);
app.use("/internal/audit", serviceAuth, auditRoutes);          // staff sending forecast (capacity vs scheduled volume)

// ─── Org-scoped routes (x-api-key + x-org-id required, rest optional) ───────
app.use("/orgs/send", serviceAuth, requireOrgId, sendRoutes);
app.use("/orgs/status", serviceAuth, requireOrgId, statusRoutes);
app.use("/orgs/campaigns", serviceAuth, requireOrgId, campaignsRoutes);
app.use("/orgs/campaigns", serviceAuth, requireOrgId, leadsRoutes);
app.use("/orgs/accounts", serviceAuth, requireOrgId, accountsRoutes);
app.use("/orgs/manual-qualifications", serviceAuth, requireOrgId, manualQualificationsRoutes);
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
          {
            // Positive-reply forward: Instantly qualified an inbound reply as
            // positive/interested → we email the full conversation thread here so
            // the agency never depends on the paid Instantly Unibox/CRM. The
            // email is a CLEAN, client-forwardable thread — subject = the
            // conversation's real subject, body = just the conversation (no
            // branding, no notes, no metadata). Rendered plain text into a
            // <pre> with an inherited font + wrapping, so it reads like a normal
            // email (not monospace) and is robust to the engine's escaping.
            name: "positive-reply-forward",
            subject: "{{subject}}",
            htmlBody:
              '<pre style="font-family:inherit;white-space:pre-wrap;word-break:break-word;margin:0">{{thread}}</pre>',
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
    console.log(`[instantly-service] running on port ${PORT}`);
    // Start the retry-stuck worker AFTER the port is bound. The worker is a
    // continuous loop processing one stuck row at a time; throughput is
    // naturally bounded by the instantly-client throttle.
    import("./lib/retry-stuck-worker")
      .then(({ startRetryStuckWorker }) => startRetryStuckWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          `[instantly-service] failed to start retry-stuck worker: ${message}`,
        );
      });

    // Seed the account lifecycle shortly after boot (fire-and-forget, AFTER the
    // port is bound — snapshot + reconcile is O(fleet size) paginated Instantly
    // calls, must never block listen). Idempotent: a subsequent boot finds the
    // lifecycle unchanged (silver persists across deploys) → no event, no PATCH.
    // Without this the send gate would be dead until the 6h placement cron fires.
    (async () => {
      const { resolvePlatformInstantlyApiKey } = await import("./lib/key-client");
      const { snapshotAccounts, reconcileLifecycle } = await import(
        "./lib/account-lifecycle-sync"
      );
      const apiKey = await resolvePlatformInstantlyApiKey({
        method: "POST",
        path: "/boot/account-lifecycle-seed",
      });
      const snapshot = await snapshotAccounts(apiKey);
      const lifecycle = await reconcileLifecycle(apiKey);
      console.log(
        `[instantly-service] account-lifecycle seed done ${JSON.stringify({ snapshot, lifecycle })}`,
      );
    })().catch((err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      console.error(
        `[instantly-service] account-lifecycle seed failed (non-fatal): ${message}`,
      );
    });
  });
}

start().catch((err) => {
  console.error("[startup] Fatal error:", err);
  process.exit(1);
});

export { app };
