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
import bouncedEmailsRoutes from "./routes/bounced-emails";
import manualQualificationsRoutes from "./routes/manual-qualifications";
import leadOptOutsRoutes from "./routes/lead-optouts";
import followupCutsRoutes from "./routes/followup-cuts";
import repliesRoutes from "./routes/replies";
import leadConversationsRoutes from "./routes/lead-conversations";
import engagedLeadsRoutes from "./routes/engaged-leads";
import writtenToLeadsRoutes from "./routes/written-to-leads";
import emailSequencesRoutes from "./routes/email-sequences";
import emailSignatureRoutes from "./routes/email-signature";
import leadSendingScheduleRoutes from "./routes/lead-sending-schedule";
import campaignSendQueueRoutes from "./routes/campaign-send-queue";
import replyVerdictsRoutes from "./routes/reply-verdicts";
import auditRoutes from "./routes/audit";
import deliveryByVerdictRoutes from "./routes/delivery-by-verdict";
import incurredSendCostsRoutes from "./routes/incurred-send-costs";
import leadSourceYieldRoutes from "./routes/lead-source-yield";
import domainBlocklistRoutes from "./routes/domain-blocklist";
import infraRoutes from "./routes/infra";
import opsRoutes from "./routes/ops";
import outreachFactsRoutes from "./routes/outreach-facts";
import unsubscribeRoutes from "./routes/unsubscribe";
import selfSendRoutes from "./routes/self-send";
import clickRoutes from "./routes/click";
import { serviceAuth } from "./middleware/serviceAuth";
import { requireOrgId } from "./middleware/requireOrgId";

const app = express();

// One reverse proxy (Caddy) sits in front of every `*.distribute.you` service.
// Without this, `req.ip` is Caddy's own docker address on every request — which
// is exactly what the tracking-hit rows recorded, throwing away the client IP on
// all 771 of them. Trusting exactly ONE hop means express takes the address
// Caddy appended to `X-Forwarded-For`, so a client forging its own header cannot
// displace it.
app.set("trust proxy", 1);

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
// Opt-out link carried by mail we dispatch ourselves. Necessarily unauthenticated
// — a prospect clicks it from their inbox — and gated entirely by the HMAC in the
// URL. Inert until an account is flipped to send_transport='smtp', since no email
// in flight carries the link yet.
app.use("/u", unsubscribeRoutes);
// Click redirect for links in mail we dispatch ourselves. Same reasoning as /u:
// unauthenticated because a prospect follows it from their inbox, gated entirely
// by the HMAC, and the destination is inside the signed payload so a URL we did
// not mint redirects nowhere.
app.use("/c", clickRoutes);

// ─── Protected public routes (x-api-key only) ──────────────────────────────
app.use("/public", serviceAuth, analyticsPublicRoutes);

// ─── Internal routes (x-api-key only, no org context) ───────────────────────
app.use("/internal/campaigns", serviceAuth, campaignsRoutes);  // reconcile + retry-stuck triggers
app.use("/internal/accounts", serviceAuth, accountsRoutes);    // list all accounts
app.use("/internal/transfer-brand", serviceAuth, transferBrandRoutes);
app.use("/internal/bounced-emails", serviceAuth, bouncedEmailsRoutes); // fleet-wide bounce lookup (human-service serve gate)
app.use("/internal/outreach-facts", serviceAuth, outreachFactsRoutes); // fleet fact feed lead-service copies by cursor
app.use("/internal", serviceAuth, domainBlocklistRoutes);      // sending domains on public blocklists (staff console)
app.use("/internal", serviceAuth, leadSourceYieldRoutes);      // lead sources ranked by bounces / refusals / blocklist hits (staff console)
app.use("/internal", serviceAuth, deliveryByVerdictRoutes);    // fleet delivery split by the verifier verdict at send (features-service)
app.use("/internal/incurred-send-costs", serviceAuth, incurredSendCostsRoutes); // late, never-charged declaration of the 08-24..10-02 sends (manual)
app.use("/internal/audit", serviceAuth, auditRoutes);          // staff sending forecast (capacity vs scheduled volume)
app.use("/internal/infra", serviceAuth, infraRoutes);          // provider inventory sync (Gandi / Mailforge / Primeforge / DFY)
app.use("/internal/ops", serviceAuth, opsRoutes);            // unified model: mailbox sync (PR 1), gold reads later
app.use("/internal/self-send", serviceAuth, selfSendRoutes);   // dispatch sweep for the smtp transport

// ─── Org-scoped routes (x-api-key + x-org-id required, rest optional) ───────
app.use("/orgs/send", serviceAuth, requireOrgId, sendRoutes);
app.use("/orgs/status", serviceAuth, requireOrgId, statusRoutes);
app.use("/orgs/campaigns", serviceAuth, requireOrgId, campaignsRoutes);
app.use("/orgs/campaigns", serviceAuth, requireOrgId, leadsRoutes);
app.use("/orgs/accounts", serviceAuth, requireOrgId, accountsRoutes);
app.use("/orgs/manual-qualifications", serviceAuth, requireOrgId, manualQualificationsRoutes);
app.use("/orgs/opt-outs", serviceAuth, requireOrgId, leadOptOutsRoutes);
app.use("/orgs/followup-cuts", serviceAuth, requireOrgId, followupCutsRoutes); // stop the rest of a sequence for leads emailed in error (not an opt-out)
app.use("/orgs/replies", serviceAuth, requireOrgId, repliesRoutes);
app.use("/orgs/conversations", serviceAuth, requireOrgId, leadConversationsRoutes);
app.use("/orgs/engaged-leads", serviceAuth, requireOrgId, engagedLeadsRoutes);
app.use("/orgs/written-to-leads", serviceAuth, requireOrgId, writtenToLeadsRoutes);
app.use("/orgs/email-sequences", serviceAuth, requireOrgId, emailSequencesRoutes);
app.use("/orgs/email-signature", serviceAuth, requireOrgId, emailSignatureRoutes);
app.use("/orgs/sending-schedule", serviceAuth, requireOrgId, leadSendingScheduleRoutes);
app.use("/orgs/send-queue", serviceAuth, requireOrgId, campaignSendQueueRoutes); // what is queued to send for some campaigns, and when the window opens (CAMPAIGN-NEXT-STEPS-1018)
app.use("/orgs/reply-verdicts", serviceAuth, requireOrgId, replyVerdictsRoutes);
app.use("/orgs", serviceAuth, requireOrgId, analyticsRoutes);

const PORT = process.env.PORT || 3011;

async function deployEmailTemplates(): Promise<void> {
  try {
    const { deployTemplates } = await import("./lib/email-client");
    const { EMAIL_TEMPLATES } = await import("./lib/email-templates");
    await deployTemplates(
      {
        appId: "instantly-service",
        templates: EMAIL_TEMPLATES,
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

    // Decide and promote self-send clicks. The `/c/` redirect records a hit
    // undecided — a corporate link scanner fetches every URL in a message before
    // the human sees it, and the signal that proves it (the same lead fetching
    // the opt-out link seconds either side) can arrive after the click. This
    // drain is the ONLY path by which a self-send click reaches silver, so it is
    // armed unconditionally, after the port is bound.
    // Keep the `messages` projection (one row per email, every typology) fresh
    // for the ops reads. State is the unique source index, so a deploy mid-tick
    // loses nothing; a 10-minute in-process interval, never a cron.
    // One current verdict per real reply (lib/reply-verdicts), refreshed every
    // two minutes from the kind events ingestion already writes.
    import("./lib/reply-verdicts-worker")
      .then(({ startReplyVerdictsWorker }) => startReplyVerdictsWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start reply-verdicts worker: ${message}`);
      });

    // The outreach fact feed (lib/outreach-facts): judge owed replies with Jev,
    // then append new facts. The first pass on an empty feed is the backfill.
    import("./lib/outreach-facts-worker")
      .then(({ startOutreachFactsWorker }) => startOutreachFactsWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start outreach-facts worker: ${message}`);
      });

    import("./lib/messages-sync-worker")
      .then(({ startMessagesSyncWorker }) => startMessagesSyncWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start messages-sync worker: ${message}`);
      });

    // Mirror what Instantly sent (followups included) within minutes, not at
    // the next daily cron: the conversation read trusts a non-empty mirror.
    import("./lib/unibox-mirror-worker")
      .then(({ startUniboxMirrorWorker }) => startUniboxMirrorWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start unibox-mirror worker: ${message}`);
      });

    // Qualify the replies Instantly never gave a verdict on. Every side effect
    // this service runs on a reply gates on the reply KIND, so a missing verdict
    // leaves a buyer unanswered and an opt-out unrecorded with nothing logged.
    // An in-process interval, never a cron — the same slip that made the hourly
    // dispatch cron deliver 6.2 runs a day would apply here.
    import("./lib/reply-qualification-worker")
      .then(({ startReplyQualificationWorker }) => startReplyQualificationWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          `[instantly-service] failed to start qualification-fallback worker: ${message}`,
        );
      });

    // Deliver, and retry, the outcome triggers this service fires (a click on one
    // of our links = a website visit, lib/website-visit-trigger). Armed after listen.
    import("./lib/outcome-trigger-worker")
      .then(({ startOutcomeTriggerWorker }) => startOutcomeTriggerWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start outcome-trigger worker: ${message}`);
      });

    import("./lib/self-send/click-promotion-worker")
      .then(({ startClickPromotionWorker }) => startClickPromotionWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          `[instantly-service] failed to start click-promotion worker: ${message}`,
        );
      });

    // The cloud networks a click scanner fetches from (`datacenter_network`
    // click verdict), refreshed daily from RIPEstat. Armed after listen.
    import("./lib/self-send/datacenter-networks-worker")
      .then(({ startDatacenterNetworksWorker }) => startDatacenterNetworksWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start datacenter-networks worker: ${message}`);
      });

    // Send the self-send steps that have come due. An in-process interval, NOT
    // a cron: `self-send-cron.yml` declares hourly and GitHub Actions delivered
    // 6.2 runs a day, leaving leads hours past due and the fleet queue growing.
    // Armed after the port is bound; every scheduling rule stays in the sweep.
    import("./lib/self-send/dispatch-scheduler")
      .then(({ startSelfSendDispatchWorker }) => startSelfSendDispatchWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          `[instantly-service] failed to start self-send dispatch worker: ${message}`,
        );
      });

    // A person who booked a meeting gets no more cold email from that brand: its own
    // drive, because the dispatch interval restarts on every deploy. Armed after listen.
    // Copy of apollo-service's verification history, read by GET
    // /internal/delivery-by-verdict. Armed after listen.
    import("./lib/verification-history-mirror-worker")
      .then(({ startVerificationHistoryMirrorWorker }) => startVerificationHistoryMirrorWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start verification-history-mirror worker: ${message}`);
      });

    // Check every sending domain on the public domain blocklists once a day; a
    // listed domain's mailboxes take no new sequence (DOMAIN-BLOCKLIST-1016).
    import("./lib/domain-blocklist-worker")
      .then(({ startDomainBlocklistWorker }) => startDomainBlocklistWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start domain-blocklist worker: ${message}`);
      });
    // Copy of lead-service's lead provenance for GET /internal/lead-source-yield
    // (SOURCE-YIELD-1016). Armed after listen.
    import("./lib/lead-provenance-sync-worker")
      .then(({ startLeadProvenanceWorker }) => startLeadProvenanceWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start lead-provenance worker: ${message}`);
      });
    import("./lib/booked-stops-cold-worker")
      .then(({ startBookedStopsWorker }) => startBookedStopsWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start booked-stops-cold worker: ${message}`);
      });

    // A send the process died in the middle of (OOM, deploy): its reservation is
    // released and its orphan step costs cancelled (lib/stale-reservations).
    import("./lib/stale-reservation-worker")
      .then(({ startStaleReservationWorker }) => startStaleReservationWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start stale-reservation worker: ${message}`);
      });

    // A queued sequence the caller sent with no zone takes the person's zone from
    // lead-service, so it is emailed in THEIR business hours. Armed after listen.
    import("./lib/queued-sequence-timezone-worker")
      .then(({ startTimezoneHealWorker }) => startTimezoneHealWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start queued-sequence-timezone worker: ${message}`);
      });

    // A conversation one of our people holds with a lead from their own Gmail:
    // folded into the lead's thread on the mirror's own cadence. Armed after listen.
    import("./lib/self-send/gmail-threads-worker")
      .then(({ startGmailThreadsWorker }) => startGmailThreadsWorker())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start gmail-threads worker: ${message}`);
      });

    // Read a prospect's reply the moment it lands: one IMAP IDLE session per
    // self-send mailbox, independent of whether anything is due to send.
    // Armed after the port is bound — ~123 logins must never block listen.
    import("./lib/self-send/inbox-watcher")
      .then(({ startInboxWatcher }) => startInboxWatcher())
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[instantly-service] failed to start inbox watcher: ${message}`);
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
