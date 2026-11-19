/**
 * Staff routes for the Microsoft seed receivers and the seed placement daily
 * series (MS-SEED-PLACEMENT-1018). Platform-scoped (no org), service-auth.
 */
import { errorMessage } from "../lib/db-error.js";
import { Router, Request, Response } from "express";

import {
  MicrosoftOAuthError,
  listMicrosoftSeedMailboxes,
  startMicrosoftAuthorization,
} from "../lib/seed-placement/microsoft-oauth";
import {
  SEED_DAILY_DEFAULT_DAYS,
  SEED_DAILY_MAX_DAYS,
  loadSeedPlacementDaily,
  refreshSeedPlacementDaily,
} from "../lib/seed-placement/daily";
import { loadOptOutComparison } from "../lib/seed-placement/opt-out-choice";
import { listPostmarkSendingDomains, onboardPostmarkDomains } from "../lib/self-send/postmark-domains";
import { pollPostmarkBounces } from "../lib/self-send/postmark-bounces";
import { listSesSendingDomains, onboardSesDomains } from "../lib/self-send/ses-domains";
import {
  MAX_COMPARISON_COPIES,
  loadPlacementComparison,
  mintComparisonTestId,
  runPlacementComparison,
} from "../lib/seed-placement/comparison";

const router = Router();

/** Gandi domains moving to Postmark: state per domain (POSTMARK-COLD-1117). */
router.get("/postmark-domains", async (_req: Request, res: Response) => {
  try {
    return res.json({ domains: await listPostmarkSendingDomains() });
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

/** Move domains one step now. `dryRun` defaults TRUE; `domains` narrows the candidates. */
router.post("/postmark-domains/onboard", async (req: Request, res: Response) => {
  const dryRun = req.body?.dryRun !== false;
  const domains = Array.isArray(req.body?.domains) ? req.body.domains.map(String) : undefined;
  try {
    return res.json(await onboardPostmarkDomains({ dryRun, domains }));
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

/** Gandi domains on Amazon SES: state per domain (SES-COLD-1119). */
router.get("/ses-domains", async (_req: Request, res: Response) => {
  try {
    return res.json({ domains: await listSesSendingDomains() });
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

/** Move domains one step toward SES now. `dryRun` defaults TRUE (read-only); `domains` narrows. */
router.post("/ses-domains/onboard", async (req: Request, res: Response) => {
  const dryRun = req.body?.dryRun !== false;
  const domains = Array.isArray(req.body?.domains) ? req.body.domains.map(String) : undefined;
  try {
    return res.json(await onboardSesDomains({ dryRun, domains }));
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

/** File Postmark's bounce reports now (the worker does it every 15 min). */
router.post("/postmark-bounces", async (_req: Request, res: Response) => {
  try {
    return res.json(await pollPostmarkBounces());
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

/**
 * One-off placement comparison (POSTMARK-PLACEMENT-1117): the same email from
 * Postmark senders and from our mailboxes to the weekly test's seed receivers.
 * 202 + background (`seed-placement-comparison: done`); read the result with
 * GET /comparison/:testId after the next sync (POST /internal/audit/seed-placement/sync).
 */
router.post("/comparison", async (req: Request, res: Response) => {
  const body = req.body ?? {};
  const subject = typeof body.subject === "string" ? body.subject.trim() : "";
  const bodyHtml = typeof body.bodyHtml === "string" ? body.bodyHtml.trim() : "";
  const postmarkSenders = Array.isArray(body.postmarkSenders) ? body.postmarkSenders : [];
  const mailboxes = Array.isArray(body.mailboxes) ? body.mailboxes : [];
  const copies = body.copies === undefined ? 1 : Number(body.copies);
  const validSenders = postmarkSenders.every(
    (s: unknown) => typeof s === "object" && s !== null && typeof (s as { email?: unknown }).email === "string" && (s as { email: string }).email.includes("@"),
  );
  if (!subject || !bodyHtml) return res.status(400).json({ error: "subject and bodyHtml are required" });
  if (!validSenders || !mailboxes.every((m: unknown) => typeof m === "string" && m.includes("@"))) {
    return res.status(400).json({ error: "postmarkSenders = [{email, firstName?, lastName?}], mailboxes = [email]" });
  }
  if (postmarkSenders.length + mailboxes.length === 0) return res.status(400).json({ error: "at least one sender is required" });
  if (!Number.isInteger(copies) || copies < 1 || copies > MAX_COMPARISON_COPIES) {
    return res.status(400).json({ error: `copies must be an integer 1..${MAX_COMPARISON_COPIES}` });
  }
  const testId = mintComparisonTestId();
  res.status(202).json({ testId });
  try {
    const summary = await runPlacementComparison({ testId, subject, bodyHtml, postmarkSenders, mailboxes, copies });
    console.log(`[instantly-service] seed-placement-comparison: done ${JSON.stringify(summary)}`);
  } catch (error) {
    console.error(`[instantly-service] seed-placement-comparison ${testId} failed: ${errorMessage(error)}`);
  }
});

/** Result per sending path x provider: inbox / promotions / spam / missing, plus per sender. */
router.get("/comparison/:testId", async (req: Request, res: Response) => {
  try {
    const result = await loadPlacementComparison(String(req.params.testId));
    if (!result) return res.status(404).json({ error: "unknown comparison test" });
    return res.json(result);
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

/**
 * Start authorizing one Microsoft mailbox as a seed receiver. Answers the code
 * and the URL a person opens to sign in as that mailbox (valid ~15 minutes);
 * the service waits for the sign-in in the background.
 */
router.post("/microsoft/authorize", async (req: Request, res: Response) => {
  const email = typeof req.body?.email === "string" ? req.body.email : "";
  if (!email.includes("@")) return res.status(400).json({ error: "body.email (a Microsoft mailbox address) is required" });
  try {
    const started = await startMicrosoftAuthorization(email);
    return res.json({
      ...started,
      instructions: `Open ${started.verificationUri}, enter ${started.userCode}, sign in as ${started.email} and accept. Then GET /internal/seed-placement/microsoft shows status "authorized".`,
    });
  } catch (error) {
    const message = errorMessage(error);
    console.error(`[instantly-service] microsoft seed authorize failed: ${message}`);
    return res.status(error instanceof MicrosoftOAuthError ? 502 : 500).json({ error: message });
  }
});

/** Every Microsoft seed mailbox and its state (never a token). */
router.get("/microsoft", async (_req: Request, res: Response) => {
  try {
    return res.json({ mailboxes: await listMicrosoftSeedMailboxes() });
  } catch (error) {
    const message = errorMessage(error);
    return res.status(500).json({ error: message });
  }
});

/** The stored daily series: Google and Microsoft inbox % per day, split by sending vendor. */
router.get("/daily", async (req: Request, res: Response) => {
  const raw = req.query.days;
  const days = raw === undefined ? SEED_DAILY_DEFAULT_DAYS : Number(raw);
  if (!Number.isInteger(days) || days < 1 || days > SEED_DAILY_MAX_DAYS) {
    return res.status(400).json({ error: `days must be an integer 1..${SEED_DAILY_MAX_DAYS}` });
  }
  try {
    return res.json(await loadSeedPlacementDaily(days));
  } catch (error) {
    const message = errorMessage(error);
    console.error(`[instantly-service] seed placement daily read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

/**
 * The three opt-out headers compared on seeds (UNSUB-3WAY-1103): totals per
 * variant and provider, the daily series, and the variant prospect mail carries.
 */
router.get("/opt-out", async (_req: Request, res: Response) => {
  try {
    return res.json(await loadOptOutComparison());
  } catch (error) {
    const message = errorMessage(error);
    console.error(`[instantly-service] seed opt-out comparison read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

/** Recompute the stored series from bronze now (every seed sync does it too). */
router.post("/daily/refresh", async (_req: Request, res: Response) => {
  try {
    return res.json({ rows: await refreshSeedPlacementDaily() });
  } catch (error) {
    const message = errorMessage(error);
    return res.status(500).json({ error: message });
  }
});

export default router;
