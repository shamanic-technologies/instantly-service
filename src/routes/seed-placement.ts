/**
 * Staff routes for the Microsoft seed receivers and the seed placement daily
 * series (MS-SEED-PLACEMENT-1018). Platform-scoped (no org), service-auth.
 */
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

const router = Router();

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
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] microsoft seed authorize failed: ${message}`);
    return res.status(error instanceof MicrosoftOAuthError ? 502 : 500).json({ error: message });
  }
});

/** Every Microsoft seed mailbox and its state (never a token). */
router.get("/microsoft", async (_req: Request, res: Response) => {
  try {
    return res.json({ mailboxes: await listMicrosoftSeedMailboxes() });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
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
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] seed placement daily read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

/** Recompute the stored series from bronze now (every seed sync does it too). */
router.post("/daily/refresh", async (_req: Request, res: Response) => {
  try {
    return res.json({ rows: await refreshSeedPlacementDaily() });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ error: message });
  }
});

export default router;
