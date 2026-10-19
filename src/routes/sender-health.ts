/**
 * `GET /internal/sender-health?days=` (staff, platform-scoped): which sending
 * mailboxes the dispatcher keeps out of prospect mail and why, now and per day
 * (`lib/sender-health-daily.ts`, SENDER-HEALTH-1019). The admin chart's read.
 */

import { Router, type Request, type Response } from "express";

import {
  SENDER_HEALTH_DEFAULT_DAYS,
  SENDER_HEALTH_MAX_DAYS,
  loadSenderHealth,
} from "../lib/sender-health-daily";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const raw = req.query.days;
  const days = raw === undefined ? SENDER_HEALTH_DEFAULT_DAYS : Number(raw);
  if (!Number.isInteger(days) || days < 1 || days > SENDER_HEALTH_MAX_DAYS) {
    return res.status(400).json({ error: `days must be an integer 1..${SENDER_HEALTH_MAX_DAYS}` });
  }
  try {
    return res.json(await loadSenderHealth(days));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] sender-health read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
