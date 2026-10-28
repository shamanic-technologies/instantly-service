/**
 * `GET /internal/sent-messages` (staff, platform-scoped, read-only;
 * FACTORY-RECORD-SEND-1028): the exact messages we sent (raw MIME, base64,
 * sha256) with what they were built from and why they left when they did
 * (`lib/self-send/send-record.ts`). Filter by campaign, lead or Message-Id.
 */

import { errorMessage } from "../lib/db-error.js";
import { Router, type Request, type Response } from "express";

import { loadSentMessages } from "../lib/self-send/send-record";

export const SENT_MESSAGES_DEFAULT_LIMIT = 50;
export const SENT_MESSAGES_MAX_LIMIT = 500;

const router = Router();

const text = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() !== "" ? value.trim() : undefined;

router.get("/", async (req: Request, res: Response) => {
  const raw = req.query.limit;
  const limit = raw === undefined ? SENT_MESSAGES_DEFAULT_LIMIT : Number(raw);
  if (!Number.isInteger(limit) || limit < 1 || limit > SENT_MESSAGES_MAX_LIMIT) {
    return res.status(400).json({ error: `limit must be an integer 1..${SENT_MESSAGES_MAX_LIMIT}` });
  }
  try {
    const messages = await loadSentMessages({
      instantlyCampaignId: text(req.query.instantlyCampaignId),
      leadEmail: text(req.query.leadEmail),
      messageId: text(req.query.messageId),
      limit,
    });
    return res.json({ messages });
  } catch (error) {
    const message = errorMessage(error);
    console.error(`[instantly-service] sent-messages read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
