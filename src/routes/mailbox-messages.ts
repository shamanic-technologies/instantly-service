/**
 * The mailbox-messages feed (lib/mailbox-messages, EVERY-PERSON-ON-THE-THREAD-1109).
 * Platform-scoped (service key only, fleet-wide), read-only, DB-only.
 *
 *  - `GET  /internal/mailbox-messages?after=&limit=&threadKey=` — versions after
 *    the cursor, or one thread's messages.
 *  - `POST /internal/mailbox-messages/sync` — re-mark a window of bronze and
 *    drain now (`{ sinceDays?, drainBatches? }`).
 */
import { errorMessage } from "../lib/db-error.js";
import { Router, Request, Response } from "express";

import {
  drainMailboxMessages,
  MAILBOX_MESSAGES_DEFAULT_LIMIT,
  markMailboxHistory,
  readMailboxMessages,
} from "../lib/mailbox-messages";
import { MailboxMessagesQuerySchema, MailboxMessagesSyncSchema } from "../schemas";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const parsed = MailboxMessagesQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.issues.map((i) => i.message).join("; ") });
  }
  try {
    const { after, limit, threadKey } = parsed.data;
    const page = await readMailboxMessages({
      after: after ?? "0",
      limit: limit ?? MAILBOX_MESSAGES_DEFAULT_LIMIT,
      ...(threadKey ? { threadKey } : {}),
    });
    return res.status(200).json(page);
  } catch (error: unknown) {
    const message = errorMessage(error);
    console.error(`[instantly-service] mailbox-messages read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

router.post("/sync", async (req: Request, res: Response) => {
  const parsed = MailboxMessagesSyncSchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  try {
    const marked =
      parsed.data.sinceDays === undefined ? 0 : await markMailboxHistory(parsed.data.sinceDays);
    const drains = [];
    for (let i = 0; i < (parsed.data.drainBatches ?? 1); i += 1) {
      const d = await drainMailboxMessages(500);
      drains.push(d);
      if (d.drained === 0) break;
    }
    return res.status(200).json({ marked, drains });
  } catch (error: unknown) {
    const message = errorMessage(error);
    console.error(`[instantly-service] mailbox-messages sync failed: ${message}`);
    return res.status(500).json({ error: "mailbox-messages sync failed", detail: message });
  }
});

export default router;
