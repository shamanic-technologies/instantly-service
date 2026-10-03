/**
 * `POST /internal/bounced-emails` — which of these addresses bounced on one of
 * our sends, fleet-wide (see lib/bounced-emails for why it is not org-scoped and
 * why every row is a hard bounce).
 *
 * Auth: `serviceAuth` only. A bounce is a fact about the address, so there is no
 * org to scope to; the caller (human-service's serve path) asks across orgs on
 * purpose.
 *
 * Fails loud: an empty list on a read failure would claim nobody bounced.
 */
import { Router, Request, Response } from "express";

import { findBouncedEmails } from "../lib/bounced-emails";
import { BouncedEmailsRequestSchema } from "../schemas";

const router = Router();

router.post("/", async (req: Request, res: Response) => {
  const parsed = BouncedEmailsRequestSchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  try {
    const bounced = await findBouncedEmails(parsed.data.emails);
    return res.status(200).json({ bounced });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] bounced-emails: read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
