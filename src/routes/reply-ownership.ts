/**
 * `POST /orgs/reply-ownership` — does the email path act on these mails?
 * (EMAIL-REPLY-ORPHAN-1027, rule in `src/lib/reply-ownership.ts`.)
 *
 * Auth: `serviceAuth` + `requireOrgId`. Fails loud: a guessed answer is either
 * a dropped reply or a doubled one.
 */
import { errorMessage } from "../lib/db-error.js";
import { Router, Request, Response } from "express";

import { readReplyOwnership } from "../lib/reply-ownership";
import { ReplyOwnershipRequestSchema } from "../schemas";

const router = Router();

router.post("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = ReplyOwnershipRequestSchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  try {
    const messages = await readReplyOwnership(
      orgId,
      parsed.data.messages.map((m) => ({ ...m, at: new Date(m.at) })),
    );
    return res.status(200).json({ messages });
  } catch (error: unknown) {
    const message = errorMessage(error);
    console.error(`[instantly-service] reply-ownership: failed for org=${orgId}: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
