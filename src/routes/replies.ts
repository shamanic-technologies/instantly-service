/**
 * Replying to a prospect who wrote back — `POST /orgs/replies`.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId` (x-org-id). x-user-id is
 * additionally required, because the reply is sent under the org's own Instantly
 * key and key-service resolves it per user.
 *
 * The caller names WHO replied and on WHICH campaign, and supplies the words.
 * Everything about the sending identity — which mailbox, which persona, which
 * thread — is resolved by this service from what already happened. See
 * lib/reply-to-lead.
 */
import { Router, Request, Response } from "express";

import { ReplyToLeadBodySchema } from "../schemas";
import { replyToLead, ReplyToLeadError } from "../lib/reply-to-lead";

const router = Router();

router.post("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }

  const parsed = ReplyToLeadBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { campaign_id, email, body_html } = parsed.data;

  try {
    const result = await replyToLead({
      orgId,
      userId,
      campaignId: campaign_id,
      leadEmail: email,
      bodyHtml: body_html,
    });
    return res.status(200).json({ success: true, reply: result });
  } catch (error: unknown) {
    // A named refusal the caller can branch on.
    if (error instanceof ReplyToLeadError) {
      return res
        .status(error.status)
        .json({ error: error.message, code: error.code });
    }
    // Anything else is a 500 carrying its real cause. This service installs no
    // global error handler, so re-throwing here would leave the request hanging
    // — and a reply that failed for a reason we cannot name must NOT be dressed
    // up as one we can, so it keeps the generic status and no `code`.
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] reply-to-lead: failed for campaign=${campaign_id} lead=${email}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

export default router;
