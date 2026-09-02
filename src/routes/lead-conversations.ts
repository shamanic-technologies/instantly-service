/**
 * Reading the conversation with a prospect — `GET /orgs/conversations`.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId` (x-org-id), plus x-user-id —
 * the SAME identity `POST /orgs/replies` needs, because on the Instantly
 * transport the thread is read with the org's own key and key-service resolves
 * it per user. A caller that can answer a lead can read what that lead wrote,
 * with nothing extra.
 *
 * Org scope is enforced in the lookup itself (`org_id = <caller>`), so a
 * campaign belonging to another org reads as absent — never as a thread.
 */
import { Router, Request, Response } from "express";

import { LeadConversationQuerySchema } from "../schemas";
import {
  fetchLeadConversation,
  LeadConversationError,
} from "../lib/lead-conversation";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }

  const parsed = LeadConversationQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { campaign_id, email } = parsed.data;

  try {
    const conversation = await fetchLeadConversation({
      orgId,
      userId,
      campaignId: campaign_id,
      leadEmail: email,
    });
    return res.status(200).json({ success: true, conversation });
  } catch (error: unknown) {
    // A named refusal the caller can branch on: a conversation nobody has on
    // record (404) is a different fact from one we hold but could not read
    // (502), and both differ from an existing conversation that is empty (200).
    if (error instanceof LeadConversationError) {
      return res
        .status(error.status)
        .json({ error: error.message, code: error.code });
    }
    // Anything else keeps the generic status and carries its real cause. This
    // service installs no global error handler, so re-throwing would leave the
    // request hanging.
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] lead-conversation: failed for campaign=${campaign_id} lead=${email}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

export default router;
