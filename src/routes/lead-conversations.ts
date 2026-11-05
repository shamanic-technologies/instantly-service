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
import { messageOf } from "../lib/db-error.js";
import { errorMessage } from "../lib/db-error.js";
import { Router, Request, Response } from "express";

import {
  LeadConversationMessagesQuerySchema,
  LeadConversationQuerySchema,
} from "../schemas";
import {
  fetchLeadConversation,
  LeadConversationError,
  type LeadConversation,
} from "../lib/lead-conversation";
import {
  pickConversationMessages,
  toConversationIndex,
} from "../lib/conversation-index";

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
        .json({ error: messageOf(error), code: error.code });
    }
    // Anything else keeps the generic status and carries its real cause. This
    // service installs no global error handler, so re-throwing would leave the
    // request hanging.
    const message = errorMessage(error);
    console.error(
      `[instantly-service] lead-conversation: failed for campaign=${campaign_id ?? "(any)"} lead=${email}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

/**
 * Shared by the two-step reads (AGENT-READS-THREAD-1105): resolve the
 * conversation exactly as `GET /` does, with the same refusals, then hand it to
 * `respond`. Never a second lookup.
 */
async function withConversation(
  req: Request,
  res: Response,
  query: { campaign_id?: string; email: string },
  respond: (c: LeadConversation) => Response,
) {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }
  try {
    const conversation = await fetchLeadConversation({
      orgId,
      userId,
      campaignId: query.campaign_id,
      leadEmail: query.email,
    });
    return respond(conversation);
  } catch (error: unknown) {
    if (error instanceof LeadConversationError) {
      return res
        .status(error.status)
        .json({ error: messageOf(error), code: error.code });
    }
    const message = errorMessage(error);
    console.error(
      `[instantly-service] lead-conversation ${req.path}: failed for campaign=${query.campaign_id ?? "(any)"} lead=${query.email}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
}

/**
 * Step 1: every message, short, NEWEST FIRST — small whatever the thread's
 * length, and a reader that truncates only ever loses the oldest lines.
 */
router.get("/index", async (req: Request, res: Response) => {
  const parsed = LeadConversationQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  return withConversation(req, res, parsed.data, (conversation) =>
    res
      .status(200)
      .json({ success: true, conversation: toConversationIndex(conversation) }),
  );
});

/** Step 2: the messages asked for by id, in full, in the order asked. */
router.get("/messages", async (req: Request, res: Response) => {
  const parsed = LeadConversationMessagesQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { ids, ...query } = parsed.data;
  return withConversation(req, res, query, (conversation) => {
    const picked = pickConversationMessages(conversation, ids);
    if (picked.missing.length > 0) {
      return res.status(404).json({
        error: `No message ${picked.missing.join(", ")} in this conversation with ${query.email}`,
        code: "message_not_found",
        missingIds: picked.missing,
      });
    }
    return res.status(200).json({
      success: true,
      campaignId: conversation.campaignId,
      leadEmail: conversation.leadEmail,
      messages: picked.messages,
    });
  });
});

export default router;
