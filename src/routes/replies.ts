/**
 * Replying to a prospect who wrote back — `POST /orgs/replies`.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId` (x-org-id). x-user-id is
 * additionally required, because the reply is sent under the org's own Instantly
 * key and key-service resolves it per user.
 *
 * The caller names WHO replied (and optionally WHICH campaign), and supplies the
 * words. Without a campaign the prospect's one thread is found here.
 * Everything about the sending identity — which mailbox, which persona, which
 * thread — is resolved by this service from what already happened. See
 * lib/reply-to-lead.
 */
import { Router, Request, Response } from "express";

import { EscalateReplyBodySchema, ReplyToLeadBodySchema } from "../schemas";
import { escalateReply, EscalateReplyError } from "../lib/escalate-reply";
import { resolveReplySender } from "../lib/human-takeover";
import { replyToLead, ReplyToLeadError } from "../lib/reply-to-lead";
import {
  AmbiguousReplyThreadError,
  resolveReplyCampaignId,
} from "../lib/resolve-reply-thread";
import { handThreadFollowupsToHuman } from "../lib/hand-followups-to-human";
import { countHumanAnswerAsOurs } from "../lib/count-human-answer";
import type { HumanTookOverError } from "../lib/reply-to-lead-error";

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
  const { email, body_html, sent_by, attachments } = parsed.data;
  // Named campaign wins; else the prospect's one thread (lib/resolve-reply-thread).
  let campaign_id = parsed.data.campaign_id;

  try {
    campaign_id = await resolveReplyCampaignId(orgId, email, campaign_id);
    const outcome = await replyToLead({
      orgId,
      userId,
      campaignId: campaign_id,
      leadEmail: email,
      bodyHtml: body_html,
      // Absent on the wire means the automated responder: it is the only caller
      // today, so the takeover gate is live without waiting on anyone to
      // declare. See `DEFAULT_REPLY_SENDER`.
      sentBy: resolveReplySender(sent_by),
      ...(attachments && attachments.length > 0
        ? { attachmentDocumentIds: attachments.map((a) => a.document_id) }
        : {}),
    });

    return res
      .status(200)
      .json({ success: true, status: "sent", reply: outcome.reply });
  } catch (error: unknown) {
    // A named refusal the caller can branch on.
    if (error instanceof ReplyToLeadError) {
      if (error instanceof AmbiguousReplyThreadError) {
        return res.status(error.status).json({
          error: error.message,
          code: error.code,
          candidates: error.candidates.map((c) => ({
            campaign_id: c.campaignId,
            last_message_at: c.lastMessageAt,
          })),
        });
      }
      // A person of the brand answered this thread by hand: one of our touches,
      // the follow-ups go on from it (owner 2026-10-15, REPLY-ANY-LEAD-1015).
      // Only a conversation held in a person's OWN mailbox (or the prospect
      // writing to it) is theirs for good: nothing automated can write there.
      const answer = (error as { answer?: HumanTookOverError["answer"] }).answer;
      if (error.code === "human_took_over" && answer && !answer.heldInOwnMailbox) {
        await countHumanAnswerAsOurs({
          orgId,
          campaignId: campaign_id,
          leadEmail: email,
          instantlyCampaignId: answer.instantlyCampaignId,
          answeredAt: answer.at,
        });
      } else if (
        (error.code === "prospect_wrote_elsewhere" || (error.code === "human_took_over" && answer?.heldInOwnMailbox)) &&
        campaign_id
      ) {
        await handThreadFollowupsToHuman({
          orgId,
          campaignId: campaign_id,
          leadEmail: email,
          reason:
            error.code === "human_took_over"
              ? "A person holds this conversation from their own mailbox (human_took_over)."
              : "The prospect last wrote to one of our people's own mailbox (prospect_wrote_elsewhere).",
          tag: "reply-to-lead",
        });
      }
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

/**
 * `POST /orgs/replies/escalate` — the responder gives this one to a human.
 *
 * Mounted on the same router as the reply itself, deliberately: they are the two
 * things a drafting worker can do with a thread, and a caller that holds the
 * campaign and the address for one holds them for the other.
 */
router.post("/escalate", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }
  // The agency-inbox send needs a run: transactional-email-service 400s without
  // one, so an escalation missing it could never reach a human. Refused here,
  // named, rather than as an opaque 500 after the thread was already read.
  const runId = res.locals.runId as string | undefined;
  if (!runId) {
    return res.status(400).json({ error: "x-run-id header is required" });
  }

  const parsed = EscalateReplyBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { campaign_id, email, question } = parsed.data;

  try {
    const escalation = await escalateReply({
      orgId,
      userId,
      runId,
      campaignId: campaign_id,
      leadEmail: email,
      question,
    });
    return res.status(200).json({ success: true, escalation });
  } catch (error: unknown) {
    if (error instanceof EscalateReplyError) {
      return res
        .status(error.status)
        .json({ error: error.message, code: error.code });
    }
    // Fail loud, and deliberately NOT partially: an escalation that could not
    // reach a human must not report success, because the ladder it would have
    // stopped is the only thing still talking to the prospect.
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] reply-escalation: failed for campaign=${campaign_id} lead=${email}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

export default router;
