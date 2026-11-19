import { Router, Request, Response } from "express";
import { SendCheckRequestSchema } from "../schemas";
import { checkPersonForSend } from "../lib/send-checks";
import { traceEvent } from "../lib/trace-event";
import { messageOf } from "../lib/db-error.js";

const router = Router();

/**
 * POST /orgs/send-checks (SEND-CHECK-BEFORE-WRITE-1120)
 *
 * The checks `POST /orgs/send` runs on the person, run now, before a script asks
 * its writer: opt-out, not a prospect, stated not to the brand, address verdict.
 * A refusal answers the SAME status and body the send would; 200 = the send
 * would not refuse the person (it may still refuse on our own queued work).
 *
 * The verdict is verified now only when no decisive one under 30 days old is
 * held (apollo-service reuses it for free); otherwise it is billed on the
 * caller's run, exactly as the send bills it. The decision is recorded, so the
 * send right after reuses the verdict at no cost. Nothing is reserved or sent.
 */
router.post("/", async (req: Request, res: Response) => {
  const parsed = SendCheckRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid request", details: parsed.error.flatten() });
  }
  const to = parsed.data.to;
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string;
  const runId = (res.locals.runId as string | undefined) ?? null;
  if (!runId || !userId) {
    // The verification may be billed: it needs the run and user it lands on, like the send.
    return res.status(400).json({
      error: "Invalid request",
      code: "send_check_requires_run",
      details: "POST /orgs/send-checks must carry x-run-id and x-user-id (a verification may be billed on the run)",
    });
  }
  const brandIds: string[] = (res.locals.headerBrandIds as string[] | undefined) ?? [];

  try {
    const answer = await checkPersonForSend({
      orgId,
      to,
      brandIds,
      runId,
      context: "check",
      identity: {
        orgId,
        userId,
        runId,
        brandIds,
        campaignId: (res.locals.headerCampaignId as string | undefined) ?? null,
        featureSlug: (res.locals.headerFeatureSlug as string | undefined) ?? null,
        workflowSlug: (res.locals.headerWorkflowSlug as string | undefined) ?? null,
      },
    });
    if (!answer.sendable) {
      const r = answer.refusal;
      traceEvent(runId, { service: "instantly-service", event: `check-${r.event}`, detail: r.detail }, req.headers).catch(() => {});
      return res.status(r.status).json(r.body);
    }
    const v = answer.verification;
    return res.json({
      sendable: true,
      to,
      verdict: v.verdict,
      reused: v.reused,
      verifiedAt: v.verifiedAt,
    });
  } catch (error: unknown) {
    console.error(`[send-checks] Failed for ${to}: ${messageOf(error)}`);
    return res.status(500).json({ error: "Send checks failed", details: messageOf(error) });
  }
});

export default router;
