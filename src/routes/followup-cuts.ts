/**
 * Follow-up cuts — a campaign stops the rest of the sequence for NAMED leads it
 * emailed in error, every other lead of the campaign untouched. Not an opt-out.
 * See lib/followup-cuts.
 *
 * Auth: `serviceAuth` + `requireOrgId`; x-user-id required on the write (the
 * audit names who cut).
 */
import { messageOf } from "../lib/db-error.js";
import { errorMessage } from "../lib/db-error.js";
import { Router, Request, Response } from "express";

import { FollowupCutsBodySchema, FollowupCutsListQuerySchema } from "../schemas";
import { cutFollowups, listFollowupCuts, LeadsNotInCampaignError } from "../lib/followup-cuts";

const router = Router();

router.post("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }
  const parsed = FollowupCutsBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { campaignId, leadEmails, reason } = parsed.data;

  try {
    const results = await cutFollowups({
      orgId,
      userId,
      campaignId,
      leadEmails,
      reason,
      caller: { method: req.method, path: req.originalUrl },
    });
    const failed = results.filter((r) => r.outcome === "failed").length;
    const body = {
      campaignId,
      cut: results.filter((r) => r.outcome === "cut").length,
      alreadyCut: results.filter((r) => r.outcome === "already_cut").length,
      failed,
      leads: results,
    };
    return res.status(failed > 0 ? 502 : 200).json(body);
  } catch (error: unknown) {
    if (error instanceof LeadsNotInCampaignError) {
      return res.status(404).json({
        error: messageOf(error),
        code: "leads_not_in_campaign",
        emails: error.emails,
      });
    }
    const message = errorMessage(error);
    console.error(`[instantly-service] followup-cuts failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = FollowupCutsListQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  try {
    const rows = await listFollowupCuts(orgId, parsed.data.campaignId);
    return res.status(200).json({
      cuts: rows.map((r) => ({
        campaignId: r.campaignId,
        email: r.leadEmail,
        reason: r.reason,
        cutBy: r.cutBy,
        holdsCancelled: r.holdsCancelled,
        cutAt: r.cutAt.toISOString(),
      })),
    });
  } catch (error: unknown) {
    const message = errorMessage(error);
    console.error(`[instantly-service] followup-cuts list failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
