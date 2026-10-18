/**
 * What is queued to send for some campaigns, and when the window next opens —
 * `GET /orgs/send-queue?campaignIds=a,b` (CAMPAIGN-NEXT-STEPS-1018).
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`. Org scope is in the query
 * (`org_id = <caller>`): another org's campaign reads as nothing queued.
 * Fail loud: a read failure is a 500, never "nothing queued".
 */
import { Router, Request, Response } from "express";

import { fetchCampaignSendQueue } from "../lib/campaign-send-queue";
import { CampaignSendQueueQuerySchema } from "../schemas";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = CampaignSendQueueQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  try {
    const queue = await fetchCampaignSendQueue({ orgId, campaignIds: parsed.data.campaignIds });
    return res.status(200).json(queue);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] send-queue: failed for org=${orgId}: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
