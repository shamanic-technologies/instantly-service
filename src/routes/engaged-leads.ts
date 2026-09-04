/**
 * The leads worth reading a conversation for — `GET /orgs/engaged-leads`.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`. No `x-user-id`: unlike
 * `/orgs/conversations` and `/orgs/replies`, this reads only our own gold table
 * and never resolves a per-user Instantly key, so requiring an identity it does
 * not use would be theatre.
 *
 * Org scope is enforced in the query itself (`org_id = <caller>`), so another
 * org's leads are not absent-by-filter but unreachable.
 */
import { Router, Request, Response } from "express";

import { fetchEngagedLeads } from "../lib/engaged-leads";
import { EngagedLeadsQuerySchema } from "../schemas";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;

  const parsed = EngagedLeadsQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { brand_id, campaign_id, since, limit } = parsed.data;

  try {
    const leads = await fetchEngagedLeads({
      orgId,
      brandId: brand_id,
      campaignId: campaign_id,
      since,
      limit,
    });
    return res.status(200).json({ success: true, count: leads.length, leads });
  } catch (error: unknown) {
    // Fail loud. An empty list returned on a read failure would claim this org
    // has nobody worth talking to, which is the one wrong answer that looks
    // exactly like a correct one.
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] engaged-leads: failed for org=${orgId}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

export default router;
