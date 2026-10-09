/**
 * Every lead we have actually written to — `GET /orgs/written-to-leads`.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`, like engaged-leads: a read
 * of our own gold table, org-scoped in the query itself.
 */
import { Router, Request, Response } from "express";

import {
  fetchWrittenToLeads,
  InvalidCursorError,
} from "../lib/written-to-leads";
import { WrittenToLeadsQuerySchema } from "../schemas";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;

  const parsed = WrittenToLeadsQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { brand_id, campaign_id, limit, cursor } = parsed.data;

  try {
    const page = await fetchWrittenToLeads({
      orgId,
      brandId: brand_id,
      campaignId: campaign_id,
      limit,
      cursor,
    });
    return res.status(200).json({
      success: true,
      count: page.leads.length,
      leads: page.leads,
      nextCursor: page.nextCursor,
    });
  } catch (error: unknown) {
    if (error instanceof InvalidCursorError) {
      return res.status(400).json({ error: error.message });
    }
    // Fail loud: an empty page on a read failure would read as "we never wrote
    // to anyone for this brand".
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] written-to-leads: failed for org=${orgId}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

export default router;
