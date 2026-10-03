/**
 * When a lead may be emailed — `GET /orgs/sending-schedule`.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`. No `x-user-id`: this reads
 * only our own tables and the send path's constants.
 *
 * Org scope is enforced in the query itself (`org_id = <caller>`), so another
 * org's lead reads as one we hold nothing for (the default schedule).
 */
import { Router, Request, Response } from "express";

import { fetchLeadSendingSchedule } from "../lib/lead-sending-schedule";
import { LeadSendingScheduleQuerySchema } from "../schemas";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;

  const parsed = LeadSendingScheduleQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { email, brand_id } = parsed.data;

  try {
    const schedule = await fetchLeadSendingSchedule({
      orgId,
      email,
      brandId: brand_id,
    });
    return res.status(200).json({ success: true, schedule });
  } catch (error: unknown) {
    // Fail loud: serving the default schedule on a read failure would tell the
    // customer we do not know a zone we may well hold.
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] sending-schedule: failed for org=${orgId}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

export default router;
