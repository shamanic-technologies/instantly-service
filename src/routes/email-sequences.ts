/**
 * A brand's email sequences with each email's status — `GET /orgs/email-sequences`.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`. Org-scoped in the query
 * itself (`org_id = x-org-id`): a brand of another org reads as an empty list
 * here; the gateway refuses it before it reaches us (brand ownership check).
 */
import { Router, Request, Response } from "express";

import {
  fetchBrandEmailSequences,
  InvalidSequenceCursorError,
} from "../lib/brand-email-sequences";
import { EmailSequencesQuerySchema } from "../schemas";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;

  const parsed = EmailSequencesQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { brandId, limit, cursor } = parsed.data;

  try {
    const page = await fetchBrandEmailSequences({ orgId, brandId, limit, cursor });
    return res.status(200).json({
      brandId,
      count: page.sequences.length,
      sequences: page.sequences,
      nextCursor: page.nextCursor,
    });
  } catch (error: unknown) {
    if (error instanceof InvalidSequenceCursorError) {
      return res.status(400).json({ error: error.message });
    }
    // Fail loud: an empty page on a read failure would read as "we never
    // wrote to anyone for this brand".
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] email-sequences: failed for org=${orgId} brand=${brandId}: ${message}`,
    );
    return res.status(500).json({ error: message });
  }
});

export default router;
