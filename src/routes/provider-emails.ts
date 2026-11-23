/**
 * The Provider reads of `email_queued` and `email_sent` (GET-BEFORE-WRITE-1123):
 * `GET /orgs/emails/queued`, `GET /orgs/emails/sent`. See lib/provider-emails.ts.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`, like every org read. A Script
 * reaches them with `ctx.get("email_queued" | "email_sent", { query })`: the
 * operations carry the OpenAPI tag `provider:<outcome>` (schemas.ts).
 */
import { Router, Request, Response } from "express";

import { errorMessage } from "../lib/db-error.js";
import {
  fetchQueuedEmails,
  fetchSentEmails,
  ProviderEmailFilterError,
  type ProviderEmailFilters,
} from "../lib/provider-emails";
import { ProviderEmailsQuerySchema, ProviderQueuedEmailsQuerySchema } from "../schemas";

const router = Router();

const bool = (v: "true" | "false" | undefined): boolean | undefined => (v === undefined ? undefined : v === "true");

router.get("/queued", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = ProviderQueuedEmailsQuerySchema.safeParse(req.query);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.message });
  const q = parsed.data;
  const filters: ProviderEmailFilters = {
    orgId,
    email: q.email,
    leadId: q.lead_id,
    brandId: q.brand_id,
    campaignId: q.campaign_id,
    since: q.since,
    before: q.before,
    firstEmail: bool(q.first_email),
    acrossOrgs: bool(q.across_orgs),
    includeStopped: bool(q.include_stopped),
    limit: q.limit,
  };
  try {
    const units = await fetchQueuedEmails(filters);
    return res.status(200).json({ outcome: "email_queued", count: units.length, units });
  } catch (error: unknown) {
    if (error instanceof ProviderEmailFilterError) return res.status(400).json({ error: error.message });
    // Fail loud: an empty answer on a read failure would read as "never emailed".
    const message = errorMessage(error);
    console.error(`[instantly-service] provider read email_queued: failed for org=${orgId}: ${message}`);
    return res.status(500).json({ error: message });
  }
});

router.get("/sent", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = ProviderEmailsQuerySchema.safeParse(req.query);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.message });
  const q = parsed.data;
  const filters: ProviderEmailFilters = {
    orgId,
    email: q.email,
    leadId: q.lead_id,
    brandId: q.brand_id,
    campaignId: q.campaign_id,
    since: q.since,
    before: q.before,
    firstEmail: bool(q.first_email),
    acrossOrgs: bool(q.across_orgs),
    limit: q.limit,
  };
  try {
    const units = await fetchSentEmails(filters);
    return res.status(200).json({ outcome: "email_sent", count: units.length, units });
  } catch (error: unknown) {
    if (error instanceof ProviderEmailFilterError) return res.status(400).json({ error: error.message });
    const message = errorMessage(error);
    console.error(`[instantly-service] provider read email_sent: failed for org=${orgId}: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
