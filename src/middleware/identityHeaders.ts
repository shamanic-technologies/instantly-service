import { Request, Response, NextFunction } from "express";

/**
 * Middleware that extracts identity and tracking headers from requests.
 *
 * Required: x-org-id, x-user-id, x-run-id (returns 400 if missing)
 * Optional: x-campaign-id, x-brand-id, x-workflow-name (workflow tracking headers)
 */
export function identityHeaders(
  req: Request,
  res: Response,
  next: NextFunction,
) {
  const orgId = req.headers["x-org-id"] as string | undefined;
  const userId = req.headers["x-user-id"] as string | undefined;
  const runId = req.headers["x-run-id"] as string | undefined;

  if (!orgId || !userId || !runId) {
    return res
      .status(400)
      .json({ error: "x-org-id, x-user-id, and x-run-id headers required" });
  }

  res.locals.orgId = orgId;
  res.locals.userId = userId;
  res.locals.runId = runId;

  // Optional workflow tracking headers — injected by workflow-service on all DAG calls
  const headerCampaignId = req.headers["x-campaign-id"] as string | undefined;
  const headerBrandId = req.headers["x-brand-id"] as string | undefined;
  const headerWorkflowName = req.headers["x-workflow-name"] as string | undefined;

  if (headerCampaignId) res.locals.headerCampaignId = headerCampaignId;
  if (headerBrandId) res.locals.headerBrandId = headerBrandId;
  if (headerWorkflowName) res.locals.headerWorkflowName = headerWorkflowName;

  next();
}
