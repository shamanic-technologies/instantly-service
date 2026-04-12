import { Request, Response, NextFunction } from "express";

/** Strip trailing commas and whitespace from a header value */
function cleanHeader(value: string | undefined): string | undefined {
  if (!value) return value;
  const cleaned = value.replace(/,+$/, "").trim();
  return cleaned || undefined;
}

/**
 * Middleware for /orgs/* routes.
 *
 * Required: x-org-id (returns 400 if missing)
 * Optional: x-user-id, x-run-id, x-campaign-id, x-brand-id, x-workflow-slug, x-feature-slug
 */
export function requireOrgId(
  req: Request,
  res: Response,
  next: NextFunction,
) {
  const orgId = cleanHeader(req.headers["x-org-id"] as string | undefined);

  if (!orgId) {
    return res
      .status(400)
      .json({ error: "x-org-id header is required" });
  }

  res.locals.orgId = orgId;

  // Optional identity headers
  const userId = cleanHeader(req.headers["x-user-id"] as string | undefined);
  const runId = cleanHeader(req.headers["x-run-id"] as string | undefined);

  if (userId) res.locals.userId = userId;
  if (runId) res.locals.runId = runId;

  // Optional workflow tracking headers — injected by workflow-service on all DAG calls
  const headerCampaignId = cleanHeader(req.headers["x-campaign-id"] as string | undefined);
  const headerBrandId = req.headers["x-brand-id"] as string | undefined;
  const headerWorkflowSlug = cleanHeader(req.headers["x-workflow-slug"] as string | undefined);
  const headerFeatureSlug = cleanHeader(req.headers["x-feature-slug"] as string | undefined);

  if (headerCampaignId) res.locals.headerCampaignId = headerCampaignId;
  if (headerBrandId) {
    res.locals.headerBrandId = headerBrandId; // raw CSV for downstream forwarding
    res.locals.headerBrandIds = String(headerBrandId).split(",").map((s) => s.trim()).filter(Boolean);
  }
  if (headerWorkflowSlug) res.locals.headerWorkflowSlug = headerWorkflowSlug;
  if (headerFeatureSlug) res.locals.headerFeatureSlug = headerFeatureSlug;

  next();
}
