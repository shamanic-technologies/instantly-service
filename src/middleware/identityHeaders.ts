import { Request, Response, NextFunction } from "express";

/**
 * Middleware that extracts x-org-id, x-user-id, and x-run-id from request headers.
 * Returns 400 if any header is missing.
 * Attaches orgId, userId, and runId to res.locals for downstream handlers.
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
  next();
}
