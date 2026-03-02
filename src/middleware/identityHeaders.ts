import { Request, Response, NextFunction } from "express";

/**
 * Middleware that extracts x-org-id and x-user-id from request headers.
 * Returns 400 if either header is missing.
 * Attaches orgId and userId to res.locals for downstream handlers.
 */
export function identityHeaders(
  req: Request,
  res: Response,
  next: NextFunction,
) {
  const orgId = req.headers["x-org-id"] as string | undefined;
  const userId = req.headers["x-user-id"] as string | undefined;

  if (!orgId || !userId) {
    return res
      .status(400)
      .json({ error: "x-org-id and x-user-id headers required" });
  }

  res.locals.orgId = orgId;
  res.locals.userId = userId;
  next();
}
