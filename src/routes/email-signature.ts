/**
 * A brand's email signature — `GET/PUT/DELETE /orgs/email-signature`.
 *
 * Our signature is the default on every email; a client replaces it for one of
 * its brands here (owner 2026-10-14). See lib/brand-email-signature for why the
 * value is pinned per thread at send time.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`. Org-scoped: the row is keyed
 * on `x-org-id`, so a brand of another org reads as "none set" here; the
 * gateway refuses it before it reaches us (brand ownership check).
 */
import { errorMessage } from "../lib/db-error.js";
import { Router, Request, Response } from "express";

import {
  deleteBrandEmailSignature,
  getBrandEmailSignature,
  setBrandEmailSignature,
  signatureTextProblem,
} from "../lib/brand-email-signature";
import { EmailSignatureBodySchema, EmailSignatureQuerySchema } from "../schemas";

const router = Router();

/** Express 4 does not catch a rejected handler: fail loud with a named 500. */
function failed(res: Response, what: string, error: unknown): Response {
  const message = errorMessage(error);
  console.error(`[email-signature] ${what} failed: ${message}`);
  return res.status(500).json({ error: message });
}

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = EmailSignatureQuerySchema.safeParse(req.query);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.message });
  const { brandId } = parsed.data;

  try {
    const stated = await getBrandEmailSignature(orgId, brandId);
    return res.status(200).json(stated ?? { brandId, signature: null, updatedAt: null });
  } catch (error: unknown) {
    return failed(res, `read org=${orgId} brand=${brandId}`, error);
  }
});

router.put("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = (res.locals.userId as string | undefined) ?? null;
  const parsed = EmailSignatureBodySchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.message });
  const { brandId, signature } = parsed.data;

  const problem = signatureTextProblem(signature);
  if (problem) return res.status(400).json({ error: problem });

  try {
    const saved = await setBrandEmailSignature(orgId, brandId, signature, userId);
    console.log(`[email-signature] org=${orgId} brand=${brandId} set (${saved.signature.length} chars)`);
    return res.status(200).json(saved);
  } catch (error: unknown) {
    return failed(res, `set org=${orgId} brand=${brandId}`, error);
  }
});

router.delete("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = EmailSignatureQuerySchema.safeParse(req.query);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.message });
  const { brandId } = parsed.data;

  try {
    const removed = await deleteBrandEmailSignature(orgId, brandId);
    console.log(`[email-signature] org=${orgId} brand=${brandId} reset to default (removed=${removed})`);
    return res.status(200).json({ brandId, signature: null, updatedAt: null });
  } catch (error: unknown) {
    return failed(res, `reset org=${orgId} brand=${brandId}`, error);
  }
});

export default router;
