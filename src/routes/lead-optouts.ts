/**
 * Recorded opt-outs — POST records that a named person asked a human to stop
 * contacting them, POST /withdrawals takes that record back, and GET returns
 * the org's consent log (withdrawn records included, marked).
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId` (x-org-id). x-user-id is
 * additionally required on both writes — a consent record with no author is not
 * a consent record.
 *
 * See lib/lead-optouts for what recording one actually does: the same
 * `lead_unsubscribed` silver event a clicked unsubscribe promotes, plus the
 * pause at the sender that the click path gets from Instantly and this path
 * cannot.
 */
import { Router, Request, Response } from "express";

import {
  LeadOptOutCreateBodySchema,
  LeadOptOutListQuerySchema,
  LeadOptOutWithdrawBodySchema,
} from "../schemas";
import {
  listLeadOptOuts,
  recordLeadOptOut,
  withdrawLeadOptOut,
  type LeadOptOutRow,
} from "../lib/lead-optouts";

const router = Router();

function serializeRow(row: LeadOptOutRow) {
  return {
    id: row.id,
    orgId: row.orgId,
    email: row.email,
    channel: row.channel,
    statedBy: row.statedBy,
    notes: row.notes,
    statedAt: row.statedAt.toISOString(),
    // Non-null ⇒ the record was taken back and no longer stands. A consumer
    // rendering it as a current opt-out is showing something nobody stands behind.
    withdrawnAt: row.withdrawnAt ? row.withdrawnAt.toISOString() : null,
    withdrawnBy: row.withdrawnBy,
  };
}

router.post("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }

  const parsed = LeadOptOutCreateBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { email, channel, notes } = parsed.data;

  const result = await recordLeadOptOut({
    orgId,
    leadEmail: email,
    channel,
    statedBy: userId,
    notes,
    payload: req.body,
  });

  res.status(200).json({
    idempotent: !result.recorded,
    campaignsAffected: result.campaignsAffected,
    campaignsStopped: result.campaignsStopped,
    optOut: serializeRow(result.optOut),
  });
});

/**
 * Withdraw the standing opt-out for a person — recorded on the wrong lead, or a
 * prospect who came back and asked to hear from us again.
 *
 * 404 `no_standing_optout` when nothing stands (nothing was ever recorded, or it
 * is already withdrawn) — an explicit refusal a caller tells apart from a 500 by
 * its `code`.
 */
router.post("/withdrawals", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }

  const parsed = LeadOptOutWithdrawBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { email, notes } = parsed.data;

  const result = await withdrawLeadOptOut({
    orgId,
    leadEmail: email,
    withdrawnBy: userId,
    notes,
  });

  if (!result.withdrawn) {
    return res.status(404).json({
      error: "No standing opt-out to withdraw for this lead",
      code: result.reason,
    });
  }

  res.status(200).json({
    campaignsAffected: result.campaignsAffected,
    optOut: serializeRow(result.optOut),
  });
});

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = LeadOptOutListQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { email, standing_only, limit } = parsed.data;

  const optOuts = await listLeadOptOuts({
    orgId,
    leadEmail: email,
    standingOnly: standing_only,
    limit,
  });

  res.json({ optOuts: optOuts.map(serializeRow) });
});

export default router;
