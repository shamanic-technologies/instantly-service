/**
 * Manual reply qualifications — POST sets a human reply classification for a
 * (campaign, lead) pair, POST /withdrawals takes that statement back, and GET
 * returns the org-scoped audit history (withdrawn statements included, marked).
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId` (x-org-id). x-user-id is
 * additionally required on POST so the bronze row carries the qualifier's id.
 *
 * Bronze: `instantly_manual_qualifications_raw` (append-only). Idempotence is
 * enforced in lib/manual-qualifications.insertManualQualification: re-POSTing
 * the same status as the latest row for (org, campaign, lead) returns 200
 * with the existing row and `idempotent: true` — no new bronze row, no side
 * effects.
 */
import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { and, eq } from "drizzle-orm";
import {
  ManualQualificationCreateBodySchema,
  ManualQualificationListQuerySchema,
  ManualQualificationWithdrawBodySchema,
} from "../schemas";
import {
  insertManualQualification,
  applyManualQualificationSideEffects,
  listManualQualifications,
  withdrawManualQualification,
} from "../lib/manual-qualifications";

const router = Router();

function serializeRow(row: {
  id: string;
  orgId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  status: string;
  replyKind: string;
  qualifiedBy: string;
  notes: string | null;
  qualifiedAt: Date;
  withdrawnAt: Date | null;
  withdrawnBy: string | null;
}) {
  return {
    id: row.id,
    orgId: row.orgId,
    campaignId: row.campaignId,
    instantlyCampaignId: row.instantlyCampaignId,
    email: row.leadEmail,
    status: row.status,
    replyKind: row.replyKind,
    qualifiedBy: row.qualifiedBy,
    notes: row.notes,
    qualifiedAt: row.qualifiedAt.toISOString(),
    // Non-null ⇒ the statement was taken back and no longer stands. A consumer
    // that renders it as a current kind is showing a kind nobody stands behind.
    withdrawnAt: row.withdrawnAt ? row.withdrawnAt.toISOString() : null,
    withdrawnBy: row.withdrawnBy,
  };
}

/** Resolve the (campaign, lead) pair to this org's campaign row, or null. */
async function findOrgCampaign(orgId: string, campaignId: string, email: string) {
  const [campaign] = await db
    .select({
      campaignId: instantlyCampaigns.campaignId,
      instantlyCampaignId: instantlyCampaigns.instantlyCampaignId,
    })
    .from(instantlyCampaigns)
    .where(
      and(
        eq(instantlyCampaigns.campaignId, campaignId),
        eq(instantlyCampaigns.leadEmail, email),
        eq(instantlyCampaigns.orgId, orgId),
      ),
    );
  if (!campaign || !campaign.campaignId) return null;
  return {
    campaignId: campaign.campaignId,
    instantlyCampaignId: campaign.instantlyCampaignId,
  };
}

router.post("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }

  const parsed = ManualQualificationCreateBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { campaign_id, email, status, notes } = parsed.data;

  const campaign = await findOrgCampaign(orgId, campaign_id, email);
  if (!campaign) {
    return res
      .status(404)
      .json({ error: "Campaign not found in this org for the given email" });
  }

  const result = await insertManualQualification({
    orgId,
    campaignId: campaign.campaignId,
    instantlyCampaignId: campaign.instantlyCampaignId,
    leadEmail: email,
    status,
    qualifiedBy: userId,
    notes,
    payload: req.body,
  });

  if (result.inserted) {
    await applyManualQualificationSideEffects({
      bronzeRowId: result.row.id,
      orgId,
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: email,
      status,
      replyKind: result.row.replyKind,
      qualifiedAt: result.row.qualifiedAt,
      rawPayload: req.body,
    });
  }

  res.status(200).json({
    idempotent: !result.inserted,
    qualification: serializeRow(result.row),
  });
});

/**
 * Withdraw the standing statement for a (campaign, lead) pair — a person who
 * picked the wrong kind taking it back.
 *
 * Append-only: nothing is deleted, the statement stays readable, and a
 * withdrawal row records that it no longer stands. After it the automatic
 * classification takes over again (the manual pin is released), so a later
 * webhook is free to classify the reply as it normally would.
 *
 * 404 `no_standing_qualification` when nobody has stated anything for this pair
 * (or the statement was already withdrawn) — an explicit refusal a caller can
 * tell apart from a 500 and from the sibling "campaign not found" 404 by its
 * `code`.
 */
router.post("/withdrawals", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string | undefined;
  if (!userId) {
    return res.status(400).json({ error: "x-user-id header is required" });
  }

  const parsed = ManualQualificationWithdrawBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { campaign_id, email, notes } = parsed.data;

  const campaign = await findOrgCampaign(orgId, campaign_id, email);
  if (!campaign) {
    return res.status(404).json({
      error: "Campaign not found in this org for the given email",
      code: "campaign_not_found",
    });
  }

  const result = await withdrawManualQualification({
    orgId,
    instantlyCampaignId: campaign.instantlyCampaignId,
    leadEmail: email,
    withdrawnBy: userId,
    notes,
  });

  if (!result.withdrawn) {
    return res.status(404).json({
      error: "No standing manual qualification to withdraw for this campaign and lead",
      code: result.reason,
    });
  }

  res.status(200).json({ qualification: serializeRow(result.qualification) });
});

router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = ManualQualificationListQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  const { campaign_id, email, limit } = parsed.data;

  const qualifications = await listManualQualifications({
    orgId,
    campaignId: campaign_id,
    leadEmail: email,
    limit,
  });

  res.json({ qualifications: qualifications.map(serializeRow) });
});

export default router;
