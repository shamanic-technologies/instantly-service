/**
 * Manual reply qualifications — POST sets a human reply classification for a
 * (campaign, lead) pair; GET returns the org-scoped audit history.
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
} from "../schemas";
import {
  insertManualQualification,
  applyManualQualificationSideEffects,
  listManualQualifications,
} from "../lib/manual-qualifications";

const router = Router();

function serializeRow(row: {
  id: string;
  orgId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  status: string;
  qualifiedBy: string;
  notes: string | null;
  qualifiedAt: Date;
}) {
  return {
    id: row.id,
    orgId: row.orgId,
    campaignId: row.campaignId,
    instantlyCampaignId: row.instantlyCampaignId,
    email: row.leadEmail,
    status: row.status,
    qualifiedBy: row.qualifiedBy,
    notes: row.notes,
    qualifiedAt: row.qualifiedAt.toISOString(),
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

  const [campaign] = await db
    .select({
      campaignId: instantlyCampaigns.campaignId,
      instantlyCampaignId: instantlyCampaigns.instantlyCampaignId,
    })
    .from(instantlyCampaigns)
    .where(
      and(
        eq(instantlyCampaigns.campaignId, campaign_id),
        eq(instantlyCampaigns.leadEmail, email),
        eq(instantlyCampaigns.orgId, orgId),
      ),
    );

  if (!campaign || !campaign.campaignId) {
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
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: email,
      status,
      qualifiedAt: result.row.qualifiedAt,
      rawPayload: req.body,
    });
  }

  res.status(200).json({
    idempotent: !result.inserted,
    qualification: serializeRow(result.row),
  });
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
