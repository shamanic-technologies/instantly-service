import { Router } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { TransferBrandRequestSchema } from "../schemas";

const router = Router();

router.post("/", async (req, res) => {
  const parsed = TransferBrandRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }

  const { brandId, sourceOrgId, targetOrgId } = parsed.data;

  // instantly_campaigns: brand_ids is text[], update solo-brand rows only
  const campaignsResult = await db.execute(sql`
    UPDATE instantly_campaigns
    SET org_id = ${targetOrgId}, updated_at = now()
    WHERE org_id = ${sourceOrgId}
      AND array_length(brand_ids, 1) = 1
      AND brand_ids[1] = ${brandId}
  `);

  const campaignsCount = Number(campaignsResult.rowCount ?? 0);

  return res.json({
    updatedTables: [
      { tableName: "instantly_campaigns", count: campaignsCount },
    ],
  });
});

export default router;
