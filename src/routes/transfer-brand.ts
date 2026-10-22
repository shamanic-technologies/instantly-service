import { errorMessage } from "../lib/db-error.js";
import { Router } from "express";
import { TransferBrandRequestSchema } from "../schemas";
import { transferBrand } from "../lib/transfer-brand";

const router = Router();

router.post("/", async (req, res) => {
  const parsed = TransferBrandRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }

  try {
    return res.json(await transferBrand(parsed.data));
  } catch (error) {
    console.error("[transfer-brand] failed:", error);
    return res.status(500).json({
      error: "Transfer failed",
      detail: errorMessage(error),
    });
  }
});

export default router;
