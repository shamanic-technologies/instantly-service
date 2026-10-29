/**
 * `GET /internal/reply-qualifications/recorded` (service key, every org, read-only; FACTORY-P3 next outcomes): every
 * reply judgment with what it was judged from, the EXACT chat-service request it sent, the whole answer and the label,
 * on either path (`service` = our own code, `script` = the stored script while switched). Read by workflow-service's
 * case recorder to prove the stored reply judge on fresh judgments. Personal data: nothing of it is logged here.
 */
import { Router, type Request, type Response } from "express";
import { errorMessage } from "../lib/db-error.js";
import { decodeCursor, readRecordedQualifications } from "../lib/reply-qualification-record";

export const RECORDED_DEFAULT_LIMIT = 50;
export const RECORDED_MAX_LIMIT = 200;

const router = Router();

router.get("/recorded", async (req: Request, res: Response) => {
  const raw = req.query.limit;
  const limit = raw === undefined ? RECORDED_DEFAULT_LIMIT : Number(raw);
  if (!Number.isInteger(limit) || limit < 1 || limit > RECORDED_MAX_LIMIT) {
    return res.status(400).json({ error: `limit must be an integer 1..${RECORDED_MAX_LIMIT}` });
  }
  const from = typeof req.query.from === "string" ? new Date(req.query.from) : null;
  if (!from || Number.isNaN(from.getTime())) return res.status(400).json({ error: "from (an ISO date) is required" });
  const to = typeof req.query.to === "string" ? new Date(req.query.to) : undefined;
  if (to && Number.isNaN(to.getTime())) return res.status(400).json({ error: "to must be an ISO date" });
  const rawCursor = typeof req.query.cursor === "string" ? req.query.cursor : null;
  const cursor = rawCursor ? decodeCursor(rawCursor) : null;
  if (rawCursor && !cursor) return res.status(400).json({ error: "cursor is not a cursor issued by this service" });
  try {
    return res.json(await readRecordedQualifications({ from, to, cursor, limit }));
  } catch (error) {
    console.error(`[instantly-service] GET /internal/reply-qualifications/recorded failed: ${errorMessage(error)}`);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
