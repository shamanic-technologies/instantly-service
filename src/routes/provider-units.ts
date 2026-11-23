/**
 * Provider reads of our Units (ENGINE-GET-ASK-1123): `GET /orgs/units/email-sent`
 * and `GET /orgs/units/email-queued`. See lib/provider-units.
 *
 * Auth: `serviceAuth` (X-API-Key) + `requireOrgId`; org-scoped in the query itself.
 */
import { Router, Request, Response } from "express";

import { errorMessage, messageOf } from "../lib/db-error.js";
import {
  InvalidCursorError,
  UnitsQueryError,
  decodeUnitCursor,
  fetchEmailQueuedUnits,
  fetchEmailSentUnits,
  parseUnitsQuery,
  type ProviderOutcome,
  type UnitsFilters,
  type UnitsPage,
} from "../lib/provider-units";
import { EmailQueuedUnitsQuerySchema, EmailSentUnitsQuerySchema } from "../schemas";

const router = Router();

function unitsHandler(
  outcome: ProviderOutcome,
  schema: typeof EmailSentUnitsQuerySchema | typeof EmailQueuedUnitsQuerySchema,
  fetchUnits: (filters: UnitsFilters) => Promise<UnitsPage<unknown>>,
) {
  return async (req: Request, res: Response) => {
    const orgId = res.locals.orgId as string;

    const parsed = schema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).json({ error: "invalid_query", message: parsed.error.message });
    }

    let filters: UnitsFilters;
    try {
      filters = parseUnitsQuery(outcome, orgId, parsed.data);
      if (filters.cursor !== undefined) decodeUnitCursor(filters.cursor);
    } catch (error: unknown) {
      if (error instanceof UnitsQueryError) {
        return res.status(400).json({ error: error.code, message: error.message });
      }
      if (error instanceof InvalidCursorError) {
        return res.status(400).json({ error: "invalid_cursor", message: messageOf(error) });
      }
      return res.status(500).json({ error: errorMessage(error) });
    }

    try {
      const page = await fetchUnits(filters);
      return res.status(200).json({
        success: true,
        outcome,
        count: page.units.length,
        units: page.units,
        nextCursor: page.nextCursor,
      });
    } catch (error: unknown) {
      // Fail loud: an empty page on a read failure would read as "never written to".
      const message = errorMessage(error);
      console.error(`[instantly-service] provider-units: ${outcome} failed for org=${orgId}: ${message}`);
      return res.status(500).json({ error: message });
    }
  };
}

router.get("/email-sent", unitsHandler("email_sent", EmailSentUnitsQuerySchema, fetchEmailSentUnits));
router.get("/email-queued", unitsHandler("email_queued", EmailQueuedUnitsQuerySchema, fetchEmailQueuedUnits));

export default router;
