/**
 * POST /internal/send-outcomes/backfill — declare to runs-service's ledger the
 * `message_sent` of every PAST email-send-step run, dated when the email left
 * (lib/send-outcomes.ts, COST-BACKFILL-1023-INSTANTLY).
 *
 * Platform-scoped, service-auth, manual only. Body `{dryRun (default TRUE), limit, redate}`
 * (`redate:true` re-states the holds ALREADY declared so runs-service moves each
 * occurrence back to the send time; one already on time is left as is).
 * dry run plans every page and writes nothing (synchronous); `dryRun:false`
 * answers 202 and declares in the background (log `send-outcomes-backfill: done`).
 * Idempotent and resumable: a re-run declares nothing twice.
 */
import { Router, Request, Response } from "express";
import { declarePastBookingCalls } from "../lib/booking-call-outcomes";
import { backfillMissingLeadIds } from "../lib/lead-id-backfill";
import { redatePastPositiveReplies } from "../lib/positive-reply-outcomes";
import { restateSendAddressConsumption } from "../lib/send-check-restate";
import { runSendOutcomesBackfill } from "../lib/send-outcomes";

const router = Router();

router.post("/backfill", async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as { dryRun?: unknown; limit?: unknown; redate?: unknown };
  const dryRun = body.dryRun !== false;
  const redate = body.redate === true;
  const limit = typeof body.limit === "number" && body.limit > 0 ? Math.floor(body.limit) : undefined;

  if (dryRun) {
    try {
      return res.json(await runSendOutcomesBackfill({ dryRun: true, limit, redate }));
    } catch (error: any) {
      console.error(`[instantly-service] send-outcomes-backfill dry-run failed: ${error?.message ?? error}`);
      return res.status(500).json({ error: error?.message ?? "dry run failed" });
    }
  }

  res.status(202).json({ accepted: true, dryRun: false, redate, limit: limit ?? null });
  runSendOutcomesBackfill({ dryRun: false, limit, redate })
    .then((summary) => console.log(`[instantly-service] send-outcomes-backfill: done ${JSON.stringify(summary)}`))
    .catch((error) => console.error(`[instantly-service] send-outcomes-backfill: FAILED ${error?.message ?? error}`));
});

/**
 * POST /internal/send-outcomes/conversations/redate — LEDGER-HISTORY-1024: the
 * positive replies declared before the dated door re-stated dated at the reply
 * (lib/positive-reply-outcomes.ts `redatePastPositiveReplies`). Body
 * `{dryRun (default TRUE)}`; tens of rows, synchronous. Idempotent.
 */
router.post("/conversations/redate", async (req: Request, res: Response) => {
  const dryRun = (req.body ?? {}).dryRun !== false;
  try {
    return res.json(await redatePastPositiveReplies({ dryRun }));
  } catch (error: any) {
    console.error(`[instantly-service] conversation redate failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "redate failed" });
  }
});

/**
 * POST /internal/send-outcomes/lead-ids/backfill — LEDGER-HISTORY-1024: name the
 * lead of every sequence marked `no_lead_id` through lead-service, then re-run
 * the send backfill so those sends are declared (lib/lead-id-backfill.ts).
 * Body `{dryRun (default TRUE)}`: dry run asks lead-service and writes nothing
 * (synchronous); `dryRun:false` answers 202 (log `lead-id-backfill: done`).
 */
router.post("/lead-ids/backfill", async (req: Request, res: Response) => {
  const dryRun = (req.body ?? {}).dryRun !== false;
  if (dryRun) {
    try {
      return res.json(await backfillMissingLeadIds({ dryRun: true }));
    } catch (error: any) {
      console.error(`[instantly-service] lead-id-backfill dry-run failed: ${error?.message ?? error}`);
      return res.status(500).json({ error: error?.message ?? "dry run failed" });
    }
  }
  res.status(202).json({ accepted: true, dryRun: false });
  backfillMissingLeadIds({ dryRun: false })
    .then(async (leadIds) => {
      const sends = await runSendOutcomesBackfill({ dryRun: false });
      console.log(`[instantly-service] lead-id-backfill: done ${JSON.stringify({ leadIds, sends })}`);
    })
    .catch((error) => console.error(`[instantly-service] lead-id-backfill: FAILED ${error?.message ?? error}`));
});

/**
 * POST /internal/send-outcomes/booking-calls — LEDGER-HISTORY-1024: the past AI
 * Instant Call rings declared as `booking_call` (lib/booking-call-outcomes.ts).
 * Body `{dryRun (default TRUE)}`; a handful of runs, synchronous. Idempotent.
 */
router.post("/booking-calls", async (req: Request, res: Response) => {
  const dryRun = (req.body ?? {}).dryRun !== false;
  try {
    return res.json(await declarePastBookingCalls({ dryRun }));
  } catch (error: any) {
    console.error(`[instantly-service] booking-call declarations failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "booking-call declarations failed" });
  }
});

/**
 * POST /internal/send-outcomes/address-consumption/restate — LEDGER-HISTORY-1024:
 * past sends re-stated to consume the address occurrence that exists
 * (`email_checked`, else `email_found`; lib/send-check-restate.ts). Body
 * `{dryRun (default TRUE), limit}`: dry run reads the ledger and counts
 * (synchronous); `dryRun:false` answers 202 (log `send-check-restate`).
 */
router.post("/address-consumption/restate", async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as { dryRun?: unknown; limit?: unknown };
  const dryRun = body.dryRun !== false;
  const limit = typeof body.limit === "number" && body.limit > 0 ? Math.floor(body.limit) : undefined;
  if (dryRun) {
    try {
      return res.json(await restateSendAddressConsumption({ dryRun: true, limit }));
    } catch (error: any) {
      console.error(`[instantly-service] send-check-restate dry-run failed: ${error?.message ?? error}`);
      return res.status(500).json({ error: error?.message ?? "dry run failed" });
    }
  }
  res.status(202).json({ accepted: true, dryRun: false, limit: limit ?? null });
  restateSendAddressConsumption({ dryRun: false, limit }).catch((error) =>
    console.error(`[instantly-service] send-check-restate: FAILED ${error?.message ?? error}`),
  );
});

export default router;
