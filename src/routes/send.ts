import { Router, Request, Response } from "express";
import { db } from "../db";
import {
  instantlyCampaigns,
  instantlyLeads,
  sequenceCosts,
  sequenceSteps,
} from "../db/schema";
import { eq, and, ne, isNotNull, sql } from "drizzle-orm";
import {
  Lead,
} from "../lib/instantly-client";
import { selectSendingAccount, sendLeadToInstantly, type SendResult } from "../lib/send-lead";
import { stepRowsFromSendPayload } from "../lib/self-send/sequence-steps";
import {
  SEND_TRANSPORT_SMTP,
  mintSelfSendCampaignId,
  resolveTransportForSend,
} from "../lib/self-send/transport";
import {
  createRun,
  updateRun,
  type TrackingHeaders,
} from "../lib/runs-client";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import { SendRequestSchema } from "../schemas";
import { traceEvent } from "../lib/trace-event";
import { refreshLeadStatusCurrent } from "../lib/status-gold";

/** Extract tracking headers from res.locals (set by requireOrgId middleware) */
function getTracking(res: Response): TrackingHeaders {
  const t: TrackingHeaders = {};
  if (res.locals.headerCampaignId) t.campaignId = res.locals.headerCampaignId;
  if (res.locals.headerBrandId) t.brandId = res.locals.headerBrandId;
  if (res.locals.headerWorkflowSlug) t.workflowSlug = res.locals.headerWorkflowSlug;
  if (res.locals.headerFeatureSlug) t.featureSlug = res.locals.headerFeatureSlug;
  if (res.locals.headerGoal) t.goal = res.locals.headerGoal;
  if (res.locals.headerBrandProfileId) t.brandProfileId = res.locals.headerBrandProfileId;
  if (res.locals.headerAudienceId) t.audienceId = res.locals.headerAudienceId;
  return t;
}

function buildAttributionMetadata(tracking: TrackingHeaders): Record<string, string> | null {
  const metadata: Record<string, string> = {};
  if (tracking.goal) metadata.goal = tracking.goal;
  if (tracking.brandProfileId) metadata.brandProfileId = tracking.brandProfileId;
  if (tracking.audienceId) metadata.audienceId = tracking.audienceId;
  return Object.keys(metadata).length > 0 ? metadata : null;
}

const router = Router();

/**
 * Sentinel prefix stored in `instantlyCampaignId` while a (campaignId,
 * leadEmail) row is RESERVED but the real Instantly campaign does not yet
 * exist. The column is notNull+unique, so each reservation carries a unique
 * `reserving:<uuid>` value; phase-2 overwrites it with the real id.
 */
const RESERVATION_PREFIX = "reserving:";

/** SQL predicate: this row is a reservation in flight (not a committed campaign). */
const isReservationSql = sql`${instantlyCampaigns.instantlyCampaignId} LIKE ${RESERVATION_PREFIX + "%"}`;

/**
 * A reservation is considered crashed/abandoned once its sentinel row is older
 * than this. A later legit retry then reclaims it (see the reserve upsert).
 * Comfortably above the synchronous reserve→send→phase-2 window.
 */
const STALE_RESERVATION_MS = 30_000;

/**
 * Release a still-open reservation so a later legit retry can re-claim the
 * (campaignId, leadEmail) pair. No-op once phase-2 has overwritten the
 * sentinel with the real `instantlyCampaignId` (the row is then a committed
 * campaign) — guarded by the `reserving:%` predicate. Fail loud: a DB error
 * here propagates.
 */
async function releaseReservation(reservedId: string): Promise<void> {
  await db
    .delete(instantlyCampaigns)
    .where(and(eq(instantlyCampaigns.id, reservedId), isReservationSql));
}

/**
 * POST /send
 * Add a lead to a multi-step sequence campaign via Instantly.
 *
 * Creates one run per sequence step:
 * - Step 1: run completed immediately, cost = actual
 * - Steps 2-N: runs stay ongoing, costs = provisioned
 *
 * Follow-up runs are completed when webhook email_sent arrives,
 * or failed on reply/bounce/unsub/not_interested/campaign error.
 *
 * Dispatch (find healthy account + create campaign + add lead + activate)
 * is delegated to `sendLeadToInstantly()` in `lib/send-lead.ts`. One-shot —
 * NSS post-activate is logged but never causes a retry (retry-stuck owns
 * the eventual catch-up 72h later if the campaign never dispatches).
 */
router.post("/", async (req: Request, res: Response) => {
  const parsed = SendRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const body = parsed.data;
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string;
  const tracking = getTracking(res);

  // Read from headers only (no body duplication)
  const brandIds: string[] = (res.locals.headerBrandIds as string[] | undefined) ?? [];
  const campaignId = tracking.campaignId ?? null;
  const campaignName = campaignId ? `Campaign ${campaignId}` : `Platform send ${body.to}`;
  const brandId = brandIds.join(",") || undefined;
  const workflowSlug = tracking.workflowSlug;
  const attributionMetadata = buildAttributionMetadata(tracking);

  console.log(`[send] POST /send to=${body.to} campaignId=${campaignId ?? "none"} brandIds=${brandIds.join(",")} subject="${body.subject}" steps=${body.sequence.length}`);
  traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-start", detail: `to=${body.to}, campaignId=${campaignId ?? "none"}, steps=${body.sequence.length}` }, req.headers).catch(() => {});

  try {
    // 0. Resolve Instantly API key (auto-resolves org vs platform key)
    const { key: apiKey, keySource } = await resolveInstantlyApiKey(orgId, userId, {
      method: "POST",
      path: "/send",
    });
    traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-key-resolved", detail: `keySource=${keySource}` }, req.headers).catch(() => {});

    // 1. No affordability gate here, deliberately.
    //
    // Sending used to authorize the org's balance against three Instantly cost
    // names. Those subscriptions (Email Outreach, Inbox Placement, the
    // pre-warmed accounts, and the MailForge / PrimeForge mailbox estate) are
    // now a FIXED cost we absorb rather than rebill, so there is no longer any
    // spend on this path to authorize — and an authorize call over an empty
    // basket would gate live sends on a question with no content.
    //
    // The credit gate is not lost, it moved UPSTREAM. A cold-email run pulls
    // the lead (Apollo credits) and generates the body (LLM tokens via
    // chat-service) before it ever reaches this route, and both still declare
    // and authorize normally, so an org out of credit fails long before a send.
    //
    // Consequence on the contract: `/orgs/send` no longer returns 402. Callers
    // that branch on it simply never take that branch.

    // 2. Per-step runs are created AFTER successful campaign activation
    const stepRuns: { step: number; runId: string }[] = [];
    // Reservation id, set once this request WINS the atomic claim below. Held
    // out here so the inner catch can release a still-open reservation.
    let reservedId: string | null = null;

    try {
      const sortedSequence = [...body.sequence].sort((a, b) => a.step - b.step);

      // 3. Lead ID conflict check: if this email already exists with a different lead_id, reject
      if (body.leadId) {
        const [conflict] = await db
          .select({ leadId: instantlyCampaigns.leadId })
          .from(instantlyCampaigns)
          .where(
            and(
              eq(instantlyCampaigns.leadEmail, body.to),
              isNotNull(instantlyCampaigns.leadId),
              ne(instantlyCampaigns.leadId, body.leadId),
            ),
          )
          .limit(1);

        if (conflict) {
          console.error(`[send] Lead ID conflict: email=${body.to} existing=${conflict.leadId} received=${body.leadId}`);
          return res.status(409).json({
            error: "Lead ID conflict",
            details: `Email ${body.to} already exists with lead_id ${conflict.leadId}, received ${body.leadId}`,
          });
        }
      }

      let savedLead: { id: string } | undefined;
      let added = 0;

      // 4. RESERVE the lead pair BEFORE the external Instantly call — atomic
      //    claim on the unique index. This makes /send idempotent under
      //    retry/concurrency: exactly one request creates the Instantly
      //    campaign; everyone else gets an idempotent 200 duplicate (NOT a 409).
      //
      //    The arbiter index depends on whether this is a platform send:
      //    - campaignId present → (campaignId, leadEmail) unique index.
      //    - campaignId NULL (platform send) → partial unique index on
      //      (runId, leadEmail) WHERE campaign_id IS NULL AND status='active'.
      //      Postgres treats NULLs as DISTINCT, so (campaignId, leadEmail) never
      //      collides when campaignId is null — every email-gateway timeout-retry
      //      would otherwise create a fresh duplicate campaign. The retry forwards
      //      the same x-run-id, so (runId, leadEmail) is the stable idempotency
      //      key (migration 0020_platform_send_dedupe.sql).
      //
      //    The row is reserved with a unique `reserving:<uuid>` sentinel in
      //    `instantlyCampaignId` (the "reservation in flight" marker) and
      //    phase-2 overwrites it with the real id once the external call wins.
      //
      //    One atomic upsert covers all cases:
      //    - no row               → INSERT → winner (fresh reservation).
      //    - row, real id         → ON CONFLICT, setWhere(reserving) false →
      //                             no-op → loser → 200 duplicate (already done).
      //    - row, sentinel, fresh → setWhere(stale) false → no-op → loser →
      //                             200 duplicate (concurrent in-flight peer).
      //    - row, sentinel, stale → setWhere true → UPDATE (reclaim) → winner
      //                             (the previous winner crashed mid-send).
      //    Winner ⇔ RETURNING is non-empty.
      const isPlatformSend = campaignId === null;
      const [reservation] = await db
        .insert(instantlyCampaigns)
        .values({
          campaignId,
          leadEmail: body.to,
          leadId: body.leadId,
          instantlyCampaignId: `${RESERVATION_PREFIX}${crypto.randomUUID()}`,
          name: campaignName,
          status: "active",
          deliveryStatus: "contacted",
          orgId,
          userId,
          brandIds,
          workflowSlug,
          featureSlug: tracking.featureSlug,
          runId: res.locals.runId as string,
          metadata: attributionMetadata,
        })
        .onConflictDoUpdate({
          target: isPlatformSend
            ? [instantlyCampaigns.runId, instantlyCampaigns.leadEmail]
            : [instantlyCampaigns.campaignId, instantlyCampaigns.leadEmail],
          // Must match the partial index predicate for the platform arbiter.
          targetWhere: isPlatformSend
            ? sql`${instantlyCampaigns.campaignId} IS NULL AND ${instantlyCampaigns.status} = 'active'`
            : undefined,
          // Stale-reservation reclaim only: take ownership for this caller (new
          // sentinel from excluded) and refresh the freshness clock.
          set: {
            instantlyCampaignId: sql`excluded.instantly_campaign_id`,
            leadId: sql`excluded.lead_id`,
            name: sql`excluded.name`,
            orgId: sql`excluded.org_id`,
            userId: sql`excluded.user_id`,
            brandIds: sql`excluded.brand_ids`,
            workflowSlug: sql`excluded.workflow_slug`,
            featureSlug: sql`excluded.feature_slug`,
            runId: sql`excluded.run_id`,
            metadata: sql`CASE
              WHEN excluded.metadata IS NULL THEN ${instantlyCampaigns.metadata}
              ELSE COALESCE(${instantlyCampaigns.metadata}, '{}'::jsonb) || excluded.metadata
            END`,
            createdAt: sql`now()`,
            updatedAt: sql`now()`,
          },
          setWhere: sql`${isReservationSql} AND ${instantlyCampaigns.createdAt} < now() - make_interval(secs => ${STALE_RESERVATION_MS / 1000})`,
        })
        .returning({ id: instantlyCampaigns.id });

      if (!reservation) {
        // Lost the claim — already processed, or a fresh concurrent peer is
        // mid-flight. Idempotent success: no Instantly campaign created here,
        // no cost declared. Same 200 shape as the historical early-return.
        console.log(`[send] Duplicate send for campaign ${campaignId ?? "none"}/${body.to} — claim already held, returning idempotent 200`);
        return res.status(200).json({
          success: true,
          campaignId,
          added: 0,
          duplicate: true,
        });
      }

      reservedId = reservation.id;

      // 5. WINNER only — dispatch lead to a healthy Instantly account.
      const lead: Lead = {
        email: body.to,
        first_name: body.firstName,
        last_name: body.lastName,
        company_name: body.company,
        variables: body.variables,
      };

      // 5. Choose the mailbox FIRST, then let its transport decide the pipe.
      //
      //    ⚠️ THE ORDER IS THE WHOLE POINT. The transport used to be read only at
      //    phase-2, AFTER the Instantly campaign had already been created — so an
      //    account flipped to 'smtp' got its lead pushed to Instantly AND picked
      //    up by our own dispatch worker, and every prospect received each email
      //    TWICE from the same mailbox. Selecting the account before the external
      //    call is what makes the two paths mutually exclusive.
      const account = await selectSendingAccount(tracking.featureSlug ?? null);
      const transport = resolveTransportForSend(account?.sendTransport);

      const sendResult: SendResult = !account
        ? { ok: false, reason: "no_healthy_accounts_available" }
        : transport === SEND_TRANSPORT_SMTP
          ? {
              // No Instantly campaign exists on this transport. The id stays
              // because the column is notNull+unique and every join hangs off it
              // — it simply becomes a local one. `added` is 1 because the lead is
              // enrolled here and now; the dispatch happens on the worker's next
              // sweep.
              ok: true,
              value: {
                instantlyCampaignId: mintSelfSendCampaignId(),
                added: 1,
                account,
              },
            }
          : await sendLeadToInstantly({
              apiKey,
              campaignName,
              subject: body.subject,
              sortedSequence,
              lead,
              bcc: body.bcc,
              timezone: body.timezone,
              featureSlug: tracking.featureSlug ?? null,
              account,
            });

      if (!sendResult.ok) {
        // Release the reservation so a later legit retry can re-claim.
        await releaseReservation(reservedId);
        reservedId = null;
        const detail = "No active Instantly accounts available for this organization";
        console.error(`[send] ${detail} for ${campaignId ?? "none"}/${body.to}`);
        return res.status(500).json({
          error: "Failed to send lead",
          details: detail,
        });
      }

      traceEvent(
        res.locals.runId as string,
        {
          service: "instantly-service",
          event: "send-campaign-created",
          detail: `instantlyCampaignId=${sendResult.value.instantlyCampaignId}, added=${sendResult.value.added}, account=${sendResult.value.account.email}`,
        },
        req.headers,
      ).catch(() => {});

      added = sendResult.value.added;

      // 6. Phase-2: attach the real Instantly campaign id to the reserved row.
      //    From here on the row is a committed campaign — release is a no-op.
      //
      //    `sendTransport` is FROZEN here from the chosen account's policy, and
      //    never re-read from the account afterwards. A sequence spans days, so
      //    following the live policy would re-route a lead's followups the moment
      //    an operator flips that mailbox — and a lead already pushed to Instantly
      //    holds no local step bodies, so its followups would simply stop. Same
      //    persist-at-write reasoning as `accountEmail` beside it.
      await db
        .update(instantlyCampaigns)
        .set({
          instantlyCampaignId: sendResult.value.instantlyCampaignId,
          accountEmail: sendResult.value.account.email,
          // The SAME value the branch above acted on, not a re-resolution:
          // the frozen column and the pipe actually taken can never disagree.
          sendTransport: transport,
          updatedAt: new Date(),
        })
        .where(eq(instantlyCampaigns.id, reservedId));

      // 6b. Persist the sequence we just committed to.
      //
      //     While Instantly dispatches, the step bodies live there and our bronze
      //     config mirror is a copy of what they hold. On the self-send transport
      //     there is nothing upstream to mirror, so the sender reads these rows —
      //     without them a flipped account would find no body and send nothing.
      //     Written for BOTH transports: the row is cheap, and having it already
      //     there is what makes a later flip a data change rather than a
      //     migration. Idempotent on (campaign, step), so a redispatch re-upserts
      //     instead of stacking a duplicate the scheduler would send twice.
      const stepRows = stepRowsFromSendPayload(body.subject, sortedSequence);
      if (stepRows.length > 0) {
        await db
          .insert(sequenceSteps)
          .values(
            stepRows.map((step) => ({
              instantlyCampaignId: sendResult.value.instantlyCampaignId,
              step: step.step,
              subject: step.subject,
              bodyHtml: step.bodyHtml,
              delayDays: step.delayDays,
            })),
          )
          .onConflictDoUpdate({
            target: [sequenceSteps.instantlyCampaignId, sequenceSteps.step],
            set: {
              subject: sql`excluded.subject`,
              bodyHtml: sql`excluded.body_html`,
              delayDays: sql`excluded.delay_days`,
              updatedAt: new Date(),
            },
          });
      }

      await refreshLeadStatusCurrent(sendResult.value.instantlyCampaignId, body.to);

      // Save lead to DB
      const [createdLead] = await db
        .insert(instantlyLeads)
        .values({
          instantlyCampaignId: sendResult.value.instantlyCampaignId,
          email: body.to,
          firstName: body.firstName,
          lastName: body.lastName,
          companyName: body.company,
          customVariables: body.variables,
          orgId,
          runId: null,
        })
        .onConflictDoNothing()
        .returning();

      if (createdLead) savedLead = createdLead;

      // 4. Create per-step runs and queue every step.
      //
      //    NO COST IS DECLARED HERE. The Instantly / MailForge / PrimeForge
      //    subscriptions are a fixed cost we absorb, so the three cost names
      //    this loop used to declare (`instantly-account-email-sent`,
      //    `instantly-domain-email-sent`, `instantly-contact-uploaded`) are no
      //    longer written to runs-service at all. Deliberately NOT replaced by
      //    a zero-priced row: a zero asserts "this cost nothing", which is
      //    false — it costs us real money, we simply stopped passing it on.
      //    Absence is the honest representation.
      //
      //    The per-step RUN stays. It is the unit of volume (how many sends,
      //    for which brand, on which campaign) and dropping it would blind
      //    every downstream stat for the sake of a billing change.
      //
      //    The `sequence_costs` row also stays, now with a NULL cost id,
      //    because that table is the send QUEUE as much as it is a ledger —
      //    see the column comment in `db/schema.ts`. One row per step now
      //    (it used to be two, one per cost name); every reader already
      //    collapses to `DISTINCT step`, so counts are unchanged.
      const parentIdentity = { orgId, userId, runId: res.locals.runId as string, tracking };
      for (const s of sortedSequence) {
        const stepRun = await createRun({
          serviceName: "instantly-service",
          taskName: `email-send-step-${s.step}`,
          brandId,
          campaignId: campaignId ?? undefined,
        }, parentIdentity);

        const stepIdentity = { orgId, userId, runId: stepRun.id, tracking };

        await db.insert(sequenceCosts).values({
          campaignId,
          // Persist the per-lead Instantly campaign id so the webhook/reconcile
          // resolvers can settle this hold even for platform sends
          // (campaignId NULL). See migration 0027.
          instantlyCampaignId: sendResult.value.instantlyCampaignId,
          leadEmail: body.to,
          step: s.step,
          runId: stepRun.id,
          costId: null,
          status: "provisioned",
        });

        await updateRun(stepRun.id, "completed", stepIdentity);

        stepRuns.push({ step: s.step, runId: stepRun.id });
      }

      traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-done", detail: `to=${body.to}, campaignId=${campaignId ?? "none"}, added=${added}, stepRuns=${stepRuns.length}` }, req.headers).catch(() => {});
      console.log(`[send] Done — to=${body.to} campaignId=${campaignId ?? "none"} added=${added} stepRuns=${stepRuns.length}`);
      res.status(200).json({
        success: true,
        campaignId,
        leadId: savedLead?.id,
        added,
        stepRuns: stepRuns.length > 0 ? stepRuns : undefined,
      });
    } catch (error: any) {
      // Fail any step runs that were already created
      for (const sr of stepRuns) {
        try {
          await updateRun(sr.runId, "failed", { orgId, userId, runId: sr.runId }, error.message);
        } catch {
          // Run may already be completed (step 1) — ignore
        }
      }
      // Release a still-open reservation (no-op once phase-2 attached the real
      // id) so a later legit retry can re-claim. Fail loud if the delete errors.
      if (reservedId) {
        await releaseReservation(reservedId);
      }
      throw error;
    }
  } catch (error: any) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      return res.status(422).json({
        error: "API key not configured for this organization",
        details: "Please configure your Instantly API key before sending emails.",
      });
    }
    traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-error", detail: error.message, level: "error" }, req.headers).catch(() => {});
    console.error(`[send] Failed to send — to=${body.to} error="${error.message}"`);
    res.status(500).json({
      error: "Failed to send email",
      details: error.message,
    });
  }
});

export default router;
