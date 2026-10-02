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
import { findRecentBrandContact, recontactRefusal } from "../lib/recontact-window";
import { findNotAProspect, notAProspectRefusal } from "../lib/not-a-prospect";
import { findStandingOptOut, optOutRefusal } from "../lib/lead-optouts";
import { resolveTransportForNewSequence } from "../lib/self-send/capability";
import {
  SEND_TRANSPORT_INSTANTLY,
  SEND_TRANSPORT_SMTP,
  mintSelfSendCampaignId,
} from "../lib/self-send/transport";
import {
  createRun,
  updateRun,
  updateCostStatus,
  type IdentityContext,
  type TrackingHeaders,
} from "../lib/runs-client";
import { authorizeCreditSpend } from "../lib/billing-client";
import { provisionStepEmailCosts, sendAuthorizeItems } from "../lib/send-costs";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import { SendRequestSchema } from "../schemas";
import { traceEvent } from "../lib/trace-event";
import { refreshLeadStatusCurrent } from "../lib/status-gold";
import { announceEvidenceChanged } from "../lib/evidence-changed";
import { readHeldLead, type HeldLead } from "../lib/held-lead";

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

/** One step of the sequence being queued: its run and its two provisioned costs. */
interface StepHold {
  step: number;
  runId: string;
  costId: string;
  domainCostId: string;
}

/**
 * Give back every step this request provisioned but never queued: cancel both
 * costs and fail the step run. Used on the refusal/failure paths only, where the
 * response is already an error, so a failed cancel is logged LOUD (the
 * provisioned row is not billable usage) rather than masking the real cause.
 */
async function abandonStepHolds(
  holds: StepHold[],
  identity: { orgId: string; userId: string; tracking: TrackingHeaders },
  reason: string,
): Promise<void> {
  for (const h of holds) {
    const stepIdentity: IdentityContext = { ...identity, runId: h.runId };
    // An id is empty when the provision itself failed — nothing to cancel.
    for (const costId of [h.costId, h.domainCostId].filter(Boolean)) {
      try {
        await updateCostStatus(h.runId, costId, "cancelled", stepIdentity);
      } catch (error: unknown) {
        console.error(
          `[send] FAILED to cancel provisioned cost ${costId} (run ${h.runId}) after "${reason}": ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
    try {
      await updateRun(h.runId, "failed", stepIdentity, reason);
    } catch (error: unknown) {
      console.error(
        `[send] FAILED to fail step run ${h.runId} after "${reason}": ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
}

/**
 * POST /send
 * Add a lead to a multi-step sequence campaign via Instantly or self-send.
 *
 * Every step is an email sent to a lead and is BILLED to the org
 * (`lib/send-costs.ts`): one run per step carrying two provisioned costs,
 * authorized once for the whole sequence, actualized when the step's real
 * `email_sent` lands, cancelled when the step can no longer send.
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

    // 1. Affordability is checked per sequence, AFTER the pre-flight refusals
    //    and the reservation (so a refused or duplicate send provisions
    //    nothing) and BEFORE any email can leave — see step 4c below.

    // 2. Per-step runs + their provisioned costs, created once the claim is won.
    //    `stepHolds` = provisioned but not yet queued (given back on failure);
    //    `stepRuns` = queued and reported to the caller.
    let stepHolds: StepHold[] = [];
    const stepRuns: { step: number; runId: string }[] = [];
    const billingIdentity = { orgId, userId, tracking };
    // Reservation id, set once this request WINS the atomic claim below. Held
    // out here so the inner catch can release a still-open reservation.
    let reservedId: string | null = null;

    try {
      const sortedSequence = [...body.sequence].sort((a, b) => a.step - b.step);

      // 3. LEAD IDENTITY — the EMAIL is the identity, lead-service owns the id.
      //
      //    lead-service resolves a person email-first (one email = one lead) and
      //    can REPOINT a person onto a new canonical lead id while the address
      //    stays the same. This used to refuse the send with a 409
      //    `lead_id_conflict` whenever the address was on file under another id,
      //    which refused a repointed person FOREVER: nothing was sent, nothing
      //    was queued, so our status read "not contacted, not queued" and
      //    lead-service's retry pool re-served them every run (one prospect: 79
      //    attempts; 19 of 24 sends in a day were this refusal).
      //
      //    So we ACCEPT the identity owner's id and re-key what we hold for this
      //    address in this org onto it, keeping the superseded id on the row's
      //    metadata. Refusing protected nothing: a second email to the same
      //    person is prevented by the gates that key on the ADDRESS — the
      //    (campaign, email) reservation below (a duplicate answers 200 with
      //    `held`), the opt-out and the per-brand re-contact window — none of
      //    which reads the lead id.
      if (body.leadId) {
        const superseded = await db
          .select({ leadId: instantlyCampaigns.leadId })
          .from(instantlyCampaigns)
          .where(
            and(
              eq(instantlyCampaigns.orgId, orgId),
              sql`lower(${instantlyCampaigns.leadEmail}) = lower(${body.to})`,
              isNotNull(instantlyCampaigns.leadId),
              ne(instantlyCampaigns.leadId, body.leadId),
            ),
          )
          .limit(1);

        if (superseded.length > 0) {
          await db
            .update(instantlyCampaigns)
            .set({
              leadId: body.leadId,
              metadata: sql`COALESCE(${instantlyCampaigns.metadata}, '{}'::jsonb) || jsonb_build_object('repointedFromLeadId', ${instantlyCampaigns.leadId})`,
              updatedAt: new Date(),
            })
            .where(
              and(
                eq(instantlyCampaigns.orgId, orgId),
                sql`lower(${instantlyCampaigns.leadEmail}) = lower(${body.to})`,
                isNotNull(instantlyCampaigns.leadId),
                ne(instantlyCampaigns.leadId, body.leadId),
              ),
            );
          console.log(
            `[send] Lead identity repointed: email=${body.to} ${superseded[0].leadId} -> ${body.leadId} (org ${orgId})`,
          );
        }
      }

      // 3a-bis. OPT-OUT — refuse outright if this person asked this org to stop.
      //
      //     Checked BEFORE the re-contact window because the two say different
      //     things and this one is stronger: the window is a three-month timing
      //     rule that LAPSES, an opt-out never does. Without this gate a
      //     recorded opt-out stopped the campaigns that existed that day and
      //     nothing else — so once the window expired we would email the person
      //     again, which is the outcome the record exists to prevent.
      //
      //     ORG-scoped, like the record itself: they asked US to stop, and
      //     honouring it for one brand while a sibling brand keeps writing is
      //     exactly what the law cares about. Placed with the other pre-flight
      //     gates, so a refused send creates no reservation, no Instantly
      //     campaign, no run and no `sequence_costs` row. Fail loud — a DB
      //     error propagates rather than waving the send through.
      const standingOptOut = await findStandingOptOut(orgId, body.to);
      if (standingOptOut) {
        const refusal = optOutRefusal(body.to, standingOptOut);
        console.warn(`[send] Refused — ${refusal.details}`);
        traceEvent(
          res.locals.runId as string,
          {
            service: "instantly-service",
            event: "send-refused-opted-out",
            detail: `to=${body.to}, channel=${standingOptOut.channel}, statedAt=${standingOptOut.statedAt.toISOString()}`,
          },
          req.headers,
        ).catch(() => {});
        return res.status(409).json(refusal);
      }

      // 3b. RE-CONTACT WINDOW — refuse a prospect this service already emailed
      //     for the same brand inside the last three months.
      //
      //     Placed BEFORE the reservation and before the account selection, so
      //     a refused send creates no reservation, no Instantly campaign, no
      //     run and no `sequence_costs` row: nothing is billed for an email
      //     that never left. Fail loud — a DB error here propagates rather
      //     than waving the send through.
      //
      //     See src/lib/recontact-window.ts for why this half of the rule can
      //     only live in this service.
      const recentContact = await findRecentBrandContact(body.to, brandIds);
      if (recentContact) {
        const refusal = recontactRefusal(body.to, recentContact);
        console.warn(`[send] Refused — ${refusal.details}`);
        traceEvent(
          res.locals.runId as string,
          {
            service: "instantly-service",
            event: "send-refused-recent-contact",
            detail: `to=${body.to}, brandId=${recentContact.brandId}, lastEmailedAt=${recentContact.lastEmailedAt.toISOString()}`,
          },
          req.headers,
        ).catch(() => {});
        return res.status(409).json(refusal);
      }

      // 3c. NOT A PROSPECT — the person told this brand they already buy from
      //     the client, or are the client. Permanent for the brand, unlike the
      //     window above. Same placement and fail-loud posture. See
      //     src/lib/not-a-prospect.ts.
      const notAProspect = await findNotAProspect(body.to, brandIds);
      if (notAProspect) {
        const refusal = notAProspectRefusal(body.to, notAProspect);
        console.warn(`[send] Refused — ${refusal.details}`);
        return res.status(409).json(refusal);
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
        //
        // The answer carries WHAT we hold, so a caller can tell a lead still
        // queued with us from a lost one. A failed lookup omits it rather than
        // failing the duplicate — the claim is held either way, and a 500 here
        // would read as a transport failure and invite yet another retry.
        let held: HeldLead | null = null;
        try {
          held = await readHeldLead(
            {
              campaignId,
              runId: (res.locals.runId as string | undefined) ?? null,
              leadEmail: body.to,
            },
            RESERVATION_PREFIX,
          );
        } catch (error) {
          console.error(
            `[send] held-lead lookup failed for ${campaignId ?? "none"}/${body.to}: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
        }
        console.log(`[send] Duplicate send for campaign ${campaignId ?? "none"}/${body.to} — claim already held (${held?.state ?? "unknown"}), returning idempotent 200`);
        return res.status(200).json({
          success: true,
          campaignId,
          added: 0,
          duplicate: true,
          ...(held ? { held } : {}),
        });
      }

      reservedId = reservation.id;

      // 4b. PROVISION — one run per step, each carrying the step's two email
      //     costs. Done before any email can leave so a cost runs-service cannot
      //     declare (422 unknown/unpriced name) blocks the send instead of
      //     sending it unbilled. Fail loud: the catch below gives back whatever
      //     was provisioned and releases the reservation.
      const parentIdentity = { orgId, userId, runId: res.locals.runId as string, tracking };
      for (const s of sortedSequence) {
        const stepRun = await createRun({
          serviceName: "instantly-service",
          taskName: `email-send-step-${s.step}`,
          brandId,
          campaignId: campaignId ?? undefined,
        }, parentIdentity);
        // Pushed before provisioning so a failed provision still fails the run.
        const hold: StepHold = { step: s.step, runId: stepRun.id, costId: "", domainCostId: "" };
        stepHolds.push(hold);
        const ids = await provisionStepEmailCosts(stepRun.id, keySource, {
          orgId,
          userId,
          runId: stepRun.id,
          tracking,
        });
        hold.costId = ids.costId;
        hold.domainCostId = ids.domainCostId;
      }

      // 4c. AUTHORIZE — an org out of credit is refused before an email leaves.
      //     Platform spend only (BYOK pays its vendor directly). A refusal is an
      //     explicit 402, never a silent send.
      if (keySource === "platform") {
        const auth = await authorizeCreditSpend(
          sendAuthorizeItems(sortedSequence.length),
          "instantly-send",
          { ...billingIdentity, runId: res.locals.runId as string },
        );
        if (!auth.sufficient) {
          await abandonStepHolds(stepHolds, billingIdentity, "insufficient_credits");
          stepHolds = [];
          await releaseReservation(reservedId);
          reservedId = null;
          console.warn(
            `[send] Refused — insufficient credits for org ${orgId} (balance=${auth.balance_cents} required=${auth.required_cents}) to=${body.to}`,
          );
          traceEvent(
            res.locals.runId as string,
            {
              service: "instantly-service",
              event: "send-refused-insufficient-credits",
              detail: `to=${body.to}, balance_cents=${auth.balance_cents}, required_cents=${auth.required_cents}`,
            },
            req.headers,
          ).catch(() => {});
          return res.status(402).json({
            error: "Insufficient credits",
            code: "insufficient_credits",
            balance_cents: auth.balance_cents,
            required_cents: auth.required_cents,
          });
        }
      }

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
      //
      //    The lead's timezone and its full sequence ride along: capacity is
      //    booked for every day this sequence will need the mailbox (D0, D+3,
      //    D+10 …), each resolved through the prospect's own local window.
      const account = await selectSendingAccount({
        featureSlug: tracking.featureSlug ?? null,
        timezone: body.timezone ?? null,
        sequence: sortedSequence,
      });
      // The pipe for a NEW sequence. With the A/B off this is exactly the
      // account's own policy; with it on, a credentialed mailbox alternates so
      // both pipes carry comparable work on the SAME mailboxes. Frozen on the
      // campaign row below, so every later step of this lead follows it.
      const transport = account
        ? await resolveTransportForNewSequence(
            { email: account.email },
            { method: "POST", path: "/orgs/send" },
          )
        : SEND_TRANSPORT_INSTANTLY;

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
        // Nothing will be sent: give back the provisioned costs, then release
        // the reservation so a later legit retry can re-claim.
        await abandonStepHolds(stepHolds, billingIdentity, sendResult.reason);
        stepHolds = [];
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
      //    `sendTransport` is FROZEN here from the decision taken above — the
      //    account's policy, or the A/B split when it is armed — and never
      //    re-read afterwards. A sequence spans days, so
      //    following the live policy would re-route a lead's followups the moment
      //    an operator flips that mailbox — and a lead already pushed to Instantly
      //    holds no local step bodies, so its followups would simply stop. Same
      //    persist-at-write reasoning as `accountEmail` beside it.
      await db
        .update(instantlyCampaigns)
        .set({
          instantlyCampaignId: sendResult.value.instantlyCampaignId,
          accountEmail: sendResult.value.account.email,
          // The lead's own timezone, raw as the caller sent it. It decides which
          // UTC day each of this sequence's sends spends on the mailbox, and a
          // self-send campaign has no bronze config to recover it from later.
          // Null when the caller supplied none — an absence, never a guess.
          timezone: body.timezone ?? null,
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

      // The gold row just written is what makes this lead read `contacted` —
      // tell lead-service now, or its change feed waits up to five minutes for
      // its own reconcile. A freshness hint: detached (never holds the send),
      // never throws, logs its own failure.
      void announceEvidenceChanged(orgId, [body.to], "contacted");

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

      // 7. Queue every step, carrying its two provisioned cost ids.
      //
      //    The `sequence_costs` row is the send QUEUE as much as the billing
      //    hold — see the column comment in `db/schema.ts`. ONE row per step
      //    (both cost ids on it); every reader counts it once per step.
      for (const h of stepHolds) {
        await db.insert(sequenceCosts).values({
          campaignId,
          // Persist the per-lead Instantly campaign id so the webhook/reconcile
          // resolvers can settle this hold even for platform sends
          // (campaignId NULL). See migration 0027.
          instantlyCampaignId: sendResult.value.instantlyCampaignId,
          leadEmail: body.to,
          step: h.step,
          runId: h.runId,
          costId: h.costId,
          domainCostId: h.domainCostId,
          status: "provisioned",
        });

        await updateRun(h.runId, "completed", { orgId, userId, runId: h.runId, tracking });

        stepRuns.push({ step: h.step, runId: h.runId });
      }
      stepHolds = [];

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
      // Give back steps provisioned but never queued (no queue row will ever
      // settle them), then fail any step runs that were already queued.
      await abandonStepHolds(stepHolds, billingIdentity, error?.message ?? "send failed");
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
