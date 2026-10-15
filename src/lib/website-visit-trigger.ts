/**
 * A CLICK ON A LINK IN ONE OF OUR EMAILS IS A WEBSITE VISIT: FIRE ITS OUTCOME TRIGGER, ONCE PER PERSON
 * (CLICK-FIRES-WEBSITE-VISIT-1015, owner 2026-10-15: "Every outcome reached fires its trigger, no exception").
 *
 * features-service publishes exactly one OUTCOME trigger per outcome (`GET /public/channels` `triggers[]`,
 * `type: "outcome"`, `outcomeKey`): `website_visit` -> `website_visited` today, READ off the catalogue, never
 * hard-coded here. campaign-service's internal trigger door (`POST /internal/trigger-events`) records the event
 * for the person and fires every live campaign answering it. lead-service rings that door for every outcome IT
 * records (its tracker, the CRM); a click on our own email is recorded HERE, so it is rung from here.
 *
 * ── WHICH CLICKS ────────────────────────────────────────────────────────────────────────────────
 *
 *  - A REAL `email_link_clicked` promoted into silver for the first time (`promoteEvent`): a webhook replay, a
 *    re-poll, a re-promotion of the same hit dedupes in silver and never reaches this. A scanner never does
 *    either: a self-send hit ruled `scanner` is never promoted (self-send/click-promotion.ts), and an inferred
 *    click (a projection, not a measurement) runs no side effect.
 *  - On a CALLER campaign of an org: a platform send has no campaign, so no (brand, offer) to fire on.
 *  - FROM NOW ON, no back-fill: a click older than `OUTCOME_FRESH_WINDOW_MS` when it is promoted (a re-sync of
 *    an old Instantly snapshot) fires nothing. Same window as lead-service's own outcome sweep.
 *
 * ── ONCE PER PERSON (lead x brand x offer) ──────────────────────────────────────────────────────
 *
 *  - Here: ONE `outcome_trigger_fires` row per (outcome, org, caller campaign, address), written BEFORE anything
 *    is sent. A second click (another step, another link) finds the row and sends nothing.
 *  - At the door: `idempotencyKey` is unique per org at campaign-service, which answers a key it already holds
 *    with the FIRST event (`replayed: true`) and records nothing new. The key sent is lead-service's OWN key for
 *    the same outcome and person (`outcomeIdempotencyKey`, a byte-for-byte mirror of lead-service
 *    `outcome-trigger-rules.ts`): a person lead-service already fired `website_visited` for from its tracker,
 *    or fires it for later, is ONE event whichever service rang first. The same key also folds two campaigns of
 *    the same (brand, offer) that clicked the same person into one event.
 *
 * ── NEVER SWALLOWED, NEVER BLOCKING ─────────────────────────────────────────────────────────────
 *
 * The click is recorded first (silver) and stands whatever happens here. The row is the retry: a delivery that
 * cannot reach the door, or cannot read the catalogue or the campaign, stays `pending` and is retried with
 * backoff by the drain (outcome-trigger-worker.ts), logged loud at every attempt; a 400 is `refused`, logged
 * and kept, never resent (the same bytes cannot succeed). Delivery runs DETACHED from the click's promotion.
 */

import { createHash } from "node:crypto";
import { and, asc, eq, inArray, lte } from "drizzle-orm";
import { db } from "../db";
import { outcomeTriggerFires } from "../db/schema";
import { getCampaignTriggerScope, recordTriggerEvent, TriggerDoorError } from "./campaign-client";
import { getOutcomeTriggers, outcomeTriggerIdFor, WEBSITE_VISIT_STEP_KEY } from "./leg-catalogue";

/** A click older than this when it is promoted fires nothing: a re-sync, never a visit reached now. */
export const OUTCOME_FRESH_WINDOW_MS = 24 * 60 * 60_000;
/** The writer delivers its own row; the drain leaves it alone this long (it would only replay it). */
export const WRITER_DELIVERY_LEASE_MS = 2 * 60_000;
/** A row the drain took is not taken again before this (a crash mid-delivery is picked up after it). */
export const DRAIN_LEASE_MS = 5 * 60_000;
export const MAX_BACKOFF_MS = 60 * 60_000;
export const DRAIN_BATCH = 100;

// ── pure ─────────────────────────────────────────────────────────────────────────────────────────

/** Trimmed, lowercased; null when it is not an address. */
export function canonicalEmail(email: string | null | undefined): string | null {
  const c = (email ?? "").trim().toLowerCase();
  return c.includes("@") ? c : null;
}

/** Whether a click dated `clickedAt` fires now (no back-fill). */
export function isFreshClick(clickedAt: Date, now: Date): boolean {
  const t = clickedAt.getTime();
  return Number.isFinite(t) && now.getTime() - t <= OUTCOME_FRESH_WINDOW_MS;
}

/** Retry delay after `attempts` failed deliveries: 1 min, doubling, capped at an hour. */
export function backoffMs(attempts: number): number {
  return Math.min(MAX_BACKOFF_MS, 60_000 * 2 ** Math.max(0, attempts - 1));
}

const sha256 = (s: string): string => createHash("sha256").update(s).digest("hex");

/**
 * campaign-service's idempotency key for an outcome reached by a person: lead-service's OWN key, byte for byte
 * (lead-service `src/lib/outcome-trigger-rules.ts` `personKeyOf` + `outcomeIdempotencyKey`), so the door
 * answers a fire of the same (outcome, brand, offer, person) from either service with ONE event. The person is
 * lead-service's lead id (the send carried it: `instantly_campaigns.lead_id`, the same id lead-service resolves
 * from the address: one lead per address); without one, the hashed canonical address, as lead-service does.
 * ⚠️ Do NOT reword: a different key is a second event for the same person.
 */
export function outcomeIdempotencyKey(input: {
  outcomeKey: string;
  brandId: string;
  offerId: string;
  leadId: string | null;
  email: string;
}): string {
  const person = input.leadId ? `lead:${input.leadId}` : `email:${sha256(`email:${input.email}`).slice(0, 32)}`;
  return `lead-service:outcome:${input.outcomeKey}:${input.brandId}:${input.offerId}:${person}`;
}

// ── the store ────────────────────────────────────────────────────────────────────────────────────

/** One fire as the drain delivers it. */
export interface OutcomeFire {
  id: string;
  outcomeKey: string;
  orgId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  leadId: string | null;
  sourceEventId: string | null;
  occurredAt: Date;
  attempts: number;
}

export type NewOutcomeFire = Omit<OutcomeFire, "id" | "attempts">;

/** How a delivery ends (or does not, yet). */
export type FireSettlement =
  | {
      status: "delivered";
      triggerId: string;
      brandId: string;
      offerId: string;
      idempotencyKey: string;
      campaignEventId: string | null;
      replayed: boolean;
    }
  | {
      status: "refused";
      triggerId: string;
      brandId: string;
      offerId: string;
      idempotencyKey: string;
      error: string;
    }
  | { status: "skipped"; skipReason: string; detail: string; triggerId?: string | null; brandId?: string | null; offerId?: string | null }
  | { status: "pending"; attempts: number; nextAttemptAt: Date; error: string };

export interface OutcomeFireStore {
  /** Write the fire once; null when this person already holds one for this outcome and campaign. */
  insertOnce(fire: NewOutcomeFire, firstAttemptAt: Date): Promise<OutcomeFire | null>;
  /** Take the pending fires due at `now`, leased until `leaseUntil` (two takers never take the same row). */
  claimDue(now: Date, leaseUntil: Date, limit: number): Promise<OutcomeFire[]>;
  /** Write how a delivery ended. Only a row still `pending` moves. */
  settle(id: string, settlement: FireSettlement, now: Date): Promise<void>;
}

type FireRow = typeof outcomeTriggerFires.$inferSelect;

function toFire(r: FireRow): OutcomeFire {
  return {
    id: r.id,
    outcomeKey: r.outcomeKey,
    orgId: r.orgId,
    campaignId: r.campaignId,
    instantlyCampaignId: r.instantlyCampaignId,
    leadEmail: r.leadEmail,
    leadId: r.leadId ?? null,
    sourceEventId: r.sourceEventId ?? null,
    occurredAt: r.occurredAt,
    attempts: r.attempts,
  };
}

/** The store on this service's database. */
export const drizzleOutcomeFireStore: OutcomeFireStore = {
  async insertOnce(fire, firstAttemptAt) {
    const rows = await db
      .insert(outcomeTriggerFires)
      .values({ ...fire, status: "pending", nextAttemptAt: firstAttemptAt })
      .onConflictDoNothing()
      .returning();
    return rows[0] ? toFire(rows[0]) : null;
  },

  async claimDue(now, leaseUntil, limit) {
    const t = outcomeTriggerFires;
    const due = db
      .select({ id: t.id })
      .from(t)
      .where(and(eq(t.status, "pending"), lte(t.nextAttemptAt, now)))
      .orderBy(asc(t.nextAttemptAt))
      .limit(limit)
      .for("update", { skipLocked: true });
    const rows = await db
      .update(t)
      .set({ nextAttemptAt: leaseUntil, updatedAt: now })
      .where(and(inArray(t.id, due), eq(t.status, "pending")))
      .returning();
    return rows.map(toFire);
  },

  async settle(id, s, now) {
    const t = outcomeTriggerFires;
    let set: Partial<typeof outcomeTriggerFires.$inferInsert>;
    switch (s.status) {
      case "delivered":
        set = {
          status: "delivered",
          triggerId: s.triggerId,
          brandId: s.brandId,
          offerId: s.offerId,
          idempotencyKey: s.idempotencyKey,
          campaignEventId: s.campaignEventId,
          replayed: s.replayed,
          lastError: null,
          deliveredAt: now,
        };
        break;
      case "refused":
        set = {
          status: "refused",
          triggerId: s.triggerId,
          brandId: s.brandId,
          offerId: s.offerId,
          idempotencyKey: s.idempotencyKey,
          lastError: s.error.slice(0, 500),
        };
        break;
      case "skipped":
        set = {
          status: "skipped",
          skipReason: s.skipReason,
          triggerId: s.triggerId ?? null,
          brandId: s.brandId ?? null,
          offerId: s.offerId ?? null,
          lastError: s.detail.slice(0, 500),
        };
        break;
      case "pending":
        set = { attempts: s.attempts, nextAttemptAt: s.nextAttemptAt, lastError: s.error.slice(0, 500) };
        break;
    }
    await db
      .update(t)
      .set({ ...set, updatedAt: now })
      .where(and(eq(t.id, id), eq(t.status, "pending")));
  },
};

// ── delivery ─────────────────────────────────────────────────────────────────────────────────────

export type DeliveryResult = "delivered" | "skipped" | "refused" | "retry";

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function label(fire: OutcomeFire): string {
  return (
    `outcome=${fire.outcomeKey} org=${fire.orgId} campaign=${fire.campaignId} ` +
    `instantlyCampaign=${fire.instantlyCampaignId} lead=${fire.leadEmail}`
  );
}

async function keepForRetry(
  fire: OutcomeFire,
  error: string,
  store: OutcomeFireStore,
  now: Date,
): Promise<DeliveryResult> {
  const attempts = fire.attempts + 1;
  const delay = backoffMs(attempts);
  console.error(
    `[instantly-service] website-visit-trigger: NOT delivered (attempt ${attempts}), retried in ${Math.round(delay / 1000)}s: ${label(fire)}: ${error}`,
  );
  await store.settle(fire.id, { status: "pending", attempts, nextAttemptAt: new Date(now.getTime() + delay), error }, now);
  return "retry";
}

async function skip(
  fire: OutcomeFire,
  store: OutcomeFireStore,
  now: Date,
  s: Extract<FireSettlement, { status: "skipped" }>,
  loud: boolean,
): Promise<DeliveryResult> {
  const line = `[instantly-service] website-visit-trigger: not fired (${s.skipReason}: ${s.detail}): ${label(fire)}`;
  if (loud) console.warn(line);
  else console.log(line);
  await store.settle(fire.id, s, now);
  return "skipped";
}

/** One attempt to ring campaign-service's door for one pending fire. Never throws. */
export async function deliverOutcomeFire(
  fire: OutcomeFire,
  store: OutcomeFireStore = drizzleOutcomeFireStore,
  now: Date = new Date(),
): Promise<DeliveryResult> {
  try {
    let triggerId: string | null;
    try {
      triggerId = outcomeTriggerIdFor(await getOutcomeTriggers(), fire.outcomeKey);
    } catch (error: unknown) {
      return await keepForRetry(fire, `catalogue unreadable: ${describe(error)}`, store, now);
    }
    // The catalogue states no trigger for this outcome: nothing CAN answer it. A catalogue fact, so loud.
    if (!triggerId) {
      return await skip(
        fire,
        store,
        now,
        { status: "skipped", skipReason: "no_outcome_trigger", detail: `the catalogue states no outcome trigger for "${fire.outcomeKey}"` },
        true,
      );
    }

    let scope: Awaited<ReturnType<typeof getCampaignTriggerScope>>;
    try {
      scope = await getCampaignTriggerScope(fire.campaignId, fire.orgId);
    } catch (error: unknown) {
      return await keepForRetry(fire, `campaign scope unreadable: ${describe(error)}`, store, now);
    }
    // campaign-service runs campaigns per (brand, offer): a campaign stating neither has no pipe to fire.
    // An ordinary absence (the positive-reply trigger reads it the same way), not a failure.
    if (!scope) {
      return await skip(fire, store, now, { status: "skipped", skipReason: "campaign_not_found", detail: "campaign-service does not know the campaign", triggerId }, false);
    }
    if (!scope.brandId) {
      return await skip(fire, store, now, { status: "skipped", skipReason: "no_brand", detail: "the campaign states no brand", triggerId }, false);
    }
    if (!scope.offerId) {
      return await skip(
        fire,
        store,
        now,
        { status: "skipped", skipReason: "no_offer", detail: "the campaign states no offer", triggerId, brandId: scope.brandId },
        false,
      );
    }
    const brandId = scope.brandId;
    const offerId = scope.offerId;

    const idempotencyKey = outcomeIdempotencyKey({
      outcomeKey: fire.outcomeKey,
      brandId,
      offerId,
      leadId: fire.leadId,
      email: fire.leadEmail,
    });

    try {
      const { event, replayed } = await recordTriggerEvent({
        orgId: fire.orgId,
        triggerId,
        brandId,
        offerId,
        leadId: fire.leadId,
        requestedByCampaignId: fire.campaignId,
        idempotencyKey,
        person: { email: fire.leadEmail },
        // A click stamped ahead of our clock is due now: campaign-service would otherwise plan it.
        occurredAt: new Date(Math.min(fire.occurredAt.getTime(), now.getTime())).toISOString(),
      });
      await store.settle(
        fire.id,
        { status: "delivered", triggerId, brandId, offerId, idempotencyKey, campaignEventId: event?.id ?? null, replayed },
        now,
      );
      console.log(
        `[instantly-service] website-visit-trigger: ${triggerId} recorded at campaign-service event=${event?.id ?? "-"}` +
          `${replayed ? " (replayed: this person already fired it)" : ""} status=${event?.status ?? "-"} ` +
          `outcome=${event?.outcome ?? "-"} skip=${event?.skipReason ?? "-"} ran=[${(event?.ranCampaignIds ?? []).join(",")}]: ${label(fire)}`,
      );
      return "delivered";
    } catch (error: unknown) {
      // 400 = campaign-service read it and refused it (unknown trigger, shape): the same bytes cannot succeed.
      if (error instanceof TriggerDoorError && error.status === 400) {
        console.error(`[instantly-service] website-visit-trigger: ${triggerId} REFUSED by campaign-service, kept: ${label(fire)}: ${error.message}`);
        await store.settle(fire.id, { status: "refused", triggerId, brandId, offerId, idempotencyKey, error: error.message }, now);
        return "refused";
      }
      return await keepForRetry(fire, describe(error), store, now);
    }
  } catch (error: unknown) {
    console.error(`[instantly-service] website-visit-trigger: delivery failed, the row stays pending: ${label(fire)}: ${describe(error)}`);
    return "retry";
  }
}

/** The click's campaign row, as `promoteEvent` holds it. */
export interface ClickedCampaign {
  instantlyCampaignId: string;
  /** The CALLER campaign (campaign-service's). Null on a platform send. */
  campaignId: string | null;
  orgId: string | null;
  /** lead-service's person id, as the send carried it. */
  leadId?: string | null;
}

/**
 * A REAL click was just promoted into silver: write the person's `website_visit` fire once and deliver it
 * DETACHED. Never throws, never waits on the door: the click stands whatever happens here.
 */
export async function noteWebsiteVisitFromClick(
  campaign: ClickedCampaign,
  leadEmail: string,
  clickedAt: Date,
  sourceEventId: string | null,
  opts: { store?: OutcomeFireStore; now?: Date } = {},
): Promise<void> {
  const store = opts.store ?? drizzleOutcomeFireStore;
  const now = opts.now ?? new Date();
  // A platform send has no caller campaign: no (brand, offer) a pipe could answer on.
  if (!campaign.orgId || !campaign.campaignId) return;
  const email = canonicalEmail(leadEmail);
  if (!email) return;
  if (!isFreshClick(clickedAt, now)) {
    console.log(
      `[instantly-service] website-visit-trigger: not fired (click at ${clickedAt.toISOString()} is older than ` +
        `${OUTCOME_FRESH_WINDOW_MS / 3_600_000}h: a re-sync, no back-fill): campaign=${campaign.instantlyCampaignId} lead=${email}`,
    );
    return;
  }

  let fire: OutcomeFire | null;
  try {
    fire = await store.insertOnce(
      {
        outcomeKey: WEBSITE_VISIT_STEP_KEY,
        orgId: campaign.orgId,
        campaignId: campaign.campaignId,
        instantlyCampaignId: campaign.instantlyCampaignId,
        leadEmail: email,
        leadId: campaign.leadId ?? null,
        sourceEventId,
        occurredAt: clickedAt,
      },
      new Date(now.getTime() + WRITER_DELIVERY_LEASE_MS),
    );
  } catch (error: unknown) {
    console.error(
      `[instantly-service] website-visit-trigger: could NOT record the fire for campaign=${campaign.instantlyCampaignId} ` +
        `lead=${email} (the click itself is recorded): ${describe(error)}`,
    );
    return;
  }
  // This person already holds a fire for this outcome on this campaign: once.
  if (!fire) return;

  void deliverOutcomeFire(fire, store);
}

/** Deliver every pending fire that is due, once each. Never throws per row. */
export async function drainOutcomeTriggerFires(
  opts: { store?: OutcomeFireStore; now?: Date; limit?: number } = {},
): Promise<Record<DeliveryResult, number>> {
  const store = opts.store ?? drizzleOutcomeFireStore;
  const now = opts.now ?? new Date();
  const counts: Record<DeliveryResult, number> = { delivered: 0, skipped: 0, refused: 0, retry: 0 };
  const due = await store.claimDue(now, new Date(now.getTime() + DRAIN_LEASE_MS), opts.limit ?? DRAIN_BATCH);
  for (const fire of due) counts[await deliverOutcomeFire(fire, store, now)]++;
  return counts;
}
