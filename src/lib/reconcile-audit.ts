/**
 * Reconciliation-audit logic (pure — no IO). Powers GET
 * /internal/audit/reconcile, the staff-only view that shows, for each
 * countable fact, OUR local number next to INSTANTLY's number side-by-side —
 * so an operator can spot a divergence on the dashboard and investigate the
 * bug behind it (a lost webhook, a lagging reconcile, a pause/throttle we
 * missed).
 *
 * Why counts and not the forecast's future dates: the sending-forecast VOLUME
 * is an inherent projection — the exact future send DATE of a step exists
 * nowhere (Instantly schedules dynamically off daily limits, business hours,
 * throttling, pauses). So the forecast timing cannot be "reconciled". What CAN
 * be reconciled are the countable facts feeding it (active campaigns, emails
 * sent, leads dispatched, contacts stored) — those have a true counterpart on
 * both sides. `pendingSends` (the forecast's volume magnitude) reconciles too:
 * Instantly does not expose a ready-made remaining-step FIELD, but it exposes
 * everything to DERIVE it — each campaign's `sequences` carry the loaded
 * followup steps (`stepCount`) and analytics carry `emails_sent_count`, so
 * (since one campaign = one lead) Instantly's remaining = `stepCount − sent`.
 *
 * Source-of-truth per metric: wherever Instantly performs the terminal action
 * (runs the campaign, dispatches the email, stores the contact against the plan
 * quota), Instantly is the source of truth and its number is the exact one; the
 * `delta` (local − instantly) is the drift our own records have accumulated.
 */

import type { CampaignAnalytics, CampaignSequenceInfo } from "./instantly-client";

/** Instantly's campaign lifecycle code for an ACTIVE campaign. */
const INSTANTLY_ACTIVE_STATUS = 1;

/** Local-side counts, read from our own silver DB. */
export interface LocalReconcileCounts {
  /** `instantly_campaigns` rows with `status='active'`. */
  activeCampaigns: number;
  /** Silver `email_sent` events (all-time). */
  emailsSent: number;
  /** Distinct `(campaign, lead)` that received ≥1 `email_sent` (= stage "sent"). */
  contactedDispatched: number;
  /** `instantly_campaigns` rows total (1 row = 1 uploaded contact). */
  contactsStored: number;
  /** Provisioned (un-sent) `sequence_costs` steps on live campaigns — the forecast volume. */
  pendingSends: number;
}

/** Instantly-side counts, aggregated from the fleet `/campaigns/analytics` array. */
export interface InstantlyReconcileCounts {
  activeCampaigns: number;
  emailsSent: number;
  contactedDispatched: number;
  contactsStored: number;
  /** Remaining un-sent steps across active campaigns, derived `stepCount − sent`. */
  pendingSends: number;
}

export type SourceOfTruth = "instantly";

export interface ReconcileMetric {
  /** Stable machine key. */
  key: string;
  /** Human label for the dashboard. */
  label: string;
  /** Our local number. */
  local: number;
  /** Instantly's number. */
  instantly: number;
  /** `local − instantly` — the drift. */
  delta: number;
  /** Authoritative side. Instantly runs the sends, so it is SoT for every metric. */
  sourceOfTruth: SourceOfTruth;
}

/**
 * Instantly's own remaining-sends count, derived per campaign. For each ACTIVE
 * campaign, remaining = `stepCount − emailsSent` (one campaign = one lead, so
 * `emails_sent_count` is how many sequence steps already fired), floored at 0.
 * A campaign present in the sequence list but absent from analytics contributes
 * its full `stepCount` (nothing sent yet). Non-active campaigns are excluded —
 * they send no more steps (mirrors the local `pendingSends` active gate).
 */
export function computeInstantlyPendingSends(
  campaigns: CampaignSequenceInfo[],
  sentByCampaign: Map<string, number>,
): number {
  let total = 0;
  for (const c of campaigns) {
    if (c.status !== INSTANTLY_ACTIVE_STATUS) continue;
    const sent = sentByCampaign.get(c.id) ?? 0;
    total += Math.max(0, c.stepCount - sent);
  }
  return total;
}

/**
 * Aggregate the fleet-wide analytics array + campaign sequence lengths into the
 * five reconcilable Instantly counts. `activeCampaigns` counts campaigns whose
 * lifecycle code is ACTIVE (1); `emailsSent`/`contactedDispatched`/
 * `contactsStored` sum across ALL campaigns (cumulative all-time counters,
 * matching the local all-time totals); `pendingSends` is derived per active
 * campaign as `stepCount − sent` (see `computeInstantlyPendingSends`).
 */
export function summarizeInstantlyCounts(
  rows: CampaignAnalytics[],
  campaigns: CampaignSequenceInfo[],
): InstantlyReconcileCounts {
  let activeCampaigns = 0;
  let emailsSent = 0;
  let contactedDispatched = 0;
  let contactsStored = 0;
  const sentByCampaign = new Map<string, number>();
  for (const r of rows) {
    if (r.campaign_status === INSTANTLY_ACTIVE_STATUS) activeCampaigns += 1;
    emailsSent += r.emails_sent_count;
    contactedDispatched += r.contacted_count;
    contactsStored += r.leads_count;
    sentByCampaign.set(r.campaign_id, r.emails_sent_count);
  }
  return {
    activeCampaigns,
    emailsSent,
    contactedDispatched,
    contactsStored,
    pendingSends: computeInstantlyPendingSends(campaigns, sentByCampaign),
  };
}

/**
 * How long a pre-aggregated Instantly snapshot stays "fresh" before an on-read
 * refresh is triggered (stale-while-revalidate). The reconcile audit is a drift
 * monitor, not a live counter — 30 min balances freshness against the cost of a
 * fleet-wide throttled Instantly sweep. See lib/reconcile-snapshot.ts.
 */
export const RECONCILE_SNAPSHOT_TTL_MS = 30 * 60 * 1000;

/**
 * True when the snapshot should be refreshed: it is missing, its timestamp is
 * invalid, or it is older than `ttlMs`. A stale snapshot is still SERVED (the
 * caller revalidates in the background) — this only decides whether to KICK a
 * refresh.
 */
export function isSnapshotStale(
  refreshedAt: Date | null,
  now: Date,
  ttlMs = RECONCILE_SNAPSHOT_TTL_MS,
): boolean {
  if (!refreshedAt || Number.isNaN(refreshedAt.getTime())) return true;
  return now.getTime() - refreshedAt.getTime() >= ttlMs;
}

function metric(
  key: string,
  label: string,
  local: number,
  instantly: number,
): ReconcileMetric {
  return { key, label, local, instantly, delta: local - instantly, sourceOfTruth: "instantly" };
}

/**
 * Pair each local count with its Instantly counterpart (and the `delta`). Order
 * is stable for the dashboard.
 *
 * Only the THREE truly-comparable facts are emitted — `activeCampaigns`,
 * `emailsSent`, `pendingSends`. Two former rows were REMOVED because their two
 * sides measure different quantities, so their delta is a structural artifact,
 * not drift an operator can act on:
 *
 *   - `contactedDispatched` ("Leads dispatched"): Instantly's fleet-aggregate
 *     `contacted_count` tracks per-step DISPATCHES (≈ `emails_sent_count`), NOT
 *     distinct leads, while the local side counts distinct `(campaign, lead)`.
 *     Comparing distinct-leads vs dispatch-count always shows a huge fake gap.
 *   - `contactsStored`: local counts ALL `instantly_campaigns` rows ever created
 *     (never decrements), while Instantly's `leads_count` is CURRENTLY-stored and
 *     drops as the armed finished-contact cleanup deletes contacts to reclaim
 *     quota — so the delta is guaranteed-positive by design, not drift.
 *
 * The two counts are still computed (snapshot + local SQL) but no longer paired
 * into the reconcile output.
 */
export function buildReconciliation(
  local: LocalReconcileCounts,
  instantly: InstantlyReconcileCounts,
): ReconcileMetric[] {
  return [
    metric("activeCampaigns", "Active campaigns", local.activeCampaigns, instantly.activeCampaigns),
    metric("emailsSent", "Emails sent", local.emailsSent, instantly.emailsSent),
    metric("pendingSends", "Pending sends (forecast volume)", local.pendingSends, instantly.pendingSends),
  ];
}
