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
 * both sides. `pendingSends` (the forecast's volume magnitude) is exposed too
 * but as LOCAL-ONLY: Instantly does not expose a remaining-step count, so there
 * is no honest counterpart to diff it against.
 *
 * Source-of-truth per metric: wherever Instantly performs the terminal action
 * (runs the campaign, dispatches the email, stores the contact against the plan
 * quota), Instantly is the source of truth and its number is the exact one; the
 * `delta` (local − instantly) is the drift our own records have accumulated.
 */

import type { CampaignAnalytics } from "./instantly-client";

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
}

export type SourceOfTruth = "instantly" | "local";

export interface ReconcileMetric {
  /** Stable machine key. */
  key: string;
  /** Human label for the dashboard. */
  label: string;
  /** Our local number. */
  local: number;
  /** Instantly's number, or `null` when Instantly exposes no counterpart. */
  instantly: number | null;
  /** `local − instantly`; `null` when there is no Instantly counterpart. */
  delta: number | null;
  /** Which side is authoritative (`local` = no Instantly counterpart exists). */
  sourceOfTruth: SourceOfTruth;
}

/**
 * Aggregate the fleet-wide per-campaign analytics array into the four
 * reconcilable Instantly counts. `activeCampaigns` counts campaigns whose
 * lifecycle code is ACTIVE (1); the other three sum across ALL campaigns
 * (their cumulative all-time counters), matching the local all-time totals.
 */
export function summarizeInstantlyCounts(
  rows: CampaignAnalytics[],
): InstantlyReconcileCounts {
  let activeCampaigns = 0;
  let emailsSent = 0;
  let contactedDispatched = 0;
  let contactsStored = 0;
  for (const r of rows) {
    if (r.campaign_status === INSTANTLY_ACTIVE_STATUS) activeCampaigns += 1;
    emailsSent += r.emails_sent_count;
    contactedDispatched += r.contacted_count;
    contactsStored += r.leads_count;
  }
  return { activeCampaigns, emailsSent, contactedDispatched, contactsStored };
}

function metric(
  key: string,
  label: string,
  local: number,
  instantly: number | null,
  sourceOfTruth: SourceOfTruth,
): ReconcileMetric {
  return {
    key,
    label,
    local,
    instantly,
    delta: instantly === null ? null : local - instantly,
    sourceOfTruth,
  };
}

/**
 * Pair each local count with its Instantly counterpart (and the `delta`). Order
 * is stable for the dashboard. `pendingSends` is emitted last, local-only
 * (Instantly exposes no remaining-step count), so its `instantly`/`delta` are
 * `null` and it renders as a single-number row.
 */
export function buildReconciliation(
  local: LocalReconcileCounts,
  instantly: InstantlyReconcileCounts,
): ReconcileMetric[] {
  return [
    metric("activeCampaigns", "Active campaigns", local.activeCampaigns, instantly.activeCampaigns, "instantly"),
    metric("emailsSent", "Emails sent", local.emailsSent, instantly.emailsSent, "instantly"),
    metric("contactedDispatched", "Leads dispatched", local.contactedDispatched, instantly.contactedDispatched, "instantly"),
    metric("contactsStored", "Contacts stored", local.contactsStored, instantly.contactsStored, "instantly"),
    metric("pendingSends", "Pending sends (forecast volume)", local.pendingSends, null, "local"),
  ];
}
