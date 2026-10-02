/**
 * Stalled FIRST emails on the Instantly transport: move them onto our own sender,
 * or close them. Never leave them `active` (instantly-service#969).
 *
 * ── The hole this closes ──────────────────────────────────────────────────────
 * A sequence handed to Instantly is Instantly's to start. Most start the same day;
 * a residue never does — the campaign and the lead both stay "active" on
 * Instantly, nothing is ever sent, and nothing on our side has a deadline on that
 * first send. Measured 2026-10-02: 566 such sequences, created 2026-06-10 to
 * 2026-10-01, 404 of them on mailboxes that had since moved onto our own sender
 * (321 on accounts Instantly had disabled), every one still holding its
 * provisioned holds and read by the ops table as "first email due today", every
 * day, for months. The self-send dispatcher could not see them (it reads
 * `send_transport='smtp'` campaigns only) and the retry-stuck worker that was
 * built for this (redispatch to ANOTHER Instantly campaign) has been switched
 * off since 2026-06-01 for spamming prospects.
 *
 * ── What happens to a stalled sequence ────────────────────────────────────────
 * After `STALLED_FIRST_EMAIL_SENDING_DAYS` full sending days without a first
 * email, the sequence is taken off Instantly (campaign PAUSED there first — the
 * only thing that guarantees Instantly cannot send it later and double up) and:
 *
 *   MOVE  — it becomes a self-send sequence: re-keyed onto a fresh `self:` id
 *           (`send_transport='smtp'`), same account, same holds, same step
 *           bodies. The dispatcher then sends it like any other first email, and
 *           re-homes it to a production mailbox with room when its own has none
 *           or holds no credential. No new campaign, no new hold, no re-charge.
 *   CLOSE — when sending it now would be wrong: no step-1 body stored, no
 *           provisioned hold left (the dispatcher's queue), assigned
 *           more than `STALLED_FIRST_EMAIL_MAX_AGE_DAYS` ago, the person opted
 *           out / replied / bounced / unsubscribed anywhere in the org, was
 *           emailed for the same brand inside the re-contact window, or a NEWER
 *           sequence already holds them for the same brand. The holds are
 *           cancelled (refund) and the row is marked paused, exactly like a
 *           stopped self-send sequence.
 *
 * Why re-key instead of flipping `send_transport` on the Instantly id: every
 * Instantly sweep (`reconcileAll`, retry-stuck, the cleanup CLIs) treats a
 * non-`self:` id as an Instantly campaign, and `reconcileAll` would read the
 * now-paused campaign and cancel the holds of the sequence we just moved.
 *
 * A row Instantly reports as already CONTACTED (our silver missed the send) is
 * left untouched and logged — never resent.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import {
  listLeadsFull,
  updateCampaignStatus,
  type LeadFull,
} from "../instantly-client";
import {
  resolveInstantlyApiKey,
  resolvePlatformInstantlyApiKey,
  type CallerInfo,
} from "../key-client";
import { findStandingOptOut } from "../lead-optouts";
import { findRecentBrandContact } from "../recontact-window";
import { cancelRemainingProvisions } from "../silver-promote";
import { refreshLeadStatusCurrent } from "../status-gold";
import { announceEvidenceChanged } from "../evidence-changed";
import { isSendingDay } from "../sending-calendar";
import { MS_PER_DAY, dateKeyUTC } from "../sending-forecast";
import {
  SELF_SEND_CAMPAIGN_LIKE,
  SEND_TRANSPORT_INSTANTLY,
  SEND_TRANSPORT_SMTP,
  mintSelfSendCampaignId,
} from "./transport";

/**
 * Full sending days (Mon-Fri, UTC) that must pass after the assignment day
 * before a never-started sequence counts as stalled. Instantly starts a healthy
 * sequence the same or the next sending day (2026-09: ~95% sent within a day);
 * three days of silence is not a queue, it is a stall.
 */
export const STALLED_FIRST_EMAIL_SENDING_DAYS = 3;

/**
 * A first email assigned longer ago than this is closed, not sent: its copy was
 * written for that moment, and a cold first touch arriving six weeks after the
 * lead was served reads as exactly that.
 */
export const STALLED_FIRST_EMAIL_MAX_AGE_DAYS = 45;

/** Rows handled per sweep — bounds the Instantly calls one tick can make. */
export const STALLED_FIRST_EMAIL_SWEEP_LIMIT = 50;

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/stalled-first-emails" };

export type StalledAction = "move" | "close";

export type CloseReason =
  | "no_step_body"
  | "no_queued_step"
  | "too_old"
  | "opted_out"
  | "lead_answered"
  | "recent_brand_contact"
  | "newer_sequence";

/** Everything the pure decision reads about one candidate. */
export interface StalledCandidate {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  leadEmail: string;
  brandIds: string[];
  createdAt: Date;
  hasFirstStepBody: boolean;
  /**
   * At least one `provisioned` hold — the dispatcher's queue IS the provisioned
   * ledger, so a sequence without one would be moved and then never sent.
   */
  hasQueuedStep: boolean;
  /** A reply / auto-reply / bounce / unsubscribe for this address in the org. */
  leadAnswered: boolean;
  /** Another sequence for this address, same org and brand, created later. */
  newerSequence: boolean;
  optedOut: boolean;
  recentBrandContact: boolean;
}

/** Sending days strictly between the assignment day and `asOf`'s day. */
export function sendingDaysElapsed(createdAt: Date, asOf: Date): number {
  const start = Date.UTC(
    createdAt.getUTCFullYear(),
    createdAt.getUTCMonth(),
    createdAt.getUTCDate(),
  );
  const todayKey = dateKeyUTC(asOf);
  let n = 0;
  for (let t = start + MS_PER_DAY; dateKeyUTC(new Date(t)) < todayKey; t += MS_PER_DAY) {
    if (isSendingDay(new Date(t))) n += 1;
  }
  return n;
}

/** True once a never-started sequence has waited past the grace period. */
export function isStalledFirstEmail(createdAt: Date, asOf: Date): boolean {
  return sendingDaysElapsed(createdAt, asOf) >= STALLED_FIRST_EMAIL_SENDING_DAYS;
}

/**
 * Move or close one stalled sequence. First matching close reason wins; a
 * sequence with none of them is moved onto our own sender.
 */
export function decideStalledFirstEmail(
  c: StalledCandidate,
  asOf: Date,
): { action: "move" } | { action: "close"; reason: CloseReason } {
  if (c.optedOut) return { action: "close", reason: "opted_out" };
  if (c.leadAnswered) return { action: "close", reason: "lead_answered" };
  if (c.newerSequence) return { action: "close", reason: "newer_sequence" };
  if (c.recentBrandContact) return { action: "close", reason: "recent_brand_contact" };
  if (asOf.getTime() - c.createdAt.getTime() > STALLED_FIRST_EMAIL_MAX_AGE_DAYS * MS_PER_DAY) {
    return { action: "close", reason: "too_old" };
  }
  if (!c.hasFirstStepBody) return { action: "close", reason: "no_step_body" };
  if (!c.hasQueuedStep) return { action: "close", reason: "no_queued_step" };
  return { action: "move" };
}

/**
 * True when Instantly's own lead record says the sequence DID start — a contact
 * timestamp or any engagement. Our silver then missed a send, and the sequence
 * must not be resent from here.
 */
export function instantlyShowsContact(leads: LeadFull[]): boolean {
  return leads.some(
    (l) =>
      Boolean(l.timestamp_last_contact) ||
      (l.email_open_count ?? 0) > 0 ||
      (l.email_reply_count ?? 0) > 0 ||
      (l.email_click_count ?? 0) > 0,
  );
}

export interface StalledSweepSummary {
  dryRun: boolean;
  candidates: number;
  stalled: number;
  moved: number;
  closed: number;
  closedByReason: Partial<Record<CloseReason, number>>;
  skippedContactedOnInstantly: number;
  failed: number;
  skippedConcurrent: boolean;
}

function emptySummary(dryRun: boolean): StalledSweepSummary {
  return {
    dryRun,
    candidates: 0,
    stalled: 0,
    moved: 0,
    closed: 0,
    closedByReason: {},
    skippedContactedOnInstantly: 0,
    failed: 0,
    skippedConcurrent: false,
  };
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  return ((result as { rows?: Record<string, unknown>[] }).rows ?? []) as Record<string, unknown>[];
}

/**
 * Never-started Instantly-transport sequences, oldest first. Hold-less rows are
 * included on purpose: an `active` row nobody will ever send is a stall whether
 * or not it still carries a hold.
 */
async function loadCandidates(): Promise<Omit<StalledCandidate, "optedOut" | "recentBrandContact">[]> {
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.campaign_id           AS "campaignId",
           c.org_id                AS "orgId",
           c.user_id               AS "userId",
           c.lead_email            AS "leadEmail",
           COALESCE(to_jsonb(c.brand_ids), '[]'::jsonb) AS "brandIds",
           c.created_at            AS "createdAt",
           EXISTS (
             SELECT 1 FROM sequence_steps s
             WHERE s.instantly_campaign_id = c.instantly_campaign_id AND s.step = 1
           ) AS "hasFirstStepBody",
           EXISTS (
             SELECT 1 FROM sequence_costs sc
             WHERE sc.status = 'provisioned'
               AND (sc.instantly_campaign_id = c.instantly_campaign_id
                    OR (sc.instantly_campaign_id IS NULL
                        AND sc.lead_email = c.lead_email
                        AND sc.campaign_id IS NOT DISTINCT FROM c.campaign_id))
           ) AS "hasQueuedStep",
           EXISTS (
             SELECT 1
             FROM instantly_campaigns o
             JOIN instantly_events e ON e.campaign_id = o.instantly_campaign_id
             WHERE lower(o.lead_email) = lower(c.lead_email)
               AND o.org_id IS NOT DISTINCT FROM c.org_id
               AND e.event_type IN ('reply_received', 'auto_reply_received', 'email_bounced', 'lead_unsubscribed')
           ) AS "leadAnswered",
           EXISTS (
             SELECT 1 FROM instantly_campaigns o
             WHERE lower(o.lead_email) = lower(c.lead_email)
               AND o.id <> c.id
               AND o.org_id IS NOT DISTINCT FROM c.org_id
               AND o.created_at > c.created_at
               AND o.instantly_campaign_id NOT LIKE 'reserving:%'
               AND (
                 o.brand_ids && c.brand_ids
                 OR (COALESCE(cardinality(c.brand_ids), 0) = 0
                     AND o.campaign_id IS NOT DISTINCT FROM c.campaign_id)
               )
           ) AS "newerSequence"
    FROM instantly_campaigns c
    WHERE c.send_transport = ${SEND_TRANSPORT_INSTANTLY}
      AND c.status = 'active'
      AND c.lead_email IS NOT NULL
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND c.instantly_campaign_id NOT LIKE ${SELF_SEND_CAMPAIGN_LIKE}
      AND c.created_at < date_trunc('day', now())
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id AND e.event_type = 'email_sent'
      )
      AND NOT EXISTS (
        SELECT 1 FROM sequence_costs sc
        WHERE sc.status = 'actual'
          AND (sc.instantly_campaign_id = c.instantly_campaign_id
               OR (sc.instantly_campaign_id IS NULL
                   AND sc.lead_email = c.lead_email
                   AND sc.campaign_id IS NOT DISTINCT FROM c.campaign_id))
      )
    ORDER BY c.created_at
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantlyCampaignId),
    campaignId: r.campaignId === null ? null : String(r.campaignId),
    orgId: r.orgId === null ? null : String(r.orgId),
    userId: r.userId === null ? null : String(r.userId),
    leadEmail: String(r.leadEmail),
    brandIds: Array.isArray(r.brandIds) ? (r.brandIds as string[]) : [],
    createdAt: new Date(r.createdAt as string),
    hasFirstStepBody: r.hasFirstStepBody === true,
    hasQueuedStep: r.hasQueuedStep === true,
    leadAnswered: r.leadAnswered === true,
    newerSequence: r.newerSequence === true,
  }));
}

/** True for an Instantly 404: the campaign is gone, so it cannot send either. */
function isNotFound(error: unknown): boolean {
  return error instanceof Error && / failed: 404 /.test(error.message);
}

/**
 * Re-key the sequence onto a fresh `self:` id, atomically. Every LIVE table keyed
 * on the campaign id follows; bronze stays on the Instantly id it was recorded
 * under. Guarded on the row still being the active, never-sent Instantly row.
 */
async function moveToSelfSend(c: StalledCandidate, asOf: Date): Promise<string | null> {
  const newId = mintSelfSendCampaignId();
  const entry = JSON.stringify({
    from: c.instantlyCampaignId,
    at: asOf.toISOString(),
    reason: "instantly_never_started",
  });
  return db.transaction(async (tx) => {
    const updated = await tx.execute(sql`
      UPDATE instantly_campaigns
      SET instantly_campaign_id = ${newId},
          send_transport = ${SEND_TRANSPORT_SMTP},
          metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{movedFromInstantly}',
            ${entry}::jsonb
          ),
          updated_at = now()
      WHERE instantly_campaign_id = ${c.instantlyCampaignId}
        AND status = 'active'
        AND send_transport = ${SEND_TRANSPORT_INSTANTLY}
      RETURNING id
    `);
    if (rowsOf(updated).length === 0) return null;

    await tx.execute(sql`
      UPDATE sequence_costs
      SET instantly_campaign_id = ${newId}, updated_at = now()
      WHERE lead_email = ${c.leadEmail}
        AND (instantly_campaign_id = ${c.instantlyCampaignId}
             OR (instantly_campaign_id IS NULL
                 AND campaign_id IS NOT DISTINCT FROM ${c.campaignId}))
    `);
    await tx.execute(sql`
      UPDATE sequence_steps SET instantly_campaign_id = ${newId}
      WHERE instantly_campaign_id = ${c.instantlyCampaignId}
    `);
    await tx.execute(sql`
      UPDATE instantly_lead_status_current SET instantly_campaign_id = ${newId}
      WHERE instantly_campaign_id = ${c.instantlyCampaignId}
    `);
    await tx.execute(sql`
      UPDATE instantly_leads SET instantly_campaign_id = ${newId}
      WHERE instantly_campaign_id = ${c.instantlyCampaignId}
    `);
    return newId;
  });
}

/** Refund what will never be sent, then take the row out of every queue. */
async function closeStalled(c: StalledCandidate, reason: CloseReason, asOf: Date): Promise<void> {
  await cancelRemainingProvisions(
    {
      instantlyCampaignId: c.instantlyCampaignId,
      campaignId: c.campaignId,
      orgId: c.orgId,
      userId: c.userId,
      runId: null,
    },
    c.leadEmail,
  );
  const entry = JSON.stringify({ reason, at: asOf.toISOString() });
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET status = 'paused',
        metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{stalledFirstEmailClosed}', ${entry}::jsonb),
        updated_at = now()
    WHERE instantly_campaign_id = ${c.instantlyCampaignId}
  `);
  await refreshLeadStatusCurrent(c.instantlyCampaignId, c.leadEmail);
  void announceEvidenceChanged(c.orgId, [c.leadEmail], "stalled_first_email_closed");
}

let sweepInFlight = false;

/**
 * Find every stalled first email and move or close up to `limit` of them.
 * `dryRun` decides and counts without touching Instantly or the DB.
 *
 * Fail-loud per row (counted `failed`, logged, retried next sweep); a row whose
 * Instantly pause fails is left exactly as it was, because moving or closing it
 * while Instantly can still send is how a prospect gets two first emails.
 */
export async function sweepStalledFirstEmails(
  options: { asOf?: Date; limit?: number; dryRun?: boolean } = {},
): Promise<StalledSweepSummary> {
  const dryRun = options.dryRun ?? false;
  if (sweepInFlight) return { ...emptySummary(dryRun), skippedConcurrent: true };
  sweepInFlight = true;
  try {
    return await sweepExclusive(
      options.asOf ?? new Date(),
      options.limit ?? STALLED_FIRST_EMAIL_SWEEP_LIMIT,
      dryRun,
    );
  } finally {
    sweepInFlight = false;
  }
}

async function sweepExclusive(
  asOf: Date,
  limit: number,
  dryRun: boolean,
): Promise<StalledSweepSummary> {
  const summary = emptySummary(dryRun);
  const candidates = await loadCandidates();
  summary.candidates = candidates.length;
  const stalled = candidates.filter((c) => isStalledFirstEmail(c.createdAt, asOf));
  summary.stalled = stalled.length;

  const keys = new Map<string, string>();
  const keyFor = async (orgId: string): Promise<string> => {
    const cached = keys.get(orgId);
    if (cached) return cached;
    const { key } = await resolveInstantlyApiKey(orgId, "system", CALLER);
    keys.set(orgId, key);
    return key;
  };

  for (const base of stalled.slice(0, limit)) {
    try {
      const c: StalledCandidate = {
        ...base,
        optedOut: base.orgId ? (await findStandingOptOut(base.orgId, base.leadEmail)) !== null : false,
        recentBrandContact: (await findRecentBrandContact(base.leadEmail, base.brandIds)) !== null,
      };
      const decision = decideStalledFirstEmail(c, asOf);

      if (dryRun) {
        if (decision.action === "move") summary.moved += 1;
        else {
          summary.closed += 1;
          summary.closedByReason[decision.reason] = (summary.closedByReason[decision.reason] ?? 0) + 1;
        }
        continue;
      }

      const apiKey = c.orgId
        ? await keyFor(c.orgId)
        : await resolvePlatformInstantlyApiKey(CALLER);

      // Ask Instantly before acting: our silver may have missed a send.
      let leads: LeadFull[] = [];
      try {
        leads = await listLeadsFull(apiKey, c.instantlyCampaignId);
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
      if (instantlyShowsContact(leads)) {
        summary.skippedContactedOnInstantly += 1;
        console.warn(
          `[instantly-service] stalled-first-emails: campaign=${c.instantlyCampaignId} lead=${c.leadEmail} shows a contact on Instantly but none in our silver — left untouched`,
        );
        continue;
      }

      // Instantly must not be able to send it once we act on it.
      try {
        await updateCampaignStatus(apiKey, c.instantlyCampaignId, "paused");
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }

      if (decision.action === "move") {
        const newId = await moveToSelfSend(c, asOf);
        if (newId) {
          summary.moved += 1;
          console.log(
            `[instantly-service] stalled-first-emails: moved campaign=${c.instantlyCampaignId} -> ${newId} lead=${c.leadEmail} (assigned ${c.createdAt.toISOString()})`,
          );
        }
      } else {
        await closeStalled(c, decision.reason, asOf);
        summary.closed += 1;
        summary.closedByReason[decision.reason] = (summary.closedByReason[decision.reason] ?? 0) + 1;
        console.log(
          `[instantly-service] stalled-first-emails: closed campaign=${c.instantlyCampaignId} lead=${c.leadEmail} (${decision.reason})`,
        );
      }
    } catch (error) {
      summary.failed += 1;
      console.error(
        `[instantly-service] stalled-first-emails: campaign=${base.instantlyCampaignId} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  console.log(`[instantly-service] stalled-first-emails: done ${JSON.stringify(summary)}`);
  return summary;
}
