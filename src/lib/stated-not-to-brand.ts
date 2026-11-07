/**
 * A person the brand's user STATED is not someone of the brand gets no automated
 * email from us, by any path, until the statement is undone
 * (DUALIST-WA-SILENCE-EMAIL, owner 2026-11-04, Thierry Matin: « On le tag dans
 * distribute.you comme PAS client de notre marque donc il ne recevra pas de
 * réponse de la part de nous »).
 *
 * The statement is a crm-service people correction (`tag_add
 * professional_not_to_brand`); crm-service owns it and its undo, so every gate
 * here ASKS it (lib/crm-client) and stores nothing. WhatsApp is refused by
 * crm-service itself (422 `stated_not_to_brand`); this is the email half, with
 * the same code.
 *
 * Where it is enforced, each like the never-lapsing gates beside it
 * (`lead-optouts.ts`, `not-a-prospect.ts`):
 *   - `POST /orgs/send` gate 3c-bis: 409 `stated_not_to_brand`, nothing reserved,
 *     billed or sent. crm-service unreadable = 502, nothing sent.
 *   - `POST /orgs/status` `sendRefusal` (lib/send-refusal), `until: null`.
 *   - `replyToLead` for every AUTOMATED reply (the responder, the hand-over, the
 *     not-a-prospect reassurance). A person of the brand typing a reply by hand
 *     (`sentBy: "human"`) is never refused.
 *   - The dispatch tick: queued sequences of a stated person are stopped before
 *     anything is selected (`stopQueuedSequencesOfStatedPeople`).
 *
 * Brand-scoped: a statement for brand A says nothing about brand B.
 */

import { errorMessage } from "./db-error.js";
import { readStatedNotToBrand, readStatedNotToBrandMany, type StatedNotToBrandRecord } from "./crm-client";
import { loadQueuedBrandSequences, type QueuedBrandSequence } from "./booked-stops-cold";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { normalizeLeadEmail } from "./lead-email";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";

export { CrmStatedReadError } from "./crm-client";
export type { StatedNotToBrandRecord } from "./crm-client";

/** Machine-readable refusal code, byte-equal to crm-service's WhatsApp 422. */
export const STATED_NOT_TO_BRAND_REFUSAL_CODE = "stated_not_to_brand";

/** One address, the send gate. Null when no brand is in scope. Throws when crm-service cannot answer. */
export async function findStatedNotToBrand(
  orgId: string,
  leadEmail: string,
  brandIds: string[],
  runId?: string | null,
): Promise<StatedNotToBrandRecord | null> {
  const key = normalizeLeadEmail(leadEmail);
  if (!key || brandIds.length === 0) return null;
  const found = await readStatedNotToBrand({ orgId, brandIds, emails: [key], runId });
  return found.get(key) ?? null;
}

/**
 * Many addresses at once (the status read), keyed by NORMALIZED address. Read from the (org, brands) answer
 * crm-service gives whole and this process caches for 30 s (STATUS-CRM-TIMEOUT-1107), never one crm-service
 * request per batch. Throws when no fresh answer exists and crm-service cannot give one.
 */
export async function findStatedNotToBrands(
  orgId: string,
  leadEmails: string[],
  brandIds: string[],
): Promise<Map<string, StatedNotToBrandRecord>> {
  if (brandIds.length === 0 || leadEmails.length === 0) return new Map();
  return readStatedNotToBrandMany({ orgId, brandIds, emails: leadEmails });
}

export function statedNotToBrandRefusal(leadEmail: string, record: StatedNotToBrandRecord) {
  return {
    error: "Stated not to be someone of this brand",
    code: STATED_NOT_TO_BRAND_REFUSAL_CODE,
    details:
      `The brand's team stated that ${leadEmail} is not someone of brand ${record.brandId ?? "(unnamed)"}` +
      `${record.correctionId ? ` (correction ${record.correctionId})` : ""}. ` +
      `No email was sent and nothing was billed; undoing the statement lifts this.`,
    brandId: record.brandId,
    correctionId: record.correctionId,
  };
}

// ─── The dispatch sweep ─────────────────────────────────────────────────────

export const STATED_SWEEP_LIMIT = 200;
const STATED_READ_CONCURRENCY = 4;

export interface StatedSweepSummary {
  /** (org, brand) pairs asked to crm-service. */
  groupsRead: number;
  /** Pairs crm-service could not answer: their sequences are held, never sent. */
  groupsUnreadable: number;
  queuedSequences: number;
  /** Queued sequences without an org: nothing to ask, left as they were. */
  orgless: number;
  stated: number;
  deferred: number;
  stoppedSelfSend: number;
  stoppedInstantly: number;
  failed: number;
}

export interface StatedSweep {
  summary: StatedSweepSummary;
  /** `instantly_campaign_id`s that MUST NOT be sent this tick (stated and not yet stopped, or unreadable). */
  notYetStopped: Set<string>;
}

/** Pure: (org, brand) groups of the queued sequences, each with its addresses and sequences. */
export function groupQueuedByOrgBrand(
  queued: QueuedBrandSequence[],
): Map<string, { orgId: string; brandId: string; emails: Set<string>; sequences: QueuedBrandSequence[] }> {
  const groups = new Map<string, { orgId: string; brandId: string; emails: Set<string>; sequences: QueuedBrandSequence[] }>();
  for (const seq of queued) {
    if (!seq.orgId) continue;
    for (const brandId of seq.brandIds) {
      const key = `${seq.orgId}\u0000${brandId}`;
      let group = groups.get(key);
      if (!group) {
        group = { orgId: seq.orgId, brandId, emails: new Set(), sequences: [] };
        groups.set(key, group);
      }
      group.emails.add(normalizeLeadEmail(seq.leadEmail));
      group.sequences.push(seq);
    }
  }
  return groups;
}

let inFlight: Promise<StatedSweep> | null = null;

/**
 * Ask crm-service, once per (org, brand) with a queued sequence, which queued
 * people the brand stated are not theirs, and stop every queued sequence of
 * such a person at that brand, on its own transport (same two pipes as
 * `booked-stops-cold.ts`).
 *
 * Failure posture: one (org, brand) crm-service cannot answer HOLDS that group's
 * sequences for this tick (never sent on a guess) and is logged loud; the rest
 * of the fleet keeps sending. An unloadable queue throws (the tick fails).
 */
export function stopQueuedSequencesOfStatedPeople(
  caller: CallerInfo,
  limit: number = STATED_SWEEP_LIMIT,
): Promise<StatedSweep> {
  if (inFlight) return inFlight;
  inFlight = sweepStatedPeople(caller, limit).finally(() => {
    inFlight = null;
  });
  return inFlight;
}

async function sweepStatedPeople(caller: CallerInfo, limit: number): Promise<StatedSweep> {
  const queued = await loadQueuedBrandSequences();
  const groups = groupQueuedByOrgBrand(queued);

  const summary: StatedSweepSummary = {
    groupsRead: groups.size,
    groupsUnreadable: 0,
    queuedSequences: queued.length,
    orgless: queued.filter((s) => !s.orgId).length,
    stated: 0,
    deferred: 0,
    stoppedSelfSend: 0,
    stoppedInstantly: 0,
    failed: 0,
  };

  const held = new Set<string>();
  const targets = new Map<string, QueuedBrandSequence>();
  // A few (org, brand) reads at a time: bounded load on crm-service, and a slow
  // answer does not hold the whole tick group after group.
  const pending = [...groups.values()];
  const readNext = async (): Promise<void> => {
    for (let group = pending.shift(); group; group = pending.shift()) {
      let stated: Map<string, StatedNotToBrandRecord>;
      try {
        stated = await readStatedNotToBrandMany({
          orgId: group.orgId,
          brandIds: [group.brandId],
          emails: [...group.emails],
        });
      } catch (error: unknown) {
        summary.groupsUnreadable += 1;
        for (const seq of group.sequences) held.add(seq.instantlyCampaignId);
        console.error(
          `[instantly-service] stated-not-to-brand: crm-service unreadable for org=${group.orgId} brand=${group.brandId}, holding its ${group.sequences.length} queued sequence(s) this tick — ${errorMessage(error)}`,
        );
        continue;
      }
      for (const seq of group.sequences) {
        if (stated.has(normalizeLeadEmail(seq.leadEmail))) targets.set(seq.instantlyCampaignId, seq);
      }
    }
  };
  await Promise.all(Array.from({ length: STATED_READ_CONCURRENCY }, readNext));

  const ordered = [...targets.values()].sort(
    (a, b) =>
      Number(isSelfSendCampaignId(a.instantlyCampaignId)) -
      Number(isSelfSendCampaignId(b.instantlyCampaignId)),
  );
  summary.stated = ordered.length;

  const notYetStopped = new Set<string>(held);
  for (const seq of ordered.slice(limit)) notYetStopped.add(seq.instantlyCampaignId);
  summary.deferred = Math.max(0, ordered.length - limit);

  for (const seq of ordered.slice(0, limit)) {
    const reason = "the brand stated this person is not someone of the brand (crm-service people correction)";
    try {
      if (isSelfSendCampaignId(seq.instantlyCampaignId)) {
        await stopSelfSendSequence(seq, seq.leadEmail, reason);
        summary.stoppedSelfSend += 1;
        continue;
      }
      const { key } = await resolveInstantlyApiKey(seq.orgId ?? "system", "system", caller);
      await updateCampaignStatus(key, seq.instantlyCampaignId, "paused");
      await stopSelfSendSequence(seq, seq.leadEmail, reason);
      summary.stoppedInstantly += 1;
    } catch (error: unknown) {
      summary.failed += 1;
      notYetStopped.add(seq.instantlyCampaignId);
      console.error(
        `[instantly-service] stated-not-to-brand: could not stop campaign=${seq.instantlyCampaignId} lead=${seq.leadEmail} — ${errorMessage(error)}`,
      );
    }
  }

  if (ordered.length > 0 || summary.groupsUnreadable > 0) {
    console.log(`[instantly-service] stated-not-to-brand: sweep ${JSON.stringify(summary)}`);
  }
  return { summary, notYetStopped };
}
