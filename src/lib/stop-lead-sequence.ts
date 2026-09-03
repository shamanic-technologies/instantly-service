/**
 * Stopping a lead's sequence on BOTH sides, whichever pipe carries it.
 *
 * `promoteEvent` already does our half of the stop for any `SEQUENCE_STOP_EVENT`:
 * it marks the row and cancels the lead's remaining provisioned holds. That is
 * the ledger. It tells the SENDER nothing.
 *
 * On the Instantly transport the sender is Instantly, and when Instantly itself
 * observed the reply its own stop-on-reply already fired — nothing more is
 * needed. This helper exists for the case where it did NOT observe it: a manual
 * qualification (created precisely because Instantly missed the reply), or a
 * reply we read out of a mailbox whose IMAP link Instantly has lost. There, the
 * only thing that stops the actual email is pausing the campaign.
 *
 * On the self-send transport there is no campaign to pause AND `reconcileAll`
 * skips a `self:` row outright, so nothing downstream would ever discover the
 * stop — both halves have to happen locally, which is what
 * `stopSelfSendSequence` does.
 *
 * Fail-soft by construction: every caller has already committed the fact that
 * the lead replied. Throwing here would fail a request (or a sweep) over the
 * side effect rather than the fact, so an error is logged and swallowed and the
 * sequence keeps running — visibly, in the log, rather than silently.
 */

import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";

export interface StopLeadSequenceInput {
  orgId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  /** Free text for the log — why this sequence is being stopped. */
  reason: string;
  /** Identity for the key-service call on the Instantly branch. */
  caller: CallerInfo;
}

/**
 * Stop the sending for one lead, on whichever transport carries it.
 *
 * Returns true when the stop was applied, false when it failed (logged). The
 * boolean is for a sweep's summary — no caller should branch its own success on
 * it, because the reply itself is already recorded either way.
 */
export async function stopLeadSequence(input: StopLeadSequenceInput): Promise<boolean> {
  try {
    if (isSelfSendCampaignId(input.instantlyCampaignId)) {
      await stopSelfSendSequence(
        {
          instantlyCampaignId: input.instantlyCampaignId,
          campaignId: null,
          orgId: input.orgId,
          userId: null,
          runId: null,
        },
        input.leadEmail,
        input.reason,
      );
      return true;
    }

    const { key } = await resolveInstantlyApiKey(input.orgId, "system", input.caller);
    await updateCampaignStatus(key, input.instantlyCampaignId, "paused");
    console.log(
      `[instantly-service] stop-lead-sequence: paused campaign=${input.instantlyCampaignId} lead=${input.leadEmail} (${input.reason})`,
    );
    return true;
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] stop-lead-sequence: pause failed for campaign=${input.instantlyCampaignId} lead=${input.leadEmail} — ${message}; sequence continues on Instantly`,
    );
    return false;
  }
}
