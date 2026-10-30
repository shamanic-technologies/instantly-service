/**
 * The body of an email WE dispatched, as the prospect received it
 * (UNIBOX-SIGNED-BODY-1030).
 *
 * A sequence step is stored UNSIGNED (`sequence_steps.body_html`): the signature
 * is appended at dispatch by `buildMessage` -> `buildEmailBodyWithSignature`
 * (the thread's pinned client signature with its sender placeholders filled,
 * else ours: the sending account's name + our brand line). Serving the stored
 * step therefore showed the Unibox, and every reader of the conversation, an
 * email nobody received: 19,454 cold emails in 30 days, 2,470 with a signature
 * (only the Instantly-native ones, whose mirror holds the real body).
 *
 * Precedence, per dispatched row:
 *   1. the html we handed to SMTP, recorded since 2026-10-28
 *      (`smtp_sent_messages.message_inputs.built.html`, FACTORY-RECORD-SEND-1028):
 *      the exact body, byte-equal;
 *   2. a sequence step sent once the body ended on the signature
 *      (`SIGNATURE_ONLY_SINCE`) and before the record existed: re-rendered with
 *      the SAME builder from the SAME inputs the dispatch worker reads (the step
 *      body, the thread's pinned signature, the sending account's name in
 *      silver `instantly_accounts`);
 *   3. anything else is served as stored, never a guessed signature: a manual
 *      reply (step 0) is already stored signed (`reply-to-lead` records the
 *      signed body); a step sent before `SIGNATURE_ONLY_SINCE` carried a footer
 *      line this builder no longer writes, and a step whose sending account is
 *      unknown has no name to sign with. Those are counted and logged loud.
 *
 * Nothing here changes what is SENT: it is a read of what already left.
 */

import type { Account } from "../instantly-client";
import { buildEmailBodyWithSignature } from "../send-lead";

/**
 * From this instant a self-sent body is exactly `buildEmailBodyWithSignature`'s
 * output: the footer line under the signature was removed on 2026-09-28 (commit
 * 14d7e4e, 18:04 UTC; deployed the same evening). Midnight after it is the
 * conservative bound: a send in between is served as stored rather than
 * re-rendered without a footer it may have carried.
 */
export const SIGNATURE_ONLY_SINCE = new Date("2026-09-29T00:00:00Z");

/** The columns of one dispatched row the choice reads. */
export interface DispatchedBodyRow {
  /** The stored body: the step's unsigned body, or a manual reply's signed one. */
  bodyHtml: string;
  /** `smtp_sent_messages.message_inputs.built.html`, when the send was recorded. */
  sentHtml?: string | null;
  /** True for a sequence step (step > 0), false for a manual reply. */
  sequenceStep: boolean;
  dispatchedAt: Date | string | null;
  /** `instantly_campaigns.signature_html`: the client's signature pinned on the thread. */
  pinnedSignatureHtml?: string | null;
  /** Whether the sending account exists in silver `instantly_accounts`. */
  accountKnown?: boolean;
  accountEmail: string;
  accountFirstName?: string | null;
  accountLastName?: string | null;
}

export type SentBodySource = "recorded" | "rendered" | "stored";

export interface SentBody {
  bodyHtml: string;
  source: SentBodySource;
  /** Why a sequence step could not be served as sent (`stored` only). */
  unsignedReason?: "sent_before_signature_only" | "account_unknown" | "no_send_date";
}

/** The body the prospect received, or the stored one with the reason it is not. */
export function sentBodyHtml(row: DispatchedBodyRow): SentBody {
  if (typeof row.sentHtml === "string" && row.sentHtml.length > 0) {
    return { bodyHtml: row.sentHtml, source: "recorded" };
  }
  if (!row.sequenceStep) return { bodyHtml: row.bodyHtml, source: "stored" };

  const at = row.dispatchedAt ? new Date(row.dispatchedAt) : null;
  if (!at || Number.isNaN(at.getTime())) {
    return { bodyHtml: row.bodyHtml, source: "stored", unsignedReason: "no_send_date" };
  }
  if (at.getTime() < SIGNATURE_ONLY_SINCE.getTime()) {
    return { bodyHtml: row.bodyHtml, source: "stored", unsignedReason: "sent_before_signature_only" };
  }
  if (!row.accountKnown) {
    return { bodyHtml: row.bodyHtml, source: "stored", unsignedReason: "account_unknown" };
  }

  // The same Account shape the dispatch worker builds from the same silver row.
  const account = {
    email: row.accountEmail,
    first_name: row.accountFirstName ?? undefined,
    last_name: row.accountLastName ?? undefined,
  } as Account;
  return {
    bodyHtml: buildEmailBodyWithSignature(row.bodyHtml, account, row.pinnedSignatureHtml ?? null),
    source: "rendered",
  };
}

/** One loud line per thread read naming every step served without its signature. */
export function logUnsignedSteps(
  instantlyCampaignId: string,
  bodies: { body: SentBody; dispatchId: string }[],
): void {
  const unsigned = bodies.filter((b) => b.body.unsignedReason !== undefined);
  if (unsigned.length === 0) return;
  console.warn(
    `[instantly-service] sent-body: campaign=${instantlyCampaignId} ${unsigned.length} sent step(s) served WITHOUT the signature the prospect received (sent version unknowable): ${unsigned
      .map((b) => `${b.dispatchId}=${b.body.unsignedReason}`)
      .join(", ")}`,
  );
}
