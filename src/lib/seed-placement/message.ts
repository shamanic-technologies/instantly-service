/**
 * The seed message (pure, no IO).
 *
 * A SEED IS THE EMAIL A PROSPECT GETS (UNSUB-3WAY-1103, owner 2026-11-03). It is
 * built by the SAME `buildMessage` as a sequence step: the sending account's
 * From name, our signature block, the List-Unsubscribe header of the variant
 * under test, no rewritten link. The old plain, header-less seed measured a
 * message no prospect ever received, so the opt-out domain every real email
 * carries (link.agent-base.ai) was never measured.
 *
 * Nothing seed-only is added: no custom header (a filter reads every header).
 * Correlation rides on the RFC 5322 `Message-Id` the server assigns, stored on
 * the dispatch row; the receiver and the test are known from that row.
 *
 * The body is a short cold-email first step like the ones the writer produces
 * (no link: 0 of 2,650 bodies written in 24 h on 2026-11-03 carried one).
 */

import type { Account } from "../instantly-client";
import { buildMessage, type BuiltMessage } from "../self-send/message";
import type { OptOutVariant } from "../self-send/opt-out-variant";

export interface SeedMessageInput {
  testId: string;
  /** The sending account as the dispatcher holds it (email + persona name). */
  account: Account;
  receiverEmail: string;
  optOut: OptOutVariant;
  /** Injected in tests; else minted like a prospect's opt-out link. */
  unsubscribeUrl?: string;
}

/**
 * Subject line. No test id or serial number: a subject that looks machine
 * generated is itself a spam signal.
 */
export function seedSubject(): string {
  return "Quick question about your Q3 planning";
}

export const SEED_BODY_HTML = [
  "<p>Hi,</p>",
  "<p>I wanted to check whether you are the right person to speak to about how your team plans capacity for the next quarter.</p>",
  "<p>If it makes sense, I can send over a short summary of what we do. If not, no problem at all.</p>",
].join("");

export function buildSeedMessage(input: SeedMessageInput): BuiltMessage {
  return buildMessage({
    account: input.account,
    signatureHtml: null,
    leadEmail: input.receiverEmail,
    subject: seedSubject(),
    bodyHtml: SEED_BODY_HTML,
    step: 1,
    // The opt-out link a prospect's email would carry, minted for this seed
    // (`seed:` campaign ids are not ours to unsubscribe: the route 404s them).
    identity: { instantlyCampaignId: input.testId, leadEmail: input.receiverEmail },
    optOut: input.optOut,
    ...(input.unsubscribeUrl ? { unsubscribeUrl: input.unsubscribeUrl } : {}),
  });
}
