/**
 * Build the message we hand to SMTP.
 *
 * The body pipeline is deliberately the EXISTING one — `buildEmailBodyWithSignature`
 * from send-lead.ts — so a self-sent email and an Instantly-sent email are
 * byte-identical apart from the opt-out link. Reimplementing the signature here
 * would fork the idempotent strip-then-append logic that two separate production
 * incidents (stacked signatures, a body reduced to a stray anchor) exist to
 * protect, and the two copies would drift on the next change to either.
 *
 * The one substitution: `{unsubscribe_link}` is Instantly's server-side merge
 * variable, so we resolve it to our own signed URL. Nothing else in the body is
 * templated.
 */

import type { Account } from "../instantly-client";
import { buildEmailBodyWithSignature } from "../send-lead";
import { rewriteLinksForTracking } from "./click-tracking";
import { buildUnsubscribeUrl, unsubscribeOrigin, unsubscribeSecret, type UnsubscribeIdentity } from "./unsubscribe";

/** Instantly's placeholder, single-braced. The `{{...}}` form never resolved. */
export const INSTANTLY_UNSUBSCRIBE_PLACEHOLDER = "{unsubscribe_link}";

export interface BuiltMessage {
  from: string;
  to: string;
  /** Visible CC. Absent on sequence sends — only a one-to-one reply carries one. */
  cc?: string;
  subject: string;
  html: string;
  headers: Record<string, string>;
  /** Set on a followup so the thread stays collapsed in the client. */
  inReplyTo?: string;
  references?: string[];
}

export interface BuildMessageInput {
  account: Account;
  leadEmail: string;
  /** Subject of step 1. Followups reuse it under `Re:`, matching Instantly. */
  subject: string;
  bodyHtml: string;
  step: number;
  identity: UnsubscribeIdentity;
  /** Message-Id of the previous step of this sequence, when there was one. */
  previousMessageId?: string | null;
  /** Every earlier Message-Id, oldest first (RFC 5322 References). */
  priorMessageIds?: readonly string[];
  /** Injected so the URL builder can be exercised without the environment. */
  unsubscribeUrl?: string;
  /** Injected in tests. `null` disables link rewriting entirely. */
  trackingOrigin?: string | null;
  trackingSecret?: string;
}

/**
 * Replace every occurrence of Instantly's placeholder with a real URL.
 *
 * Global rather than first-match: a body that somehow carries two placeholders
 * must not ship one live link and one dead one.
 */
export function resolveUnsubscribePlaceholder(html: string, url: string): string {
  return html.split(INSTANTLY_UNSUBSCRIBE_PLACEHOLDER).join(url);
}

/**
 * Followups reuse step 1's subject prefixed with `Re:`, which is what Instantly
 * does and what keeps the thread collapsed. An `Re:` already present is not
 * doubled — clients render `Re: Re:` verbatim and it reads as machine-sent.
 */
export function subjectForStep(subject: string, step: number): string {
  if (step <= 1) return subject;
  return /^\s*re:/i.test(subject) ? subject : `Re: ${subject}`;
}

/**
 * The From header. Uses the account's own name so the display name, the
 * signature and the mailbox all agree under multi-persona sending — the same
 * reasoning as `buildDefaultSignature`.
 */
export function buildFromHeader(account: Account): string {
  const name = [account.first_name, account.last_name].filter(Boolean).join(" ").trim();
  if (!name) return account.email;
  // Quote the display name and escape what RFC 5322 forbids raw inside a quoted
  // string, so an apostrophe or a comma in a persona name cannot split the header.
  const escaped = name.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  return `"${escaped}" <${account.email}>`;
}

/**
 * Assemble the message.
 *
 * `List-Unsubscribe` + `List-Unsubscribe-Post` are the RFC 8058 one-click pair
 * that drives the mailbox-native unsubscribe button, and Gmail requires them of
 * bulk senders. Instantly set them for us via `insert_unsubscribe_header`; when
 * we dispatch, we set them ourselves. They complement the visible footer link
 * rather than replacing it — the footer is what a prospect actually clicks.
 */
export function buildMessage(input: BuildMessageInput): BuiltMessage {
  const url = input.unsubscribeUrl ?? buildUnsubscribeUrl(input.identity);

  const withSignature = buildEmailBodyWithSignature(input.bodyHtml, input.account);
  const withOptOut = resolveUnsubscribePlaceholder(withSignature, url);

  // Route every OUTBOUND link through our own redirect so clicks are observable.
  // Runs last, on the final html, and skips anything already on our origin —
  // which is exactly the opt-out link we just resolved. Without this,
  // `email_link_clicked` never fires on this transport, so `stop-on-click` would
  // silently never pause a self-sent lead on a visit-first funnel.
  const html =
    input.trackingOrigin === null
      ? withOptOut
      : rewriteLinksForTracking(
          withOptOut,
          {
            instantlyCampaignId: input.identity.instantlyCampaignId,
            leadEmail: input.identity.leadEmail,
            step: input.step,
          },
          input.trackingOrigin ?? unsubscribeOrigin(),
          input.trackingSecret ?? unsubscribeSecret(),
        );

  const headers: Record<string, string> = {
    "List-Unsubscribe": `<${url}>`,
    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
  };

  const references = [...(input.priorMessageIds ?? [])];

  return {
    from: buildFromHeader(input.account),
    to: input.leadEmail,
    subject: subjectForStep(input.subject, input.step),
    html,
    headers,
    ...(input.previousMessageId ? { inReplyTo: input.previousMessageId } : {}),
    ...(references.length > 0 ? { references } : {}),
  };
}
