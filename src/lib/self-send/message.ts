/**
 * Build the message we hand to SMTP.
 *
 * The body pipeline is deliberately the EXISTING one — `buildEmailBodyWithSignature`
 * from send-lead.ts — so a self-sent email and an Instantly-sent email are
 * byte-identical. Reimplementing the signature here
 * would fork the idempotent strip-then-append logic that two separate production
 * incidents (stacked signatures, a body reduced to a stray anchor) exist to
 * protect, and the two copies would drift on the next change to either.
 *
 * The opt-out never appears in the body (the body ends on the signature, no
 * footer); how the HEADER offers it is the measured variant
 * (`opt-out-variant.ts`, UNSUB-3WAY-1103). `resolveUnsubscribePlaceholder` is
 * kept so a body that still carries Instantly's `{unsubscribe_link}` never
 * ships it verbatim.
 *
 * ONE build path for every email of ours to a mailbox: a prospect's sequence
 * step AND the inbox-placement seed (`seed-placement/message.ts` calls this),
 * so a seed measures the email a prospect gets.
 */

import type { Account } from "../instantly-client";
import { buildEmailBodyWithSignature } from "../send-lead";
import { fillSenderTokensInSubject } from "../sender-placeholders";
import { optOutHeaders, optOutTarget, type OptOutVariant } from "./opt-out-variant";
import { buildUnsubscribeUrl, type UnsubscribeIdentity } from "./unsubscribe";

/** Instantly's placeholder, single-braced. The `{{...}}` form never resolved. */
export const INSTANTLY_UNSUBSCRIBE_PLACEHOLDER = "{unsubscribe_link}";

export interface BuiltMessage {
  from: string;
  to: string;
  /** Visible CC. Absent on sequence sends — only a one-to-one reply carries one. */
  cc?: string;
  /** Blind copy. Only a hand-over carries one (the agency inbox observing). */
  bcc?: string;
  subject: string;
  html: string;
  headers: Record<string, string>;
  /** Set on a followup so the thread stays collapsed in the client. */
  inReplyTo?: string;
  references?: string[];
  /** Real MIME attachments. Only a one-to-one reply carries any (offer documents). */
  attachments?: { filename: string; content: Buffer; contentType: string }[];
}

export interface BuildMessageInput {
  account: Account;
  /** The client's signature pinned on the thread; null/absent = ours. */
  signatureHtml?: string | null;
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
  /** How the List-Unsubscribe header offers the opt-out (UNSUB-3WAY-1103). */
  optOut: OptOutVariant;
  /** Injected so the URL builder can be exercised without the environment. */
  unsubscribeUrl?: string;
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
 * NO body link is rewritten (UNSUB-3WAY-1103, owner 2026-11-03): a link leaves
 * as written, never through our tracking domain (link.agent-base.ai), whatever
 * opt-out variant wins. The opt-out rides the List-Unsubscribe header only, in
 * the shape the variant says (tracker one-click pair, the sending mailbox
 * itself, or none).
 */
export function buildMessage(input: BuildMessageInput): BuiltMessage {
  const target = optOutTarget(input.optOut, {
    trackerUrl: () => input.unsubscribeUrl ?? buildUnsubscribeUrl(input.identity),
    senderEmail: input.account.email,
  });

  const withSignature = buildEmailBodyWithSignature(
    input.bodyHtml,
    input.account,
    input.signatureHtml ?? null,
  );
  const html = resolveUnsubscribePlaceholder(withSignature, target ?? "");

  const headers = optOutHeaders(input.optOut, target);

  const references = [...(input.priorMessageIds ?? [])];

  return {
    from: buildFromHeader(input.account),
    to: input.leadEmail,
    subject: subjectForStep(fillSenderTokensInSubject(input.subject, input.account), input.step),
    html,
    headers,
    ...(input.previousMessageId ? { inReplyTo: input.previousMessageId } : {}),
    ...(references.length > 0 ? { references } : {}),
  };
}
