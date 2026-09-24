/**
 * Replace the OLD linked opt-out footer in sequence steps already pushed to
 * Instantly with the current link-free line.
 *
 * Why: a controlled placement test on 2026-09-24 isolated the visible
 * `unsubscribe` anchor in our footer as the Gmail spam trigger (15 of 48 in spam
 * with it, 0 of 50 without, same senders, receivers, body and minute). v0.82.33
 * made `UNSUBSCRIBE_FOOTER_HTML` link-free for every NEW build, and self-send
 * steps are signed at dispatch so they pick it up on their own. A sequence on the
 * Instantly transport is different: its step bodies were built once, at
 * `/orgs/send`, and live inside the Instantly campaign — so every step still to
 * go out keeps the old anchor until it is rewritten there.
 *
 * Pure: no IO. The sweep in `linked-footer-cleanup.ts` reads the live bodies from
 * Instantly and PATCHes them back.
 *
 * Only UN-DISPATCHED steps are rewritten (step > lastSentStep), exactly like the
 * escaped-newline repair: an email that already went out is history, and
 * rewriting it would only alter the record Instantly holds.
 */

import { UNSUBSCRIBE_FOOTER_HTML } from "./send-lead";

/**
 * The old footer as Instantly stores it. Instantly's sanitizer turns `&nbsp;`
 * into a real U+00A0 (and may keep the entity), so the spacer paragraph accepts
 * any of the three; the apostrophe is accepted in its common encodings. Anchored
 * on the sentence rather than on the exact attribute list so a sanitizer
 * reordering `style` cannot hide a footer from the repair.
 */
const OLD_LINKED_FOOTER =
  /<p>(?:&nbsp;| |\s)*<\/p>\s*<p[^>]*>\s*Don(?:'|&#39;|&#x27;|’|&rsquo;)t want to hear from me again\?[\s\S]*?<\/p>/g;

/** True when a body still carries the old linked opt-out footer. */
export function hasLinkedFooter(body: string): boolean {
  OLD_LINKED_FOOTER.lastIndex = 0;
  return OLD_LINKED_FOOTER.test(body);
}

/**
 * Swap every old linked footer for the current link-free line. Idempotent: the
 * output carries no old footer, so a second pass is a no-op.
 */
export function replaceLinkedFooter(body: string): string {
  OLD_LINKED_FOOTER.lastIndex = 0;
  return body.replace(OLD_LINKED_FOOTER, UNSUBSCRIBE_FOOTER_HTML);
}

export interface FooterStepBody {
  /** 0-based position in the Instantly `steps` array. */
  index: number;
  body: string;
}

export interface FooterFix {
  index: number;
  before: string;
  after: string;
}

export interface FooterFixPlan {
  fixes: FooterFix[];
  /** 1-based step numbers that carry the old footer but were already sent. */
  skippedAlreadySent: number[];
}

/**
 * Decide which steps to rewrite. `lastSentStep` is the highest step with a REAL
 * (`inferred = false`) `email_sent` event, 0 when nothing went out yet. Steps are
 * 1-based there and 0-based in Instantly's array, hence `index + 1`.
 */
export function planFooterFixes(
  steps: FooterStepBody[],
  lastSentStep: number,
): FooterFixPlan {
  const fixes: FooterFix[] = [];
  const skippedAlreadySent: number[] = [];
  for (const step of steps) {
    if (!hasLinkedFooter(step.body)) continue;
    const stepNumber = step.index + 1;
    if (stepNumber <= lastSentStep) {
      skippedAlreadySent.push(stepNumber);
      continue;
    }
    fixes.push({ index: step.index, before: step.body, after: replaceLinkedFooter(step.body) });
  }
  return { fixes, skippedAlreadySent };
}
