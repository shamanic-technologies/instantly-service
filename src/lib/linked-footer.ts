/**
 * Remove the retired opt-out footers from sequence steps already pushed to
 * Instantly, so the pending emails end on the signature like every new build.
 *
 * Two footers were retired, both of which sat after the signature:
 *   - the linked one (`Don't want to hear from me again? <a>unsubscribe</a>`),
 *     measured 2026-09-24 as the Gmail spam trigger (15 of 48 in spam with it,
 *     0 of 50 with no footer);
 *   - its link-free successor (`Not relevant? Reply "stop" ...`), removed
 *     2026-09-28 on the owner's decision.
 * New builds carry neither, and self-send steps are signed at dispatch so they
 * pick that up on their own. A sequence on the Instantly transport is different:
 * its step bodies were built once, at `/orgs/send`, and live inside the Instantly
 * campaign, so every step still to go out keeps its footer until rewritten there.
 *
 * Pure: no IO. The sweep in `linked-footer-cleanup.ts` reads the live bodies from
 * Instantly and PATCHes them back.
 *
 * Only UN-DISPATCHED steps are rewritten (step > lastSentStep), exactly like the
 * escaped-newline repair: an email that already went out is history, and
 * rewriting it would only alter the record Instantly holds.
 */

/** The blank spacer paragraph each footer opened with. Instantly's sanitizer
 * turns `&nbsp;` into a real U+00A0 (and may keep the entity). */
const SPACER = String.raw`(?:<p>(?:&nbsp;|\u00a0|\s)*<\/p>\s*)?`;
const APOS = String.raw`(?:'|&#39;|&#x27;|’|&rsquo;)`;
const QUOTE = String.raw`(?:"|&quot;|&#34;|“|”|&ldquo;|&rdquo;)`;

/**
 * Either retired footer as Instantly stores it. Anchored on the sentence rather
 * than on the exact attribute list so a sanitizer reordering `style` cannot hide
 * a footer from the repair; the apostrophe and quotes are accepted in their
 * common encodings.
 */
const RETIRED_FOOTER = new RegExp(
  SPACER +
    String.raw`<p[^>]*>\s*(?:Don` + APOS + String.raw`t want to hear from me again\?|Not relevant\? Reply ` + QUOTE + "stop" + QUOTE + String.raw`)[\s\S]*?<\/p>`,
  "g",
);

/** True when a body still carries a retired opt-out footer. */
export function hasLinkedFooter(body: string): boolean {
  RETIRED_FOOTER.lastIndex = 0;
  return RETIRED_FOOTER.test(body);
}

/**
 * Remove every retired footer. Idempotent: the output carries none, so a second
 * pass is a no-op.
 */
export function replaceLinkedFooter(body: string): string {
  RETIRED_FOOTER.lastIndex = 0;
  return body.replace(RETIRED_FOOTER, "");
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
