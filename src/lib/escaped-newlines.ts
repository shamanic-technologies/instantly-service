/**
 * Repair email bodies that carry LITERAL escape sequences where a line break
 * was meant — i.e. the two characters backslash + `n` rendered as visible text
 * to the prospect instead of a paragraph break.
 *
 * Historic bug (producer side): content-generation-service converts the LLM's
 * plain-text email body to HTML by splitting on REAL newline characters. When
 * the model's structured output returns the body with newlines OVER-ESCAPED,
 * the body string holds `\` + `n` rather than U+000A, so the conversion finds
 * nothing to split, emits one unbroken paragraph, and the escape sequences
 * survive into the delivered HTML. Measured against prod on 2026-07-31: 53 of
 * 40,979 delivered campaign configs (~0.13%), continuous from 2026-05-27, across
 * every cold-email workflow and both the sales and PR features — model
 * behaviour, not one broken prompt. The producer fix lives in
 * content-generation-service; this module repairs the sequence steps that
 * Instantly has NOT dispatched yet, so the pending follow-ups render correctly.
 *
 * Pure — no IO. The CLI (`scripts/cleanup-escaped-newlines.ts`) does the
 * Instantly reads/PATCHes.
 */

/** Literal backslash-n / backslash-r-backslash-n as they appear in a body. */
const ESCAPED_NEWLINE_RE = /\\r\\n|\\n/;
const ESCAPED_NEWLINE_RE_G = /\\r\\n|\\n/g;

/** Opening `<p>` tag, capturing its attributes so a split carries them over. */
const PARAGRAPH_RE = /<p(\s[^>]*)?>([\s\S]*?)<\/p>/gi;

/**
 * True when the body carries at least one literal escape sequence where a line
 * break belongs. This is the detector the CLI selects on — a body with only
 * REAL newlines is not affected.
 */
export function hasEscapedNewlines(body: string): boolean {
  return ESCAPED_NEWLINE_RE.test(body);
}

/**
 * Turn literal escape sequences into the HTML the prospect should have seen:
 * a doubled sequence becomes a paragraph break, a single one a `<br>`.
 *
 * Operates PER `<p>` block so a split re-opens the paragraph with the SAME
 * attributes (the unsubscribe footer is a styled `<p>`; its styling must
 * survive). Text outside any `<p>` block degrades to `<br>` only — never
 * synthesising an unbalanced `</p><p>` pair into markup that has no paragraph
 * to close.
 *
 * Idempotent by construction: the output contains no literal escape sequence,
 * so a second pass is a no-op. Also structure-preserving — the signature block
 * and the unsubscribe footer appended by `buildEmailBodyWithSignature` carry no
 * literal escapes, so they come out byte-identical and the strip-then-append
 * idempotency of the send path still holds.
 */
export function fixEscapedNewlines(body: string): string {
  if (!hasEscapedNewlines(body)) return body;

  let out = "";
  let cursor = 0;

  PARAGRAPH_RE.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = PARAGRAPH_RE.exec(body)) !== null) {
    const [whole, rawAttrs, inner] = match;
    out += replaceOutsideParagraph(body.slice(cursor, match.index));
    out += rebuildParagraph(rawAttrs ?? "", inner);
    cursor = match.index + whole.length;
  }

  out += replaceOutsideParagraph(body.slice(cursor));
  return out;
}

/**
 * Rebuild one `<p>` block: a doubled escape opens a new paragraph with the same
 * attributes, a single one becomes a `<br>`. An empty chunk (three or more
 * consecutive escapes) is dropped rather than emitting a blank paragraph.
 */
function rebuildParagraph(attrs: string, inner: string): string {
  if (!hasEscapedNewlines(inner)) return `<p${attrs}>${inner}</p>`;

  const open = `<p${attrs}>`;
  const chunks = splitOnDoubleEscape(inner)
    .map((chunk) => chunk.replace(ESCAPED_NEWLINE_RE_G, "<br>"))
    .filter((chunk) => chunk.trim().length > 0);

  if (chunks.length === 0) return `${open}</p>`;
  return chunks.map((chunk) => `${open}${chunk}</p>`).join("");
}

/** Split on a doubled literal escape (`\n\n`, `\r\n\r\n`, or a mix). */
function splitOnDoubleEscape(text: string): string[] {
  return text.split(/(?:\\r\\n|\\n){2}/);
}

/**
 * Outside a `<p>` block there is no paragraph to close, so every literal escape
 * — doubled or not — degrades to a `<br>`. Never fabricates a `</p><p>` pair.
 */
function replaceOutsideParagraph(text: string): string {
  if (!hasEscapedNewlines(text)) return text;
  return text.replace(ESCAPED_NEWLINE_RE_G, "<br>");
}

export interface SequenceStepBody {
  /** 0-based index in the campaign's `sequences[0].steps` array. */
  index: number;
  body: string;
}

export interface StepFix {
  index: number;
  before: string;
  after: string;
}

export interface StepFixPlan {
  /** Steps that are un-sent AND dirty — the ones to PATCH. */
  fixes: StepFix[];
  /** Dirty steps Instantly has already dispatched — left alone, counted only. */
  skippedSentDirty: number[];
}

/**
 * Decide which sequence steps to rewrite.
 *
 * `lastSentStep` is the highest 1-based step number for which a REAL (non-
 * inferred) `email_sent` event exists — 0 when nothing has been dispatched yet.
 * Config steps are 0-based, so config index `i` is step `i + 1`.
 *
 * An already-dispatched step is NEVER rewritten: the email is gone, rewriting
 * it changes nothing for the prospect and would silently rewrite the historical
 * record Instantly holds. It is reported instead, so a run makes the untouched
 * damage visible rather than hiding it.
 */
export function planStepFixes(
  steps: SequenceStepBody[],
  lastSentStep: number,
): StepFixPlan {
  const fixes: StepFix[] = [];
  const skippedSentDirty: number[] = [];

  for (const step of steps) {
    if (!hasEscapedNewlines(step.body)) continue;
    const stepNumber = step.index + 1;
    if (stepNumber <= lastSentStep) {
      skippedSentDirty.push(stepNumber);
      continue;
    }
    fixes.push({
      index: step.index,
      before: step.body,
      after: fixEscapedNewlines(step.body),
    });
  }

  return { fixes, skippedSentDirty };
}
