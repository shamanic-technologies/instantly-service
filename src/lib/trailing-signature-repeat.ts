/**
 * A body that ENDS with the signature about to be appended loses that trailing
 * copy, so the email carries the signature once (DOUBLE-SIGNATURE-1113).
 *
 * Since SENDER-NAME-BODY-1111 the writer's prompt shows the thread's signature
 * word for word, and some models copy it as the body's last paragraph
 * (`<p>{full_name}<br>Beyond Logic</p>`). `stripAccountSignature` only catches a
 * block after a `--` separator; the writer wrote none, so the send appended
 * `--` + the same lines and the prospect read the signature twice
 * (46 first emails 2026-11-12..13). The writer is trusted (no prompt change, no
 * validator, no retry): this is a deterministic cleanup at build, after it,
 * valid for every model.
 *
 * Scope, on purpose narrow: ONLY a trailing run of the signature's own lines,
 * in order, exactly (HTML `<p>`/`<br>`/`<div>` or plain-text line breaks between
 * them, whitespace/`&nbsp;` tolerated), with nothing after it but closing tags,
 * `<br>` and whitespace. Compared AFTER the body's sender tokens are filled
 * with the sending mailbox, so `{full_name}` and `Kevin Lourd` both match a
 * signature signed by Kevin Lourd. Anything else is returned unchanged.
 * Pure, total, never throws.
 */

/** Text lines of a signature block (`<p>A<br>B</p>` -> ["A", "B"]). */
export function signatureLines(signatureHtml: string): string[] {
  return signatureHtml
    .split(/<br\s*\/?>|<\/?(?:p|div)\b[^>]*>|\r?\n/i)
    .map((l) => l.trim())
    .filter((l) => l !== "");
}

const escapeRe = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** One signature line as a pattern: inner spacing and `&amp;` spelling tolerated. */
function linePattern(line: string): string {
  return line
    .split(/\s+/)
    .map((word) => escapeRe(word).replace(/&amp;/g, "(?:&amp;|&)"))
    .join("(?:\\s|&nbsp;)+");
}

/** Between two signature lines: at least one line break, in any form. */
const BREAK = "(?:\\r?\\n|<br\\s*\\/?>|<\\/?(?:p|div)\\b[^>]*>)";
const LINE_BREAK = `(?:[ \\t]|&nbsp;)*${BREAK}(?:[ \\t]|&nbsp;|${BREAK})*`;
/** After the last line: closing tags, `<br>`, whitespace only. */
const TAIL = "(?:\\s|&nbsp;|<br\\s*\\/?>|<\\/(?:p|div)>)*";
/** Before the first line: start of the body, a newline or the end of a tag. */
const LINE_START = "(?:^|(?<=[\\n>]))(?:[ \\t]|&nbsp;)*";

const TAG_RE = /<(\/?)(p|div)\b[^>]*>/gi;

/** Remove ONE trailing copy, or return null when the body does not end with it. */
function stripOnce(body: string, lines: string[]): string | null {
  const re = new RegExp(`${LINE_START}${lines.map(linePattern).join(LINE_BREAK)}${TAIL}$`);
  const m = re.exec(body);
  if (!m) return null;
  const removed = body.slice(m.index);
  // Keep the closing tags whose opener sits BEFORE the cut, so the HTML stays balanced.
  const open: string[] = [];
  const keptClosers: string[] = [];
  for (const t of removed.matchAll(TAG_RE)) {
    const name = t[2].toLowerCase();
    if (!t[1]) open.push(name);
    else if (open[open.length - 1] === name) open.pop();
    else keptClosers.push(`</${name}>`);
  }
  const head = body
    .slice(0, m.index)
    // "<p>Thanks,<br>" + cut -> "<p>Thanks," ; an opener left empty goes too.
    .replace(/(?:\s|&nbsp;|<br\s*\/?>)*$/i, "");
  let out = head + keptClosers.join("");
  out = out.replace(/<(p|div)\b[^>]*>(?:\s|&nbsp;|<br\s*\/?>)*<\/\1>\s*$/i, "").replace(/\s+$/, "");
  return out;
}

/**
 * The body without its trailing copies of `signatureHtml`; every copy at the
 * very end goes (so the build stays idempotent), nothing else is touched.
 */
export function stripTrailingSignatureRepeat(body: string, signatureHtml: string): string {
  const lines = signatureLines(signatureHtml);
  if (lines.length === 0 || !body) return body;
  let current = body;
  for (;;) {
    const next = stripOnce(current, lines);
    if (next === null || next === current) return current;
    current = next;
  }
}
