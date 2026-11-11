/**
 * Sender placeholders in the email BODY and SUBJECT (SENDER-NAME-BODY-1111).
 *
 * A script writes the email before we know which mailbox sends it (mailboxes
 * rotate, each with its own persona in the From line). The same four tokens the
 * signature takes (`{first_name}`, `{last_name}`, `{full_name}`,
 * `{sender_email}`, `SIGNATURE_PLACEHOLDERS`) are filled here, per email, from
 * the mailbox that actually sends it, so "My name is {first_name}" always
 * matches the From name and the signature.
 *
 * Guarantees (same as the signature fill, `fillSenderPlaceholders`):
 *   - TOTAL, never throws: this runs inside the per-step dispatch loop, where a
 *     throw is retried every tick and the email never leaves.
 *   - A value the mailbox lacks renders EMPTY (one adjacent space swallowed so
 *     no double space is left), with a warning naming the mailbox.
 *   - No placeholder token survives in what leaves.
 *   - The writer is trusted: ONLY the four exact single-brace tokens are
 *     touched. Every other brace, `{{mustache}}` (Instantly variables,
 *     `{{accountSignature}}`) and `{unsubscribe_link}` stay byte-for-byte, and a
 *     text with no token is returned unchanged (same string).
 *   - Deterministic per mailbox: a thread keeps one mailbox, so every email of
 *     it (follow-ups, replies) renders the same name.
 */

import { SIGNATURE_PLACEHOLDERS, type SignaturePlaceholder, type SignatureSender } from "./brand-email-signature";

/** One of the four tokens, single-braced, never part of a `{{...}}`. */
const SENDER_TOKEN_RE = new RegExp(
  `(?<!\\{)\\{(${SIGNATURE_PLACEHOLDERS.join("|")})\\}(?!\\})`,
  "g",
);

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** The four values for this mailbox (braces removed, trimmed). Pure. */
export function senderPlaceholderValues(
  sender: SignatureSender,
): Record<SignaturePlaceholder, string> {
  const clean = (v: string | null | undefined) => (v ?? "").replace(/[{}]/g, "").trim();
  const first = clean(sender.first_name);
  const last = clean(sender.last_name);
  return {
    first_name: first,
    last_name: last,
    full_name: [first, last].filter(Boolean).join(" "),
    sender_email: clean(sender.email),
  };
}

/**
 * Fill the sender tokens of a body (`html: true`, values HTML-escaped) or a
 * subject (`html: false`, plain text) with the mailbox that sends it. Pure
 * apart from the warning log.
 */
export function fillSenderTokens(
  text: string,
  sender: SignatureSender,
  opts: { html: boolean; where: string },
): string {
  if (!text || !text.includes("{")) return text;
  SENDER_TOKEN_RE.lastIndex = 0;
  if (!SENDER_TOKEN_RE.test(text)) return text;
  SENDER_TOKEN_RE.lastIndex = 0;

  const values = senderPlaceholderValues(sender);
  const missing: string[] = [];
  const filled = text.replace(
    SENDER_TOKEN_RE,
    (token, name: SignaturePlaceholder) => {
      const value = values[name];
      if (value) return opts.html ? escapeHtml(value) : value;
      missing.push(token);
      return EMPTY_MARK;
    },
  );
  if (missing.length === 0) return filled;

  console.warn(
    `[instantly-service] ${opts.where}: ${missing.join(", ")} empty for sender ${sender.email}; rendered without it`,
  );
  return filled
    // "is {full_name} and" -> "is and", "Hi {first_name}," -> "Hi,"
    .replace(new RegExp(`[ \\t]?${EMPTY_MARK}(?=[ \\t,.;:!?)]|$)`, "g"), "")
    .replace(new RegExp(EMPTY_MARK, "g"), "");
}

/** Private-use sentinel for an empty value while tidying; never left in output. */
const EMPTY_MARK = "";

/** Body of an email: HTML, values escaped. */
export function fillSenderTokensInBody(html: string, sender: SignatureSender): string {
  return fillSenderTokens(html, sender, { html: true, where: "body" });
}

/** Subject of an email: plain text. */
export function fillSenderTokensInSubject(subject: string, sender: SignatureSender): string {
  return fillSenderTokens(subject, sender, { html: false, where: "subject" });
}
