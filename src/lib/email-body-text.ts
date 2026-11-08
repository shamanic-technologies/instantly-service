/**
 * Whether an email body holds anything a person can read.
 *
 * A deterministic cleanup rule, never a judgment of the text: once the HTML
 * tags, the character entities and the whitespace are taken away, the body
 * must keep at least one letter or digit, in any script. A body that fails it
 * is EMPTY to the person who opens it, and an empty email is never sent
 * (`POST /orgs/send` and `POST /orgs/replies` refuse it with a 400 before any
 * spend; the self-send dispatcher skips a stored step that fails it, loudly).
 *
 * Why not `body !== ""`: what reached prospects in prod was never a literal
 * empty string. 2026-10-28..11-03 four emails left with the bodies `<p></p>`
 * (a sequence step the writer left empty), `,`, `>` and
 * `]</think><5e9c7f0d>{` (a model's broken answer), each a non-empty string
 * that the old `=== ""` checks let through.
 */
export function hasVisibleText(html: string | null | undefined): boolean {
  if (typeof html !== "string") return false;
  const text = html
    .replace(/<[^>]*>/g, " ")
    .replace(/&(?:#\d+|#x[0-9a-f]+|[a-z][a-z0-9]*);/gi, " ");
  return /[\p{L}\p{N}]/u.test(text);
}

/** The refusal message both send routes give for a body with nothing readable in it. */
export const EMPTY_EMAIL_BODY_MESSAGE =
  "empty_email_body: the email body has no readable text (no letter or digit once HTML tags and entities are removed); an empty email is never sent";
