/**
 * The invisible boundary between an email's body and its signature
 * (SIG-NO-DASHES-1117). The signature is wrapped in a zero-box `<div>` whose
 * `style` is the marker; see `signatureBlockHtml` in `send-lead.ts`.
 *
 * Its own module so the text readers (`htmlToText`) and the builder share it
 * with no import cycle.
 */
export const SIGNATURE_WRAPPER_OPEN = '<div style="margin:0;padding:0;border:0">';
export const SIGNATURE_WRAPPER_CLOSE = "</div>";

/** The wrapper's opening tag, tolerant of the spacing a sanitizer may give the style. */
export const SIGNATURE_WRAPPER_MARKER =
  /<div\s+style\s*=\s*"\s*margin\s*:\s*0(?:px)?\s*;\s*padding\s*:\s*0(?:px)?\s*;\s*border\s*:\s*0(?:px)?\s*;?\s*"\s*>/i;

/**
 * The wrapper turned back into the retired `--` line, for TEXT readers only
 * (`htmlToText`): every reader that cuts our signature off a text body (spoken
 * thread, quoted history) keys on a `--` line, so it keeps doing so. Never on a
 * body that is sent.
 */
export function signatureWrapperAsDashLine(html: string): string {
  return html.replace(new RegExp(SIGNATURE_WRAPPER_MARKER.source, "gi"), "<p>--</p><div>");
}
