/**
 * How a cold email offers its opt-out, and the three ways we measure
 * (UNSUB-3WAY-1103, owner 2026-11-03: « Si on teste, on teste les 3 options,
 * les deux que tu dis et rien »). Pure, no IO.
 *
 * - `tracker_link`: the RFC 8058 one-click pair on our neutral opt-out domain
 *   (`SELF_SEND_PUBLIC_URL`, link.agent-base.ai). What every email carried
 *   until this experiment, so it stays the default until the seeds decide.
 * - `mailbox_only`: `List-Unsubscribe: <mailto:the sending mailbox>`. No outside
 *   domain in the message at all; the mailbox client's unsubscribe button sends
 *   an email to our own mailbox, which the reply reader files as an opt-out.
 *   No `List-Unsubscribe-Post`: RFC 8058 one-click is defined for https only.
 * - `none`: no List-Unsubscribe header. A reply asking us to stop still ends
 *   every follow-up (reply opt-out classifier).
 *
 * Gmail's one-click requirement binds senders above 5,000 emails a day to
 * Gmail; each of our domains sends ~50.
 */

export const OPT_OUT_VARIANTS = ["tracker_link", "mailbox_only", "none"] as const;
export type OptOutVariant = (typeof OPT_OUT_VARIANTS)[number];

/** Today's behaviour, kept until every variant has the expert-cited sample. */
export const DEFAULT_OPT_OUT_VARIANT: OptOutVariant = "tracker_link";

export function isOptOutVariant(value: unknown): value is OptOutVariant {
  return typeof value === "string" && (OPT_OUT_VARIANTS as readonly string[]).includes(value);
}

/** The opt-out target a variant puts in the message, or null for `none`. */
export function optOutTarget(
  variant: OptOutVariant,
  input: { trackerUrl: () => string; senderEmail: string },
): string | null {
  if (variant === "tracker_link") return input.trackerUrl();
  if (variant === "mailbox_only") return `mailto:${input.senderEmail}?subject=unsubscribe`;
  return null;
}

/** The headers a variant adds to the message. */
export function optOutHeaders(variant: OptOutVariant, target: string | null): Record<string, string> {
  if (variant === "none" || target === null) return {};
  if (variant === "mailbox_only") return { "List-Unsubscribe": `<${target}>` };
  return {
    "List-Unsubscribe": `<${target}>`,
    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
  };
}

/**
 * Which variant one seed carries. Rotating over (sender, receiver, test) gives
 * every sender all three variants spread over its receivers and every receiver
 * all three over the senders, so no variant rides one mailbox's reputation; the
 * test offset moves each pair to another variant on the next test. Per-sender
 * volume is unchanged (one seed per receiver, as before).
 */
export function seedOptOutVariant(senderIndex: number, receiverIndex: number, testOffset: number): OptOutVariant {
  const n = OPT_OUT_VARIANTS.length;
  return OPT_OUT_VARIANTS[(((senderIndex + receiverIndex + testOffset) % n) + n) % n];
}

/** Test offset from the UTC day the test runs (whole days since epoch). */
export function seedTestOffset(asOf: Date): number {
  return Math.floor(asOf.getTime() / 86_400_000);
}
