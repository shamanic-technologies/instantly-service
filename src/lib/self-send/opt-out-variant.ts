/**
 * How a cold email offers its opt-out, and the three ways we measure
 * (UNSUB-3WAY-1103, owner 2026-11-03: « Si on teste, on teste les 3 options,
 * les deux que tu dis et rien »). Pure, no IO.
 *
 * - `tracker_link`: the RFC 8058 one-click pair on our neutral opt-out domain
 *   (`SELF_SEND_PUBLIC_URL`, link.agent-base.ai). What every email carried
 *   until this experiment.
 * - `mailbox_only`: `List-Unsubscribe: <mailto:the sending mailbox>`. No outside
 *   domain in the message at all; the mailbox client's unsubscribe button sends
 *   an email (subject `unsubscribe`) to our own mailbox, from the address we
 *   wrote to; the poll promotes a real `lead_unsubscribed` for it.
 *   No `List-Unsubscribe-Post`: RFC 8058 one-click is defined for https only.
 * - `none`: no List-Unsubscribe header. A reply asking us to stop still ends
 *   every follow-up (reply opt-out classifier).
 *
 * Gmail's one-click requirement binds senders above 5,000 emails a day to
 * Gmail; each of our domains sends ~50.
 */

export const OPT_OUT_VARIANTS = ["tracker_link", "mailbox_only", "none"] as const;
export type OptOutVariant = (typeof OPT_OUT_VARIANTS)[number];

/**
 * What prospect mail carries until every variant has the expert-cited sample:
 * the sending mailbox only, no outside domain (owner 2026-11-03, « En attendant
 * le test, quel est le défaut à mettre en prod ? » → « go » on mailbox_only).
 * `none` stays test-only (fewer easy exits = more spam complaints).
 */
export const DEFAULT_OPT_OUT_VARIANT: OptOutVariant = "mailbox_only";

/** Subject of the email a mailbox client's unsubscribe button sends us. */
export const MAILTO_UNSUBSCRIBE_SUBJECT = "unsubscribe";

/**
 * True iff an inbound subject is our `mailto:` unsubscribe (the client copies
 * the `?subject=` we minted; Gmail/Outlook send it as-is).
 */
export function isMailtoUnsubscribeSubject(subject: string | null | undefined): boolean {
  return (subject ?? "").trim().toLowerCase() === MAILTO_UNSUBSCRIBE_SUBJECT;
}

/**
 * The sequences a mailto unsubscribe stops (pure): every sequence THIS mailbox
 * sent to the sender's exact address, latest step each. The sender's address
 * is the proof: the button sends from the address we wrote to.
 */
export function mailtoUnsubscribeSends<T extends { instantlyCampaignId: string; leadEmail: string; step: number }>(
  senderAddress: string | null,
  knownSends: Iterable<T>,
): T[] {
  if (!senderAddress) return [];
  const wanted = senderAddress.trim().toLowerCase();
  const byCampaign = new Map<string, T>();
  for (const send of knownSends) {
    if (send.leadEmail.trim().toLowerCase() !== wanted) continue;
    const held = byCampaign.get(send.instantlyCampaignId);
    if (!held || send.step > held.step) byCampaign.set(send.instantlyCampaignId, send);
  }
  return [...byCampaign.values()];
}

export function isOptOutVariant(value: unknown): value is OptOutVariant {
  return typeof value === "string" && (OPT_OUT_VARIANTS as readonly string[]).includes(value);
}

/** The opt-out target a variant puts in the message, or null for `none`. */
export function optOutTarget(
  variant: OptOutVariant,
  input: { trackerUrl: () => string; senderEmail: string },
): string | null {
  if (variant === "tracker_link") return input.trackerUrl();
  if (variant === "mailbox_only") return `mailto:${input.senderEmail}?subject=${MAILTO_UNSUBSCRIBE_SUBJECT}`;
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
