/**
 * Who asked for a reply to be sent through `POST /orgs/replies`.
 *
 * `POST /orgs/replies` had no actor at all: the responder's draft and a person's
 * own words arrived through the same door. The field is frozen onto the bronze
 * row at write (the caller already knows, nothing downstream can re-derive it)
 * and decides only the gates an AUTOMATED reply alone goes through (a hand-over,
 * a person the brand stated is not theirs).
 *
 * ⚠️ A PERSON'S HAND REPLY NEVER LOCKS A THREAD (EVERY-REPLY-LEAD-EVENT-1105,
 * owner 2026-11-05, undoing the 2026-10-13 rule « a thread a person moved to
 * their own mailbox STAYS theirs »): « Juste si Sophie détecte que j'ai répondu
 * à la main ça doit être dans l'unibox et influencer sa décision de followup
 * now ou plus tard. » The hand reply stays in the person's Unibox timeline,
 * where the AI reads it; the AI decides now / later / nothing. There is no
 * takeover gate here any more, and none must come back.
 */

export type ReplySender = "human" | "automation";

/**
 * What an absent `sent_by` means: `automation`. The responder is the only
 * caller that omits it; a human surface declares `human` explicitly.
 */
export const DEFAULT_REPLY_SENDER: ReplySender = "automation";

/** Resolve the wire value, absent or not, to the vocabulary above. */
export function resolveReplySender(
  value: ReplySender | null | undefined,
): ReplySender {
  return value ?? DEFAULT_REPLY_SENDER;
}
