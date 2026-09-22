/**
 * WHO the client's sales rep is, for the two sends that copy them.
 *
 * A positive reply produces two things a human reads: the thread is forwarded
 * the moment it lands, and — on the AI meeting-booking channel — we answer the
 * prospect in their own thread to book a meeting. Both already reach the agency
 * inbox. Neither reached the CLIENT, so a customer could not see the words their
 * own prospect wrote, and could not see us negotiating on their behalf.
 *
 * ⚠️ ONE RESOLVER, READ BY BOTH SENDS. Two copies of "who is copied" is how the
 * forward and the reply come to name different people for one brand, minutes
 * apart, on the same conversation. brand-service holds one rep per brand; this
 * is the one place this service asks who it is.
 *
 * ⚠️ RESOLVED HERE, NEVER ACCEPTED FROM A CALLER. `reply-to-lead` already
 * refuses a caller-supplied sending identity, on the reasoning that it would let
 * a message arrive from a mailbox the prospect has never heard from. A caller-
 * supplied copy list is the same failure pointed at the other end of the
 * message: it would let a prospect's private reply be forwarded to an address
 * the brand never named. The brand comes from state this service already holds.
 *
 * ⚠️ ABSENCE CHANGES NOTHING, AND A FAILURE IS NOT AN ABSENCE. A brand that
 * stated no rep — 185 of 188 brands — sends exactly what it sent before this
 * existed, silently, which is not an error. A brand-service that is UNREACHABLE
 * is a different fact: it is logged loudly and then also sends as before, because
 * neither send may start failing over who gets copied. One of them runs inside a
 * third-party webhook that disables the whole subscription on repeated failures,
 * which has already cost this service a six-day outage once.
 */

import { getSalesRep } from "./brand-client";

/**
 * The addresses to put in VISIBLE copy for this brand — empty when there is
 * nobody to copy, or when we could not find out.
 *
 * Returns a LIST rather than a single address because that is the shape both
 * transports and transactional-email-service take, and because it composes with
 * the agency inbox at the call site without either of them special-casing the
 * other. It is at most one entry today: a brand states one rep.
 *
 * A brand id we do not have is not an error — a platform send carries no brand,
 * and a campaign row predating brand tagging carries none either.
 */
export async function salesRepCopyList(
  brandId: string | null | undefined,
  orgId: string | null | undefined,
): Promise<string[]> {
  if (!brandId || !orgId) return [];

  try {
    const rep = await getSalesRep(brandId, orgId);
    return rep.email ? [rep.email] : [];
  } catch (error) {
    // LOUD, and then out of the way. "We could not find out who the rep is" must
    // never read as "this brand wants nobody copied", and must never be the
    // reason a prospect's reply goes unanswered.
    console.error(
      `[instantly-service] sales-rep-copy: could not resolve the rep for brand=${brandId} org=${orgId}; sending without a copy`,
      error,
    );
    return [];
  }
}
