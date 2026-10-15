// The identity key of an address, shared by every per-person lookup (opt-outs,
// not-a-prospect, verifier verdicts, follow-up threads).

/**
 * Trim + lowercase. A prospect written `Joe@X.com` on one send and `joe@x.com`
 * on the next is the same inbox, and matching case-sensitively would let
 * exactly that pair through.
 */
export function normalizeLeadEmail(email: string): string {
  return email.trim().toLowerCase();
}
