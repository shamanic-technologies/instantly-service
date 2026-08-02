/**
 * Canonical shapes for the sending-infrastructure inventory.
 *
 * The fleet buys domains and mailboxes from FOUR vendors, and until now only
 * Instantly (and only at the ACCOUNT grain) existed in code. These types are
 * the silver-layer vocabulary every provider client normalises into, so the
 * gold reads never branch on which vendor a domain came from.
 *
 * A domain can legitimately be reported by more than one provider (Gandi is the
 * registrar while the mailboxes live elsewhere), so the silver row is keyed on
 * (provider, domain) and the per-domain rollup is derived on read. Do NOT add a
 * "primary provider" column — that would force a precedence guess into storage.
 */

/** Vendor families. Gandi's three organisations share this one value; the org is carried in `providerAccount`. */
export type InfraProvider = "gandi" | "mailforge" | "primeforge" | "instantly-dfy";

export const INFRA_PROVIDERS: readonly InfraProvider[] = [
  "gandi",
  "mailforge",
  "primeforge",
  "instantly-dfy",
] as const;

/**
 * What the provider does for this domain.
 * - `registrar` — sells/renews the domain name itself.
 * - `mailbox`   — hosts the mailboxes on it.
 * - `prewarm`   — delivered it pre-warmed (Instantly DFY).
 * A vendor that does several keeps the most specific role it is billed for.
 */
export type InfraDomainRole = "registrar" | "mailbox" | "prewarm";

export interface ProviderDomain {
  provider: InfraProvider;
  /** Which vendor account/organisation reported it (`org2`, a workspace id). Null when the vendor has one tenant. */
  providerAccount: string | null;
  /** The vendor's own id for the row, when it has one. */
  externalId: string | null;
  domain: string;
  role: InfraDomainRole;
  /** The vendor's own status string, verbatim — never mapped to a local enum. */
  status: string | null;
  createdAtProvider: Date | null;
  expiresAt: Date | null;
  /** Null when the vendor does not expose the flag (not "false"). */
  autorenew: boolean | null;
  deletionScheduled: boolean;
  cancelledAt: Date | null;
  /** Price the VENDOR reports for this domain. Null when the vendor exposes none — never a guess. */
  priceCents: number | null;
  priceCurrency: string | null;
  payload: unknown;
}

export interface ProviderMailbox {
  provider: InfraProvider;
  providerAccount: string | null;
  externalId: string | null;
  email: string;
  domain: string;
  status: string | null;
  createdAtProvider: Date | null;
  payload: unknown;
}

export interface ProviderInventory {
  domains: ProviderDomain[];
  mailboxes: ProviderMailbox[];
  /** Vendor-level facts worth mirroring (plan ids, prepaid balance). One row per scope. */
  accountScopes: { scope: string; payload: unknown }[];
}

/** Parse a vendor timestamp into a Date, or null. Never throws, never invents "now". */
export function parseProviderDate(value: unknown): Date | null {
  if (typeof value !== "string" || value.length === 0) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}
