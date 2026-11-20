/**
 * Who serves a domain's DNS, read from the domain's own NS records (PF-CLOUDFLARE-1120).
 *
 * The answer decides which API writes a record for that domain (SES DKIM / MAIL
 * FROM, Email Routing): Gandi LiveDNS or Cloudflare. It is a FACT read live, never
 * a stored list of domains: a domain moving from Gandi or Primeforge to Cloudflare
 * is picked up the moment its nameservers change, and a domain whose DNS sits
 * anywhere else is reported (named by its NS) and never written to.
 */
import { Resolver } from "node:dns/promises";

export type DnsHost =
  | { host: "cloudflare"; nameservers: string[] }
  | { host: "gandi"; nameservers: string[] }
  | { host: "unsupported"; nameservers: string[] };

/** Pure: every NS must agree, else the domain is mid-move and nothing is written. */
export function dnsHostOf(nameservers: readonly string[]): DnsHost {
  const ns = nameservers.map((n) => n.trim().toLowerCase().replace(/\.$/, "")).filter(Boolean).sort();
  if (ns.length > 0 && ns.every((n) => n.endsWith(".ns.cloudflare.com"))) return { host: "cloudflare", nameservers: ns };
  if (ns.length > 0 && ns.every((n) => n.endsWith(".gandi.net"))) return { host: "gandi", nameservers: ns };
  return { host: "unsupported", nameservers: ns };
}

let resolver: Resolver | null = null;

/** Public resolvers, so a box-local cache never answers for a move that already happened. */
function publicResolver(): Resolver {
  if (!resolver) {
    resolver = new Resolver({ timeout: 5_000, tries: 2 });
    resolver.setServers(["1.1.1.1", "8.8.8.8"]);
  }
  return resolver;
}

/** The domain's DNS host. Throws when the NS lookup itself fails (retried next pass). */
export async function resolveDnsHost(domain: string): Promise<DnsHost> {
  return dnsHostOf(await publicResolver().resolveNs(domain));
}
