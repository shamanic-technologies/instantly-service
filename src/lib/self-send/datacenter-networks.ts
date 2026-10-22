/**
 * Which networks no person clicks from (CLICK-HUMANS-1018).
 *
 * Mail security detonates links from compute clouds: Microsoft Defender from
 * Azure, Mimecast / Proofpoint / Barracuda from AWS and Google Cloud, cheaper
 * fetchers from OVH, Hetzner, DigitalOcean. A person clicks from their phone,
 * home or office. So a click whose address one of these operators announces is
 * a positive machine fingerprint (`datacenter_network`, `click-classification.ts`).
 *
 * The list is the OPERATORS (a provider list, never a client list); their
 * prefixes come from what each operator's ASNs announce, read from RIPEstat
 * (`announced-prefixes`, one public source for every operator, IPv4 and IPv6),
 * mirrored into `datacenter_networks` and refreshed daily by
 * `datacenter-networks-worker.ts`. An operator whose read fails or comes back
 * empty KEEPS its previous rows (a failed refresh never opens the door).
 *
 * ⚠️ NOT in the list on purpose: security PROXIES people browse through every
 * day (Zscaler, Netskope, Cloudflare WARP, iCloud Private Relay) and the
 * consumer VPN networks (M247, Datacamp). Their egress carries real people.
 */

import { errorMessage } from "../db-error.js";
import { sql } from "drizzle-orm";

import { db } from "../../db";

export interface DatacenterOperator {
  operator: string;
  asns: number[];
  /** Mail domains of the operator's own staff: they click from its network. */
  ownDomains: string[];
}

export const DATACENTER_OPERATORS: DatacenterOperator[] = [
  { operator: "Amazon Web Services", asns: [16509, 14618], ownDomains: ["amazon.com", "aws.com", "amazon.co.uk", "amazon.de", "amazon.fr"] },
  { operator: "Microsoft Azure", asns: [8075], ownDomains: ["microsoft.com"] },
  { operator: "Google Cloud", asns: [15169, 396982], ownDomains: ["google.com"] },
  { operator: "OVHcloud", asns: [16276], ownDomains: ["ovhcloud.com", "ovh.com", "ovh.net"] },
  { operator: "Hetzner", asns: [24940], ownDomains: ["hetzner.com", "hetzner.de"] },
  { operator: "DigitalOcean", asns: [14061], ownDomains: ["digitalocean.com"] },
  { operator: "Oracle Cloud", asns: [31898], ownDomains: ["oracle.com"] },
  { operator: "Akamai Linode", asns: [63949], ownDomains: ["akamai.com", "linode.com"] },
  { operator: "Vultr", asns: [20473], ownDomains: ["vultr.com"] },
  { operator: "Alibaba Cloud", asns: [45102], ownDomains: ["alibaba-inc.com", "alibabacloud.com"] },
];

/** The cadence lives in the DB (deploys restart timers): refresh when older. */
export const DATACENTER_REFRESH_EVERY_MS = 23 * 60 * 60 * 1000;

const RIPESTAT_URL = "https://stat.ripe.net/data/announced-prefixes/data.json";

/** node-postgres resolves `db.execute` to a QueryResult OBJECT, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** The prefixes an ASN announces, per RIPEstat. Throws on anything unreadable. */
export async function fetchAnnouncedPrefixes(
  asn: number,
  fetchImpl: typeof fetch = fetch,
): Promise<string[]> {
  const res = await fetchImpl(`${RIPESTAT_URL}?resource=AS${asn}`, {
    headers: { "User-Agent": "distribute-instantly-service" },
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) throw new Error(`RIPEstat AS${asn}: HTTP ${res.status}`);
  const body = (await res.json()) as { data?: { prefixes?: Array<{ prefix?: unknown }> } };
  const prefixes = (body.data?.prefixes ?? [])
    .map((p) => (typeof p.prefix === "string" ? p.prefix : null))
    .filter((p): p is string => p !== null && /^[0-9a-fA-F:.]+\/\d{1,3}$/.test(p));
  if (prefixes.length === 0) throw new Error(`RIPEstat AS${asn}: no prefix announced`);
  return [...new Set(prefixes)];
}

export interface DatacenterSyncSummary {
  asnsSynced: number;
  prefixes: number;
  failed: Array<{ asn: number; error: string }>;
}

/** Refresh every operator's prefixes. Per ASN: replace in one transaction, or keep. */
export async function syncDatacenterNetworks(
  fetchImpl: typeof fetch = fetch,
): Promise<DatacenterSyncSummary> {
  const summary: DatacenterSyncSummary = { asnsSynced: 0, prefixes: 0, failed: [] };
  for (const op of DATACENTER_OPERATORS) {
    for (const asn of op.asns) {
      try {
        const prefixes = await fetchAnnouncedPrefixes(asn, fetchImpl);
        await db.transaction(async (tx) => {
          await tx.execute(sql`DELETE FROM datacenter_networks WHERE asn = ${asn}`);
          for (let i = 0; i < prefixes.length; i += 1000) {
            const chunk = prefixes.slice(i, i + 1000);
            await tx.execute(sql`
              INSERT INTO datacenter_networks (asn, prefix, operator, operator_domains, fetched_at)
              SELECT ${asn}, network(p::inet), ${op.operator}, ${sql.param(op.ownDomains)}::text[], now()
              FROM unnest(${sql.param(chunk)}::text[]) AS p
              ON CONFLICT (asn, prefix) DO NOTHING
            `);
          }
        });
        summary.asnsSynced += 1;
        summary.prefixes += prefixes.length;
      } catch (error: unknown) {
        const message = errorMessage(error);
        summary.failed.push({ asn, error: message });
        console.error(
          `[instantly-service] datacenter-networks: AS${asn} (${op.operator}) NOT refreshed, previous prefixes kept: ${message}`,
        );
      }
    }
  }
  return summary;
}

/** When the list was last refreshed, null when it was never loaded. */
export async function datacenterNetworksFetchedAt(): Promise<Date | null> {
  const rows = rowsOf(
    await db.execute(sql`SELECT max(fetched_at) AS at FROM datacenter_networks`),
  );
  const at = rows[0]?.at;
  return at ? new Date(String(at)) : null;
}
