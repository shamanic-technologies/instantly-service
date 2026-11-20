/**
 * Cloudflare API, for the domains whose DNS Cloudflare serves (PF-CLOUDFLARE-1120:
 * the 15 domains bought through Primeforge, moved to Cloudflare because the
 * owner keeps them after Primeforge's Google mailboxes end on 2026-11-29).
 *
 * Token = key-service platform key `cloudflare-dns` (account token: DNS edit on
 * every zone + Email Routing). Never a token in code or env.
 */
import { resolvePlatformKey, type CallerInfo } from "../key-client";

export const CLOUDFLARE_DNS_PROVIDER = "cloudflare-dns";
/** The Cloudflare account holding our zones (and Email Routing destination addresses). */
export const CLOUDFLARE_ACCOUNT_ID = "1132e5bee7f6bad001161cf13ca427b3";

const API = "https://api.cloudflare.com/client/v4";

export class CloudflareApiError extends Error {}

export interface CloudflareClient {
  call<T = unknown>(method: string, path: string, body?: unknown): Promise<T>;
}

/** A client on the given token (tests pass a fake `fetchImpl`). */
export function cloudflareClient(token: string, fetchImpl: typeof fetch = fetch): CloudflareClient {
  return {
    async call<T>(method: string, path: string, body?: unknown): Promise<T> {
      const response = await fetchImpl(`${API}${path}`, {
        method,
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      const text = await response.text();
      let parsed: { success?: boolean; result?: unknown; errors?: unknown } = {};
      try {
        parsed = JSON.parse(text);
      } catch {
        // not JSON: reported below
      }
      if (!response.ok || parsed.success === false) {
        throw new CloudflareApiError(`Cloudflare ${method} ${path}: ${response.status} ${text.slice(0, 300)}`);
      }
      return parsed.result as T;
    },
  };
}

export async function loadCloudflareClient(caller: CallerInfo): Promise<CloudflareClient> {
  return cloudflareClient(await resolvePlatformKey(CLOUDFLARE_DNS_PROVIDER, caller));
}

export interface CloudflareZone {
  id: string;
  name: string;
  status: string;
}

/** The ACTIVE zone of a domain. Throws when absent or not active (nothing is written to a pending zone). */
export async function activeZone(client: CloudflareClient, domain: string): Promise<CloudflareZone> {
  const zones = await client.call<CloudflareZone[]>("GET", `/zones?name=${encodeURIComponent(domain)}`);
  const zone = zones[0];
  if (!zone) throw new CloudflareApiError(`no Cloudflare zone for ${domain}`);
  if (zone.status !== "active") throw new CloudflareApiError(`Cloudflare zone ${domain} is ${zone.status}, not active`);
  return zone;
}

export interface CloudflareRecord {
  id: string;
  type: string;
  name: string;
  content: string;
  priority?: number;
}

/** A record as Cloudflare stores it, from our zone-relative `{name, type, value}` (pure). */
export function toCloudflareRecord(
  domain: string,
  record: { name: string; type: "CNAME" | "MX" | "TXT"; value: string },
): { type: string; name: string; content: string; priority?: number } {
  const name = record.name === "@" || record.name === "" ? domain : `${record.name}.${domain}`;
  if (record.type === "MX") {
    const [priority, host] = record.value.trim().split(/\s+/);
    return { type: "MX", name, content: (host ?? "").replace(/\.$/, ""), priority: Number(priority) };
  }
  if (record.type === "CNAME") return { type: "CNAME", name, content: record.value.replace(/\.$/, "") };
  return { type: "TXT", name, content: record.value };
}

function sameContent(a: string, b: string): boolean {
  const norm = (s: string) => s.trim().toLowerCase().replace(/\.$/, "").replace(/^"|"$/g, "");
  return norm(a) === norm(b);
}

/**
 * Make exactly this record exist (idempotent): created when absent, updated when
 * a record of the same name + type holds another value, untouched when equal.
 * Never proxied. One record per name + type: our names (`<token>._domainkey`,
 * `ses`) hold nothing else.
 */
export async function upsertRecord(
  client: CloudflareClient,
  zoneId: string,
  record: { type: string; name: string; content: string; priority?: number },
): Promise<"created" | "updated" | "unchanged"> {
  const existing = await client.call<CloudflareRecord[]>(
    "GET",
    `/zones/${zoneId}/dns_records?type=${record.type}&name=${encodeURIComponent(record.name)}`,
  );
  const body = { ...record, ttl: 300, proxied: false };
  const current = existing[0];
  if (!current) {
    await client.call("POST", `/zones/${zoneId}/dns_records`, body);
    return "created";
  }
  if (sameContent(current.content, record.content) && (record.priority === undefined || current.priority === record.priority)) {
    return "unchanged";
  }
  await client.call("PUT", `/zones/${zoneId}/dns_records/${current.id}`, body);
  return "updated";
}
