/**
 * Replies to a domain whose mailboxes are gone, routed back to us
 * (PF-CLOUDFLARE-1120, owner 2026-11-19/20).
 *
 * ⚠️ WHY. The 15 domains bought through Primeforge keep sending (through SES,
 * `ses-domains.ts`) after Primeforge deletes their Google mailboxes on
 * 2026-11-29. A prospect answers the address that wrote to them; with the
 * mailbox gone, that answer would bounce at Google. So the domain's inbound
 * moves to Cloudflare Email Routing (free): a catch-all forwards EVERY message
 * to any address of the domain to the staff Gmail {@link REPLY_ROUTING_DESTINATION},
 * which google-service mirrors; `forwarded-replies.ts` files each one on the
 * thread its In-Reply-To/References names, under the alias that sent it (the
 * same exact-key correlation as the IMAP poll), on every pass of
 * `email-routing-worker.ts`.
 *
 * WHEN the root MX moves is a stored fact per domain (`mx_switch_after`, set by
 * `POST /internal/self-send/email-routing/schedule`), never a guess: before it
 * the Google mailboxes still receive (IMAP poll, seed receivers, warmup), after
 * it Cloudflare does. Moving it is the only write to a domain's ROOT records
 * this service ever makes: the non-Cloudflare root MX and the root SPF that is
 * not Cloudflare's are deleted, then Cloudflare's own MX/SPF/DKIM are written
 * by Cloudflare (`POST /email/routing/dns`). Cold mail is untouched (SES MAIL
 * FROM `ses.<domain>` carries its SPF; DKIM is SES's). Until that date only the
 * catch-all rule is prepared (harmless: no mail reaches Cloudflare).
 *
 * Which domains: those scheduled, whose NS are Cloudflare's (read live,
 * `dns/dns-host.ts`); a brand domain (`instantly_domain_policy` reason `brand`)
 * is refused at scheduling (its people's own mail lives there).
 *
 * LIVE (= `live_since`): Email Routing enabled and `ready`, the catch-all
 * forwarding to the destination, the destination verified. Readers:
 * `inbound-blindness.ts` (a live domain is READ, through this route) and the
 * forwarded-reply pass. `loadDomainsInboundRoutedAway` (switched, or due before
 * a given instant) keeps seed tests from using those mailboxes as receivers.
 */
import { sql } from "drizzle-orm";

import { db } from "../../db";
import { errorMessage } from "../db-error.js";
import type { CallerInfo } from "../key-client";
import { resolveDnsHost, type DnsHost } from "../dns/dns-host";
import {
  activeZone,
  CLOUDFLARE_ACCOUNT_ID,
  loadCloudflareClient,
  type CloudflareClient,
  type CloudflareRecord,
} from "../dns/cloudflare";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/email-routing" };

/**
 * Where every routed message goes: the staff Gmail google-service mirrors
 * (`STAFF_ADDRESSES` there), the only place `forwarded-replies.ts` reads.
 */
export const REPLY_ROUTING_DESTINATION = "kevin.lourd@gmail.com";

function rowsOf<T>(result: unknown): T[] {
  return (((result as { rows?: unknown[] }).rows ?? []) as T[]);
}

export interface RoutingRow {
  domain: string;
  mxSwitchAfter: Date | null;
  mxSwitchedAt: Date | null;
  liveSince: Date | null;
}

/** Pure: the root records the switch deletes (foreign MX, foreign SPF). Never anything else. */
export function foreignRootRecords(domain: string, records: readonly CloudflareRecord[]): CloudflareRecord[] {
  const apex = domain.toLowerCase();
  return records.filter((r) => {
    if (r.name.toLowerCase() !== apex) return false;
    if (r.type === "MX") return !/\.mx\.cloudflare\.net\.?$/i.test(r.content.trim());
    if (r.type === "TXT") {
      const content = r.content.replace(/^"|"$/g, "").trim().toLowerCase();
      return content.startsWith("v=spf1") && !content.includes("_spf.mx.cloudflare.net");
    }
    return false;
  });
}

/** Pure: is this catch-all ours (forward to the destination, enabled)? */
export function catchAllIsOurs(rule: { enabled?: boolean; actions?: { type: string; value?: string[] }[] } | null): boolean {
  if (!rule || rule.enabled !== true) return false;
  return (rule.actions ?? []).some(
    (a) => a.type === "forward" && (a.value ?? []).map((v) => v.toLowerCase()).includes(REPLY_ROUTING_DESTINATION),
  );
}

/** Pure: is the switch due? */
export function switchDue(row: Pick<RoutingRow, "mxSwitchAfter" | "mxSwitchedAt">, asOf: Date): boolean {
  return row.mxSwitchedAt === null && row.mxSwitchAfter !== null && row.mxSwitchAfter.getTime() <= asOf.getTime();
}

export interface RoutingSummary {
  dryRun: boolean;
  domains: number;
  live: string[];
  switched: string[];
  wouldSwitch: string[];
  waitingForSwitchDate: { domain: string; mxSwitchAfter: string }[];
  pending: { domain: string; reason: string }[];
  failed: { domain: string; error: string }[];
}

async function loadRows(only?: readonly string[]): Promise<RoutingRow[]> {
  const rows = rowsOf<{ domain: string; mxSwitchAfter: Date | null; mxSwitchedAt: Date | null; liveSince: Date | null }>(
    await db.execute(sql`
      SELECT domain, mx_switch_after AS "mxSwitchAfter", mx_switched_at AS "mxSwitchedAt", live_since AS "liveSince"
      FROM email_routing_domains ORDER BY domain
    `),
  );
  const wanted = only && only.length > 0 ? new Set(only.map((d) => d.toLowerCase())) : null;
  return rows
    .filter((r) => !wanted || wanted.has(r.domain))
    .map((r) => ({
      domain: r.domain,
      mxSwitchAfter: r.mxSwitchAfter ? new Date(r.mxSwitchAfter) : null,
      mxSwitchedAt: r.mxSwitchedAt ? new Date(r.mxSwitchedAt) : null,
      liveSince: r.liveSince ? new Date(r.liveSince) : null,
    }));
}

async function saveRow(
  domain: string,
  patch: { zoneId?: string; ruleReady?: boolean; switched?: boolean; live?: boolean; lastError: string | null },
): Promise<void> {
  await db.execute(sql`
    UPDATE email_routing_domains SET
      zone_id = COALESCE(${patch.zoneId ?? null}, zone_id),
      destination = ${REPLY_ROUTING_DESTINATION},
      rule_ready_at = CASE WHEN ${patch.ruleReady ?? false} THEN COALESCE(rule_ready_at, now()) ELSE rule_ready_at END,
      mx_switched_at = CASE WHEN ${patch.switched ?? false} THEN COALESCE(mx_switched_at, now()) ELSE mx_switched_at END,
      live_since = CASE
        WHEN ${patch.live === undefined} THEN live_since
        WHEN ${patch.live ?? false} THEN COALESCE(live_since, now())
        ELSE NULL END,
      last_error = ${patch.lastError},
      checked_at = now()
    WHERE domain = ${domain}
  `);
}

async function destinationVerified(client: CloudflareClient): Promise<boolean> {
  const addresses = await client.call<{ email: string; verified: string | null }[]>(
    "GET",
    `/accounts/${CLOUDFLARE_ACCOUNT_ID}/email/routing/addresses?per_page=50`,
  );
  const ours = addresses.find((a) => a.email.toLowerCase() === REPLY_ROUTING_DESTINATION);
  if (ours) return Boolean(ours.verified);
  // Cloudflare emails a verification link to the destination; until clicked it
  // is pending (nothing is forwarded to an unverified destination).
  await client.call("POST", `/accounts/${CLOUDFLARE_ACCOUNT_ID}/email/routing/addresses`, { email: REPLY_ROUTING_DESTINATION });
  return false;
}

/**
 * One pass over every scheduled domain: prepare the catch-all, move the MX when
 * due, read whether the route is live. Idempotent, one step further each pass.
 * Dry run (default) reads and reports, writes nothing anywhere.
 */
export async function syncEmailRouting(
  options: {
    dryRun?: boolean;
    domains?: string[];
    asOf?: Date;
    cloudflare?: CloudflareClient;
    resolveDnsHost?: (domain: string) => Promise<DnsHost>;
  } = {},
): Promise<RoutingSummary> {
  const dryRun = options.dryRun !== false;
  const asOf = options.asOf ?? new Date();
  const rows = await loadRows(options.domains);
  const summary: RoutingSummary = {
    dryRun,
    domains: rows.length,
    live: [],
    switched: [],
    wouldSwitch: [],
    waitingForSwitchDate: [],
    pending: [],
    failed: [],
  };
  if (rows.length === 0) return summary;
  const client = options.cloudflare ?? (await loadCloudflareClient(CALLER));
  const dnsHostOf = options.resolveDnsHost ?? resolveDnsHost;
  const verified = dryRun ? true : await destinationVerified(client);

  for (const row of rows) {
    const domain = row.domain;
    try {
      const host = await dnsHostOf(domain);
      if (host.host !== "cloudflare") {
        const reason = `dns_host_not_cloudflare:${host.nameservers.join(",") || "no_ns"}`;
        summary.pending.push({ domain, reason });
        if (!dryRun) await saveRow(domain, { lastError: reason, live: false });
        continue;
      }
      const zone = await activeZone(client, domain);
      const rule = await client.call<{ enabled?: boolean; actions?: { type: string; value?: string[] }[] } | null>(
        "GET",
        `/zones/${zone.id}/email/routing/rules/catch_all`,
      );
      let ruleReady = catchAllIsOurs(rule);
      if (!ruleReady && !dryRun) {
        await client.call("PUT", `/zones/${zone.id}/email/routing/rules/catch_all`, {
          actions: [{ type: "forward", value: [REPLY_ROUTING_DESTINATION] }],
          matchers: [{ type: "all" }],
          enabled: true,
        });
        ruleReady = true;
      }

      let switchedNow = false;
      if (switchDue(row, asOf)) {
        if (!verified) {
          summary.pending.push({ domain, reason: "destination_unverified" });
          await saveRow(domain, { zoneId: zone.id, ruleReady, lastError: "destination_unverified" });
          continue;
        }
        if (dryRun) {
          summary.wouldSwitch.push(domain);
        } else {
          const records = await client.call<CloudflareRecord[]>("GET", `/zones/${zone.id}/dns_records?per_page=100&name=${encodeURIComponent(domain)}`);
          for (const record of foreignRootRecords(domain, records)) {
            await client.call("DELETE", `/zones/${zone.id}/dns_records/${record.id}`);
          }
          await client.call("POST", `/zones/${zone.id}/email/routing/dns`, {});
          switchedNow = true;
          summary.switched.push(domain);
        }
      } else if (row.mxSwitchedAt === null && row.mxSwitchAfter) {
        summary.waitingForSwitchDate.push({ domain, mxSwitchAfter: row.mxSwitchAfter.toISOString() });
      }

      const settings = await client.call<{ enabled?: boolean; status?: string }>("GET", `/zones/${zone.id}/email/routing`);
      const live = settings.enabled === true && settings.status === "ready" && ruleReady && verified;
      if (live) summary.live.push(domain);
      else if (row.mxSwitchedAt || switchedNow) {
        summary.pending.push({ domain, reason: `routing_${settings.enabled ? settings.status ?? "unknown" : "disabled"}` });
      }
      if (!dryRun) {
        await saveRow(domain, {
          zoneId: zone.id,
          ruleReady,
          switched: switchedNow || settings.enabled === true,
          live,
          lastError: live || !(row.mxSwitchedAt || switchedNow) ? null : `routing_${settings.status ?? "unknown"}`,
        });
      }
    } catch (error) {
      const message = errorMessage(error).slice(0, 500);
      summary.failed.push({ domain, error: message });
      // A failed read changes nothing it did not see: live stays as it was.
      if (!dryRun) await saveRow(domain, { lastError: message });
      console.error(`[instantly-service] email-routing ${domain} failed: ${message}`);
    }
  }
  return summary;
}

export class EmailRoutingScheduleError extends Error {}

/**
 * Register domains for routed inbound and when their root MX may move. A brand
 * domain (policy `brand`) is refused: its people's own mail lives there.
 * Re-scheduling a domain already switched changes nothing (the move is done).
 */
export async function scheduleEmailRouting(domains: readonly string[], mxSwitchAfter: Date | null): Promise<RoutingRow[]> {
  const wanted = [...new Set(domains.map((d) => d.trim().toLowerCase()).filter(Boolean))];
  if (wanted.length === 0) throw new EmailRoutingScheduleError("no domain given");
  const brand = rowsOf<{ domain: string }>(
    await db.execute(sql`
      SELECT domain FROM instantly_domain_policy WHERE reason = 'brand' AND domain = ANY(${sql.param(wanted)}::text[])
    `),
  );
  if (brand.length > 0) {
    throw new EmailRoutingScheduleError(`refused: brand domain(s) ${brand.map((b) => b.domain).join(", ")} carry our people's own mail`);
  }
  for (const domain of wanted) {
    await db.execute(sql`
      INSERT INTO email_routing_domains (domain, destination, mx_switch_after)
      VALUES (${domain}, ${REPLY_ROUTING_DESTINATION}, ${mxSwitchAfter})
      ON CONFLICT (domain) DO UPDATE SET
        mx_switch_after = CASE WHEN email_routing_domains.mx_switched_at IS NULL
          THEN EXCLUDED.mx_switch_after ELSE email_routing_domains.mx_switch_after END
    `);
  }
  return loadRows(wanted);
}

export async function listEmailRoutingDomains(): Promise<Record<string, unknown>[]> {
  return rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT domain, zone_id AS "zoneId", destination, rule_ready_at AS "ruleReadyAt",
             mx_switch_after AS "mxSwitchAfter", mx_switched_at AS "mxSwitchedAt",
             live_since AS "liveSince", last_error AS "lastError", checked_at AS "checkedAt"
      FROM email_routing_domains ORDER BY domain
    `),
  );
}

/** Domains whose inbound is routed and live (read through the forward, not a login). */
export async function loadLiveRoutedDomains(): Promise<Set<string>> {
  const rows = rowsOf<{ domain: string }>(
    await db.execute(sql`SELECT domain FROM email_routing_domains WHERE live_since IS NOT NULL`),
  );
  return new Set(rows.map((r) => r.domain.toLowerCase()));
}

/**
 * Domains whose mailboxes receive nothing new by `asOf`: MX already moved, or
 * due to move by then. A mailbox there is no seed receiver.
 */
export async function loadDomainsInboundRoutedAway(asOf: Date): Promise<Set<string>> {
  const rows = rowsOf<{ domain: string }>(
    await db.execute(sql`
      SELECT domain FROM email_routing_domains
      WHERE mx_switched_at IS NOT NULL OR (mx_switch_after IS NOT NULL AND mx_switch_after <= ${asOf.toISOString()}::timestamp)
    `),
  );
  return new Set(rows.map((r) => r.domain.toLowerCase()));
}

/** Every address of ours on the live routed domains, present or not (a retired mailbox's thread still gets answers). */
export async function loadRoutedAddresses(domains: ReadonlySet<string>): Promise<string[]> {
  if (domains.size === 0) return [];
  const rows = rowsOf<{ email: string }>(
    await db.execute(sql`
      SELECT DISTINCT lower(email) AS email FROM instantly_accounts
      WHERE lower(split_part(email, '@', 2)) = ANY(${sql.param([...domains])}::text[])
    `),
  );
  return rows.map((r) => r.email);
}
