/**
 * Put our Gandi sending domains on Amazon SES (SES-COLD-1119, owner 2026-11-19:
 * cheaper than Postmark (~$3 vs ~$38 a month for 27k emails) and no domain cap;
 * measured the same day, same cold email from growthservice.org to our 12 seeds:
 * SES Gmail 9 Primary / 20 Promotions / 1 spam, Postmark 9 / 21 / 0, Google
 * Workspace 9 / 18 / 0).
 *
 * Candidates (`loadSesCandidates`): every domain of ours, ANY vendor, with a
 * present address in production or recovery, brand/product and retiring domains
 * of `instantly_domain_policy` excluded. Which API writes its records is read
 * from its NS each pass (`dns/dns-host.ts`, PF-CLOUDFLARE-1120): Gandi LiveDNS
 * or Cloudflare; any other DNS host = pending `dns_host_unsupported:<ns>`,
 * nothing created at SES (the 3 Primeforge domains whose NS still sit at
 * Primeforge go live on their own once their NS reach Cloudflare).
 *
 * Each pass (worker every 15 min, idempotent, one step further each time):
 *   0. the cold EVENT pipeline exists: our configuration set
 *      (`instantly-cold-email`), our SNS topic, its HTTPS subscription to
 *      `/webhooks/ses` (confirmed by the webhook itself), and the set's
 *      event destination (bounce, complaint, delivery delay, reject) on it.
 *      Never the newsletter's set or topic;
 *   1. the domain is an SES identity (Easy DKIM), defaulting to our set;
 *   2. its custom MAIL FROM is `ses.<domain>` (SPF aligned for DMARC);
 *   3. the 3 DKIM CNAMEs and the MAIL FROM subdomain's MX + SPF TXT are written
 *      at the domain's DNS host: Gandi LiveDNS (the org's token,
 *      `gandi-<provider_account>`) or Cloudflare (platform key `cloudflare-dns`,
 *      active zone only). NOTHING else: never the root MX/SPF, so the
 *      mailboxes keep receiving replies;
 *   4. live once SES says verified for sending, DKIM SUCCESS, MAIL FROM SUCCESS
 *      AND the event subscription is confirmed (no bounce may go unseen).
 *      A domain that loses any of these is taken off SES (live_since cleared):
 *      its mail goes back to its mailbox's SMTP.
 * A failure is stored on the domain (`last_error`) and retried next pass.
 */
import { sql } from "drizzle-orm";

import { db } from "../../db";
import { errorMessage } from "../db-error.js";
import { resolvePlatformKey, type CallerInfo } from "../key-client";
import {
  loadSesAccount,
  sesApi,
  snsApi,
  xmlValue,
  SES_COLD_CONFIGURATION_SET,
  SES_COLD_EVENT_DESTINATION,
  SES_COLD_TOPIC_NAME,
  SES_COLD_WEBHOOK_URL,
  type SesAccount,
} from "../aws/ses-account";
import { clearLivePipesCache } from "./outbound";
import { resolveDnsHost, type DnsHost } from "../dns/dns-host";
import {
  activeZone,
  loadCloudflareClient,
  toCloudflareRecord,
  upsertRecord,
  type CloudflareClient,
} from "../dns/cloudflare";

const CALLER: CallerInfo = { method: "POST", path: "/internal/ses-sending/onboard" };

/** The MAIL FROM subdomain label: `ses.<domain>`. The ONLY name that gets an MX. */
export const SES_MAIL_FROM_LABEL = "ses";
export const SES_COLD_EVENT_TYPES = ["BOUNCE", "COMPLAINT", "DELIVERY_DELAY", "REJECT"] as const;

function rowsOf<T>(result: unknown): T[] {
  return (((result as { rows?: unknown[] }).rows ?? []) as T[]);
}

export interface DnsRecord {
  name: string;
  type: "CNAME" | "MX" | "TXT";
  value: string;
}

/**
 * Every record SES needs for one domain (pure): 3 Easy DKIM CNAMEs and the MAIL
 * FROM subdomain's MX + SPF. Names are relative to the zone; none is the apex.
 */
export function sesDnsRecords(region: string, tokens: readonly string[]): DnsRecord[] {
  return [
    ...tokens.map((t) => ({ name: `${t}._domainkey`, type: "CNAME" as const, value: `${t}.dkim.amazonses.com.` })),
    { name: SES_MAIL_FROM_LABEL, type: "MX", value: `10 feedback-smtp.${region}.amazonses.com.` },
    { name: SES_MAIL_FROM_LABEL, type: "TXT", value: '"v=spf1 include:amazonses.com ~all"' },
  ];
}

/** Only DKIM CNAMEs and the MAIL FROM subdomain are ever written, on any DNS host (pure guard). */
export function assertSesRecordAllowed(domain: string, record: DnsRecord): void {
  if (record.name === "@" || record.name === "" || (record.type === "MX" && record.name !== SES_MAIL_FROM_LABEL)) {
    throw new Error(`refused to write ${record.type} on "${record.name}" of ${domain}: only DKIM CNAMEs and the MAIL FROM subdomain`);
  }
}

async function writeGandiRecord(token: string, domain: string, record: DnsRecord): Promise<void> {
  assertSesRecordAllowed(domain, record);
  const response = await fetch(
    `https://api.gandi.net/v5/livedns/domains/${domain}/records/${encodeURIComponent(record.name)}/${record.type}`,
    {
      method: "PUT",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ rrset_values: [record.value], rrset_ttl: 300 }),
    },
  );
  if (!response.ok) {
    throw new Error(`Gandi LiveDNS PUT ${record.name} ${record.type} on ${domain}: ${response.status} ${await response.text()}`);
  }
}

async function writeCloudflareRecord(client: CloudflareClient, zoneId: string, domain: string, record: DnsRecord): Promise<void> {
  assertSesRecordAllowed(domain, record);
  await upsertRecord(client, zoneId, toCloudflareRecord(domain, record));
}

export interface SesCandidate {
  domain: string;
  /** The Gandi organisation of a Gandi-bought domain (its LiveDNS token), else null. */
  gandiOrg: string | null;
}

/**
 * Every sending domain of ours, any vendor, with a present address in production
 * or recovery; brand/product and retiring domains (`instantly_domain_policy`)
 * excluded. Where its DNS lives is NOT read here: it is asked each pass.
 */
export async function loadSesCandidates(only?: readonly string[]): Promise<SesCandidate[]> {
  const rows = rowsOf<{ domain: string; org: string | null }>(
    await db.execute(sql`
      SELECT i.domain, max(CASE WHEN i.provider = 'gandi' THEN i.provider_account END) AS org
      FROM infra_domains i
      JOIN instantly_accounts a
        ON lower(split_part(a.email, '@', 2)) = i.domain
       AND a.absent_since IS NULL
       AND a.lifecycle_status IN ('in_production', 'in_recovery')
      WHERE i.absent_since IS NULL
        AND NOT EXISTS (SELECT 1 FROM instantly_domain_policy p WHERE p.domain = i.domain)
      GROUP BY i.domain
      ORDER BY i.domain
    `),
  );
  const wanted = only && only.length > 0 ? new Set(only.map((d) => d.toLowerCase())) : null;
  return rows.filter((r) => !wanted || wanted.has(r.domain)).map((r) => ({ domain: r.domain, gandiOrg: r.org }));
}

export interface EventPipeline {
  configurationSet: string;
  topicArn: string;
  subscriptionConfirmed: boolean;
}

function failIf(status: number, text: string, what: string, ok: number[] = [200]): void {
  if (!ok.includes(status)) throw new Error(`${what}: ${status} ${text.slice(0, 300)}`);
}

/** Step 0: our configuration set, topic, subscription and event destination. Idempotent. */
export async function ensureColdEventPipeline(account: SesAccount): Promise<EventPipeline> {
  const set = SES_COLD_CONFIGURATION_SET;
  const existing = await sesApi(account, "GET", `/v2/email/configuration-sets/${set}`);
  if (existing.status === 404) {
    const created = await sesApi(account, "POST", "/v2/email/configuration-sets", {
      ConfigurationSetName: set,
      ReputationOptions: { ReputationMetricsEnabled: true },
      SendingOptions: { SendingEnabled: true },
    });
    failIf(created.status, created.text, `SES create configuration set ${set}`);
  } else {
    failIf(existing.status, existing.text, `SES get configuration set ${set}`);
  }

  const topic = await snsApi(account, { Action: "CreateTopic", Name: SES_COLD_TOPIC_NAME });
  failIf(topic.status, topic.text, `SNS CreateTopic ${SES_COLD_TOPIC_NAME}`);
  const topicArn = xmlValue(topic.text, "TopicArn");
  if (topicArn !== account.topicArn) throw new Error(`SNS topic ${topicArn} is not the pinned ${account.topicArn}`);

  const subs = await snsApi(account, { Action: "ListSubscriptionsByTopic", TopicArn: topicArn });
  failIf(subs.status, subs.text, "SNS ListSubscriptionsByTopic");
  const members = [...subs.text.matchAll(/<member>([\s\S]*?)<\/member>/g)].map((m) => ({
    endpoint: xmlValue(m[1], "Endpoint"),
    arn: xmlValue(m[1], "SubscriptionArn"),
  }));
  const ours = members.filter((m) => m.endpoint === SES_COLD_WEBHOOK_URL);
  const subscriptionConfirmed = ours.some((m) => (m.arn ?? "").startsWith("arn:"));
  if (!subscriptionConfirmed) {
    // New, or still pending: (re)asks SNS to post the confirmation, which the
    // webhook confirms after verifying its signature and topic.
    const sub = await snsApi(account, {
      Action: "Subscribe",
      TopicArn: topicArn,
      Protocol: "https",
      Endpoint: SES_COLD_WEBHOOK_URL,
      ReturnSubscriptionArn: "true",
    });
    failIf(sub.status, sub.text, "SNS Subscribe");
  }

  const destination = {
    Enabled: true,
    MatchingEventTypes: [...SES_COLD_EVENT_TYPES],
    SnsDestination: { TopicArn: topicArn },
  };
  const dests = await sesApi(account, "GET", `/v2/email/configuration-sets/${set}/event-destinations`);
  failIf(dests.status, dests.text, "SES get event destinations");
  const current = ((dests.json.EventDestinations ?? []) as Record<string, unknown>[]).find(
    (d) => d.Name === SES_COLD_EVENT_DESTINATION,
  );
  if (!current) {
    const created = await sesApi(account, "POST", `/v2/email/configuration-sets/${set}/event-destinations`, {
      EventDestinationName: SES_COLD_EVENT_DESTINATION,
      EventDestination: destination,
    });
    failIf(created.status, created.text, "SES create event destination");
  } else {
    const types = [...((current.MatchingEventTypes ?? []) as string[])].sort().join(",");
    const topicOf = (current.SnsDestination as { TopicArn?: string } | undefined)?.TopicArn;
    if (current.Enabled !== true || types !== [...SES_COLD_EVENT_TYPES].sort().join(",") || topicOf !== topicArn) {
      const updated = await sesApi(
        account,
        "PUT",
        `/v2/email/configuration-sets/${set}/event-destinations/${SES_COLD_EVENT_DESTINATION}`,
        { EventDestination: destination },
      );
      failIf(updated.status, updated.text, "SES update event destination");
    }
  }
  return { configurationSet: set, topicArn, subscriptionConfirmed };
}

export interface SesIdentityState {
  exists: boolean;
  verifiedForSending: boolean;
  dkimStatus: string | null;
  dkimTokens: string[];
  mailFromDomain: string | null;
  mailFromStatus: string | null;
  configurationSet: string | null;
}

export function identityStateOf(status: number, body: Record<string, unknown>): SesIdentityState {
  if (status === 404) {
    return { exists: false, verifiedForSending: false, dkimStatus: null, dkimTokens: [], mailFromDomain: null, mailFromStatus: null, configurationSet: null };
  }
  const dkim = (body.DkimAttributes ?? {}) as { Status?: string; Tokens?: string[] };
  const mailFrom = (body.MailFromAttributes ?? {}) as { MailFromDomain?: string; MailFromDomainStatus?: string };
  return {
    exists: true,
    verifiedForSending: body.VerifiedForSendingStatus === true,
    dkimStatus: dkim.Status ?? null,
    dkimTokens: Array.isArray(dkim.Tokens) ? dkim.Tokens : [],
    mailFromDomain: mailFrom.MailFromDomain ?? null,
    mailFromStatus: mailFrom.MailFromDomainStatus ?? null,
    configurationSet: typeof body.ConfigurationSetName === "string" ? body.ConfigurationSetName : null,
  };
}

/** Whether a domain may send through SES, and if not why (pure). */
export function sesLiveVerdict(
  domain: string,
  state: SesIdentityState,
  subscriptionConfirmed: boolean,
): { live: boolean; reason: string | null } {
  if (!state.exists) return { live: false, reason: "identity_missing" };
  if (state.mailFromDomain !== `${SES_MAIL_FROM_LABEL}.${domain}`) return { live: false, reason: "mail_from_not_set" };
  if (state.dkimStatus !== "SUCCESS") return { live: false, reason: `dkim_${(state.dkimStatus ?? "unknown").toLowerCase()}` };
  if (state.mailFromStatus !== "SUCCESS") return { live: false, reason: `mail_from_${(state.mailFromStatus ?? "unknown").toLowerCase()}` };
  if (!state.verifiedForSending) return { live: false, reason: "not_verified_for_sending" };
  if (state.configurationSet !== SES_COLD_CONFIGURATION_SET) return { live: false, reason: "configuration_set_not_ours" };
  if (!subscriptionConfirmed) return { live: false, reason: "events_not_confirmed" };
  return { live: true, reason: null };
}

async function readIdentity(account: SesAccount, domain: string): Promise<SesIdentityState> {
  const got = await sesApi(account, "GET", `/v2/email/identities/${domain}`);
  if (got.status !== 404) failIf(got.status, got.text, `SES get identity ${domain}`);
  return identityStateOf(got.status, got.json);
}

async function saveState(
  domain: string,
  state: SesIdentityState,
  live: boolean,
  dnsWritten: boolean,
  lastError: string | null,
  dnsHost: string | null,
): Promise<void> {
  await db.execute(sql`
    INSERT INTO ses_sending_domains
      (domain, dkim_tokens, dkim_status, mail_from_domain, mail_from_status, verified_for_sending,
       dns_written_at, live_since, last_error, checked_at, dns_host)
    VALUES (${domain}, ${JSON.stringify(state.dkimTokens)}::jsonb, ${state.dkimStatus}, ${state.mailFromDomain},
            ${state.mailFromStatus}, ${state.verifiedForSending}, CASE WHEN ${dnsWritten} THEN now() END,
            CASE WHEN ${live} THEN now() END, ${lastError}, now(), ${dnsHost})
    ON CONFLICT (domain) DO UPDATE SET
      dns_host = COALESCE(EXCLUDED.dns_host, ses_sending_domains.dns_host),
      dkim_tokens = EXCLUDED.dkim_tokens,
      dkim_status = EXCLUDED.dkim_status,
      mail_from_domain = EXCLUDED.mail_from_domain,
      mail_from_status = EXCLUDED.mail_from_status,
      verified_for_sending = EXCLUDED.verified_for_sending,
      dns_written_at = COALESCE(EXCLUDED.dns_written_at, ses_sending_domains.dns_written_at),
      live_since = CASE WHEN ${live} THEN COALESCE(ses_sending_domains.live_since, now()) ELSE NULL END,
      last_error = EXCLUDED.last_error,
      checked_at = now()
  `);
}

async function saveError(domain: string, message: string, dnsHost: string | null = null): Promise<void> {
  // A failed pass changes nothing it did not see: live stays as it was.
  await db.execute(sql`
    INSERT INTO ses_sending_domains (domain, last_error, checked_at, dns_host) VALUES (${domain}, ${message}, now(), ${dnsHost})
    ON CONFLICT (domain) DO UPDATE SET last_error = EXCLUDED.last_error, checked_at = now(),
      dns_host = COALESCE(EXCLUDED.dns_host, ses_sending_domains.dns_host)
  `);
}

/** Pure: the reason a domain cannot be onboarded on this DNS host, or null when it can. */
export function dnsHostBlocker(dnsHost: DnsHost, gandiOrg: string | null): string | null {
  if (dnsHost.host === "unsupported") return `dns_host_unsupported:${dnsHost.nameservers.join(",") || "no_ns"}`;
  if (dnsHost.host === "gandi" && !gandiOrg) return "dns_host_gandi_without_gandi_org";
  return null;
}

export interface SesOnboardingSummary {
  dryRun: boolean;
  candidates: number;
  pipeline: EventPipeline | null;
  live: string[];
  becameLive: string[];
  wentOffline: string[];
  pending: { domain: string; reason: string }[];
  wouldCreate: string[];
  failed: { domain: string; error: string }[];
}

export async function onboardSesDomains(
  options: {
    dryRun?: boolean;
    domains?: string[];
    account?: SesAccount;
    /** Test seam: the NS read. */
    resolveDnsHost?: (domain: string) => Promise<DnsHost>;
    /** Test seam: the Cloudflare client. */
    cloudflare?: CloudflareClient;
  } = {},
): Promise<SesOnboardingSummary> {
  const dryRun = options.dryRun !== false;
  const candidates = await loadSesCandidates(options.domains);
  const account = options.account ?? (await loadSesAccount(CALLER));
  const dnsHostOf = options.resolveDnsHost ?? resolveDnsHost;
  const summary: SesOnboardingSummary = {
    dryRun,
    candidates: candidates.length,
    pipeline: null,
    live: [],
    becameLive: [],
    wentOffline: [],
    pending: [],
    wouldCreate: [],
    failed: [],
  };
  const wasLive = new Set(
    rowsOf<{ domain: string }>(await db.execute(sql`SELECT domain FROM ses_sending_domains WHERE live_since IS NOT NULL`)).map((r) => r.domain),
  );

  // Step 0. In a dry run only read whether the subscription is confirmed.
  if (!dryRun) {
    summary.pipeline = await ensureColdEventPipeline(account);
  }
  const subscriptionConfirmed = summary.pipeline?.subscriptionConfirmed ?? false;

  const gandiTokens = new Map<string, string>();
  let cloudflare: CloudflareClient | null = options.cloudflare ?? null;
  for (const candidate of candidates) {
    const domain = candidate.domain;
    let host: DnsHost | null = null;
    try {
      // Where its records can be written, read first: a domain we cannot write
      // to is never created at SES (nothing half-done to clean up).
      host = await dnsHostOf(domain);
      const blocker = dnsHostBlocker(host, candidate.gandiOrg);
      let state = await readIdentity(account, domain);
      if (blocker && !(state.dkimStatus === "SUCCESS" && state.mailFromStatus === "SUCCESS")) {
        summary.pending.push({ domain, reason: blocker });
        if (!dryRun) await saveError(domain, blocker, host.host);
        continue;
      }
      if (dryRun) {
        if (!state.exists) summary.wouldCreate.push(domain);
        else {
          const verdict = sesLiveVerdict(domain, state, true);
          if (verdict.live) summary.live.push(domain);
          else summary.pending.push({ domain, reason: verdict.reason ?? "unknown" });
        }
        continue;
      }
      if (!state.exists) {
        const created = await sesApi(account, "POST", "/v2/email/identities", {
          EmailIdentity: domain,
          ConfigurationSetName: SES_COLD_CONFIGURATION_SET,
        });
        failIf(created.status, created.text, `SES create identity ${domain}`);
        state = await readIdentity(account, domain);
      }
      if (state.configurationSet !== SES_COLD_CONFIGURATION_SET) {
        const set = await sesApi(account, "PUT", `/v2/email/identities/${domain}/configuration-set`, {
          ConfigurationSetName: SES_COLD_CONFIGURATION_SET,
        });
        failIf(set.status, set.text, `SES set configuration set on ${domain}`);
      }
      if (state.mailFromDomain !== `${SES_MAIL_FROM_LABEL}.${domain}`) {
        const mf = await sesApi(account, "PUT", `/v2/email/identities/${domain}/mail-from`, {
          MailFromDomain: `${SES_MAIL_FROM_LABEL}.${domain}`,
          BehaviorOnMxFailure: "USE_DEFAULT_VALUE",
        });
        failIf(mf.status, mf.text, `SES set MAIL FROM on ${domain}`);
      }
      let dnsWritten = false;
      if (state.dkimStatus !== "SUCCESS" || state.mailFromStatus !== "SUCCESS") {
        if (state.dkimTokens.length === 0) throw new Error(`SES gave no DKIM tokens for ${domain}`);
        const records = sesDnsRecords(account.region, state.dkimTokens);
        if (host.host === "cloudflare") {
          cloudflare = cloudflare ?? (await loadCloudflareClient(CALLER));
          const zone = await activeZone(cloudflare, domain);
          for (const record of records) await writeCloudflareRecord(cloudflare, zone.id, domain, record);
        } else {
          if (!candidate.gandiOrg) throw new Error(`no Gandi organisation recorded for ${domain}`);
          let gandi = gandiTokens.get(candidate.gandiOrg);
          if (!gandi) {
            gandi = await resolvePlatformKey(`gandi-${candidate.gandiOrg}`, CALLER);
            gandiTokens.set(candidate.gandiOrg, gandi);
          }
          for (const record of records) await writeGandiRecord(gandi, domain, record);
        }
        dnsWritten = true;
      }
      state = await readIdentity(account, domain);
      const verdict = sesLiveVerdict(domain, state, subscriptionConfirmed);
      await saveState(domain, state, verdict.live, dnsWritten, verdict.reason, host.host);
      if (verdict.live) {
        summary.live.push(domain);
        if (!wasLive.has(domain)) summary.becameLive.push(domain);
      } else {
        summary.pending.push({ domain, reason: verdict.reason ?? "unknown" });
        if (wasLive.has(domain)) summary.wentOffline.push(domain);
      }
    } catch (error) {
      const message = errorMessage(error).slice(0, 500);
      summary.failed.push({ domain, error: message });
      if (!dryRun) await saveError(domain, message, host?.host ?? null);
      console.error(`[instantly-service] ses onboarding ${domain} failed: ${message}`);
    }
  }
  if (summary.becameLive.length > 0 || summary.wentOffline.length > 0) clearLivePipesCache();
  return summary;
}

export async function listSesSendingDomains(): Promise<Record<string, unknown>[]> {
  return rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT domain, dkim_status AS "dkimStatus", mail_from_domain AS "mailFromDomain",
             mail_from_status AS "mailFromStatus", verified_for_sending AS "verifiedForSending",
             dns_written_at AS "dnsWrittenAt", live_since AS "liveSince", last_error AS "lastError",
             dns_host AS "dnsHost",
             checked_at AS "checkedAt"
      FROM ses_sending_domains ORDER BY live_since NULLS LAST, domain
    `),
  );
}
