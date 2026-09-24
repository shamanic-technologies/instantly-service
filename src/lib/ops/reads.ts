/**
 * The ops reads over the unified model — one read per object, same ids
 * everywhere: domain, mailbox (login), address (email), the sending infra
 * rollup, threads and messages. Every figure is either a fact read from a
 * silver row, a re-application of a rule the service already runs (ops/pure.ts),
 * or a labelled estimate. Nothing here decides anything.
 */

import { sql } from "drizzle-orm";
import { db } from "../../db";
import { loadEffectiveRates, loadInventoryDomains } from "../infra-gold";
import {
  costPerEmailCents,
  indexRates,
  monthlyCostForDomain,
  splitDomainCost,
  vendorReportsMailboxes,
  type PriceRate,
} from "../infra-pricing";
import { describeFx, loadLatestEurUsd, toUsdCents } from "../fx-rates";
import { summarizeDns, type DnsRecordRow, type DnsRecordType, type DnsSummary } from "../domain-dns-sync";
import { fetchLatestDeliveryByAccount, fetchLifecycleByEmail } from "../account-lifecycle-sync";
import { capForAccount } from "../account-lifecycle";
import { fetchRecentDailyVolume, sustainedForMailbox } from "../recent-send-volume";
import { warmupBudgetFor } from "../warmup/plan";
import { computeCapacitySummary } from "../sending-forecast";
import { loadMailboxLogins } from "../self-send/mailbox-credentials";
import type { CallerInfo } from "../key-client";
import { loadAccountHealth } from "./account-health-read";
import {
  estimatePaidToDate,
  evidenceExpiresAt,
  poolDelivery,
  projectNextSeedTest,
  rampProjection,
  ratePerMille,
} from "./pure";

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function iso(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const d = v instanceof Date ? v : new Date(String(v));
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function num(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

// ─── Shared loaders ──────────────────────────────────────────────────────────

/** Latest photograph per (domain, record, selector). */
async function loadLatestDns(): Promise<Map<string, DnsRecordRow[]>> {
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT DISTINCT ON (domain, record_type, COALESCE(selector, ''))
             domain, record_type, selector, name, values, error, fetched_at
      FROM domain_dns_raw
      ORDER BY domain, record_type, COALESCE(selector, ''), fetched_at DESC
    `),
  );
  const byDomain = new Map<string, DnsRecordRow[]>();
  for (const r of rows) {
    const domain = String(r.domain);
    const list = byDomain.get(domain) ?? [];
    list.push({
      domain,
      recordType: String(r.record_type) as DnsRecordType,
      selector: r.selector === null || r.selector === undefined ? null : String(r.selector),
      name: String(r.name),
      values: Array.isArray(r.values) ? (r.values as string[]) : [],
      error: r.error === null || r.error === undefined ? null : String(r.error),
    });
    byDomain.set(domain, list);
  }
  return byDomain;
}

interface VolumeRow {
  key: string;
  kind: string;
  direction: string;
  count: number;
}

/** Message counts per (key, kind, direction) over a trailing window, keyed on a chosen column. */
async function loadVolume(
  keyExpr: "mailbox_login" | "account_email" | "domain",
  days: number,
): Promise<Map<string, VolumeRow[]>> {
  const keySql =
    keyExpr === "domain"
      ? sql`split_part(account_email, '@', 2)`
      : keyExpr === "mailbox_login"
        ? sql`COALESCE(mailbox_login, account_email)`
        : sql`account_email`;
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT ${keySql} AS key, kind, direction, count(*)::int AS count
      FROM messages
      WHERE occurred_at >= now() - make_interval(days => ${days})
        AND (direction = 'in' OR outcome = 'sent')
      GROUP BY 1, 2, 3
    `),
  );
  const out = new Map<string, VolumeRow[]>();
  for (const r of rows) {
    const key = String(r.key);
    const list = out.get(key) ?? [];
    list.push({ key, kind: String(r.kind), direction: String(r.direction), count: num(r.count) });
    out.set(key, list);
  }
  return out;
}

export interface VolumeSummary {
  outreach: number;
  warmup: number;
  seed: number;
  repliesIn: number;
  bouncesIn: number;
  bounceRatePerMille: number | null;
}

function summarizeVolume(rows: VolumeRow[] | undefined): VolumeSummary {
  const by = (kind: string, direction: string) =>
    (rows ?? []).filter((r) => r.kind === kind && r.direction === direction).reduce((a, r) => a + r.count, 0);
  const outreach = by("outreach", "out") + by("manual_reply", "out");
  const bounces = by("bounce", "in");
  return {
    outreach,
    warmup: by("warmup", "out") + by("warmup_reply", "out"),
    seed: by("seed", "out"),
    repliesIn: by("reply", "in") + by("auto_reply", "in"),
    bouncesIn: bounces,
    bounceRatePerMille: ratePerMille(bounces, outreach),
  };
}

interface MailboxRowDb {
  login: string;
  domain: string;
  provider: string | null;
  poolType: string | null;
  subscription: string | null;
  credentialSource: string;
  vendorCreatedAt: string | null;
  vendorPrewarmedAt: string | null;
  importedAt: string | null;
  absentSince: string | null;
  syncedAt: string | null;
}

async function loadMailboxRows(): Promise<MailboxRowDb[]> {
  return rowsOf<Record<string, unknown>>(await db.execute(sql`SELECT * FROM mailboxes ORDER BY login`)).map(
    (r) => ({
      login: String(r.login),
      domain: String(r.domain),
      provider: r.provider === null ? null : String(r.provider),
      poolType: r.pool_type === null ? null : String(r.pool_type),
      subscription: r.subscription === null ? null : String(r.subscription),
      credentialSource: String(r.credential_source),
      vendorCreatedAt: iso(r.vendor_created_at),
      vendorPrewarmedAt: iso(r.vendor_prewarmed_at),
      importedAt: iso(r.imported_at),
      absentSince: iso(r.absent_since),
      syncedAt: iso(r.synced_at),
    }),
  );
}

interface AddressRowDb {
  email: string;
  domain: string;
  mailboxLogin: string | null;
  lifecycleStatus: string | null;
  lifecycleReason: string | null;
  sendTransport: string;
  dailyLimit: number | null;
  instantlyStatus: number | null;
  warmupScore: number | null;
  timestampCreated: string | null;
  vendorPrewarmedAt: string | null;
  absentSince: string | null;
}

async function loadAddressRows(): Promise<AddressRowDb[]> {
  return rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT email, split_part(email, '@', 2) AS domain, mailbox_login, lifecycle_status, lifecycle_reason,
             send_transport, daily_limit, instantly_status, warmup_score, timestamp_created,
             vendor_prewarmed_at, absent_since
      FROM instantly_accounts
      ORDER BY email
    `),
  ).map((r) => ({
    email: String(r.email).toLowerCase(),
    domain: String(r.domain).toLowerCase(),
    mailboxLogin: r.mailbox_login === null ? null : String(r.mailbox_login),
    lifecycleStatus: r.lifecycle_status === null ? null : String(r.lifecycle_status),
    lifecycleReason: r.lifecycle_reason === null ? null : String(r.lifecycle_reason),
    sendTransport: String(r.send_transport),
    dailyLimit: r.daily_limit === null ? null : num(r.daily_limit),
    instantlyStatus: r.instantly_status === null ? null : num(r.instantly_status),
    warmupScore: r.warmup_score === null ? null : num(r.warmup_score),
    timestampCreated: iso(r.timestamp_created),
    vendorPrewarmedAt: iso(r.vendor_prewarmed_at),
    absentSince: iso(r.absent_since),
  }));
}

function countBy<T>(rows: T[], key: (r: T) => string | null): Record<string, number> {
  const out: Record<string, number> = {};
  for (const r of rows) {
    const k = key(r) ?? "unclassified";
    out[k] = (out[k] ?? 0) + 1;
  }
  return out;
}

// ─── Domains ─────────────────────────────────────────────────────────────────

export async function readDomains(asOf: Date = new Date()) {
  const [inventory, rates, dnsByDomain, addresses, mailboxes, delivery, volume30, fx] = await Promise.all([
    loadInventoryDomains(),
    loadEffectiveRates(asOf),
    loadLatestDns(),
    loadAddressRows(),
    loadMailboxRows(),
    fetchLatestDeliveryByAccount(),
    loadVolume("domain", 30),
    loadLatestEurUsd(),
  ]);
  const indexed = indexRates(rates);
  const addressesByDomain = new Map<string, AddressRowDb[]>();
  for (const a of addresses) {
    if (a.absentSince) continue;
    addressesByDomain.set(a.domain, [...(addressesByDomain.get(a.domain) ?? []), a]);
  }
  const mailboxesByDomain = new Map<string, MailboxRowDb[]>();
  for (const m of mailboxes) mailboxesByDomain.set(m.domain, [...(mailboxesByDomain.get(m.domain) ?? []), m]);

  return {
    asOf: asOf.toISOString(),
    // The rate every `cost.usd` figure was converted at. Null = none on record,
    // and then every `cost.usd` figure is null too — never a guessed rate.
    fx: describeFx(fx),
    domains: inventory.map((domain) => {
      const monthly = monthlyCostForDomain(domain, indexed);
      const perEmail = costPerEmailCents(monthly, domain.sentLast30d);
      const split = splitDomainCost(domain, indexed);
      const monthlyUsd = toUsdCents(monthly?.cents ?? null, monthly?.currency ?? null, fx);
      const paidToDate =
        monthly && domain.cancelledAt === null
          ? estimatePaidToDate(monthly.cents, monthly.currency, domainStart(domain), asOf)
          : null;
      const addrs = addressesByDomain.get(domain.domain) ?? [];
      const pooled = poolDelivery(
        addrs.map((a) => delivery.get(a.email)).filter((d): d is NonNullable<typeof d> => Boolean(d)),
      );
      const dns = dnsByDomain.get(domain.domain);
      return {
        domain: domain.domain,
        provider: domain.provider,
        role: domain.role,
        status: domain.status,
        expiresAt: domain.expiresAt?.toISOString() ?? null,
        autorenew: domain.autorenew,
        deletionScheduled: domain.deletionScheduled,
        cancelledAt: domain.cancelledAt?.toISOString() ?? null,
        absentSince: domain.absentSince?.toISOString() ?? null,
        purchasedAt: null as string | null,
        vendorMailboxes: domain.mailboxCount,
        // False when the vendor's inventory never reports mailboxes (Instantly
        // DFY), so its 0 means "not reported" and must not read as a mismatch.
        vendorReportsMailboxes: vendorReportsMailboxes(domain.provider),
        mailboxes: (mailboxesByDomain.get(domain.domain) ?? []).length,
        addresses: { total: addrs.length, byLifecycle: countBy(addrs, (a) => a.lifecycleStatus) },
        sentLast30d: domain.sentLast30d,
        volume30d: summarizeVolume(volume30.get(domain.domain)),
        delivery: pooled,
        dns: dns ? summarizeDns(dns) : (null as DnsSummary | null),
        cost: {
          monthlyCents: monthly?.cents ?? null,
          currency: monthly?.currency ?? null,
          source: monthly?.source ?? null,
          perEmailCents: perEmail ? Number(perEmail.cents.toFixed(4)) : null,
          recurringMonthlyCents: split.recurringMonthlyCents,
          renewalCents: split.renewalCents,
          renewalAt: split.renewalAt?.toISOString() ?? null,
          paidToDate,
          // The same amounts in USD at `fx`, so a consumer can state ONE total.
          usd: {
            monthlyCents: monthlyUsd,
            perEmailCents:
              monthlyUsd !== null && domain.sentLast30d > 0
                ? Number((monthlyUsd / domain.sentLast30d).toFixed(4))
                : null,
            recurringMonthlyCents: toUsdCents(split.recurringMonthlyCents, split.currency, fx),
            renewalCents: toUsdCents(split.renewalCents, split.currency, fx),
            paidToDateCents: paidToDate ? toUsdCents(paidToDate.cents, paidToDate.currency, fx) : null,
          },
        },
      };
    }),
  };
}

function domainStart(domain: { absentSince: Date | null; expiresAt: Date | null }): Date | null {
  // Vendors report expiry, not purchase; a yearly registration started a year
  // before it expires. That is the only date we hold, so it is the one used.
  return domain.expiresAt ? new Date(domain.expiresAt.getTime() - 365 * 24 * 60 * 60 * 1000) : null;
}

// ─── Mailboxes ───────────────────────────────────────────────────────────────

function mailboxMonthlyRate(rates: Map<string, PriceRate>, provider: string | null): PriceRate | null {
  return provider ? (rates.get(`${provider}|mailbox-month|`) ?? null) : null;
}

export async function readMailboxes(asOf: Date = new Date()) {
  const [mailboxes, addresses, rates, delivery, volume7, volume30, recentVolume, lifecycle] =
    await Promise.all([
      loadMailboxRows(),
      loadAddressRows(),
      loadEffectiveRates(asOf),
      fetchLatestDeliveryByAccount(),
      loadVolume("mailbox_login", 7),
      loadVolume("mailbox_login", 30),
      fetchRecentDailyVolume(),
      fetchLifecycleByEmail(),
    ]);
  const indexed = indexRates(rates);
  const addressesByLogin = new Map<string, AddressRowDb[]>();
  for (const a of addresses) {
    const login = a.mailboxLogin ?? a.email;
    addressesByLogin.set(login, [...(addressesByLogin.get(login) ?? []), a]);
  }

  return {
    asOf: asOf.toISOString(),
    mailboxes: mailboxes.map((m) => {
      const addrs = addressesByLogin.get(m.login) ?? [];
      const live = addrs.filter((a) => !a.absentSince);
      const sustained = sustainedForMailbox(recentVolume, addrs.map((a) => a.email));
      // The cap the selector would offer: the minimum across the mailbox's
      // aliases (an operator lowering one means it for the mailbox).
      const caps = live.map((a) =>
        capForAccount(
          {
            daily_limit: a.dailyLimit,
            sendTransport: a.sendTransport,
            vendorPrewarmedAt: a.vendorPrewarmedAt,
            timestamp_created: a.timestampCreated,
          },
          sustained,
          lifecycle.get(a.email)?.sendTransport,
          asOf,
        ),
      );
      const cap = caps.length ? Math.min(...caps) : 0;
      const rate = mailboxMonthlyRate(indexed, m.provider);
      const pooled = poolDelivery(
        addrs.map((a) => delivery.get(a.email)).filter((d): d is NonNullable<typeof d> => Boolean(d)),
      );
      return {
        ...m,
        addresses: addrs.map((a) => ({
          email: a.email,
          lifecycleStatus: a.lifecycleStatus,
          lifecycleReason: a.lifecycleReason,
          sendTransport: a.sendTransport,
          dailyLimit: a.dailyLimit,
          absentSince: a.absentSince,
        })),
        addressesByLifecycle: countBy(live, (a) => a.lifecycleStatus),
        sustainedDaily: sustained,
        effectiveDailyCap: cap,
        rampProjection: rampProjection(sustained, undefined, asOf),
        warmupBudgetToday: warmupBudgetFor(cap),
        delivery: pooled,
        evidenceExpiresAt: evidenceExpiresAt(pooled.testedAt),
        volume7d: summarizeVolume(volume7.get(m.login)),
        volume30d: summarizeVolume(volume30.get(m.login)),
        cost: rate
          ? {
              monthlyCents: rate.unitCents,
              currency: rate.currency,
              source: rate.source,
              paidToDate: estimatePaidToDate(rate.unitCents, rate.currency, m.vendorCreatedAt ?? m.importedAt, asOf),
            }
          : null,
      };
    }),
  };
}

// ─── Addresses ───────────────────────────────────────────────────────────────

export async function readAddresses(caller: CallerInfo) {
  const [health, addresses, delivery, volume7, history] = await Promise.all([
    loadAccountHealth(caller),
    loadAddressRows(),
    fetchLatestDeliveryByAccount(),
    loadVolume("account_email", 7),
    loadLifecycleHistory(),
  ]);
  const asOf = health.asOf;
  const byEmail = new Map(addresses.map((a) => [a.email, a]));
  return {
    asOf: asOf.toISOString(),
    accounts: health.accounts.map((row) => {
      const a = byEmail.get(row.email.toLowerCase());
      const d = delivery.get(row.email);
      const testedAt = d?.testedAt ? new Date(d.testedAt) : null;
      const sustained = row.effectiveDailyCap ?? 0;
      return {
        ...row,
        mailboxLogin: a?.mailboxLogin ?? null,
        sendTransport: a?.sendTransport ?? null,
        evidenceExpiresAt: evidenceExpiresAt(d?.testedAt ?? null),
        nextSeedTest: projectNextSeedTest(testedAt, asOf),
        rampProjection: rampProjection(Math.min(sustained, row.dailyLimit ?? sustained), row.dailyLimit ?? undefined, asOf),
        volume7d: summarizeVolume(volume7.get(row.email.toLowerCase())),
        lifecycleHistory: history.get(row.email.toLowerCase()) ?? [],
      };
    }),
  };
}

async function loadLifecycleHistory(limitPerAccount = 10) {
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT account_email, from_status, to_status, reason, health_score, delivery_pct, created_at
      FROM (
        SELECT e.*, row_number() OVER (PARTITION BY account_email ORDER BY created_at DESC) AS rn
        FROM instantly_account_lifecycle_events e
      ) t
      WHERE rn <= ${limitPerAccount}
      ORDER BY account_email, created_at DESC
    `),
  );
  const out = new Map<string, Array<Record<string, unknown>>>();
  for (const r of rows) {
    const email = String(r.account_email).toLowerCase();
    out.set(email, [
      ...(out.get(email) ?? []),
      {
        fromStatus: r.from_status ?? null,
        toStatus: r.to_status,
        reason: r.reason ?? null,
        healthScore: r.health_score === null ? null : num(r.health_score),
        deliveryPct: r.delivery_pct === null ? null : num(r.delivery_pct),
        at: iso(r.created_at),
      },
    ]);
  }
  return out;
}

// ─── Sending infra ───────────────────────────────────────────────────────────

export async function readInfra(caller: CallerInfo, asOf: Date = new Date()) {
  const [health, mailboxes, addresses, recentVolume, mailboxOf, volume7, queue, policies] = await Promise.all([
    loadAccountHealth(caller),
    loadMailboxRows(),
    loadAddressRows(),
    fetchRecentDailyVolume(),
    loadMailboxLogins(caller),
    loadVolume("account_email", 7),
    loadQueueByAccount(),
    loadExclusions(),
  ]);
  const poolOfLogin = new Map(mailboxes.map((m) => [m.login, m.poolType ?? "unknown"]));
  const poolOfEmail = new Map(addresses.map((a) => [a.email, poolOfLogin.get(a.mailboxLogin ?? a.email) ?? "unknown"]));
  const pools = [...new Set([...poolOfEmail.values()])].sort();

  const fleet = computeCapacitySummary(health.rawAccounts, health.lifecycleByEmail, recentVolume, mailboxOf, asOf);

  const perPool = pools.map((pool) => {
    const accounts = health.rawAccounts.filter((a) => poolOfEmail.get(String(a.email).toLowerCase()) === pool);
    const capacity = computeCapacitySummary(accounts, health.lifecycleByEmail, recentVolume, mailboxOf, asOf);
    const emails = accounts.map((a) => String(a.email).toLowerCase());
    const rows = emails.flatMap((e) => volume7.get(e) ?? []);
    const placement = poolDelivery(
      emails.map((e) => health.placementByEmail.get(e)).filter((p): p is NonNullable<typeof p> => Boolean(p)).map((p) => ({
        inboxCount: Math.round(p.inboxPct),
        seedTotal: 100,
        testedAt: p.testedAt,
      })),
    );
    return {
      pool,
      mailboxes: mailboxes.filter((m) => (m.poolType ?? "unknown") === pool).length,
      addresses: emails.length,
      byLifecycle: countBy(emails, (e) => health.lifecycleByEmail.get(e)?.status ?? null),
      dailyCapacity: capacity.dailyCapacity,
      inProduction: capacity.healthyAccountCount,
      queuedSteps: emails.reduce((s, e) => s + (queue.get(e) ?? 0), 0),
      volume7d: summarizeVolume(rows),
      /** Mean of the pool's latest per-address inbox percentages (each test weighted equally). */
      inboxPctMean: placement.inboxPct,
    };
  });

  return {
    asOf: asOf.toISOString(),
    fleet: {
      dailyCapacity: fleet.dailyCapacity,
      healthyAccountCount: fleet.healthyAccountCount,
      totalAccountCount: fleet.totalAccountCount,
      blockedDomainCount: fleet.blockedDomainCount,
      queuedSteps: [...queue.values()].reduce((a, b) => a + b, 0),
      byLifecycle: countBy([...health.lifecycleByEmail.values()], (l) => l.status),
    },
    pools: perPool,
    exclusions: policies,
  };
}

async function loadQueueByAccount(): Promise<Map<string, number>> {
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT lower(c.account_email) AS email, count(DISTINCT (sc.instantly_campaign_id, sc.step))::int AS steps
      FROM sequence_costs sc
      JOIN instantly_campaigns c ON c.instantly_campaign_id = sc.instantly_campaign_id
      WHERE sc.status = 'provisioned' AND c.status = 'active' AND c.account_email IS NOT NULL
      GROUP BY 1
    `),
  );
  return new Map(rows.map((r) => [String(r.email), num(r.steps)]));
}

async function loadExclusions() {
  const [domains, features] = await Promise.all([
    db.execute(sql`SELECT domain, reason, note FROM instantly_domain_policy ORDER BY domain`),
    db.execute(sql`SELECT account_email, feature_slug FROM instantly_account_feature_policy ORDER BY 1`),
  ]);
  return {
    domainPolicy: rowsOf<Record<string, unknown>>(domains).map((r) => ({
      domain: String(r.domain),
      reason: String(r.reason),
      note: r.note === null ? null : String(r.note),
    })),
    featureReservations: rowsOf<Record<string, unknown>>(features).map((r) => ({
      accountEmail: String(r.account_email),
      featureSlug: String(r.feature_slug),
    })),
  };
}

// ─── Threads & messages ──────────────────────────────────────────────────────

export interface ThreadFilters {
  limit: number;
  cursor?: string | null;
  kind?: string;
  account?: string;
  mailbox?: string;
  domain?: string;
  counterparty?: string;
  orgId?: string;
  campaignId?: string;
  direction?: "in" | "out";
  since?: string;
  until?: string;
  hasInbound?: boolean;
  placement?: string;
}

/** Cursor = `<lastAt ISO>|<threadId>`; opaque to callers. */
export function encodeCursor(lastAt: string, id: string): string {
  return Buffer.from(`${lastAt}|${id}`).toString("base64url");
}
export function decodeCursor(cursor: string): { lastAt: string; id: string } | null {
  try {
    const raw = Buffer.from(cursor, "base64url").toString("utf8");
    const idx = raw.indexOf("|");
    if (idx <= 0) return null;
    const lastAt = raw.slice(0, idx);
    if (Number.isNaN(new Date(lastAt).getTime())) return null;
    return { lastAt, id: raw.slice(idx + 1) };
  } catch {
    return null;
  }
}

function messageWhere(f: Omit<ThreadFilters, "limit" | "cursor" | "hasInbound">) {
  const clauses = [sql`true`];
  if (f.kind) clauses.push(sql`m.kind = ${f.kind}`);
  if (f.direction) clauses.push(sql`m.direction = ${f.direction}`);
  if (f.account) clauses.push(sql`m.account_email = ${f.account.toLowerCase()}`);
  if (f.mailbox) clauses.push(sql`COALESCE(m.mailbox_login, m.account_email) = ${f.mailbox.toLowerCase()}`);
  if (f.domain) clauses.push(sql`split_part(m.account_email, '@', 2) = ${f.domain.toLowerCase()}`);
  if (f.counterparty) clauses.push(sql`m.counterparty ILIKE ${"%" + f.counterparty.toLowerCase() + "%"}`);
  if (f.orgId) clauses.push(sql`m.org_id = ${f.orgId}`);
  if (f.campaignId) clauses.push(sql`m.campaign_id = ${f.campaignId}`);
  if (f.since) clauses.push(sql`m.occurred_at >= ${f.since}::timestamptz`);
  if (f.until) clauses.push(sql`m.occurred_at < ${f.until}::timestamptz`);
  if (f.placement) clauses.push(sql`m.placement = ${f.placement}`);
  return sql.join(clauses, sql` AND `);
}

export async function readThreads(f: ThreadFilters) {
  const cursor = f.cursor ? decodeCursor(f.cursor) : null;
  const where = messageWhere(f);
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      WITH t AS (
        SELECT m.thread_id,
               min(m.occurred_at) AS first_at,
               max(m.occurred_at) AS last_at,
               count(*)::int AS message_count,
               count(*) FILTER (WHERE m.direction = 'in')::int AS inbound_count,
               count(*) FILTER (WHERE m.direction = 'out')::int AS outbound_count,
               min(m.account_email) AS account_email,
               min(COALESCE(m.mailbox_login, m.account_email)) AS mailbox_login,
               min(m.counterparty) AS counterparty,
               min(m.instantly_campaign_id) AS instantly_campaign_id,
               min(m.org_id) AS org_id,
               min(m.campaign_id) AS campaign_id,
               min(m.transport) AS transport,
               (array_agg(m.kind ORDER BY m.occurred_at ASC))[1] AS kind,
               (array_agg(m.subject ORDER BY m.occurred_at ASC))[1] AS subject,
               (array_agg(m.placement ORDER BY m.occurred_at ASC))[1] AS placement
        FROM messages m
        WHERE ${where}
        GROUP BY m.thread_id
      )
      SELECT t.*, c.delivery_status, c.reply_classification, g.reply_kind, c.lead_email, c.brand_ids
      FROM t
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = t.instantly_campaign_id
      LEFT JOIN instantly_lead_status_current g ON g.instantly_campaign_id = t.instantly_campaign_id
      WHERE ${f.hasInbound === undefined ? sql`true` : f.hasInbound ? sql`t.inbound_count > 0` : sql`t.inbound_count = 0`}
        AND ${cursor ? sql`(t.last_at, t.thread_id) < (${cursor.lastAt}::timestamptz, ${cursor.id})` : sql`true`}
      ORDER BY t.last_at DESC, t.thread_id DESC
      LIMIT ${f.limit + 1}
    `),
  );
  const page = rows.slice(0, f.limit);
  const last = page[page.length - 1];
  return {
    threads: page.map((r) => ({
      threadId: String(r.thread_id),
      kind: String(r.kind),
      subject: r.subject === null ? null : String(r.subject),
      accountEmail: String(r.account_email),
      mailboxLogin: String(r.mailbox_login),
      counterparty: r.counterparty === null ? null : String(r.counterparty),
      transport: String(r.transport),
      instantlyCampaignId: r.instantly_campaign_id === null ? null : String(r.instantly_campaign_id),
      orgId: r.org_id === null ? null : String(r.org_id),
      campaignId: r.campaign_id === null ? null : String(r.campaign_id),
      leadEmail: r.lead_email === null || r.lead_email === undefined ? null : String(r.lead_email),
      brandIds: Array.isArray(r.brand_ids) ? (r.brand_ids as string[]) : null,
      deliveryStatus: r.delivery_status === null || r.delivery_status === undefined ? null : String(r.delivery_status),
      replyClassification: r.reply_classification === null || r.reply_classification === undefined ? null : String(r.reply_classification),
      replyKind: r.reply_kind === null || r.reply_kind === undefined ? null : String(r.reply_kind),
      placement: r.placement === null ? null : String(r.placement),
      messageCount: num(r.message_count),
      inboundCount: num(r.inbound_count),
      outboundCount: num(r.outbound_count),
      firstAt: iso(r.first_at),
      lastAt: iso(r.last_at),
    })),
    nextCursor: rows.length > f.limit && last ? encodeCursor(String(iso(last.last_at)), String(last.thread_id)) : null,
  };
}

export interface MessageFilters extends Omit<ThreadFilters, "hasInbound"> {
  threadId?: string;
}

export async function readMessages(f: MessageFilters) {
  const cursor = f.cursor ? decodeCursor(f.cursor) : null;
  const where = messageWhere(f);
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT m.*
      FROM messages m
      WHERE ${where}
        AND ${f.threadId ? sql`m.thread_id = ${f.threadId}` : sql`true`}
        AND ${cursor ? sql`(m.occurred_at, m.id) < (${cursor.lastAt}::timestamptz, ${cursor.id})` : sql`true`}
      ORDER BY m.occurred_at DESC, m.id DESC
      LIMIT ${f.limit + 1}
    `),
  );
  const page = rows.slice(0, f.limit);
  const last = page[page.length - 1];
  return {
    messages: page.map(mapMessageRow),
    nextCursor: rows.length > f.limit && last ? encodeCursor(String(iso(last.occurred_at)), String(last.id)) : null,
  };
}

function mapMessageRow(r: Record<string, unknown>) {
  const s = (k: string) => (r[k] === null || r[k] === undefined ? null : String(r[k]));
  return {
    id: String(r.id),
    sourceTable: String(r.source_table),
    sourceRowId: String(r.source_row_id),
    messageId: s("message_id"),
    direction: String(r.direction),
    kind: String(r.kind),
    transport: String(r.transport),
    accountEmail: String(r.account_email),
    mailboxLogin: s("mailbox_login"),
    counterparty: s("counterparty"),
    subject: s("subject"),
    instantlyCampaignId: s("instantly_campaign_id"),
    step: r.step === null || r.step === undefined ? null : num(r.step),
    threadId: String(r.thread_id),
    contextRef: s("context_ref"),
    orgId: s("org_id"),
    campaignId: s("campaign_id"),
    outcome: String(r.outcome),
    placement: s("placement"),
    spfPass: r.spf_pass === null || r.spf_pass === undefined ? null : Boolean(r.spf_pass),
    dkimPass: r.dkim_pass === null || r.dkim_pass === undefined ? null : Boolean(r.dkim_pass),
    dmarcPass: r.dmarc_pass === null || r.dmarc_pass === undefined ? null : Boolean(r.dmarc_pass),
    occurredAt: iso(r.occurred_at),
  };
}

/** The body, read from the bronze row the message came from. Null when that source holds no body. */
export async function readMessageBody(id: string): Promise<{ text: string | null; html: string | null; source: string } | null> {
  const [m] = rowsOf<Record<string, unknown>>(
    await db.execute(sql`SELECT source_table, source_row_id, instantly_campaign_id, step FROM messages WHERE id = ${id}`),
  );
  if (!m) return null;
  const source = String(m.source_table);
  const rowId = String(m.source_row_id);
  switch (source) {
    case "instantly_emails_raw": {
      const [r] = rowsOf<Record<string, unknown>>(
        await db.execute(sql`SELECT payload->'body'->>'text' AS text, payload->'body'->>'html' AS html FROM instantly_emails_raw WHERE id = ${rowId}`),
      );
      return { text: (r?.text as string | null) ?? null, html: (r?.html as string | null) ?? null, source };
    }
    case "smtp_dispatch_raw": {
      const [r] = rowsOf<Record<string, unknown>>(
        await db.execute(sql`
          SELECT COALESCE(s.body_html, d.payload->>'bodyHtml') AS html
          FROM smtp_dispatch_raw d
          LEFT JOIN sequence_steps s ON s.instantly_campaign_id = d.instantly_campaign_id AND s.step = d.step
          WHERE d.id = ${rowId}
        `),
      );
      return { text: null, html: (r?.html as string | null) ?? null, source };
    }
    case "imap_messages_raw": {
      const [r] = rowsOf<Record<string, unknown>>(
        await db.execute(sql`SELECT payload->>'textSnippet' AS text FROM imap_messages_raw WHERE id = ${rowId}`),
      );
      return { text: (r?.text as string | null) ?? null, html: null, source };
    }
    default:
      // Warmup and seed bodies are generated per send and not stored beside the dispatch.
      return { text: null, html: null, source };
  }
}
