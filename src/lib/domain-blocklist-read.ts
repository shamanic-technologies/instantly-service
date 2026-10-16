/**
 * Staff read of the sending-domain blocklists (DOMAIN-BLOCKLIST-1016),
 * `GET /internal/domain-blocklist`: the current state per domain and list, the
 * event history, and DAILY series (one chart per sub-topic, owner 2026-10-16).
 *
 * Every series is UTC-day bucketed, one point per day of the window, zero
 * included. Listing-based series (domains listed, mailboxes out, sends from
 * listed domains) can only start at our first check: before it nobody looked,
 * and a day nobody looked is not a clean day — those days are `null`, never 0.
 * The bounce series reads the DSNs we stored, so it reaches back to the first
 * one we hold (2026-09-01).
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { BLOCKLISTS, type BlocklistKey } from "./domain-blocklist";
import { loadSendingDomains } from "./domain-blocklist-sync";

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

/**
 * A database instant as ISO. A `timestamptz` arrives as a Date or as
 * `2026-10-16 06:00:00+00` (offset without minutes); a naive `timestamp` has no
 * offset and is UTC on this box.
 */
export function iso(value: Date | string | null | undefined): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "object" && typeof (value as Date).getTime === "function") {
    const t = (value as Date).getTime();
    return Number.isNaN(t) ? null : new Date(t).toISOString();
  }
  let s = String(value).trim().replace(" ", "T");
  if (/[+-]\d{2}$/.test(s)) s = `${s}:00`;
  else if (!/[Zz]|[+-]\d{2}:?\d{2}$/.test(s)) s = `${s}Z`;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

const MS_DAY = 86_400_000;

export function utcDay(at: Date): string {
  return at.toISOString().slice(0, 10);
}

/** The window's days, oldest first, ending on `asOf`'s UTC day. */
export function windowDays(asOf: Date, days: number): string[] {
  const end = Date.UTC(asOf.getUTCFullYear(), asOf.getUTCMonth(), asOf.getUTCDate());
  return Array.from({ length: days }, (_, i) => utcDay(new Date(end - (days - 1 - i) * MS_DAY)));
}

/**
 * A delivery-failure notice whose words cite a blocklist (Postgres `~*`). One
 * home: the blocklist read and the source-yield read (SOURCE-YIELD-1016) both use it.
 */
export const BLOCKLIST_CITATION_PATTERN = "(surbl|spamhaus|uribl|block ?list|black ?list|dnsbl|\\mrbl\\M)";

/** Bounce classes a DSN can cite. `other_list` = names a blocklist we do not track. */
export const CITED_LISTS = ["surbl", "spamhaus", "uribl", "other_list"] as const;
export type CitedList = (typeof CITED_LISTS)[number];

export interface ListingEvent {
  domain: string;
  list: BlocklistKey;
  type: "listed" | "delisted";
  at: Date;
  mailboxes: string[];
}

export interface ListingSeries {
  domainsListed: Array<{ day: string; total: number | null; byList: Record<BlocklistKey, number> | null }>;
  mailboxesOutOfNewSequences: Array<{ day: string; mailboxes: number | null }>;
  /** Per day, the sending domains listed on ANY list at some point of the day. */
  listedDomainsByDay: Map<string, Set<string>>;
}

/**
 * Listing intervals -> per-day series. Pure. A (domain, list) counts on a day
 * when it was listed at any moment of that day (a listing and its delisting the
 * same day count that day). Days before `monitoredSince` are null.
 */
export function buildListingSeries(
  events: readonly ListingEvent[],
  days: readonly string[],
  monitoredSince: Date | null,
  asOf: Date,
): ListingSeries {
  const startDay = monitoredSince ? utcDay(monitoredSince) : null;
  const intervals: Array<{ domain: string; list: BlocklistKey; from: string; to: string; mailboxes: string[] }> = [];
  const open = new Map<string, { from: string; mailboxes: string[] }>();
  for (const e of [...events].sort((a, b) => a.at.getTime() - b.at.getTime())) {
    const key = `${e.domain}|${e.list}`;
    if (e.type === "listed") {
      if (!open.has(key)) open.set(key, { from: utcDay(e.at), mailboxes: e.mailboxes });
    } else {
      const o = open.get(key);
      if (o) {
        intervals.push({ domain: e.domain, list: e.list, from: o.from, to: utcDay(e.at), mailboxes: o.mailboxes });
        open.delete(key);
      }
    }
  }
  for (const [key, o] of open) {
    const [domain, list] = key.split("|") as [string, BlocklistKey];
    intervals.push({ domain, list, from: o.from, to: utcDay(asOf), mailboxes: o.mailboxes });
  }

  const listedDomainsByDay = new Map<string, Set<string>>();
  const domainsListed: ListingSeries["domainsListed"] = [];
  const mailboxesOut: ListingSeries["mailboxesOutOfNewSequences"] = [];
  for (const day of days) {
    if (startDay === null || day < startDay) {
      domainsListed.push({ day, total: null, byList: null });
      mailboxesOut.push({ day, mailboxes: null });
      continue;
    }
    const byList = Object.fromEntries(BLOCKLISTS.map((l) => [l.key, 0])) as Record<BlocklistKey, number>;
    const domains = new Set<string>();
    const mailboxes = new Set<string>();
    for (const iv of intervals) {
      if (iv.from <= day && day <= iv.to) {
        byList[iv.list] += 1;
        domains.add(iv.domain);
        for (const m of iv.mailboxes) mailboxes.add(m);
      }
    }
    listedDomainsByDay.set(day, domains);
    domainsListed.push({ day, total: domains.size, byList });
    mailboxesOut.push({ day, mailboxes: mailboxes.size });
  }
  return { domainsListed, mailboxesOutOfNewSequences: mailboxesOut, listedDomainsByDay };
}

export interface DomainBlocklistRead {
  asOf: string;
  monitoredSince: string | null;
  lists: Array<{ key: BlocklistKey; name: string; zone: string; removalUrl: string; removalAutomated: boolean; removalNote: string }>;
  domains: Array<{
    domain: string;
    mailboxes: string[];
    listed: boolean;
    outOfNewSequences: boolean;
    lists: Array<{
      list: BlocklistKey;
      state: "listed" | "clean" | "unknown";
      answer: string | null;
      labels: string[];
      listedSince: string | null;
      lastCheckedAt: string | null;
      lastAnsweredAt: string | null;
      lastOutcome: string | null;
      lastError: string | null;
    }>;
  }>;
  events: Array<{
    id: string;
    domain: string;
    list: string;
    type: string;
    answer: string | null;
    labels: string[];
    mailboxes: string[];
    detail: unknown;
    occurredAt: string;
    alert: { status: string; attempts: number; lastError: string | null; alertedAt: string | null } | null;
  }>;
  series: {
    days: string[];
    domainsListed: ListingSeries["domainsListed"];
    mailboxesOutOfNewSequences: ListingSeries["mailboxesOutOfNewSequences"];
    sendsFromListedDomains: Array<{ day: string; sends: number | null }>;
    blocklistBounces: Array<{ day: string; total: number; byList: Record<CitedList, number> }>;
    removalRequests: Array<{ day: string; linksIssued: number; requested: number }>;
    delistings: Array<{ day: string; delistings: number }>;
  };
}

export const DOMAIN_BLOCKLIST_DEFAULT_DAYS = 60;
export const DOMAIN_BLOCKLIST_MAX_DAYS = 365;

export async function loadDomainBlocklistRead(days: number, asOf: Date = new Date()): Promise<DomainBlocklistRead> {
  const dayList = windowDays(asOf, days);
  const since = `${dayList[0]}T00:00:00.000Z`;

  const [mailboxesByDomain, statusResult, eventResult, lifecycleResult, bounceResult] = await Promise.all([
    loadSendingDomains(),
    db.execute(sql`
      SELECT domain, list, listed, answer, labels, listed_since AS "listedSince",
             last_checked_at AS "lastCheckedAt", last_answered_at AS "lastAnsweredAt",
             first_answered_at AS "firstAnsweredAt",
             last_outcome AS "lastOutcome", last_error AS "lastError"
      FROM domain_blocklist_status
    `),
    db.execute(sql`
      SELECT id, domain, list, event_type AS "type", answer, labels, mailboxes, detail,
             occurred_at AS "occurredAt", alert_status AS "alertStatus", alert_attempts AS "alertAttempts",
             alert_last_error AS "alertLastError", alerted_at AS "alertedAt"
      FROM domain_blocklist_events
      ORDER BY occurred_at DESC, id
    `),
    db.execute(sql`
      SELECT lower(email) AS "email" FROM instantly_accounts
      WHERE absent_since IS NULL AND lifecycle_reason = 'domain_blocklisted'
    `),
    db.execute(sql`
      SELECT to_char(date_trunc('day', COALESCE(received_at, polled_at)), 'YYYY-MM-DD') AS "day",
             CASE
               WHEN payload->>'textSnippet' ~* 'surbl' THEN 'surbl'
               WHEN payload->>'textSnippet' ~* 'spamhaus' THEN 'spamhaus'
               WHEN payload->>'textSnippet' ~* 'uribl' THEN 'uribl'
               ELSE 'other_list'
             END AS "list",
             COUNT(*)::int AS "n"
      FROM imap_messages_raw
      WHERE kind = 'bounce'
        AND polled_at >= ${since}::timestamptz - interval '2 days'
        AND COALESCE(received_at, polled_at) >= ${since}::timestamptz
        AND payload->>'textSnippet' ~* ${BLOCKLIST_CITATION_PATTERN}
      GROUP BY 1, 2
    `),
  ]);

  const statuses = rowsOf<{
    domain: string; list: BlocklistKey; listed: boolean; answer: string | null; labels: string[] | null;
    listedSince: Date | string | null; lastCheckedAt: Date | string | null; lastAnsweredAt: Date | string | null;
    firstAnsweredAt: Date | string | null;
    lastOutcome: string | null; lastError: string | null;
  }>(statusResult);
  const events = rowsOf<{
    id: string; domain: string; list: BlocklistKey; type: string; answer: string | null; labels: string[] | null;
    mailboxes: string[] | null; detail: unknown; occurredAt: Date | string; alertStatus: string | null;
    alertAttempts: number; alertLastError: string | null; alertedAt: Date | string | null;
  }>(eventResult);
  const outMailboxes = new Set(rowsOf<{ email: string }>(lifecycleResult).map((r) => r.email));

  const answeredAts = statuses.map((s) => iso(s.firstAnsweredAt)).filter((s): s is string => s !== null).sort();
  const monitoredSince = answeredAts[0] ? new Date(answeredAts[0]) : null;

  // Domains: every sending domain, plus any domain we hold a state for.
  const statusByDomain = new Map<string, typeof statuses>();
  for (const s of statuses) {
    const list = statusByDomain.get(s.domain) ?? [];
    list.push(s);
    statusByDomain.set(s.domain, list);
  }
  const allDomains = [...new Set([...mailboxesByDomain.keys(), ...statusByDomain.keys()])].sort();
  const domains = allDomains.map((domain) => {
    const mailboxes = mailboxesByDomain.get(domain) ?? [];
    const rows = statusByDomain.get(domain) ?? [];
    const lists = BLOCKLISTS.map((def) => {
      const r = rows.find((x) => x.list === def.key);
      const state: "listed" | "clean" | "unknown" =
        !r || r.lastAnsweredAt === null ? "unknown" : r.listed ? "listed" : "clean";
      return {
        list: def.key,
        state,
        answer: r?.answer ?? null,
        labels: r?.labels ?? [],
        listedSince: iso(r?.listedSince ?? null),
        lastCheckedAt: iso(r?.lastCheckedAt ?? null),
        lastAnsweredAt: iso(r?.lastAnsweredAt ?? null),
        lastOutcome: r?.lastOutcome ?? null,
        lastError: r?.lastError ?? null,
      };
    });
    return {
      domain,
      mailboxes,
      listed: lists.some((l) => l.state === "listed"),
      outOfNewSequences: mailboxes.length > 0 && mailboxes.every((m) => outMailboxes.has(m)),
      lists,
    };
  });

  const listing = buildListingSeries(
    events
      .filter((e) => e.type === "listed" || e.type === "delisted")
      .map((e) => ({
        domain: e.domain,
        list: e.list,
        type: e.type as "listed" | "delisted",
        at: new Date(iso(e.occurredAt)!),
        mailboxes: e.mailboxes ?? [],
      })),
    dayList,
    monitoredSince,
    asOf,
  );

  // Sends are read only where they can count: days since monitoring began, on
  // domains listed in that span. A full-window scan of the event log was 10 s.
  const everListed = [...new Set([...listing.listedDomainsByDay.values()].flatMap((d) => [...d]))];
  const sendFrom = dayList.find((d) => listing.listedDomainsByDay.has(d)) ?? null;
  const sendRows =
    everListed.length === 0 || sendFrom === null
      ? []
      : rowsOf<{ day: string; domain: string; n: number }>(
          await db.execute(sql`
            SELECT to_char(date_trunc('day', e.timestamp), 'YYYY-MM-DD') AS "day",
                   split_part(lower(e.account_email), '@', 2) AS "domain",
                   COUNT(*)::int AS "n"
            FROM instantly_events e
            WHERE e.event_type = 'email_sent' AND e.inferred = false
              -- created_at is indexed and never earlier than the send it records.
              AND e.created_at >= ${`${sendFrom}T00:00:00.000Z`}::timestamptz
              AND e.timestamp >= ${`${sendFrom}T00:00:00.000Z`}::timestamptz
              AND split_part(lower(e.account_email), '@', 2) = ANY(${sql.param(everListed)}::text[])
            GROUP BY 1, 2
          `),
        );
  const sendsByDayDomain = new Map<string, number>();
  for (const r of sendRows) {
    sendsByDayDomain.set(`${r.day}|${r.domain}`, Number(r.n));
  }
  const sendsFromListedDomains = dayList.map((day) => {
    const listed = listing.listedDomainsByDay.get(day);
    if (!listed) return { day, sends: null };
    let sends = 0;
    for (const d of listed) sends += sendsByDayDomain.get(`${day}|${d}`) ?? 0;
    return { day, sends };
  });

  const bounceByDay = new Map<string, Record<CitedList, number>>();
  for (const r of rowsOf<{ day: string; list: CitedList; n: number }>(bounceResult)) {
    const rec = bounceByDay.get(r.day) ?? { surbl: 0, spamhaus: 0, uribl: 0, other_list: 0 };
    rec[r.list] += Number(r.n);
    bounceByDay.set(r.day, rec);
  }
  const blocklistBounces = dayList.map((day) => {
    const byList = bounceByDay.get(day) ?? { surbl: 0, spamhaus: 0, uribl: 0, other_list: 0 };
    return { day, total: CITED_LISTS.reduce((s, k) => s + byList[k], 0), byList };
  });

  const countEvents = (day: string, type: string) =>
    events.filter((e) => e.type === type && iso(e.occurredAt)!.slice(0, 10) === day).length;

  return {
    asOf: asOf.toISOString(),
    monitoredSince: monitoredSince?.toISOString() ?? null,
    lists: BLOCKLISTS.map((l) => ({
      key: l.key,
      name: l.name,
      zone: l.zone,
      removalUrl: l.removalUrl(""),
      removalAutomated: l.removalAutomated,
      removalNote: l.removalNote,
    })),
    domains,
    events: events.map((e) => ({
      id: e.id,
      domain: e.domain,
      list: e.list,
      type: e.type,
      answer: e.answer,
      labels: e.labels ?? [],
      mailboxes: e.mailboxes ?? [],
      detail: e.detail ?? null,
      occurredAt: iso(e.occurredAt)!,
      alert:
        e.alertStatus === null
          ? null
          : { status: e.alertStatus, attempts: Number(e.alertAttempts), lastError: e.alertLastError, alertedAt: iso(e.alertedAt) },
    })),
    series: {
      days: dayList,
      domainsListed: listing.domainsListed,
      mailboxesOutOfNewSequences: listing.mailboxesOutOfNewSequences,
      sendsFromListedDomains,
      blocklistBounces,
      removalRequests: dayList.map((day) => ({
        day,
        linksIssued: countEvents(day, "removal_link_issued"),
        requested: countEvents(day, "removal_requested"),
      })),
      delistings: dayList.map((day) => ({ day, delistings: countEvents(day, "delisted") })),
    },
  };
}
