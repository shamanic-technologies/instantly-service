/**
 * Sending-domain blocklists (DOMAIN-BLOCKLIST-1016) — the IO half.
 *
 * Once a day (lib/domain-blocklist-worker.ts) every domain we send from is
 * looked up on each list of `BLOCKLISTS`. A fresh honest answer moves
 * `domain_blocklist_status`; a change writes an event (append-only). A NEW
 * listing also writes a `removal_link_issued` event and queues its ONE staff
 * alert; the lifecycle is reconciled right away so the domain's mailboxes stop
 * taking new sequences within the same pass, and come back the day a fresh
 * check reads the domain clean.
 *
 * Fail-loud per list: a list whose canary is not answered as listed (or whose
 * nameservers cannot be found) answers NOTHING this run — every domain on it is
 * `unanswered` and keeps its stored state.
 */

import { errorMessage, errorString } from "./db-error.js";
import dns from "node:dns/promises";
import { sql } from "drizzle-orm";

import { db } from "../db";
import { domainBlocklistEvents } from "../db/schema";
import {
  BLOCKLISTS,
  blocklistByKey,
  canaryHonest,
  domainOf,
  planTransitions,
  readLookup,
  type BlocklistDefinition,
  type BlocklistKey,
  type CurrentState,
  type DomainVerdict,
  type LookupResult,
} from "./domain-blocklist";

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

const LOOKUP_TIMEOUT_MS = 5_000;
const LOOKUP_CONCURRENCY = 5;

/** A function that resolves one name to A records, mapped onto `LookupResult`. */
export type Lookup = (name: string) => Promise<LookupResult>;

function toLookupResult(error: unknown): LookupResult {
  const code = (error as { code?: string })?.code ?? errorString(error);
  // NXDOMAIN / NODATA = the list's honest "not listed".
  if (code === "ENOTFOUND" || code === "ENODATA") return { kind: "nxdomain" };
  return { kind: "error", code };
}

function resolverLookup(resolver: dns.Resolver | typeof dns): Lookup {
  return async (name) => {
    try {
      return { kind: "answer", addresses: await resolver.resolve4(name) };
    } catch (error) {
      return toLookupResult(error);
    }
  };
}

/**
 * How we reach one list. `authoritative` = the zone's own nameservers, queried
 * directly from this box (our own recursion): the shared resolver is refused by
 * Spamhaus and URIBL, which answer it with a refusal code that looks like a listing.
 */
export async function lookupFor(list: BlocklistDefinition): Promise<Lookup> {
  if (list.resolver === "system") return resolverLookup(dns);
  const nameservers = await dns.resolveNs(list.zone);
  const ips: string[] = [];
  for (const ns of nameservers) {
    try {
      ips.push(...(await dns.resolve4(ns)));
    } catch {
      // One nameserver without an address is not the list; the others answer.
    }
  }
  if (ips.length === 0) throw new Error(`no nameserver address for ${list.zone}`);
  const resolver = new dns.Resolver({ timeout: LOOKUP_TIMEOUT_MS, tries: 2 });
  resolver.setServers(ips.slice(0, 4));
  return resolverLookup(resolver);
}

/** Every domain we send from (an address present in silver), with its addresses. */
export async function loadSendingDomains(): Promise<Map<string, string[]>> {
  const result = await db.execute(sql`
    SELECT lower(email) AS "email"
    FROM instantly_accounts
    WHERE absent_since IS NULL
    ORDER BY 1
  `);
  const byDomain = new Map<string, string[]>();
  for (const { email } of rowsOf<{ email: string }>(result)) {
    const domain = domainOf(email);
    if (!domain) continue;
    const list = byDomain.get(domain) ?? [];
    list.push(email);
    byDomain.set(domain, list);
  }
  return byDomain;
}

export interface ListRunResult {
  list: BlocklistKey;
  verdicts: Array<{ domain: string; list: BlocklistKey; verdict: DomainVerdict }>;
  /** Why the whole list is unanswered this run; null when it answered. */
  unanswered: string | null;
}

async function mapLimit<T, R>(items: readonly T[], limit: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const i = next++;
        out[i] = await fn(items[i]!);
      }
    }),
  );
  return out;
}

/** Look every domain up on one list, behind the list's canary. */
export async function checkList(
  list: BlocklistDefinition,
  domains: readonly string[],
  lookupFactory: (list: BlocklistDefinition) => Promise<Lookup> = lookupFor,
): Promise<ListRunResult> {
  const unansweredAll = (why: string): ListRunResult => ({
    list: list.key,
    unanswered: why,
    verdicts: domains.map((domain) => ({
      domain,
      list: list.key,
      verdict: { outcome: "unanswered", error: why },
    })),
  });

  let lookup: Lookup;
  try {
    lookup = await lookupFactory(list);
  } catch (error) {
    return unansweredAll(`resolver: ${errorMessage(error)}`);
  }
  const canary = await lookup(`${list.canary}.${list.zone}`);
  if (!canaryHonest(list, canary)) {
    const seen = canary.kind === "answer" ? canary.addresses.join(",") : canary.kind === "error" ? canary.code : "nxdomain";
    return unansweredAll(`canary ${list.canary} not answered as listed (${seen})`);
  }
  const verdicts = await mapLimit(domains, LOOKUP_CONCURRENCY, async (domain) => ({
    domain,
    list: list.key,
    verdict: readLookup(list, await lookup(`${domain}.${list.zone}`)),
  }));
  return { list: list.key, verdicts, unanswered: null };
}

async function loadCurrentStates(): Promise<CurrentState[]> {
  const result = await db.execute(sql`
    SELECT domain, list, listed, listed_since AS "listedSince" FROM domain_blocklist_status
  `);
  return rowsOf<{ domain: string; list: BlocklistKey; listed: boolean; listedSince: Date | null }>(result);
}

/** The earliest bounce citing each list, per sending domain: "since when" evidence for staff. */
async function firstCitingBounces(): Promise<Map<string, string>> {
  const result = await db.execute(sql`
    SELECT split_part(lower(account_email), '@', 2) AS "domain",
           MIN(COALESCE(received_at, polled_at)) AS "firstAt"
    FROM imap_messages_raw
    WHERE kind = 'bounce' AND payload->>'textSnippet' ~* 'surbl|spamhaus|uribl'
    GROUP BY 1
  `);
  const out = new Map<string, string>();
  for (const r of rowsOf<{ domain: string; firstAt: Date | string | null }>(result)) {
    if (r.firstAt) out.set(r.domain, new Date(r.firstAt instanceof Date ? r.firstAt : `${String(r.firstAt).replace(" ", "T")}Z`).toISOString());
  }
  return out;
}

export interface BlocklistCheckSummary {
  domains: number;
  lists: Array<{ list: BlocklistKey; answered: boolean; listed: number; clean: number; unanswered: number; reason: string | null }>;
  listed: number;
  delisted: number;
  /** Lifecycle reconcile run because something changed; null when nothing did. */
  lifecycle: unknown;
}

/**
 * One full check. Writes state + events; reconciles the lifecycle when a
 * listing appeared or ended. Never throws for one list's failure (it is
 * reported `answered: false`); throws when the database write fails.
 */
export async function runBlocklistCheck(
  options: {
    asOf?: Date;
    lookupFactory?: (list: BlocklistDefinition) => Promise<Lookup>;
    reconcile?: () => Promise<unknown>;
  } = {},
): Promise<BlocklistCheckSummary> {
  const asOf = options.asOf ?? new Date();
  const mailboxesByDomain = await loadSendingDomains();
  const domains = [...mailboxesByDomain.keys()].sort();

  const runs: ListRunResult[] = [];
  for (const list of BLOCKLISTS) {
    runs.push(await checkList(list, domains, options.lookupFactory));
  }

  const current = await loadCurrentStates();
  const allVerdicts = runs.flatMap((r) => r.verdicts);
  const transitions = planTransitions(current, allVerdicts);
  const citing = transitions.some((t) => t.type === "listed") ? await firstCitingBounces() : new Map<string, string>();

  await db.transaction(async (tx) => {
    for (const { domain, list, verdict } of allVerdicts) {
      if (verdict.outcome === "unanswered") {
        // State untouched: a refusal is not a check. Only the attempt is recorded.
        await tx.execute(sql`
          INSERT INTO domain_blocklist_status (domain, list, listed, labels, last_checked_at, last_outcome, last_error, updated_at)
          VALUES (${domain}, ${list}, false, '{}'::text[], ${asOf.toISOString()}::timestamptz, 'unanswered', ${verdict.error}, now())
          ON CONFLICT (domain, list) DO UPDATE
            SET last_checked_at = EXCLUDED.last_checked_at,
                last_outcome = 'unanswered',
                last_error = EXCLUDED.last_error,
                updated_at = now()
        `);
        continue;
      }
      const listed = verdict.outcome === "listed";
      const answer = listed ? verdict.answer : null;
      const labels = listed ? verdict.labels : [];
      await tx.execute(sql`
        INSERT INTO domain_blocklist_status
          (domain, list, listed, answer, labels, listed_since, last_checked_at, last_answered_at, first_answered_at, last_outcome, last_error, updated_at)
        VALUES (${domain}, ${list}, ${listed}, ${answer}, ${sql.param(labels)}::text[],
                ${listed ? asOf.toISOString() : null}::timestamptz,
                ${asOf.toISOString()}::timestamptz, ${asOf.toISOString()}::timestamptz,
                ${asOf.toISOString()}::timestamptz, ${verdict.outcome}, NULL, now())
        ON CONFLICT (domain, list) DO UPDATE
          SET listed = EXCLUDED.listed,
              answer = EXCLUDED.answer,
              labels = EXCLUDED.labels,
              -- A listing keeps the date we FIRST saw it; a clean answer clears it.
              listed_since = CASE
                WHEN EXCLUDED.listed AND domain_blocklist_status.listed THEN domain_blocklist_status.listed_since
                ELSE EXCLUDED.listed_since END,
              last_checked_at = EXCLUDED.last_checked_at,
              last_answered_at = EXCLUDED.last_answered_at,
              first_answered_at = COALESCE(domain_blocklist_status.first_answered_at, EXCLUDED.first_answered_at),
              last_outcome = EXCLUDED.last_outcome,
              last_error = NULL,
              updated_at = now()
      `);
    }

    for (const t of transitions) {
      const def = blocklistByKey(t.list)!;
      const mailboxes = mailboxesByDomain.get(t.domain) ?? [];
      if (t.type === "listed") {
        await tx.insert(domainBlocklistEvents).values({
          domain: t.domain,
          list: t.list,
          eventType: "listed",
          answer: t.answer,
          labels: t.labels,
          mailboxes,
          detail: {
            zone: def.zone,
            firstBlocklistBounceAt: citing.get(t.domain) ?? null,
            actionTaken: "mailboxes_out_of_new_sequences",
          },
          occurredAt: asOf,
          // No staff email (owner 2026-10-18: « Arrête de m'envoyer des emails pour ça »):
          // the admin chart is the surface. `alert_status` stays null.
        });
        await tx.insert(domainBlocklistEvents).values({
          domain: t.domain,
          list: t.list,
          eventType: "removal_link_issued",
          mailboxes,
          detail: {
            method: def.removalAutomated ? "automated" : "link",
            url: def.removalUrl(t.domain),
            note: def.removalNote,
          },
          occurredAt: asOf,
        });
      } else {
        await tx.insert(domainBlocklistEvents).values({
          domain: t.domain,
          list: t.list,
          eventType: "delisted",
          mailboxes,
          detail: { zone: def.zone },
          occurredAt: asOf,
        });
      }
    }
  });

  let lifecycle: unknown = null;
  if (transitions.length > 0) {
    lifecycle = await (options.reconcile ?? reconcileNow)();
  }

  const listed = transitions.filter((t) => t.type === "listed").length;
  const delisted = transitions.length - listed;
  return {
    domains: domains.length,
    lists: runs.map((r) => ({
      list: r.list,
      answered: r.unanswered === null,
      listed: r.verdicts.filter((v) => v.verdict.outcome === "listed").length,
      clean: r.verdicts.filter((v) => v.verdict.outcome === "clean").length,
      unanswered: r.verdicts.filter((v) => v.verdict.outcome === "unanswered").length,
      reason: r.unanswered,
    })),
    listed,
    delisted,
    lifecycle,
  };
}

/** The same reconcile accounts-sync runs, so a listing reaches the send pool now. */
async function reconcileNow(): Promise<unknown> {
  const { resolvePlatformInstantlyApiKey } = await import("./key-client");
  const { reconcileLifecycle } = await import("./account-lifecycle-sync");
  const apiKey = await resolvePlatformInstantlyApiKey({
    method: "POST",
    path: "/internal/domain-blocklist/check",
  });
  return reconcileLifecycle(apiKey);
}

/** When the last full check ran (any list), or null if never. */
export async function lastCheckAt(): Promise<Date | null> {
  const result = await db.execute(sql`SELECT MAX(last_checked_at) AS "at" FROM domain_blocklist_status`);
  const at = rowsOf<{ at: Date | string | null }>(result)[0]?.at ?? null;
  if (at === null) return null;
  return at instanceof Date ? at : new Date(at);
}

/**
 * Record that a person filed a removal request (the forms are not ours to
 * drive). Append-only; staff call it after filing.
 */
export async function recordRemovalRequest(input: {
  domain: string;
  list: BlocklistKey;
  note: string | null;
  requestedBy: string | null;
}): Promise<{ id: string }> {
  const def = blocklistByKey(input.list);
  if (!def) throw new Error(`unknown list ${input.list}`);
  const [row] = await db
    .insert(domainBlocklistEvents)
    .values({
      domain: input.domain.trim().toLowerCase(),
      list: input.list,
      eventType: "removal_requested",
      detail: { method: "manual", url: def.removalUrl(input.domain), note: input.note, requestedBy: input.requestedBy },
    })
    .returning({ id: domainBlocklistEvents.id });
  return { id: row!.id };
}
