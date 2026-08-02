/**
 * Provider-infrastructure sync — Bronze + Silver for the four vendors that sell
 * us domains and mailboxes (issue #555).
 *
 * Until this existed, only Instantly was known to the code, and only at the
 * ACCOUNT grain: a domain was a substring of an email address, so nothing could
 * answer "who did we buy this from, when does it expire, and what does it cost".
 *
 * Shape: each provider is fetched independently, written to bronze verbatim,
 * upserted into silver, and then swept for rows the vendor stopped reporting.
 * A provider that fails does NOT abort the others — its failure is counted and
 * logged, and the run only throws when EVERY provider failed (a run that healed
 * nothing must not read as green).
 *
 * Absence is swept ONLY for a provider whose fetch SUCCEEDED. A transient
 * vendor outage must never flag that vendor's whole estate as gone.
 *
 * Spends nothing metered — every call is an inventory read against a flat
 * subscription — so no run/cost is declared, consistent with the other
 * flat-subscription reads in this service.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import {
  infraDomains,
  infraMailboxes,
  providerAccountRaw,
  providerDomainsRaw,
  providerMailboxesRaw,
} from "../db/schema";
import { resolvePlatformKey, type CallerInfo } from "./key-client";
import { fetchGandiInventory, type GandiOrgCredential } from "./providers/gandi-client";
import { fetchInstantlyDfyInventory } from "./providers/instantly-dfy-client";
import { fetchMailforgeInventory } from "./providers/mailforge-client";
import { fetchPrimeforgeInventory } from "./providers/primeforge-client";
import type { InfraProvider, ProviderInventory } from "./providers/types";

/**
 * key-service provider names for Gandi's three organisations. Gandi issues one
 * token per organisation and a token sees only its own domains, so each org is
 * its own platform-key row. The suffix after `gandi-` becomes the row's
 * `providerAccount`, so an ops read can tell which organisation holds a domain.
 */
export const GANDI_KEY_PROVIDERS = ["gandi-org1", "gandi-org2", "gandi-org3"] as const;

export interface ProviderFailure {
  provider: string;
  message: string;
}

export interface InfraSyncSummary {
  providersAttempted: number;
  providersSucceeded: number;
  providersFailed: number;
  domainsUpserted: number;
  mailboxesUpserted: number;
  accountScopesRecorded: number;
  domainsMarkedAbsent: number;
  mailboxesMarkedAbsent: number;
  failures: ProviderFailure[];
}

/** One unit of work: a vendor credential and how to read its inventory. */
export interface ProviderTask {
  /** Silver `provider` value. Gandi's three orgs all use `gandi`. */
  provider: InfraProvider;
  /** key-service provider name — distinct from the silver value for Gandi. */
  keyProvider: string;
  fetch: (key: string) => Promise<ProviderInventory>;
}

/**
 * The fixed task list. Gandi contributes three tasks (one per organisation)
 * that all write under the single `gandi` silver provider; each task sweeps
 * absence only for its own organisation, so one org's outage cannot flag
 * another org's domains as gone.
 */
export function buildProviderTasks(): ProviderTask[] {
  const gandiTasks: ProviderTask[] = GANDI_KEY_PROVIDERS.map((keyProvider) => {
    const account = keyProvider.replace("gandi-", "");
    return {
      provider: "gandi" as const,
      keyProvider,
      fetch: (key: string) => {
        const credential: GandiOrgCredential = { account, token: key };
        return fetchGandiInventory(credential);
      },
    };
  });

  return [
    ...gandiTasks,
    { provider: "mailforge", keyProvider: "mailforge", fetch: fetchMailforgeInventory },
    { provider: "primeforge", keyProvider: "primeforge", fetch: fetchPrimeforgeInventory },
    { provider: "instantly-dfy", keyProvider: "instantly", fetch: fetchInstantlyDfyInventory },
  ];
}

/** Empty summary — the accumulator every run starts from. */
export function emptySummary(attempted: number): InfraSyncSummary {
  return {
    providersAttempted: attempted,
    providersSucceeded: 0,
    providersFailed: 0,
    domainsUpserted: 0,
    mailboxesUpserted: 0,
    accountScopesRecorded: 0,
    domainsMarkedAbsent: 0,
    mailboxesMarkedAbsent: 0,
    failures: [],
  };
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

/** Bronze + Silver writes for one task's inventory. */
async function persistInventory(
  task: ProviderTask,
  inventory: ProviderInventory,
  now: Date,
): Promise<{ domains: number; mailboxes: number; scopes: number }> {
  for (const domain of inventory.domains) {
    await db.insert(providerDomainsRaw).values({
      provider: domain.provider,
      providerAccount: domain.providerAccount,
      domain: domain.domain,
      payload: domain.payload as Record<string, unknown>,
      fetchedAt: now,
    });

    const values = {
      provider: domain.provider,
      domain: domain.domain,
      providerAccount: domain.providerAccount,
      externalId: domain.externalId,
      role: domain.role,
      status: domain.status,
      createdAtProvider: domain.createdAtProvider,
      expiresAt: domain.expiresAt,
      autorenew: domain.autorenew,
      deletionScheduled: domain.deletionScheduled,
      cancelledAt: domain.cancelledAt,
      priceCents: domain.priceCents,
      priceCurrency: domain.priceCurrency,
      lastSeenAt: now,
      absentSince: null,
    };

    await db
      .insert(infraDomains)
      .values(values)
      .onConflictDoUpdate({
        target: [infraDomains.provider, infraDomains.domain],
        // `firstSeenAt` is deliberately absent — it must keep its original value.
        set: {
          providerAccount: values.providerAccount,
          externalId: values.externalId,
          role: values.role,
          status: values.status,
          createdAtProvider: values.createdAtProvider,
          expiresAt: values.expiresAt,
          autorenew: values.autorenew,
          deletionScheduled: values.deletionScheduled,
          cancelledAt: values.cancelledAt,
          priceCents: values.priceCents,
          priceCurrency: values.priceCurrency,
          lastSeenAt: now,
          absentSince: null,
        },
      });
  }

  for (const mailbox of inventory.mailboxes) {
    await db.insert(providerMailboxesRaw).values({
      provider: mailbox.provider,
      providerAccount: mailbox.providerAccount,
      email: mailbox.email,
      domain: mailbox.domain,
      payload: mailbox.payload as Record<string, unknown>,
      fetchedAt: now,
    });

    await db
      .insert(infraMailboxes)
      .values({
        provider: mailbox.provider,
        email: mailbox.email,
        domain: mailbox.domain,
        providerAccount: mailbox.providerAccount,
        externalId: mailbox.externalId,
        status: mailbox.status,
        createdAtProvider: mailbox.createdAtProvider,
        lastSeenAt: now,
        absentSince: null,
      })
      .onConflictDoUpdate({
        target: [infraMailboxes.provider, infraMailboxes.email],
        set: {
          domain: mailbox.domain,
          providerAccount: mailbox.providerAccount,
          externalId: mailbox.externalId,
          status: mailbox.status,
          createdAtProvider: mailbox.createdAtProvider,
          lastSeenAt: now,
          absentSince: null,
        },
      });
  }

  for (const scope of inventory.accountScopes) {
    await db.insert(providerAccountRaw).values({
      provider: task.provider,
      scope: scope.scope,
      payload: scope.payload as Record<string, unknown>,
      fetchedAt: now,
    });
  }

  return {
    domains: inventory.domains.length,
    mailboxes: inventory.mailboxes.length,
    scopes: inventory.accountScopes.length,
  };
}

/**
 * Flag rows this task's vendor stopped reporting. Scoped to (provider,
 * providerAccount) so one Gandi organisation's sweep never touches another's,
 * and keyed on `last_seen_at` rather than a `NOT IN (…)` list — a per-domain
 * bind list grows with the estate and eventually hits Postgres' 65,534-parameter
 * ceiling.
 *
 * Rows are flagged, never deleted: a domain disappearing IS the fact worth
 * keeping (it lapsed, or it was transferred away).
 */
async function sweepAbsences(
  task: ProviderTask,
  providerAccount: string | null,
  now: Date,
): Promise<{ domains: number; mailboxes: number }> {
  const accountFilter = providerAccount
    ? sql`AND provider_account = ${providerAccount}`
    : sql``;

  const domains = await db.execute(sql`
    UPDATE infra_domains
       SET absent_since = ${now}
     WHERE provider = ${task.provider}
       AND absent_since IS NULL
       AND last_seen_at < ${now}
       ${accountFilter}
    RETURNING domain
  `);

  const mailboxes = await db.execute(sql`
    UPDATE infra_mailboxes
       SET absent_since = ${now}
     WHERE provider = ${task.provider}
       AND absent_since IS NULL
       AND last_seen_at < ${now}
       ${accountFilter}
    RETURNING email
  `);

  return {
    domains: rowsOf<{ domain: string }>(domains).length,
    mailboxes: rowsOf<{ email: string }>(mailboxes).length,
  };
}

/**
 * Poll every provider into bronze + silver.
 *
 * Throws only when EVERY provider failed — a run that ingested nothing must not
 * report success. Any partial failure is surfaced in `failures` and logged.
 */
export async function syncProviderInfra(caller: CallerInfo): Promise<InfraSyncSummary> {
  const tasks = buildProviderTasks();
  const summary = emptySummary(tasks.length);

  for (const task of tasks) {
    const now = new Date();
    const label = task.keyProvider;

    try {
      const key = await resolvePlatformKey(task.keyProvider, caller);
      const inventory = await task.fetch(key);
      const written = await persistInventory(task, inventory, now);

      // Gandi rows carry the organisation; every other vendor has one tenant.
      const providerAccount =
        task.provider === "gandi" ? task.keyProvider.replace("gandi-", "") : null;
      const swept = await sweepAbsences(task, providerAccount, now);

      summary.providersSucceeded += 1;
      summary.domainsUpserted += written.domains;
      summary.mailboxesUpserted += written.mailboxes;
      summary.accountScopesRecorded += written.scopes;
      summary.domainsMarkedAbsent += swept.domains;
      summary.mailboxesMarkedAbsent += swept.mailboxes;

      console.log(
        `[instantly-service] infra-sync: ${label} → ${written.domains} domain(s), ${written.mailboxes} mailbox(es), ${swept.domains} newly absent`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      summary.providersFailed += 1;
      summary.failures.push({ provider: label, message });
      console.error(`[instantly-service] infra-sync: ${label} FAILED — ${message}`);
    }
  }

  if (summary.providersSucceeded === 0) {
    throw new Error(
      `[instantly-service] infra-sync: every provider failed — ${summary.failures
        .map((f) => `${f.provider}: ${f.message}`)
        .join(" | ")}`,
    );
  }

  return summary;
}
