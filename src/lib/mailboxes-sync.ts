/**
 * Mailbox sync — persist the address → real-mailbox grouping (migration 0053).
 *
 * A MAILBOX is the login a provider enforces its quota, reputation and
 * credential at; a sending ADDRESS is what a prospect sees. Every day-budget
 * in this service already groups by the login, each rebuilding the grouping
 * in memory from the credential map. This projects that grouping into
 * `mailboxes` + `instantly_accounts.mailbox_login` so the ops reads can join to
 * it instead of re-deriving it.
 *
 * ⚠️ A PROJECTION, never an input. The transport is decided by the credential
 * we actually hold (`self-send/capability.ts`) and the dispatch grain by the
 * live login map (`loadMailboxLogins`). A stale row here changes what the
 * dashboard shows, never what is sent.
 *
 * Pure derivation in `deriveMailboxes`; IO in `syncMailboxes`.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { mailboxes } from "../db/schema";
import type { CallerInfo } from "./key-client";
import { providerFillRank } from "./send-lead";
import { loadMailboxLoginEntries, type CredentialSource } from "./self-send/mailbox-credentials";

export type PoolType = "google-workspace" | "gandi-relay" | "mailforge-relay" | "dfy-google";
export type Subscription = "standard" | "prewarmed" | "dfy";

/** The vendor → pool the mail actually leaves through. */
export const POOL_TYPE_BY_PROVIDER: Record<string, PoolType> = {
  gandi: "gandi-relay",
  mailforge: "mailforge-relay",
  primeforge: "google-workspace",
  "instantly-dfy": "dfy-google",
};

export interface CredentialInput {
  address: string;
  login: string;
  source: CredentialSource;
}

export interface AccountInput {
  email: string;
  timestampCreated: Date | null;
  vendorPrewarmedAt: Date | null;
  absentSince: Date | null;
}

export interface VendorMailboxInput {
  provider: string;
  email: string;
  createdAtProvider: Date | null;
  absentSince: Date | null;
}

export interface DomainProviderInput {
  domain: string;
  provider: string;
}

export interface MailboxSyncInput {
  credentials: CredentialInput[];
  accounts: AccountInput[];
  vendorMailboxes: VendorMailboxInput[];
  domainProviders: DomainProviderInput[];
}

export interface MailboxRow {
  login: string;
  domain: string;
  provider: string | null;
  poolType: PoolType | null;
  subscription: Subscription | null;
  credentialSource: CredentialSource | "none";
  vendorCreatedAt: Date | null;
  vendorPrewarmedAt: Date | null;
  importedAt: Date | null;
  absentSince: Date | null;
}

export interface MailboxDerivation {
  mailboxes: MailboxRow[];
  /** address → login, for every Instantly account. */
  accountLogins: Array<{ email: string; login: string }>;
}

export interface MailboxSyncSummary {
  mailboxes: number;
  addresses: number;
  /** Addresses whose login the credential map did not know (their own mailbox). */
  addressesWithoutCredential: number;
  /** Vendor mailboxes carrying no Instantly address at all. */
  vendorOnlyMailboxes: number;
}

function domainOf(email: string): string {
  const at = email.lastIndexOf("@");
  return at < 0 ? email : email.slice(at + 1);
}

function minDate(a: Date | null, b: Date | null): Date | null {
  if (!a) return b;
  if (!b) return a;
  return a < b ? a : b;
}

/**
 * The provider a domain is billed by, when several report it: the one that
 * fills earliest, identical to the send selector's own tie-break.
 */
export function primaryProviderByDomain(rows: DomainProviderInput[]): Map<string, string> {
  const out = new Map<string, string>();
  for (const { domain, provider } of rows) {
    const current = out.get(domain);
    if (!current || providerFillRank(provider, null) < providerFillRank(current, null)) {
      out.set(domain, provider);
    }
  }
  return out;
}

/**
 * Group addresses under their real mailbox.
 *
 * - An address the credential map knows resolves to that credential's login.
 * - An address it does NOT know is its OWN mailbox — the Primeforge and DFY
 *   reading, and the safe one: never silently folded onto another login.
 * - A vendor mailbox with no Instantly address still gets a row (a paid
 *   mailbox nobody sends from is exactly the row an inventory must show).
 * - Dates aggregate to the earliest across the mailbox's aliases; absence
 *   only when EVERY alias is absent (and the vendor row, when there is one).
 */
export function deriveMailboxes(input: MailboxSyncInput): MailboxDerivation {
  const credentialByAddress = new Map(
    input.credentials.map((c) => [c.address.trim().toLowerCase(), c]),
  );
  const providerByDomain = primaryProviderByDomain(input.domainProviders);
  const vendorByEmail = new Map(
    input.vendorMailboxes.map((m) => [m.email.trim().toLowerCase(), m]),
  );

  const byLogin = new Map<string, MailboxRow & { aliasCount: number; allAliasesAbsent: boolean }>();

  function rowFor(login: string): MailboxRow & { aliasCount: number; allAliasesAbsent: boolean } {
    let row = byLogin.get(login);
    if (!row) {
      const domain = domainOf(login);
      const provider = providerByDomain.get(domain) ?? null;
      const credential = credentialByAddress.get(login);
      const vendor = vendorByEmail.get(login);
      row = {
        login,
        domain,
        provider,
        poolType: provider ? (POOL_TYPE_BY_PROVIDER[provider] ?? null) : null,
        subscription: provider ? (provider === "instantly-dfy" ? "dfy" : "standard") : null,
        credentialSource: credential?.source ?? "none",
        vendorCreatedAt: vendor?.createdAtProvider ?? null,
        vendorPrewarmedAt: null,
        importedAt: null,
        absentSince: null,
        aliasCount: 0,
        allAliasesAbsent: true,
      };
      byLogin.set(login, row);
    }
    return row;
  }

  const accountLogins: Array<{ email: string; login: string }> = [];

  for (const account of input.accounts) {
    const email = account.email.trim().toLowerCase();
    const credential = credentialByAddress.get(email);
    const login = credential?.login ?? email;
    const row = rowFor(login);
    // A credentialed alias carries its source onto the mailbox; an own-mailbox
    // address already resolved its own credential in rowFor.
    if (credential && row.credentialSource === "none") row.credentialSource = credential.source;
    row.aliasCount += 1;
    row.importedAt = minDate(row.importedAt, account.timestampCreated);
    row.vendorPrewarmedAt = minDate(row.vendorPrewarmedAt, account.vendorPrewarmedAt);
    if (account.absentSince === null) row.allAliasesAbsent = false;
    else row.absentSince = minDate(row.absentSince, account.absentSince);
    accountLogins.push({ email, login });
  }

  for (const vendor of input.vendorMailboxes) {
    const login = vendor.email.trim().toLowerCase();
    const row = rowFor(login);
    if (row.vendorCreatedAt === null) row.vendorCreatedAt = vendor.createdAtProvider;
  }

  const rows: MailboxRow[] = [];
  for (const row of byLogin.values()) {
    const vendor = vendorByEmail.get(row.login);
    const absentSince =
      row.aliasCount === 0
        ? (vendor?.absentSince ?? null)
        : row.allAliasesAbsent && (!vendor || vendor.absentSince !== null)
          ? minDate(row.absentSince, vendor?.absentSince ?? null)
          : null;
    const subscription: Subscription | null =
      row.provider === "primeforge" && row.vendorPrewarmedAt ? "prewarmed" : row.subscription;
    const { aliasCount: _a, allAliasesAbsent: _b, ...rest } = row;
    rows.push({ ...rest, absentSince, subscription });
  }
  rows.sort((a, b) => a.login.localeCompare(b.login));
  accountLogins.sort((a, b) => a.email.localeCompare(b.email));
  return { mailboxes: rows, accountLogins };
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function toDate(value: unknown): Date | null {
  if (value === null || value === undefined) return null;
  const d = value instanceof Date ? value : new Date(String(value));
  return Number.isNaN(d.getTime()) ? null : d;
}

/** Read every input from its own source, derive, upsert. */
export async function syncMailboxes(caller: CallerInfo): Promise<MailboxSyncSummary> {
  const entries = await loadMailboxLoginEntries(caller);
  const credentials: CredentialInput[] = [...entries].map(([address, e]) => ({
    address,
    login: e.login,
    source: e.source,
  }));

  const [accountsRes, vendorRes, domainsRes] = await Promise.all([
    db.execute(sql`
      SELECT email, timestamp_created, vendor_prewarmed_at, absent_since
      FROM instantly_accounts
    `),
    db.execute(sql`
      SELECT provider, email, created_at_provider, absent_since
      FROM infra_mailboxes
    `),
    db.execute(sql`
      SELECT domain, provider
      FROM infra_domains
      WHERE absent_since IS NULL AND cancelled_at IS NULL
    `),
  ]);

  const accounts = rowsOf<Record<string, unknown>>(accountsRes).map((r) => ({
    email: String(r.email),
    timestampCreated: toDate(r.timestamp_created),
    vendorPrewarmedAt: toDate(r.vendor_prewarmed_at),
    absentSince: toDate(r.absent_since),
  }));
  const vendorMailboxes = rowsOf<Record<string, unknown>>(vendorRes).map((r) => ({
    provider: String(r.provider),
    email: String(r.email),
    createdAtProvider: toDate(r.created_at_provider),
    absentSince: toDate(r.absent_since),
  }));
  const domainProviders = rowsOf<Record<string, unknown>>(domainsRes).map((r) => ({
    domain: String(r.domain),
    provider: String(r.provider),
  }));

  const derived = deriveMailboxes({ credentials, accounts, vendorMailboxes, domainProviders });
  const now = new Date();

  for (const row of derived.mailboxes) {
    await db
      .insert(mailboxes)
      .values({ ...row, syncedAt: now })
      .onConflictDoUpdate({
        target: mailboxes.login,
        set: {
          domain: row.domain,
          provider: row.provider,
          poolType: row.poolType,
          subscription: row.subscription,
          credentialSource: row.credentialSource,
          vendorCreatedAt: row.vendorCreatedAt,
          vendorPrewarmedAt: row.vendorPrewarmedAt,
          importedAt: row.importedAt,
          absentSince: row.absentSince,
          syncedAt: now,
        },
      });
  }

  for (const { email, login } of derived.accountLogins) {
    await db.execute(sql`
      UPDATE instantly_accounts SET mailbox_login = ${login}
      WHERE lower(email) = ${email} AND mailbox_login IS DISTINCT FROM ${login}
    `);
  }

  const credentialed = new Set(credentials.map((c) => c.address));
  const summary: MailboxSyncSummary = {
    mailboxes: derived.mailboxes.length,
    addresses: derived.accountLogins.length,
    addressesWithoutCredential: derived.accountLogins.filter((a) => !credentialed.has(a.email))
      .length,
    vendorOnlyMailboxes: derived.mailboxes.filter(
      (m) => !derived.accountLogins.some((a) => a.login === m.login),
    ).length,
  };
  console.log(`[instantly-service] mailboxes-sync: ${JSON.stringify(summary)}`);
  return summary;
}
