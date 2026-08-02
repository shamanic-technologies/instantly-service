/**
 * Mailforge (Salesforge) client — domains + mailboxes.
 *
 * ⚠️ The auth header is `Authorization: <key>` RAW — no `Bearer`, and NOT
 * `X-Mailforge-Key` (that form 401s on the REST host; it is the MCP gateway's
 * header). The skill doc claimed the REST host rejects the key outright and
 * that the MCP gateway was the only programmatic surface — that is wrong, and
 * it mattered: the REST host is the ONLY place `priceCents` is exposed.
 * Verified live 2026-08-02.
 *
 * Pagination is `limit` + `offset`, capped at 100 per page.
 */

import {
  parseProviderDate,
  type ProviderDomain,
  type ProviderInventory,
  type ProviderMailbox,
} from "./types";

const MAILFORGE_API_URL = "https://api.mailforge.ai/public";
const PAGE_SIZE = 100;
const USER_AGENT = "Mozilla/5.0 (compatible; instantly-service/1.0)";

export class MailforgeApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "MailforgeApiError";
  }
}

async function mailforgeGet<T>(path: string, key: string): Promise<T> {
  const response = await fetch(`${MAILFORGE_API_URL}${path}`, {
    headers: { Authorization: key, "User-Agent": USER_AGENT },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new MailforgeApiError(
      response.status,
      `Mailforge GET ${path} failed: ${response.status} - ${body.slice(0, 300)}`,
    );
  }

  return response.json() as Promise<T>;
}

/** The REST host returns a bare array on some routes and `{results}` on others. */
function unwrap<T>(body: unknown): T[] {
  if (Array.isArray(body)) return body as T[];
  if (body && typeof body === "object" && Array.isArray((body as { results?: unknown }).results)) {
    return (body as { results: T[] }).results;
  }
  return [];
}

// ─── Raw vendor shapes ────────────────────────────────────────────────────────

export interface MailforgeRawDomain {
  id?: string;
  workspaceId?: string;
  sld: string;
  tld: string;
  status?: string;
  priceCents?: number;
  expiresAt?: string;
  createdAt?: string;
  autoRenewStatus?: string;
}

export interface MailforgeRawMailbox {
  id?: string;
  workspaceId?: string;
  email: string;
  domain: string;
  status?: string;
  createdAt?: string;
}

// ─── Normalisers (pure) ───────────────────────────────────────────────────────

export function normalizeMailforgeDomain(raw: MailforgeRawDomain): ProviderDomain {
  return {
    provider: "mailforge",
    providerAccount: raw.workspaceId ?? null,
    externalId: raw.id ?? null,
    domain: `${raw.sld}.${raw.tld}`.toLowerCase(),
    // Mailforge sells the domain AND hosts the mailboxes on its own relay.
    role: "mailbox",
    status: raw.status ?? null,
    createdAtProvider: parseProviderDate(raw.createdAt),
    expiresAt: parseProviderDate(raw.expiresAt),
    // `autoRenewStatus` is a free-text vendor field and ships EMPTY in practice.
    // Empty is unknown, not "off" — report null rather than a fabricated false.
    autorenew: raw.autoRenewStatus ? raw.autoRenewStatus === "enabled" : null,
    deletionScheduled: false,
    cancelledAt: null,
    priceCents: typeof raw.priceCents === "number" ? raw.priceCents : null,
    priceCurrency: typeof raw.priceCents === "number" ? "USD" : null,
    payload: raw,
  };
}

export function normalizeMailforgeMailbox(raw: MailforgeRawMailbox): ProviderMailbox {
  return {
    provider: "mailforge",
    providerAccount: raw.workspaceId ?? null,
    externalId: raw.id ?? null,
    email: raw.email.toLowerCase(),
    domain: raw.domain.toLowerCase(),
    status: raw.status ?? null,
    createdAtProvider: parseProviderDate(raw.createdAt),
    payload: raw,
  };
}

// ─── Paginated readers ────────────────────────────────────────────────────────

async function paginate<T>(path: string, key: string): Promise<T[]> {
  const out: T[] = [];

  for (let offset = 0; ; offset += PAGE_SIZE) {
    const separator = path.includes("?") ? "&" : "?";
    const page = unwrap<T>(
      await mailforgeGet<unknown>(`${path}${separator}limit=${PAGE_SIZE}&offset=${offset}`, key),
    );
    out.push(...page);
    if (page.length < PAGE_SIZE) break;
  }

  return out;
}

export async function listMailforgeDomains(key: string): Promise<ProviderDomain[]> {
  const raw = await paginate<MailforgeRawDomain>("/domains", key);
  return raw.map(normalizeMailforgeDomain);
}

export async function listMailforgeMailboxes(key: string): Promise<ProviderMailbox[]> {
  const raw = await paginate<MailforgeRawMailbox>("/mailboxes", key);
  return raw.map(normalizeMailforgeMailbox);
}

export async function fetchMailforgeInventory(key: string): Promise<ProviderInventory> {
  return {
    domains: await listMailforgeDomains(key),
    mailboxes: await listMailforgeMailboxes(key),
    accountScopes: [],
  };
}
