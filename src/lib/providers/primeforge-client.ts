/**
 * Primeforge client — Google Workspace domains + mailboxes.
 *
 * ⚠️ Auth is `Authorization: <key>` RAW (no `Bearer`); `X-Primeforge-Key` is the
 * MCP gateway's header and 401s here. The Mailforge key 401s too — they are
 * distinct credentials.
 *
 * ⚠️ `limit` is capped at 100: `limit=200` returns an EMPTY list rather than an
 * error (same trap as Instantly's own pagination). Walk with `offset`.
 *
 * Primeforge exposes NO billing surface at all — `/subscriptions`, `/billing`,
 * `/invoices`, `/plans`, `/prices`, `/orders` and `/usage` every one 404s
 * (probed live 2026-08-02). Its cost is therefore a versioned rate card in
 * `infra_price_rates`, and until that rate is supplied the spend read reports
 * null. Do NOT reintroduce a price guess here.
 */

import {
  parseProviderDate,
  type ProviderDomain,
  type ProviderInventory,
  type ProviderMailbox,
} from "./types";

const PRIMEFORGE_API_URL = "https://api.primeforge.ai/public";
const PAGE_SIZE = 100;
const USER_AGENT = "Mozilla/5.0 (compatible; instantly-service/1.0)";

export class PrimeforgeApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "PrimeforgeApiError";
  }
}

interface PrimeforgePage<T> {
  pagination?: { offset: number; limit: number };
  results?: T[];
}

async function primeforgeGet<T>(path: string, key: string): Promise<PrimeforgePage<T>> {
  const response = await fetch(`${PRIMEFORGE_API_URL}${path}`, {
    headers: { Authorization: key, "User-Agent": USER_AGENT },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new PrimeforgeApiError(
      response.status,
      `Primeforge GET ${path} failed: ${response.status} - ${body.slice(0, 300)}`,
    );
  }

  return response.json() as Promise<PrimeforgePage<T>>;
}

// ─── Raw vendor shapes ────────────────────────────────────────────────────────

export interface PrimeforgeRawDomain {
  id?: string;
  workspaceId?: string;
  sld: string;
  tld: string;
  status?: string;
  platform?: string;
  deletionScheduled?: boolean;
  expiresAt?: string;
  createdAt?: string;
}

export interface PrimeforgeRawMailbox {
  id?: string;
  workspaceId?: string;
  /** Some rows carry `address`, others `username` + `domain`. */
  address?: string;
  username?: string;
  domain?: string;
  domainId?: string;
  status?: string;
  createdAt?: string;
}

// ─── Normalisers (pure) ───────────────────────────────────────────────────────

/**
 * Returns null for a row with no domain name yet — Primeforge keeps a `pending`
 * placeholder with empty `sld`/`tld` while a purchase is in flight. Skipping it
 * is honest; storing `"."` would create a phantom domain in the inventory.
 */
export function normalizePrimeforgeDomain(
  raw: PrimeforgeRawDomain,
): ProviderDomain | null {
  if (!raw.sld || !raw.tld) return null;

  return {
    provider: "primeforge",
    providerAccount: raw.workspaceId ?? null,
    externalId: raw.id ?? null,
    domain: `${raw.sld}.${raw.tld}`.toLowerCase(),
    role: "mailbox",
    status: raw.status ?? null,
    createdAtProvider: parseProviderDate(raw.createdAt),
    expiresAt: parseProviderDate(raw.expiresAt),
    autorenew: null,
    deletionScheduled: raw.deletionScheduled === true,
    cancelledAt: null,
    priceCents: null,
    priceCurrency: null,
    payload: raw,
  };
}

/** Returns null when the row carries no resolvable address (nothing to key on). */
export function normalizePrimeforgeMailbox(
  raw: PrimeforgeRawMailbox,
  domainById: Map<string, string>,
): ProviderMailbox | null {
  const domain =
    raw.domain?.toLowerCase() ??
    (raw.domainId ? domainById.get(raw.domainId) : undefined) ??
    raw.address?.split("@")[1]?.toLowerCase();

  const email = raw.address?.toLowerCase() ?? (raw.username && domain ? `${raw.username}@${domain}` : undefined);

  if (!email || !domain) return null;

  return {
    provider: "primeforge",
    providerAccount: raw.workspaceId ?? null,
    externalId: raw.id ?? null,
    email,
    domain,
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
    const body = await primeforgeGet<T>(`${path}${separator}limit=${PAGE_SIZE}&offset=${offset}`, key);
    const page = body.results ?? [];
    out.push(...page);
    if (page.length < PAGE_SIZE) break;
  }

  return out;
}

export async function listPrimeforgeRawDomains(key: string): Promise<PrimeforgeRawDomain[]> {
  return paginate<PrimeforgeRawDomain>("/domains", key);
}

export async function fetchPrimeforgeInventory(key: string): Promise<ProviderInventory> {
  const rawDomains = await listPrimeforgeRawDomains(key);
  const domains = rawDomains
    .map(normalizePrimeforgeDomain)
    .filter((d): d is ProviderDomain => d !== null);

  const domainById = new Map<string, string>();
  for (const raw of rawDomains) {
    if (raw.id && raw.sld && raw.tld) domainById.set(raw.id, `${raw.sld}.${raw.tld}`.toLowerCase());
  }

  const rawMailboxes = await paginate<PrimeforgeRawMailbox>("/mailboxes", key);
  const mailboxes = rawMailboxes
    .map((raw) => normalizePrimeforgeMailbox(raw, domainById))
    .filter((m): m is ProviderMailbox => m !== null);

  const workspaces = await paginate<Record<string, unknown>>("/workspaces", key);

  return {
    domains,
    mailboxes,
    accountScopes: workspaces.map((w) => ({ scope: `workspace:${String(w.id)}`, payload: w })),
  };
}
