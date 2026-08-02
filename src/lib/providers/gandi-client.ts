/**
 * Gandi API v5 client — domains (registrar) + mailboxes (mail host).
 *
 * Gandi issues ONE token per organisation and a token sees ONLY its own
 * organisation's domains, so the caller passes `{ account, token }` per org and
 * the results are unioned. A 403 on an email route therefore means "wrong org
 * token for this domain", not "no mailboxes" — we never sweep a domain with a
 * token that did not list it.
 *
 * Pagination is `page` + `per_page` (max 100 in practice); an empty array ends
 * the walk. Verified live 2026-08-02 against all three org tokens.
 */

import {
  parseProviderDate,
  type ProviderDomain,
  type ProviderInventory,
  type ProviderMailbox,
} from "./types";

const GANDI_API_URL = "https://api.gandi.net/v5";
const PAGE_SIZE = 100;
// Gandi's edge rejects the default Node/urllib agent string on some paths.
const USER_AGENT = "Mozilla/5.0 (compatible; instantly-service/1.0)";

export interface GandiOrgCredential {
  /** Short organisation label carried onto every row (`org1`, `org2`, `org3`). */
  account: string;
  token: string;
}

export class GandiApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "GandiApiError";
  }
}

async function gandiGet<T>(path: string, token: string): Promise<T> {
  const response = await fetch(`${GANDI_API_URL}${path}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      "User-Agent": USER_AGENT,
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new GandiApiError(
      response.status,
      `Gandi GET ${path} failed: ${response.status} - ${body.slice(0, 300)}`,
    );
  }

  return response.json() as Promise<T>;
}

// ─── Raw vendor shapes (only the fields we read) ──────────────────────────────

export interface GandiRawDomain {
  fqdn: string;
  id?: string;
  autorenew?: boolean;
  dates?: { created_at?: string; registry_ends_at?: string };
  status?: string[];
}

export interface GandiRawMailbox {
  id?: string;
  address: string;
  domain: string;
  login?: string;
  mailbox_type?: string;
  expires_at?: string;
}

/** `GET /domain/check?processes=renew` — the renewal quote for one domain. */
export interface GandiRawPriceCheck {
  currency?: string;
  products?: {
    name?: string;
    process?: string;
    prices?: {
      duration_unit?: string;
      min_duration?: number;
      price_after_taxes?: number;
      price_before_taxes?: number;
      discount?: boolean;
    }[];
  }[];
}

// ─── Normalisers (pure) ───────────────────────────────────────────────────────

export function normalizeGandiDomain(
  raw: GandiRawDomain,
  account: string,
): ProviderDomain {
  return {
    provider: "gandi",
    providerAccount: account,
    externalId: raw.id ?? null,
    domain: raw.fqdn.toLowerCase(),
    role: "registrar",
    status: raw.status?.join(",") ?? null,
    createdAtProvider: parseProviderDate(raw.dates?.created_at),
    expiresAt: parseProviderDate(raw.dates?.registry_ends_at),
    autorenew: typeof raw.autorenew === "boolean" ? raw.autorenew : null,
    deletionScheduled: false,
    cancelledAt: null,
    // Filled by the renewal quote below. Per-domain rather than a single vendor
    // rate because Gandi prices by TLD: `.dev` renews at EUR 38.38/yr while a
    // `.com` is a third of that, so one blended "Gandi rate" would be wrong on
    // every row.
    priceCents: null,
    priceCurrency: null,
    payload: raw,
  };
}

/**
 * Pick the ONE-YEAR renewal price out of a check response, tax included.
 *
 * Gandi returns several tiers — a 1-year price and discounted multi-year ones.
 * We take the 1-year, undiscounted tier because that is what an autorenew
 * actually bills; quoting the cheapest multi-year tier would understate the
 * running cost of every domain. Returns null when the response carries no
 * usable price rather than guessing one.
 */
export function extractGandiRenewalPrice(
  raw: GandiRawPriceCheck,
): { priceCents: number; currency: string } | null {
  const currency = raw.currency;
  if (!currency) return null;

  const renewProduct = raw.products?.find((product) => product.process === "renew");
  const oneYear = renewProduct?.prices?.find(
    (price) => price.min_duration === 1 && price.duration_unit === "y",
  );

  const amount = oneYear?.price_after_taxes;
  if (typeof amount !== "number" || !Number.isFinite(amount)) return null;

  return { priceCents: Math.round(amount * 100), currency };
}

/**
 * The yearly renewal quote for one domain.
 *
 * One extra call per domain per sync (~41 today). Gandi has no bulk pricing
 * endpoint, and the alternative — a single hardcoded "Gandi rate" — would be
 * wrong on every row, since the price is per TLD.
 */
export async function fetchGandiRenewalPrice(
  credential: GandiOrgCredential,
  domain: string,
): Promise<{ priceCents: number; currency: string } | null> {
  const raw = await gandiGet<GandiRawPriceCheck>(
    `/domain/check?name=${encodeURIComponent(domain)}&processes=renew`,
    credential.token,
  );
  return extractGandiRenewalPrice(raw);
}

export function normalizeGandiMailbox(
  raw: GandiRawMailbox,
  account: string,
): ProviderMailbox {
  return {
    provider: "gandi",
    providerAccount: account,
    externalId: raw.id ?? null,
    email: raw.address.toLowerCase(),
    domain: raw.domain.toLowerCase(),
    status: raw.mailbox_type ?? null,
    createdAtProvider: null,
    payload: raw,
  };
}

// ─── Paginated readers ────────────────────────────────────────────────────────

/** Every domain the org token can see. Walks `page` until a page comes back empty. */
export async function listGandiDomains(
  credential: GandiOrgCredential,
): Promise<ProviderDomain[]> {
  const out: ProviderDomain[] = [];

  for (let page = 1; ; page += 1) {
    const items = await gandiGet<GandiRawDomain[]>(
      `/domain/domains?per_page=${PAGE_SIZE}&page=${page}`,
      credential.token,
    );
    if (items.length === 0) break;
    for (const item of items) out.push(normalizeGandiDomain(item, credential.account));
    if (items.length < PAGE_SIZE) break;
  }

  return out;
}

/**
 * Mailboxes for one domain. Called only for domains the SAME token listed, so a
 * non-200 is a real error and propagates — a swallowed 403 would silently
 * under-report the fleet's mailboxes.
 */
export async function listGandiMailboxes(
  credential: GandiOrgCredential,
  domain: string,
): Promise<ProviderMailbox[]> {
  const items = await gandiGet<GandiRawMailbox[]>(
    `/email/mailboxes/${domain}`,
    credential.token,
  );
  return items.map((item) => normalizeGandiMailbox(item, credential.account));
}

/**
 * Full Gandi inventory for one organisation: its domains, their renewal prices,
 * and their mailboxes.
 *
 * A price lookup that fails does NOT fail the domain — the inventory (who owns
 * what, when it expires) is the load-bearing half and must survive a pricing
 * hiccup. The domain is kept with a null price, which the spend read already
 * reports honestly as unpriced. A failure on the INVENTORY calls still
 * propagates: a silently short domain list would under-report the estate.
 */
export async function fetchGandiInventory(
  credential: GandiOrgCredential,
): Promise<ProviderInventory> {
  const domains = await listGandiDomains(credential);

  for (const domain of domains) {
    try {
      const price = await fetchGandiRenewalPrice(credential, domain.domain);
      if (price) {
        domain.priceCents = price.priceCents;
        domain.priceCurrency = price.currency;
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn(
        `[instantly-service] gandi: no renewal quote for ${domain.domain} — ${message}`,
      );
    }
  }

  const mailboxes: ProviderMailbox[] = [];
  for (const domain of domains) {
    mailboxes.push(...(await listGandiMailboxes(credential, domain.domain)));
  }

  return { domains, mailboxes, accountScopes: [] };
}
