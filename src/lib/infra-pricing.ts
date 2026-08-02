/**
 * Pure pricing + waste logic over the provider inventory (issue #555, PR 2).
 *
 * The one invariant that governs this whole file: a figure we cannot source is
 * NULL, never a substitute. Primeforge exposes no billing surface at all, so its
 * domains carry no cost until a human supplies the rate — and a plausible-looking
 * placeholder would silently become the fleet's cost-per-email denominator, which
 * is worse than an empty column. Every amount therefore travels with its
 * provenance (`api` from the vendor, `rate-card` from a versioned row).
 */

/** A rate-card row, already filtered to the effective one for its key. */
export interface PriceRate {
  provider: string;
  scope: "domain-year" | "mailbox-month" | "plan-month";
  item: string;
  unitCents: number;
  currency: string;
  source: "api" | "rate-card";
  note: string | null;
}

/** One (provider, domain) silver row joined with what we know about it. */
export interface InventoryDomain {
  provider: string;
  domain: string;
  role: string;
  status: string | null;
  expiresAt: Date | null;
  autorenew: boolean | null;
  deletionScheduled: boolean;
  cancelledAt: Date | null;
  absentSince: Date | null;
  /** Vendor-reported price for this specific domain, when it reports one. */
  priceCents: number | null;
  priceCurrency: string | null;
  /** Mailboxes the VENDOR reports on this domain. */
  mailboxCount: number;
  /** Live (non-absent) Instantly sending accounts on this domain. */
  instantlyAccountCount: number;
  /** Of those, how many are in the live-send pool. */
  inProductionCount: number;
  /** Real dispatches from this domain's accounts over the trailing 30 days. */
  sentLast30d: number;
}

export interface MoneyAmount {
  cents: number;
  currency: string;
  source: "api" | "rate-card";
}

/** Index the rate card for lookup. Later `effective_from` rows must win, so callers pass them ordered. */
export function indexRates(rates: PriceRate[]): Map<string, PriceRate> {
  const byKey = new Map<string, PriceRate>();
  for (const rate of rates) {
    byKey.set(`${rate.provider}|${rate.scope}|${rate.item}`, rate);
  }
  return byKey;
}

/**
 * Monthly cost of one (provider, domain) row.
 *
 * A vendor-reported per-domain price is a YEARLY registration/renewal figure —
 * that is what both Mailforge and Gandi quote — so it is divided by 12. Mailbox
 * rates are already monthly and are multiplied by the mailboxes the vendor
 * reports, not by the Instantly accounts: we pay the vendor for what IT hosts,
 * and the two counts genuinely differ (the legacy Gandi domains run dozens of
 * Instantly accounts against a single Gandi mailbox).
 *
 * Returns null when nothing prices this row.
 */
export function monthlyCostForDomain(
  domain: InventoryDomain,
  rates: Map<string, PriceRate>,
): MoneyAmount | null {
  // A cancelled or deprovisioned domain still shows up in the inventory, but we
  // have stopped paying for it — reporting its old rate would inflate spend.
  if (domain.cancelledAt) return null;

  const parts: MoneyAmount[] = [];

  if (domain.priceCents !== null && domain.priceCurrency) {
    parts.push({
      cents: domain.priceCents / 12,
      currency: domain.priceCurrency,
      source: "api",
    });
  } else {
    const yearly = rates.get(`${domain.provider}|domain-year|`);
    if (yearly) {
      parts.push({ cents: yearly.unitCents / 12, currency: yearly.currency, source: yearly.source });
    }
  }

  const perMailbox = rates.get(`${domain.provider}|mailbox-month|`);
  if (perMailbox && domain.mailboxCount > 0) {
    parts.push({
      cents: perMailbox.unitCents * domain.mailboxCount,
      currency: perMailbox.currency,
      source: perMailbox.source,
    });
  }

  if (parts.length === 0) return null;

  // Mixing currencies inside ONE domain would need an FX rate we may not have,
  // and no vendor prices a single domain in two currencies — so if it ever
  // happens, report nothing rather than a silently wrong sum.
  const currencies = new Set(parts.map((p) => p.currency));
  if (currencies.size > 1) return null;

  return {
    cents: Math.round(parts.reduce((sum, p) => sum + p.cents, 0)),
    currency: parts[0].currency,
    // If any component came from the rate card, the total is only as good as it.
    source: parts.some((p) => p.source === "rate-card") ? "rate-card" : "api",
  };
}

/** Cost per email actually sent, over the same trailing window. Null when unpriced or silent. */
export function costPerEmailCents(
  monthly: MoneyAmount | null,
  sentLast30d: number,
): { cents: number; currency: string } | null {
  if (!monthly || sentLast30d <= 0) return null;
  return { cents: monthly.cents / sentLast30d, currency: monthly.currency };
}

export type WasteReason =
  | "paid_no_sending_accounts"
  | "cancelled_by_vendor"
  | "deletion_scheduled"
  | "expiring_within_30d";

export interface WasteFinding {
  domain: string;
  provider: string;
  reason: WasteReason;
  detail: string;
  monthlyCostCents: number | null;
  currency: string | null;
  expiresAt: Date | null;
}

/**
 * Report-only. This never cancels an autorenew or schedules a deletion: the
 * blast radius of a false positive is losing a domain, and three of the flagged
 * ones are brand domains we hold deliberately. A human decides.
 */
export function classifyWaste(
  domains: InventoryDomain[],
  rates: Map<string, PriceRate>,
  asOf: Date,
): WasteFinding[] {
  const findings: WasteFinding[] = [];
  const soon = new Date(asOf.getTime() + 30 * 24 * 60 * 60 * 1000);

  for (const domain of domains) {
    // A row the vendor stopped reporting is already gone; it is not ongoing waste.
    if (domain.absentSince) continue;

    const monthly = monthlyCostForDomain(domain, rates);
    const base = {
      domain: domain.domain,
      provider: domain.provider,
      monthlyCostCents: monthly?.cents ?? null,
      currency: monthly?.currency ?? null,
      expiresAt: domain.expiresAt,
    };

    if (domain.cancelledAt) {
      findings.push({
        ...base,
        reason: "cancelled_by_vendor",
        detail: `Order cancelled ${domain.cancelledAt.toISOString().slice(0, 10)}; the mailboxes are deprovisioned and the domain is not reusable.`,
      });
      continue;
    }

    if (domain.deletionScheduled) {
      findings.push({
        ...base,
        reason: "deletion_scheduled",
        detail: "The vendor has this domain scheduled for deletion.",
      });
      continue;
    }

    if (domain.instantlyAccountCount === 0) {
      findings.push({
        ...base,
        reason: "paid_no_sending_accounts",
        detail: "Owned and billed, with no Instantly sending account on it.",
      });
    }

    if (domain.expiresAt && domain.expiresAt <= soon && domain.expiresAt > asOf) {
      findings.push({
        ...base,
        reason: "expiring_within_30d",
        detail: `Expires ${domain.expiresAt.toISOString().slice(0, 10)}${
          domain.autorenew === false ? " with autorenew OFF" : ""
        }.`,
      });
    }
  }

  return findings;
}

export interface SpendByProvider {
  provider: string;
  domainCount: number;
  mailboxCount: number;
  monthlyCents: number;
  currency: string;
  source: "api" | "rate-card" | "mixed";
}

export interface SpendSummary {
  byProvider: SpendByProvider[];
  /** Monthly totals per currency. No conversion is attempted here. */
  monthlyByCurrency: { currency: string; cents: number }[];
  /** Vendors whose domains we hold but cannot price at all. */
  unpricedProviders: string[];
  /** Domains carrying no cost, across all vendors. */
  unpricedDomainCount: number;
}

/**
 * Totals stay PER CURRENCY. Gandi bills in EUR and everyone else in USD, and a
 * single blended number would need an FX rate — which would then be a fourth
 * unsourced figure on a page whose whole point is that every number says where
 * it came from. The caller converts if it wants to, with a rate it owns.
 */
export function summarizeSpend(
  domains: InventoryDomain[],
  rates: Map<string, PriceRate>,
): SpendSummary {
  const perProvider = new Map<string, { domains: number; mailboxes: number; cents: number; currency: string; sources: Set<string> }>();
  const unpriced = new Set<string>();
  let unpricedDomainCount = 0;

  for (const domain of domains) {
    if (domain.absentSince) continue;

    const monthly = monthlyCostForDomain(domain, rates);
    if (!monthly) {
      // A cancelled domain is legitimately unpriced (we stopped paying), so it
      // is not evidence that the vendor is unpriceable.
      if (!domain.cancelledAt) {
        unpriced.add(domain.provider);
        unpricedDomainCount += 1;
      }
      continue;
    }

    const key = `${domain.provider}|${monthly.currency}`;
    const entry = perProvider.get(key) ?? {
      domains: 0,
      mailboxes: 0,
      cents: 0,
      currency: monthly.currency,
      sources: new Set<string>(),
    };
    entry.domains += 1;
    entry.mailboxes += domain.mailboxCount;
    entry.cents += monthly.cents;
    entry.sources.add(monthly.source);
    perProvider.set(key, entry);
  }

  const byProvider: SpendByProvider[] = [...perProvider.entries()]
    .map(([key, entry]) => ({
      provider: key.split("|")[0],
      domainCount: entry.domains,
      mailboxCount: entry.mailboxes,
      monthlyCents: entry.cents,
      currency: entry.currency,
      source: (entry.sources.size > 1 ? "mixed" : [...entry.sources][0]) as "api" | "rate-card" | "mixed",
    }))
    .sort((a, b) => b.monthlyCents - a.monthlyCents);

  const perCurrency = new Map<string, number>();
  for (const row of byProvider) {
    perCurrency.set(row.currency, (perCurrency.get(row.currency) ?? 0) + row.monthlyCents);
  }

  return {
    byProvider,
    monthlyByCurrency: [...perCurrency.entries()]
      .map(([currency, cents]) => ({ currency, cents }))
      .sort((a, b) => b.cents - a.cents),
    unpricedProviders: [...unpriced].sort(),
    unpricedDomainCount,
  };
}

/**
 * Plan-level subscriptions — what the Instantly workspace costs regardless of
 * how many domains sit in it. Kept apart from per-domain spend so the two are
 * never double-counted into one "infrastructure" figure.
 */
export function summarizePlanSpend(rates: PriceRate[]): { item: string; monthlyCents: number; currency: string; note: string | null }[] {
  return rates
    .filter((rate) => rate.scope === "plan-month")
    .map((rate) => ({
      item: rate.item,
      monthlyCents: rate.unitCents,
      currency: rate.currency,
      note: rate.note,
    }))
    .sort((a, b) => b.monthlyCents - a.monthlyCents);
}
