/**
 * Provider-infrastructure routes (platform-scoped, no org) — issue #555.
 *
 * Mounted at `/internal/infra` behind `serviceAuth`, the same tier as
 * `/internal/audit`. PR 1 exposes the ingestion trigger only; the gold reads
 * (`/domains`, `/waste`, `/spend`) land in PR 2 once a first sync has shown what
 * the vendors actually return.
 */

import { Router, Request, Response } from "express";
import { syncProviderInfra } from "../lib/infra-sync";
import { syncMailboxes } from "../lib/mailboxes-sync";
import { syncDomainDns } from "../lib/domain-dns-sync";
import { describeFx, loadLatestEurUsd, syncFxRates, toUsdCents } from "../lib/fx-rates";
import { loadEffectiveRates, loadInventoryDomains } from "../lib/infra-gold";
import {
  classifyWaste,
  costPerEmailCents,
  indexRates,
  monthlyCostForDomain,
  splitDomainCost,
  summarizePlanSpend,
  summarizeSpend,
  vendorReportsMailboxes,
} from "../lib/infra-pricing";
import { getOrSetCachedStats } from "../lib/stats-cache";

const router = Router();

/**
 * POST /internal/infra/sync
 *
 * Polls Gandi (three organisations), Mailforge, Primeforge and Instantly DFY
 * into bronze, upserts silver, and flags rows a vendor stopped reporting.
 * Read-only against every vendor and free of metered spend, so it is safe to
 * run daily.
 *
 * 202 + background; watch logs for `infra-sync: done`. A single vendor failing
 * is counted in `failures` and does not stop the others; the run only throws
 * when every vendor failed.
 */
router.post("/sync", async (_req: Request, res: Response) => {
  const runId = crypto.randomUUID();
  res.status(202).json({ accepted: true, runId });
  console.log(`[infra] infra-sync: dispatched run=${runId}`);

  (async () => {
    const summary = await syncProviderInfra({
      method: "POST",
      path: "/internal/infra/sync",
    });
    console.log(`[infra] infra-sync: done run=${runId} ${JSON.stringify(summary)}`);
    // The mailbox grouping reads what this sync just wrote, so it runs AFTER it,
    // awaited — not as a sibling cron that would race the writes it depends on.
    const mailboxSummary = await syncMailboxes({ method: "POST", path: "/internal/infra/sync" });
    console.log(`[infra] mailboxes-sync: done run=${runId} ${JSON.stringify(mailboxSummary)}`);
    // The DNS photograph reads the domain list this sync just refreshed.
    const dnsSummary = await syncDomainDns();
    console.log(`[infra] dns-sync: done run=${runId} ${JSON.stringify(dnsSummary)}`);
    // Last, so an ECB outage costs only today's rate, never the inventory.
    const fxSummary = await syncFxRates();
    console.log(`[infra] fx-sync: done run=${runId} ${JSON.stringify(fxSummary)}`);
  })().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[infra] infra-sync run=${runId} failed: ${message}`);
  });
});

/**
 * POST /internal/infra/fx-sync
 *
 * Fetch today's ECB EUR → USD reference rate and record it. Synchronous (one
 * small request) so a hand-run right after a deploy answers with the rate it
 * stored. Idempotent per reference day. Also runs at the end of every
 * `/internal/infra/sync`.
 */
router.post("/fx-sync", async (_req: Request, res: Response) => {
  try {
    res.json(await syncFxRates());
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[infra] fx-sync failed: ${message}`);
    res.status(502).json({ error: "Failed to fetch the ECB reference rate", detail: message });
  }
});

/**
 * GET /internal/infra/domains
 *
 * One row per (provider, domain): who we buy it from, when it expires, how many
 * mailboxes the VENDOR hosts on it, how many Instantly accounts actually send
 * from it, and what it costs per month and per email.
 *
 * The mailbox count and the account count are deliberately both shown and are
 * routinely different — the legacy relayed domains run dozens of Instantly
 * accounts against a single vendor mailbox. That gap is a finding, not a bug.
 *
 * Every amount carries `source`; a domain nothing prices reports null rather
 * than a substitute figure.
 */
router.get("/domains", async (_req: Request, res: Response) => {
  try {
    const payload = await getOrSetCachedStats("infra-domains", async () => {
      const asOf = new Date();
      const [domains, rates, fx] = await Promise.all([
        loadInventoryDomains(),
        loadEffectiveRates(asOf),
        loadLatestEurUsd(),
      ]);
      const indexed = indexRates(rates);

      return {
        asOf: asOf.toISOString(),
        // Every `usd` figure below was converted at this rate. Null = no rate on
        // record, so every `usd` figure is null too — never a guessed one.
        fx: describeFx(fx),
        domains: domains.map((domain) => {
          const monthly = monthlyCostForDomain(domain, indexed);
          const perEmail = costPerEmailCents(monthly, domain.sentLast30d);
          const split = splitDomainCost(domain, indexed);
          const monthlyUsd = toUsdCents(monthly?.cents ?? null, monthly?.currency ?? null, fx);

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
            vendorMailboxes: domain.mailboxCount,
            vendorReportsMailboxes: vendorReportsMailboxes(domain.provider),
            instantlyAccounts: domain.instantlyAccountCount,
            inProductionAccounts: domain.inProductionCount,
            sentLast30d: domain.sentLast30d,
            monthlyCostCents: monthly?.cents ?? null,
            currency: monthly?.currency ?? null,
            costSource: monthly?.source ?? null,
            costPerEmailCents: perEmail ? Number(perEmail.cents.toFixed(4)) : null,
            // The same money split by WHEN cancelling actually saves it: the
            // mailbox subscription stops immediately, the registration is
            // already paid until `renewalAt` and is only avoided then.
            recurringMonthlyCents: split.recurringMonthlyCents,
            renewalCents: split.renewalCents,
            renewalAt: split.renewalAt?.toISOString() ?? null,
            // The same amounts in USD at `fx`, so a consumer can state ONE total.
            usd: {
              monthlyCostCents: monthlyUsd,
              // From the USD monthly, not a rounded per-email figure: a per-email
              // cost is fractional cents and rounding it would erase it.
              costPerEmailCents:
                monthlyUsd !== null && domain.sentLast30d > 0
                  ? Number((monthlyUsd / domain.sentLast30d).toFixed(4))
                  : null,
              recurringMonthlyCents: toUsdCents(split.recurringMonthlyCents, split.currency, fx),
              renewalCents: toUsdCents(split.renewalCents, split.currency, fx),
            },
          };
        }),
      };
    });

    res.json(payload);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[infra] domains failed: ${message}`);
    res.status(500).json({ error: "Failed to load the provider inventory" });
  }
});

/**
 * GET /internal/infra/waste
 *
 * Domains we are billed for that nothing is using, plus the ones a vendor has
 * cancelled or scheduled for deletion, plus anything expiring inside 30 days.
 *
 * REPORT-ONLY, deliberately. It never cancels an autorenew and never schedules
 * a deletion: a false positive costs us a domain, and several of the flagged
 * ones are brand domains we hold on purpose. A human decides.
 */
router.get("/waste", async (_req: Request, res: Response) => {
  try {
    const payload = await getOrSetCachedStats("infra-waste", async () => {
      const asOf = new Date();
      const [domains, rates] = await Promise.all([
        loadInventoryDomains(),
        loadEffectiveRates(asOf),
      ]);

      const findings = classifyWaste(domains, indexRates(rates), asOf);

      return {
        asOf: asOf.toISOString(),
        findingCount: findings.length,
        findings: findings.map((finding) => ({
          ...finding,
          expiresAt: finding.expiresAt?.toISOString() ?? null,
        })),
      };
    });

    res.json(payload);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[infra] waste failed: ${message}`);
    res.status(500).json({ error: "Failed to classify infrastructure waste" });
  }
});

/**
 * GET /internal/infra/spend
 *
 * Monthly run-rate by vendor, plus the workspace subscriptions that are owed
 * regardless of domain count.
 *
 * Totals are kept PER CURRENCY (Gandi bills in EUR, everyone else in USD) and
 * ALSO stated as one USD figure at the ECB reference rate on record (`fx`), so
 * the rate is always named beside the blend. With no rate on record the USD
 * total is null rather than a sum at a guessed rate.
 *
 * `unpricedProviders` is the honest hole: a vendor listed there has domains we
 * hold and no rate anywhere, so its cost is missing from the totals rather than
 * estimated into them.
 */
router.get("/spend", async (_req: Request, res: Response) => {
  try {
    const payload = await getOrSetCachedStats("infra-spend", async () => {
      const asOf = new Date();
      const [domains, rates, fx] = await Promise.all([
        loadInventoryDomains(),
        loadEffectiveRates(asOf),
        loadLatestEurUsd(),
      ]);

      const indexed = indexRates(rates);
      const summary = summarizeSpend(domains, indexed);
      // Converted PER DOMAIN and then summed — the exact figures the domain reads
      // serve as `usd` — rather than converting each currency's total once.
      // Rounding per row vs once per total differs by a few cents, and the page
      // sums the rows, so this total must be built the same way to agree with it.
      const usdParts = domains
        .map((domain) => monthlyCostForDomain(domain, indexed))
        .filter((m): m is NonNullable<typeof m> => m !== null)
        .map((m) => toUsdCents(m.cents, m.currency, fx));
      const monthlyTotalUsdCents = usdParts.some((c) => c === null)
        ? null
        : usdParts.reduce<number>((sum, c) => sum + (c ?? 0), 0);

      // Sends are counted per DOMAIN, not per inventory row. A domain reported
      // by two vendors (the registrar and the mail host) has two rows carrying
      // the same send count, and summing the rows would double the denominator
      // and halve every cost-per-email on the page.
      const sentByDomain = new Map<string, number>();
      for (const domain of domains) {
        if (domain.absentSince) continue;
        sentByDomain.set(domain.domain, domain.sentLast30d);
      }
      const totalSent = [...sentByDomain.values()].reduce((sum, n) => sum + n, 0);

      return {
        asOf: asOf.toISOString(),
        ...summary,
        fx: describeFx(fx),
        monthlyTotalUsdCents,
        planSubscriptions: summarizePlanSpend(rates),
        sentLast30d: totalSent,
        // Cost per email per currency, over the same trailing window. Only the
        // priced share of the fleet is in the numerator, so this is a FLOOR
        // while `unpricedProviders` is non-empty — stated, not hidden.
        costPerEmailByCurrency:
          totalSent > 0
            ? summary.monthlyByCurrency.map(({ currency, cents }) => ({
                currency,
                cents: Number((cents / totalSent).toFixed(4)),
              }))
            : [],
      };
    });

    res.json(payload);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[infra] spend failed: ${message}`);
    res.status(500).json({ error: "Failed to summarise infrastructure spend" });
  }
});

export default router;
