/**
 * Instantly DFY adapter — turns pre-warmed domain ORDERS into inventory rows.
 *
 * Instantly is both a sending platform and, through DFY, a domain+mailbox
 * vendor. Only the orders endpoint knows which domains came pre-warmed, so this
 * is what closes the "provisioning class is not exposed" gap the repo docs
 * recorded as permanent: it is not on the ACCOUNT object, but it is on the
 * ORDER. A cancelled order keeps its row with `cancelledAt` set — the domain is
 * deprovisioned and must show up in the waste read, not vanish.
 *
 * Mailboxes are deliberately NOT emitted here: DFY mailboxes are the Instantly
 * accounts this service already mirrors in `instantly_accounts`, and duplicating
 * them into `infra_mailboxes` would double-count the fleet.
 *
 * ONE DOMAIN CAN CARRY SEVERAL ORDER ROWS, so the rows are collapsed before
 * they are emitted. Instantly returns a single row per untouched domain and
 * SPLITS it the moment one of its mailboxes is cancelled individually — so a
 * partially-cancelled domain reports one active row plus one cancelled row.
 * `infra_domains` is keyed on `(provider, domain)`, meaning an un-collapsed
 * emission upserts the rows one after another and the LAST one wins: whichever
 * order the vendor happened to return decides whether a live domain reads as
 * cancelled. That is not a cosmetic ordering bug — `monthlyCostForDomain` and
 * `splitDomainCost` both return nothing for a cancelled domain and the waste
 * read flags it `cancelled_by_vendor` ("deprovisioned, not reusable"), so a
 * domain still carrying production mailboxes would vanish from spend and be
 * reported as safe to delete. A domain is cancelled only when EVERY one of its
 * rows is.
 */

import { getCurrentWorkspace, listDfyOrders, type DfyOrder } from "../instantly-client";
import { parseProviderDate, type ProviderDomain, type ProviderInventory } from "./types";

export function normalizeDfyOrder(raw: DfyOrder): ProviderDomain {
  const cancelledAt = parseProviderDate(raw.timestamp_cancelled);

  return {
    provider: "instantly-dfy",
    providerAccount: raw.workspace_id ?? null,
    externalId: null,
    domain: raw.domain.toLowerCase(),
    role: "prewarm",
    status: cancelledAt ? "cancelled" : "active",
    createdAtProvider: parseProviderDate(raw.timestamp_created),
    // The order carries no renewal date; DFY billing is a monthly per-account
    // rate plus a yearly domain fee, both held in the rate card.
    expiresAt: null,
    autorenew: null,
    deletionScheduled: false,
    cancelledAt,
    priceCents: null,
    priceCurrency: null,
    payload: raw,
  };
}

/**
 * Collapse the order rows of one domain into the single inventory row
 * `infra_domains` stores for it.
 *
 * The domain is cancelled only when EVERY row is, and it then carries the
 * LATEST cancellation — the moment we actually stopped paying for any of it.
 * One surviving active row keeps the whole domain active, which is the
 * conservative direction: reporting a live domain as dead removes it from
 * spend and offers it up for deletion, while reporting a dead one as live
 * merely overstates cost until the next sweep.
 *
 * `createdAtProvider` takes the EARLIEST row — a domain was ordered once, and
 * the extra rows are splits of that order, not later purchases. The payload
 * keeps every row so bronze loses nothing.
 */
export function collapseDfyOrders(orders: DfyOrder[]): ProviderDomain[] {
  const byDomain = new Map<string, ProviderDomain[]>();

  for (const order of orders) {
    const row = normalizeDfyOrder(order);
    const existing = byDomain.get(row.domain);
    if (existing) existing.push(row);
    else byDomain.set(row.domain, [row]);
  }

  const collapsed: ProviderDomain[] = [];

  for (const rows of byDomain.values()) {
    const allCancelled = rows.every((row) => row.cancelledAt !== null);
    const cancelledAt = allCancelled
      ? rows.reduce<Date | null>(
          (latest, row) =>
            latest === null || (row.cancelledAt as Date) > latest
              ? (row.cancelledAt as Date)
              : latest,
          null,
        )
      : null;

    const createdAtProvider = rows.reduce<Date | null>((earliest, row) => {
      if (row.createdAtProvider === null) return earliest;
      if (earliest === null || row.createdAtProvider < earliest) return row.createdAtProvider;
      return earliest;
    }, null);

    collapsed.push({
      ...rows[0],
      status: cancelledAt ? "cancelled" : "active",
      createdAtProvider,
      cancelledAt,
      payload: rows.length === 1 ? rows[0].payload : rows.map((row) => row.payload),
    });
  }

  return collapsed;
}

export async function fetchInstantlyDfyInventory(
  apiKey: string,
): Promise<ProviderInventory> {
  const orders = await listDfyOrders(apiKey);
  const workspace = await getCurrentWorkspace(apiKey);

  return {
    domains: collapseDfyOrders(orders),
    mailboxes: [],
    accountScopes: [{ scope: "workspace:current", payload: workspace }],
  };
}
