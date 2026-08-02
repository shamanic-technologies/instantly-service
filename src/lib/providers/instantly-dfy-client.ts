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

export async function fetchInstantlyDfyInventory(
  apiKey: string,
): Promise<ProviderInventory> {
  const orders = await listDfyOrders(apiKey);
  const workspace = await getCurrentWorkspace(apiKey);

  return {
    domains: orders.map(normalizeDfyOrder),
    mailboxes: [],
    accountScopes: [{ scope: "workspace:current", payload: workspace }],
  };
}
