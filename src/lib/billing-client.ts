/**
 * HTTP client for billing-service credit authorization.
 *
 * Before any paid platform operation, call authorizeCreditSpend().
 * If the balance is insufficient billing-service will attempt a Stripe
 * auto-reload within the same request before responding.
 *
 * Only required when costSource === "platform".
 * BYOK (costSource === "org") operations skip authorization entirely.
 *
 * Send costName + quantity — billing-service resolves the price internally.
 */

const BILLING_SERVICE_URL = process.env.BILLING_SERVICE_URL || "http://localhost:3020";
const BILLING_SERVICE_API_KEY = process.env.BILLING_SERVICE_API_KEY || "";

// ─── Types ──────────────────────────────────────────────────────────────────

export interface CostItem {
  costName: string;
  quantity: number;
}

export interface BillingIdentity {
  orgId: string;
  userId: string;
  runId: string;
  campaignId?: string;
  brandId?: string;
  workflowName?: string;
}

export interface AuthorizeResult {
  sufficient: boolean;
  balance_cents: number;
  required_cents: number;
}

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Request credit authorization from billing-service.
 *
 * Send the same costName + quantity used when recording costs via runs-service.
 * billing-service resolves unit prices internally.
 *
 * @returns { sufficient, balance_cents, required_cents }
 * @throws on network / non-JSON errors
 */
export async function authorizeCreditSpend(
  items: CostItem[],
  description: string,
  identity: BillingIdentity,
): Promise<AuthorizeResult> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-API-Key": BILLING_SERVICE_API_KEY,
    "x-org-id": identity.orgId,
    "x-user-id": identity.userId,
    "x-run-id": identity.runId,
  };
  if (identity.campaignId) headers["x-campaign-id"] = identity.campaignId;
  if (identity.brandId) headers["x-brand-id"] = identity.brandId;
  if (identity.workflowName) headers["x-workflow-name"] = identity.workflowName;

  const response = await fetch(`${BILLING_SERVICE_URL}/v1/credits/authorize`, {
    method: "POST",
    headers,
    body: JSON.stringify({ items, description }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `billing-service POST /v1/credits/authorize failed: ${response.status} - ${errorText}`,
    );
  }

  return response.json() as Promise<AuthorizeResult>;
}
