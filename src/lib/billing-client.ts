/**
 * HTTP client for billing-service credit authorization.
 *
 * Before an org-billed email leaves, `/orgs/send` asks billing-service whether
 * the org can afford it. If the balance is insufficient billing-service attempts
 * a Stripe auto-reload within the same request before answering.
 *
 * Only for platform spend (`costSource === "platform"`); BYOK skips it.
 * Send costName + quantity — billing-service resolves the price itself.
 *
 * Fail loud: a missing key, a non-2xx or a non-JSON answer throws. An org is
 * never sent to on a question billing could not answer.
 */
import type { IdentityContext } from "./runs-client";

const BILLING_SERVICE_URL = process.env.BILLING_SERVICE_URL || "http://localhost:3020";

export interface AuthorizeItem {
  costName: string;
  quantity: number;
}

export interface AuthorizeResult {
  sufficient: boolean;
  balance_cents: number | string;
  required_cents: number | string;
}

export async function authorizeCreditSpend(
  items: AuthorizeItem[],
  description: string,
  identity: IdentityContext,
): Promise<AuthorizeResult> {
  const apiKey = process.env.BILLING_SERVICE_API_KEY;
  if (!apiKey) {
    throw new Error("BILLING_SERVICE_API_KEY is not set: cannot authorize send spend");
  }
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-API-Key": apiKey,
    "x-org-id": identity.orgId,
    "x-user-id": identity.userId,
  };
  if (identity.runId) headers["x-run-id"] = identity.runId;
  const t = identity.tracking;
  if (t?.campaignId) headers["x-campaign-id"] = t.campaignId;
  if (t?.brandId) headers["x-brand-id"] = t.brandId;
  if (t?.workflowSlug) headers["x-workflow-slug"] = t.workflowSlug;
  if (t?.featureSlug) headers["x-feature-slug"] = t.featureSlug;
  if (t?.goal) headers["x-goal"] = t.goal;
  if (t?.brandProfileId) headers["x-brand-profile-id"] = t.brandProfileId;
  if (t?.audienceId) headers["x-audience-id"] = t.audienceId;

  const response = await fetch(`${BILLING_SERVICE_URL}/v1/customer_balance/authorize`, {
    method: "POST",
    headers,
    body: JSON.stringify({ items, description }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `billing-service POST /v1/customer_balance/authorize failed: ${response.status} - ${errorText}`,
    );
  }

  const result = (await response.json()) as AuthorizeResult;
  if (typeof result?.sufficient !== "boolean") {
    throw new Error(
      `billing-service POST /v1/customer_balance/authorize answered without a boolean "sufficient": ${JSON.stringify(result)}`,
    );
  }
  return result;
}
