/**
 * HTTP client for runs-service
 * BLOCKING: must succeed before operations proceed
 */

const RUNS_SERVICE_URL = process.env.RUNS_SERVICE_URL || "http://localhost:3006";
const RUNS_SERVICE_API_KEY = process.env.RUNS_SERVICE_API_KEY || "";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface TrackingHeaders {
  campaignId?: string;
  brandId?: string;
  workflowName?: string;
  featureSlug?: string;
}

export interface IdentityContext {
  orgId: string;
  userId: string;
  runId?: string;
  tracking?: TrackingHeaders;
}

export interface Run {
  id: string;
  parentRunId: string | null;
  organizationId: string;
  userId: string | null;
  brandId: string | null;
  campaignId: string | null;
  serviceName: string;
  taskName: string;
  status: string;
  startedAt: string;
  completedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface RunCost {
  id: string;
  runId: string;
  costName: string;
  costSource: "platform" | "org";
  quantity: string;
  unitCostInUsdCents: string;
  totalCostInUsdCents: string;
  status: "actual" | "provisioned" | "cancelled";
  createdAt: string;
}

export interface CreateRunParams {
  serviceName: string;
  taskName: string;
  brandId?: string;
  campaignId?: string;
}

export interface CostItem {
  costName: string;
  quantity: number;
  costSource: "platform" | "org";
  status?: "actual" | "provisioned";
}

// ─── HTTP helpers ────────────────────────────────────────────────────────────

async function runsRequest<T>(
  path: string,
  identity: IdentityContext,
  options: { method?: string; body?: unknown } = {}
): Promise<T> {
  const { method = "GET", body } = options;

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-API-Key": RUNS_SERVICE_API_KEY,
    "x-org-id": identity.orgId,
    "x-user-id": identity.userId,
  };
  if (identity.runId) {
    headers["x-run-id"] = identity.runId;
  }
  if (identity.tracking?.campaignId) {
    headers["x-campaign-id"] = identity.tracking.campaignId;
  }
  if (identity.tracking?.brandId) {
    headers["x-brand-id"] = identity.tracking.brandId;
  }
  if (identity.tracking?.workflowName) {
    headers["x-workflow-name"] = identity.tracking.workflowName;
  }
  if (identity.tracking?.featureSlug) {
    headers["x-feature-slug"] = identity.tracking.featureSlug;
  }

  const response = await fetch(`${RUNS_SERVICE_URL}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `runs-service ${method} ${path} failed: ${response.status} - ${errorText}`
    );
  }

  return response.json() as Promise<T>;
}

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Create a run. Identity headers provide org/user/parent:
 * - x-org-id → organizationId
 * - x-user-id → userId
 * - x-run-id → parentRunId (caller's run ID becomes the parent)
 */
export async function createRun(params: CreateRunParams, identity: IdentityContext): Promise<Run> {
  return runsRequest<Run>("/v1/runs", identity, {
    method: "POST",
    body: params,
  });
}

export async function updateRun(
  runId: string,
  status: "completed" | "failed",
  identity: IdentityContext,
  error?: string
): Promise<Run> {
  return runsRequest<Run>(`/v1/runs/${runId}`, identity, {
    method: "PATCH",
    body: { status, error },
  });
}

export async function addCosts(
  runId: string,
  items: CostItem[],
  identity: IdentityContext
): Promise<{ costs: RunCost[] }> {
  return runsRequest<{ costs: RunCost[] }>(`/v1/runs/${runId}/costs`, identity, {
    method: "POST",
    body: { items },
  });
}

export async function updateCostStatus(
  runId: string,
  costId: string,
  status: "actual" | "cancelled",
  identity: IdentityContext
): Promise<RunCost> {
  return runsRequest<RunCost>(`/v1/runs/${runId}/costs/${costId}`, identity, {
    method: "PATCH",
    body: { status },
  });
}
