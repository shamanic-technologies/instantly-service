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
  workflowSlug?: string;
  featureSlug?: string;
  goal?: string;
  brandProfileId?: string;
  audienceId?: string;
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
  brandId?: string | null;
  campaignId?: string | null;
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
  if (identity.tracking?.workflowSlug) {
    headers["x-workflow-slug"] = identity.tracking.workflowSlug;
  }
  if (identity.tracking?.featureSlug) {
    headers["x-feature-slug"] = identity.tracking.featureSlug;
  }
  if (identity.tracking?.goal) {
    headers["x-goal"] = identity.tracking.goal;
  }
  if (identity.tracking?.brandProfileId) {
    headers["x-brand-profile-id"] = identity.tracking.brandProfileId;
  }
  if (identity.tracking?.audienceId) {
    headers["x-audience-id"] = identity.tracking.audienceId;
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
  const body: CreateRunParams = {
    serviceName: params.serviceName,
    taskName: params.taskName,
  };
  if (params.brandId?.trim()) body.brandId = params.brandId;
  if (params.campaignId?.trim()) body.campaignId = params.campaignId;

  return runsRequest<Run>("/v1/runs", identity, {
    method: "POST",
    body,
  });
}

/**
 * Fetch a single run by ID. Returns `null` on 404 (or other lookup failure)
 * so callers can branch on "parent gone" without try/catching every site.
 */
export async function getRun(runId: string, identity: IdentityContext): Promise<Run | null> {
  try {
    return await runsRequest<Run>(`/v1/runs/${runId}`, identity);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    // runsRequest formats errors as `... failed: <status> - <body>`; 404 means
    // the run doesn't exist (deleted / wrong ID). 403 means the caller's
    // identity isn't allowed to read it (cross-org). Both are "unusable
    // parent" from retry-stuck's perspective.
    if (/failed: 40[34]\b/.test(message)) {
      return null;
    }
    throw error;
  }
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

/**
 * True when a runs-service error is a TERMINAL 404 — the run (or its cost) no
 * longer exists (retention purged it), so the operation can NEVER succeed on
 * retry. Deliberately narrower than `getRun`'s `40[34]` matcher: only 404 is
 * terminal for a cost actualize/cancel. A 403 is a transient cross-org/auth
 * condition and 5xx/timeout/connection errors are transient too — none of those
 * should trigger a local cancel. Matches the `... failed: <status> - <body>`
 * shape `runsRequest` throws.
 */
export function isRunGoneError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /failed: 404\b/.test(message);
}

export async function updateCostStatus(
  runId: string,
  costId: string,
  status: "actual" | "provisioned" | "cancelled",
  identity: IdentityContext
): Promise<RunCost> {
  return runsRequest<RunCost>(`/v1/runs/${runId}/costs/${costId}`, identity, {
    method: "PATCH",
    body: { status },
  });
}
