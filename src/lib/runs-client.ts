import { errorMessage } from "./db-error.js";
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
    const message = errorMessage(error);
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
  const message = errorMessage(error);
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

/** A run as `GET /v1/runs` lists it: no cost rows, the sum of its own provisioned ones. */
export interface ListedRun {
  id: string;
  parentRunId: string | null;
  serviceName: string;
  taskName: string;
  status: string;
  startedAt: string;
  ownProvisionedCostInUsdCents: string;
}

/**
 * Runs of the caller's org (`x-org-id`) matching `filters`, every page at once
 * (no `limit`: the filters keep the set small). Throws on any failure.
 */
export async function listRuns(
  filters: { serviceName: string; status: string; startedAfter: string; startedBefore: string },
  identity: IdentityContext,
): Promise<ListedRun[]> {
  const qs = new URLSearchParams(filters).toString();
  const body = await runsRequest<{ runs: ListedRun[] }>(`/v1/runs?${qs}`, identity);
  return Array.isArray(body.runs) ? body.runs : [];
}

/** One run with its own cost rows, or null when the run does not exist (404). */
export async function getRunWithCosts(
  runId: string,
  identity: IdentityContext,
): Promise<(Run & { costs: RunCost[] }) | null> {
  try {
    const run = await runsRequest<Run & { costs?: RunCost[] }>(`/v1/runs/${runId}`, identity);
    return { ...run, costs: Array.isArray(run.costs) ? run.costs : [] };
  } catch (error) {
    if (isRunGoneError(error)) return null;
    throw error;
  }
}

/** A trace event on a run (`POST /v1/runs/:id/events`, see trace-event.ts). */
export interface RunEvent {
  event: string;
  detail: string | null;
  createdAt: string;
}

/** The events traced on a run, or null when the run does not exist (404). */
export async function listRunEvents(runId: string, identity: IdentityContext): Promise<RunEvent[] | null> {
  try {
    const body = await runsRequest<{ events?: RunEvent[] }>(`/v1/runs/${runId}/events`, identity);
    return Array.isArray(body.events) ? body.events : [];
  } catch (error) {
    if (isRunGoneError(error)) return null;
    throw error;
  }
}

/** One cost item declared late as real spend that was never charged (`/internal/incurred-costs/*`). */
export interface IncurredCostItemRequest {
  runId: string;
  costName: string;
  quantity: number;
  /** When the spend really happened (ISO). Becomes the cost row's date. */
  incurredAt: string;
  /** Per-run dedup key: a re-sent item writes nothing. */
  idempotencyKey: string;
}

export type IncurredCostOutcome = "to_record" | "recorded" | "already_recorded" | "invalid";

export interface IncurredCostsResult {
  itemCount: number;
  toRecordCount: number;
  alreadyRecordedCount: number;
  invalidCount: number;
  /** Gross, of the items to record only. Decimal string. */
  totalCostInUsdCents: string;
  netTotalCostInUsdCents: string;
  alreadyRecordedTotalCostInUsdCents: string;
  unitPrices: Record<string, string>;
  /** Apply only. */
  recordedCount?: number;
  items: Array<{
    runId: string;
    idempotencyKey: string;
    costName: string;
    outcome: IncurredCostOutcome;
    costId: string | null;
    status: string | null;
    incurredAt: string | null;
    totalCostInUsdCents: string | null;
    netCostInUsdCents: string | null;
    error: string | null;
  }>;
}

/**
 * Record (or, `mode: "preview"`, price without writing) costs that REALLY
 * happened and that the platform decided never to charge. runs-service writes
 * each row born `refunded`: in no charge predicate at any instant, counted by
 * every incurred read, dated at `incurredAt`. Platform-scoped (service key, no
 * org headers); at most 500 items per call. Throws on any non-2xx (a 422 on
 * apply = some item invalid, nothing written).
 */
export async function incurredCosts(
  mode: "preview" | "apply",
  items: IncurredCostItemRequest[],
  audit: { reason: string; authorizedBy: string },
): Promise<IncurredCostsResult> {
  const response = await fetch(`${RUNS_SERVICE_URL}/internal/incurred-costs/${mode}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-API-Key": RUNS_SERVICE_API_KEY },
    body: JSON.stringify(mode === "apply" ? { items, ...audit } : { items }),
  });
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`runs-service POST /internal/incurred-costs/${mode} failed: ${response.status} - ${errorText}`);
  }
  return response.json() as Promise<IncurredCostsResult>;
}
