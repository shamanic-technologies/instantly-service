/**
 * HTTP client for runs-service
 * BLOCKING: must succeed before operations proceed
 */

const RUNS_SERVICE_URL = process.env.RUNS_SERVICE_URL || "http://localhost:3006";
const RUNS_SERVICE_API_KEY = process.env.RUNS_SERVICE_API_KEY || "";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface Run {
  id: string;
  parentRunId: string | null;
  organizationId: string;
  userId: string | null;
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
  quantity: string;
  unitCostInUsdCents: string;
  totalCostInUsdCents: string;
  createdAt: string;
}

interface RunsOrganization {
  id: string;
  externalId: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateRunParams {
  organizationId: string;
  serviceName: string;
  taskName: string;
  parentRunId?: string;
  userId?: string;
}

export interface CostItem {
  costName: string;
  quantity: number;
}

// ─── HTTP helpers ────────────────────────────────────────────────────────────

async function runsRequest<T>(
  path: string,
  options: { method?: string; body?: unknown } = {}
): Promise<T> {
  const { method = "GET", body } = options;

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-API-Key": RUNS_SERVICE_API_KEY,
  };

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

// ─── Org cache (in-memory, per process) ──────────────────────────────────────

const orgCache = new Map<string, string>();

// ─── Public API ──────────────────────────────────────────────────────────────

export async function ensureOrganization(clerkOrgId: string): Promise<string> {
  const cached = orgCache.get(clerkOrgId);
  if (cached) return cached;

  const org = await runsRequest<RunsOrganization>("/v1/organizations", {
    method: "POST",
    body: { externalId: clerkOrgId },
  });

  orgCache.set(clerkOrgId, org.id);
  return org.id;
}

export async function createRun(params: CreateRunParams): Promise<Run> {
  return runsRequest<Run>("/v1/runs", {
    method: "POST",
    body: params,
  });
}

export async function updateRun(
  runId: string,
  status: "completed" | "failed",
  error?: string
): Promise<Run> {
  return runsRequest<Run>(`/v1/runs/${runId}`, {
    method: "PATCH",
    body: { status, error },
  });
}

export async function addCosts(
  runId: string,
  items: CostItem[]
): Promise<{ costs: RunCost[] }> {
  return runsRequest<{ costs: RunCost[] }>(`/v1/runs/${runId}/costs`, {
    method: "POST",
    body: { items },
  });
}
