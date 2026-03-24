/**
 * HTTP client for key-service
 * Resolves API keys at runtime via the unified GET /keys/:provider/decrypt endpoint.
 * Returns both the key and its source ("platform" or "org") for cost tracking.
 */

const KEY_SERVICE_URL = process.env.KEY_SERVICE_URL || "http://localhost:3001";
const KEY_SERVICE_API_KEY = process.env.KEY_SERVICE_API_KEY || "";

const CALLER_SERVICE = "instantly";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface CallerInfo {
  method: string;
  path: string;
}

export interface KeyResolution {
  key: string;
  keySource: "platform" | "org";
}

interface DecryptKeyResponse {
  provider: string;
  key: string;
  keySource: "platform" | "org";
}

// ─── Errors ─────────────────────────────────────────────────────────────────

export class KeyServiceError extends Error {
  constructor(
    public readonly statusCode: number,
    message: string,
  ) {
    super(message);
    this.name = "KeyServiceError";
  }
}

// ─── HTTP helpers ────────────────────────────────────────────────────────────

interface TrackingHeaders {
  campaignId?: string;
  brandId?: string;
  workflowName?: string;
  featureSlug?: string;
}

async function keyServiceRequest<T>(
  path: string,
  caller: CallerInfo,
  identity: { orgId: string; userId: string; tracking?: TrackingHeaders },
): Promise<T> {
  const headers: Record<string, string> = {
    "x-api-key": KEY_SERVICE_API_KEY,
    "x-org-id": identity.orgId,
    "x-user-id": identity.userId,
    "X-Caller-Service": CALLER_SERVICE,
    "X-Caller-Method": caller.method,
    "X-Caller-Path": caller.path,
  };
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

  const response = await fetch(`${KEY_SERVICE_URL}${path}`, {
    method: "GET",
    headers,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new KeyServiceError(
      response.status,
      `key-service GET ${path} failed: ${response.status} - ${errorText}`,
    );
  }

  return response.json() as Promise<T>;
}

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Resolve the Instantly API key for a request.
 * Uses the unified GET /keys/:provider/decrypt endpoint which auto-resolves
 * between org and platform keys based on the org's preference.
 *
 * @param orgId - Internal org UUID (required)
 * @param userId - Internal user UUID (required for logging; use "system" for cron jobs)
 * @param caller - Caller context for key-service tracking
 * @returns { key, keySource } where keySource is "platform" or "org"
 */
export async function resolveInstantlyApiKey(
  orgId: string,
  userId: string,
  caller: CallerInfo,
): Promise<KeyResolution> {
  const result = await keyServiceRequest<DecryptKeyResponse>(
    `/keys/instantly/decrypt`,
    caller,
    { orgId, userId },
  );
  return { key: result.key, keySource: result.keySource };
}
