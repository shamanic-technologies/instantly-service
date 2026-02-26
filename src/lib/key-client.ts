/**
 * HTTP client for key-service
 * Decrypts app-level and BYOK (per-org) API keys at runtime.
 */

const KEY_SERVICE_URL = process.env.KEY_SERVICE_URL || "http://localhost:3001";
const KEY_SERVICE_API_KEY = process.env.KEY_SERVICE_API_KEY || "";

const CALLER_SERVICE = "instantly";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface CallerInfo {
  method: string;
  path: string;
}

interface DecryptKeyResponse {
  provider: string;
  key: string;
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

async function keyServiceRequest<T>(
  path: string,
  caller: CallerInfo,
): Promise<T> {
  const headers: Record<string, string> = {
    "x-api-key": KEY_SERVICE_API_KEY,
    "X-Caller-Service": CALLER_SERVICE,
    "X-Caller-Method": caller.method,
    "X-Caller-Path": caller.path,
  };

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

export async function decryptAppKey(
  provider: string,
  appId: string,
  caller: CallerInfo,
): Promise<string> {
  const result = await keyServiceRequest<DecryptKeyResponse>(
    `/internal/app-keys/${encodeURIComponent(provider)}/decrypt?appId=${encodeURIComponent(appId)}`,
    caller,
  );
  return result.key;
}

export async function decryptByokKey(
  provider: string,
  orgId: string,
  caller: CallerInfo,
): Promise<string> {
  const result = await keyServiceRequest<DecryptKeyResponse>(
    `/internal/keys/${encodeURIComponent(provider)}/decrypt?orgId=${encodeURIComponent(orgId)}`,
    caller,
  );
  return result.key;
}

/**
 * Resolve the Instantly API key for a request.
 * - If orgId is provided: use the org's BYOK key (NO fallback to app key).
 * - If orgId is null/undefined: use the shared app key (service-level ops).
 */
export async function resolveInstantlyApiKey(
  orgId: string | null | undefined,
  caller: CallerInfo,
): Promise<string> {
  if (orgId) {
    return decryptByokKey("instantly", orgId, caller);
  }
  return decryptAppKey("instantly", "instantly-service", caller);
}
