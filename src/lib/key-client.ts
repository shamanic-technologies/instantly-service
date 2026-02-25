/**
 * HTTP client for key-service
 * Decrypts app-level API keys at runtime.
 */

const KEY_SERVICE_URL = process.env.KEY_SERVICE_URL || "http://localhost:3001";
const KEY_SERVICE_API_KEY = process.env.KEY_SERVICE_API_KEY || "";

const CALLER_SERVICE = "instantly";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface CallerInfo {
  method: string;
  path: string;
}

interface DecryptAppKeyResponse {
  provider: string;
  key: string;
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
    throw new Error(
      `key-service GET ${path} failed: ${response.status} - ${errorText}`
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
  const result = await keyServiceRequest<DecryptAppKeyResponse>(
    `/internal/app-keys/${encodeURIComponent(provider)}/decrypt?appId=${encodeURIComponent(appId)}`,
    caller,
  );
  return result.key;
}
