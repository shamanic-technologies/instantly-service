/**
 * Resolve the SHARED cold-email workspace's Instantly key for platform-scoped
 * (no-org) fleet views.
 *
 * These endpoints have no org identity, so they target the shared workspace
 * directly via `INSTANTLY_API_KEY` — the same key the audit / heal / cleanup
 * CLIs use against prod (key-service resolves keys per-org and is not reachable
 * here without an org). Fail loud when unset: no key ⇒ no fleet source ⇒ 500,
 * never a fabricated result.
 *
 * Shared by GET /internal/audit/sending-forecast and
 * GET /internal/audit/account-health so both derive the workspace identically.
 */
export function resolvePlatformInstantlyKey(): string {
  const key = process.env.INSTANTLY_API_KEY?.trim();
  if (!key) {
    throw new Error(
      "INSTANTLY_API_KEY not configured — cannot resolve the shared workspace for the platform audit view",
    );
  }
  return key;
}
