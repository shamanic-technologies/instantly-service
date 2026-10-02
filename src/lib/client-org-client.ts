/**
 * client-service client — ONE read: the Clerk org id of an org we know by its
 * internal UUID.
 *
 * Why it exists: the distribute.you dashboard addresses an org by its Clerk id
 * (`/v2/orgs/org_…/…`), while every service, this one included, holds the
 * internal UUID. A link from an email into the dashboard has to carry the Clerk
 * id or it lands on a 404. client-service owns that mapping
 * (`GET /internal/orgs/{orgId}` → `{ id, externalId, name }`).
 *
 * Fails LOUD on a non-2xx: the caller (the client email) decides what a missing
 * id means for its link, and it must know an unreachable client-service from an
 * org that has no Clerk id. Do not grow this into a general org mirror.
 */

export async function getExternalOrgId(orgId: string): Promise<string | null> {
  const url = process.env.CLIENT_SERVICE_URL;
  const apiKey = process.env.CLIENT_SERVICE_API_KEY;
  if (!url || !apiKey) {
    throw new Error("CLIENT_SERVICE_URL or CLIENT_SERVICE_API_KEY is not set");
  }

  const response = await fetch(`${url}/internal/orgs/${encodeURIComponent(orgId)}`, {
    headers: { "x-api-key": apiKey },
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `client-service GET /internal/orgs/${orgId} failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as { externalId?: string | null };
  const externalId = typeof body.externalId === "string" ? body.externalId.trim() : "";
  return externalId || null;
}
