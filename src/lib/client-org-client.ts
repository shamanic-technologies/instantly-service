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
 *
 * Second read: `listOrgMembers`, who to tell when a reply needs the client
 * (lib/notify-brand-team: the AI's escalation to the brand's own users).
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

/** A person of the client org we can email. */
export interface OrgMember {
  email: string;
  firstName: string | null;
}

/**
 * The org's members with an email address, oldest first. Membership is
 * client-service's own reading (`GET /internal/users?orgId=`, the same rows its
 * member check reads). Rows without an address are dropped. Fails LOUD.
 *
 * `allMembers`: every member Clerk holds for the org, people in several orgs
 * included (`&allMembers=true`, client-service v0.27.18). Opt-in: staff are
 * Clerk members of the client orgs they set up, so this list names them too.
 */
export async function listOrgMembers(
  orgId: string,
  options: { allMembers?: boolean } = {},
): Promise<OrgMember[]> {
  const url = process.env.CLIENT_SERVICE_URL;
  const apiKey = process.env.CLIENT_SERVICE_API_KEY;
  if (!url || !apiKey) {
    throw new Error("CLIENT_SERVICE_URL or CLIENT_SERVICE_API_KEY is not set");
  }

  const query = `orgId=${encodeURIComponent(orgId)}${options.allMembers ? "&allMembers=true" : ""}`;
  const response = await fetch(`${url}/internal/users?${query}`, {
    headers: { "x-api-key": apiKey },
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`client-service GET /internal/users failed: ${response.status} - ${detail.slice(0, 200)}`);
  }
  const body = (await response.json()) as { users?: Array<{ email?: unknown; firstName?: unknown }> };
  if (!Array.isArray(body.users)) throw new Error("client-service GET /internal/users returned no `users` array");
  const seen = new Set<string>();
  const members: OrgMember[] = [];
  for (const u of body.users) {
    const email = typeof u.email === "string" ? u.email.trim() : "";
    if (!email || seen.has(email.toLowerCase())) continue;
    seen.add(email.toLowerCase());
    const firstName = typeof u.firstName === "string" && u.firstName.trim() ? u.firstName.trim() : null;
    members.push({ email, firstName });
  }
  return members;
}
