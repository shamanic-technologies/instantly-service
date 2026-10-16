/**
 * The owner's identity (person + company), read from kevinlourd.com, its owner
 * (owner 2026-10-16: "For my personal or business info, you can ask the API or
 * MCP of kevinlourd.com repo, it is their scope of knowledge / responsibility to
 * keep accurate info updated"). Never guessed from the CRM or hardcoded here.
 *
 * `GET {LEDGER_API_URL}/api/v1/identity`, `Authorization: Bearer {LEDGER_API_KEY}`
 * (the admin app's key, shared with costs-service). Every field is present, an
 * unset one is `null`. Throws when a fact a removal request needs is unset, naming
 * it, so the attempt is stored as failed with the reason the owner must fix.
 */
import type { RemovalContact } from "./blocklist-removal";

export interface OwnerIdentity {
  person: Record<string, string | null>;
  company: Record<string, string | null>;
  updatedAt: string | null;
}

export async function fetchOwnerIdentity(fetchImpl: typeof fetch = fetch): Promise<OwnerIdentity> {
  const url = process.env.LEDGER_API_URL?.trim();
  const key = process.env.LEDGER_API_KEY?.trim();
  if (!url || !key) throw new Error("LEDGER_API_URL / LEDGER_API_KEY are not set");
  const res = await fetchImpl(`${url.replace(/\/$/, "")}/api/v1/identity`, {
    headers: { Authorization: `Bearer ${key}` },
    signal: AbortSignal.timeout(15_000),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`kevinlourd.com GET /api/v1/identity ${res.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text) as OwnerIdentity;
}

/**
 * The facts a removal form needs, or a throw naming each one unset. Pure.
 *
 * A removal request is filed by the COMPANY (the domains are distribute.you's),
 * so it reaches the business email and phone (`company.*`), never the person's
 * private ones (kevinlourd.com #343: `person` = personal, `company` = business).
 * The filer's name is the person's legal name.
 */
export function contactFromIdentity(identity: OwnerIdentity): RemovalContact {
  const need = {
    fullName: identity.person.fullName,
    email: identity.company.email,
    phone: identity.company.phone,
    organization: identity.company.legalName,
    address: identity.company.registeredOffice,
    website: identity.company.website,
  };
  const missing = Object.entries(need)
    .filter(([, v]) => typeof v !== "string" || v.trim() === "")
    .map(([k]) => ({ fullName: "person.fullName", email: "company.email", phone: "company.phone", organization: "company.legalName", address: "company.registeredOffice", website: "company.website" })[k] ?? k);
  if (missing.length > 0) {
    throw new Error(`not set on kevinlourd.com (Identity page): ${missing.join(", ")}`);
  }
  return Object.fromEntries(Object.entries(need).map(([k, v]) => [k, (v as string).trim()])) as unknown as RemovalContact;
}

export async function loadOwnerContact(): Promise<RemovalContact> {
  return contactFromIdentity(await fetchOwnerIdentity());
}
