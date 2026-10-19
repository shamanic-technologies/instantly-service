/**
 * OAuth for the Microsoft seed receivers (Outlook.com, Microsoft 365).
 * MS-SEED-PLACEMENT-1018.
 *
 * Microsoft refuses password (basic auth) IMAP on both consumer and business
 * mailboxes, so a seed receiver there is read through Microsoft Graph with a
 * delegated token. A person authorizes the mailbox ONCE with the OAuth 2.0
 * device code flow (RFC 8628): a staff call starts it and answers a short code
 * and a URL; the person opens the URL, types the code, signs in as the mailbox.
 * This service polls the token endpoint in the background, verifies the signed-in
 * mailbox IS the one asked for, and stores the refresh token. Every later read
 * exchanges the refresh token for an access token and stores the rotated refresh
 * token, so the mailbox stays readable without anyone touching it again.
 *
 * Client id: Microsoft's own public "Microsoft Graph Command Line Tools" app by
 * default (multi-tenant + personal accounts, device code enabled, no secret).
 * `MS_SEED_OAUTH_CLIENT_ID` overrides it with an app of ours once one exists.
 * Tenant `common`: one flow for Outlook.com and any Microsoft 365 tenant. $0.
 */

import { eq, sql } from "drizzle-orm";

import { db } from "../../db";
import { microsoftSeedMailboxes } from "../../db/schema";
import { espForMicrosoftReceiver } from "./seeds";

export const DEFAULT_MS_SEED_CLIENT_ID = "14d82eec-204b-4c2f-b7e8-296a70dab67e";
export const MS_SEED_TENANT = "common";
/** Read the mailbox and confirm who signed in; offline_access returns a refresh token. */
export const MS_SEED_SCOPE = "https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/User.Read offline_access";
/** Consumer (MSA) accounts all carry this tenant id in their tokens. */
export const MSA_CONSUMER_TENANT_ID = "9188040d-6c67-4c5b-b112-36a304b66dad";

const LOGIN_BASE = "https://login.microsoftonline.com";
const GRAPH_BASE = "https://graph.microsoft.com/v1.0";

export function msSeedClientId(): string {
  const configured = process.env.MS_SEED_OAUTH_CLIENT_ID?.trim();
  return configured ? configured : DEFAULT_MS_SEED_CLIENT_ID;
}

export type MicrosoftSeedMailboxStatus = "pending" | "authorized" | "failed" | "expired";

export class MicrosoftOAuthError extends Error {
  constructor(
    message: string,
    readonly code: string | null = null,
  ) {
    super(message);
    this.name = "MicrosoftOAuthError";
  }
}

// ─── Pure interpreters ───────────────────────────────────────────────────────

export interface DeviceCodeGrant {
  deviceCode: string;
  userCode: string;
  verificationUri: string;
  expiresInSeconds: number;
  intervalSeconds: number;
  message: string;
}

/** The device-code endpoint's answer, or a loud error naming Microsoft's own words. */
export function parseDeviceCodeResponse(status: number, body: unknown): DeviceCodeGrant {
  const b = (body ?? {}) as Record<string, unknown>;
  if (status !== 200 || typeof b.device_code !== "string" || typeof b.user_code !== "string") {
    throw new MicrosoftOAuthError(
      `device code refused (${status}): ${String(b.error_description ?? b.error ?? "no device_code in answer")}`,
      typeof b.error === "string" ? b.error : null,
    );
  }
  return {
    deviceCode: b.device_code,
    userCode: b.user_code,
    verificationUri: typeof b.verification_uri === "string" ? b.verification_uri : "https://microsoft.com/devicelogin",
    expiresInSeconds: Number(b.expires_in ?? 900),
    intervalSeconds: Number(b.interval ?? 5),
    message: typeof b.message === "string" ? b.message : "",
  };
}

export type TokenPollOutcome =
  | { kind: "tokens"; accessToken: string; refreshToken: string; expiresInSeconds: number }
  | { kind: "pending" }
  | { kind: "slow_down" }
  | { kind: "failed"; error: string; description: string };

/**
 * One answer of the token endpoint while polling a device code (RFC 8628 §3.5).
 * `authorization_pending` / `slow_down` keep polling; anything else ends it.
 */
export function interpretTokenPoll(status: number, body: unknown): TokenPollOutcome {
  const b = (body ?? {}) as Record<string, unknown>;
  if (status === 200 && typeof b.access_token === "string") {
    if (typeof b.refresh_token !== "string") {
      return {
        kind: "failed",
        error: "no_refresh_token",
        description: "Microsoft returned no refresh token (offline_access not granted)",
      };
    }
    return {
      kind: "tokens",
      accessToken: b.access_token,
      refreshToken: b.refresh_token,
      expiresInSeconds: Number(b.expires_in ?? 3600),
    };
  }
  const error = typeof b.error === "string" ? b.error : `http_${status}`;
  if (error === "authorization_pending") return { kind: "pending" };
  if (error === "slow_down") return { kind: "slow_down" };
  return { kind: "failed", error, description: String(b.error_description ?? "") };
}

/** The tenant id inside a Microsoft access token (no signature check: we only label). */
export function tenantIdOfToken(accessToken: string): string | null {
  const payload = accessToken.split(".")[1];
  if (!payload) return null;
  try {
    const claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as { tid?: unknown };
    return typeof claims.tid === "string" ? claims.tid : null;
  } catch {
    // An opaque (non-JWT) consumer token is normal; the kind then comes from /me.
    return null;
  }
}

/**
 * `consumer` (Outlook.com) or `business` (a Microsoft 365 tenant), from the
 * token's tenant id; a consumer token is often opaque, so then from the domain.
 */
export function accountKindFor(tenantId: string | null, email: string): "consumer" | "business" | null {
  if (tenantId === MSA_CONSUMER_TENANT_ID) return "consumer";
  if (tenantId) return "business";
  return /@(outlook|hotmail|live|msn)\.[a-z.]+$/i.test(email) ? "consumer" : null;
}

/** The addresses Graph `/me` says the signed-in mailbox answers to, lowercased. */
export function signedInAddresses(me: unknown): string[] {
  const m = (me ?? {}) as Record<string, unknown>;
  return [m.mail, m.userPrincipalName]
    .filter((v): v is string => typeof v === "string" && v.includes("@"))
    .map((v) => v.trim().toLowerCase());
}

// ─── IO ──────────────────────────────────────────────────────────────────────

async function postForm(url: string, form: Record<string, string>): Promise<{ status: number; body: unknown }> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams(form).toString(),
  });
  const text = await res.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { error: `http_${res.status}`, error_description: text.slice(0, 300) };
  }
  return { status: res.status, body };
}

export async function graphGet(accessToken: string, pathOrUrl: string): Promise<unknown> {
  const url = pathOrUrl.startsWith("http") ? pathOrUrl : `${GRAPH_BASE}${pathOrUrl}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  const text = await res.text();
  if (!res.ok) {
    throw new MicrosoftOAuthError(`Graph ${res.status} on ${url.replace(GRAPH_BASE, "")}: ${text.slice(0, 300)}`);
  }
  return text ? JSON.parse(text) : null;
}

export interface MicrosoftAuthorizationStart {
  email: string;
  userCode: string;
  verificationUri: string;
  expiresAt: string;
  message: string;
}

/** Polls in flight in this process, keyed by mailbox, so a second start never doubles one. */
const polling = new Map<string, Promise<void>>();

/**
 * Start authorizing a Microsoft seed mailbox: ask Microsoft for a device code,
 * store it, poll for the person's sign-in in the background, answer the code.
 */
export async function startMicrosoftAuthorization(emailRaw: string): Promise<MicrosoftAuthorizationStart> {
  const email = emailRaw.trim().toLowerCase();
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) throw new MicrosoftOAuthError(`not an email address: ${emailRaw}`);
  const clientId = msSeedClientId();

  const { status, body } = await postForm(`${LOGIN_BASE}/${MS_SEED_TENANT}/oauth2/v2.0/devicecode`, {
    client_id: clientId,
    scope: MS_SEED_SCOPE,
  });
  const grant = parseDeviceCodeResponse(status, body);
  const expiresAt = new Date(Date.now() + grant.expiresInSeconds * 1000);

  // A re-authorization keeps the old refresh token until the new one lands: a
  // started-but-abandoned flow must not make an authorized mailbox unreadable.
  await db
    .insert(microsoftSeedMailboxes)
    .values({
      email,
      status: "pending",
      clientId,
      tenant: MS_SEED_TENANT,
      deviceCode: grant.deviceCode,
      userCode: grant.userCode,
      verificationUri: grant.verificationUri,
      deviceCodeExpiresAt: expiresAt,
      pollIntervalSeconds: grant.intervalSeconds,
      lastError: null,
    })
    .onConflictDoUpdate({
      target: microsoftSeedMailboxes.email,
      set: {
        status: sql`CASE WHEN ${microsoftSeedMailboxes.refreshToken} IS NULL THEN 'pending' ELSE ${microsoftSeedMailboxes.status} END`,
        clientId,
        tenant: MS_SEED_TENANT,
        deviceCode: grant.deviceCode,
        userCode: grant.userCode,
        verificationUri: grant.verificationUri,
        deviceCodeExpiresAt: expiresAt,
        pollIntervalSeconds: grant.intervalSeconds,
        lastError: null,
        updatedAt: new Date(),
      },
    });

  pollInBackground(email);
  console.log(
    `[ms-seed-oauth] authorization started for ${email}: open ${grant.verificationUri} and enter ${grant.userCode} (expires ${expiresAt.toISOString()})`,
  );
  return {
    email,
    userCode: grant.userCode,
    verificationUri: grant.verificationUri,
    expiresAt: expiresAt.toISOString(),
    message: grant.message,
  };
}

function pollInBackground(email: string): void {
  if (polling.has(email)) return;
  const run = pollDeviceCode(email)
    .catch((error: unknown) => {
      console.error(
        `[ms-seed-oauth] poll for ${email} failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    })
    .finally(() => polling.delete(email));
  polling.set(email, run);
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function setStatus(email: string, values: Partial<typeof microsoftSeedMailboxes.$inferInsert>): Promise<void> {
  await db
    .update(microsoftSeedMailboxes)
    .set({ ...values, updatedAt: new Date() })
    .where(eq(microsoftSeedMailboxes.email, email));
}

/**
 * Poll the token endpoint until the person signs in, the code expires, or
 * Microsoft refuses. Reads the device code from the row on every pass, so a
 * newer start supersedes this loop cleanly.
 */
export async function pollDeviceCode(email: string): Promise<void> {
  const [initial] = await db.select().from(microsoftSeedMailboxes).where(eq(microsoftSeedMailboxes.email, email));
  if (!initial?.deviceCode) return;
  const deviceCode = initial.deviceCode;
  let intervalMs = Math.max(1, initial.pollIntervalSeconds ?? 5) * 1000;

  for (;;) {
    await sleep(intervalMs);
    const [row] = await db.select().from(microsoftSeedMailboxes).where(eq(microsoftSeedMailboxes.email, email));
    if (!row || row.deviceCode !== deviceCode) return; // superseded by a newer start
    if (row.deviceCodeExpiresAt && row.deviceCodeExpiresAt.getTime() < Date.now()) {
      await setStatus(email, {
        status: row.refreshToken ? row.status : "failed",
        deviceCode: null,
        lastError: "device code expired before anyone signed in; start again",
      });
      console.warn(`[ms-seed-oauth] ${email}: device code expired, nobody signed in`);
      return;
    }

    const { status, body } = await postForm(`${LOGIN_BASE}/${row.tenant}/oauth2/v2.0/token`, {
      grant_type: "urn:ietf:params:oauth:grant-type:device_code",
      client_id: row.clientId,
      device_code: deviceCode,
    });
    const outcome = interpretTokenPoll(status, body);
    if (outcome.kind === "pending") continue;
    if (outcome.kind === "slow_down") {
      intervalMs += 5000;
      continue;
    }
    if (outcome.kind === "failed") {
      await setStatus(email, {
        status: row.refreshToken ? row.status : "failed",
        deviceCode: null,
        lastError: `${outcome.error}: ${outcome.description}`.slice(0, 500),
      });
      console.error(`[ms-seed-oauth] ${email}: authorization failed: ${outcome.error} ${outcome.description}`);
      return;
    }

    // Signed in: it must be the mailbox asked for, or we would read someone else's.
    const me = await graphGet(outcome.accessToken, "/me?$select=mail,userPrincipalName");
    const addresses = signedInAddresses(me);
    if (!addresses.includes(email)) {
      await setStatus(email, {
        status: row.refreshToken ? row.status : "failed",
        deviceCode: null,
        lastError: `signed in as ${addresses.join(", ") || "an unknown account"}, not ${email}; start again and sign in as ${email}`,
      });
      console.error(`[ms-seed-oauth] ${email}: wrong account signed in (${addresses.join(", ")})`);
      return;
    }

    const tid = tenantIdOfToken(outcome.accessToken);
    await setStatus(email, {
      status: "authorized",
      refreshToken: outcome.refreshToken,
      accountKind: accountKindFor(tid, email),
      deviceCode: null,
      userCode: null,
      authorizedAt: new Date(),
      lastRefreshedAt: new Date(),
      lastError: null,
    });
    cacheAccessToken(email, outcome.accessToken, outcome.expiresInSeconds);
    console.log(`[ms-seed-oauth] ${email}: authorized`);
    return;
  }
}

/** Resume polls a deploy interrupted. Fire-and-forget after `listen`. */
export async function resumePendingMicrosoftAuthorizations(): Promise<void> {
  const rows = await db.select().from(microsoftSeedMailboxes);
  for (const row of rows) {
    if (row.deviceCode && row.deviceCodeExpiresAt && row.deviceCodeExpiresAt.getTime() > Date.now()) {
      pollInBackground(row.email);
    }
  }
}

const accessTokens = new Map<string, { token: string; expiresAt: number }>();

function cacheAccessToken(email: string, token: string, expiresInSeconds: number): void {
  // Two minutes of margin so a read never starts on a token about to lapse.
  accessTokens.set(email, { token, expiresAt: Date.now() + (expiresInSeconds - 120) * 1000 });
}

/**
 * A live access token for an authorized mailbox. Refreshes when needed and
 * stores the ROTATED refresh token (Microsoft issues a new one on each use; the
 * new one is the one that keeps working). A refused refresh marks the mailbox
 * `expired` loudly: it needs a new authorization.
 */
export async function getMicrosoftAccessToken(email: string): Promise<string> {
  const cached = accessTokens.get(email);
  if (cached && cached.expiresAt > Date.now()) return cached.token;
  return refreshMicrosoftToken(email);
}

export async function refreshMicrosoftToken(email: string): Promise<string> {
  const [row] = await db.select().from(microsoftSeedMailboxes).where(eq(microsoftSeedMailboxes.email, email));
  if (!row?.refreshToken) throw new MicrosoftOAuthError(`${email} is not authorized (status ${row?.status ?? "absent"})`);

  const { status, body } = await postForm(`${LOGIN_BASE}/${row.tenant}/oauth2/v2.0/token`, {
    grant_type: "refresh_token",
    client_id: row.clientId,
    refresh_token: row.refreshToken,
    scope: MS_SEED_SCOPE,
  });
  const outcome = interpretTokenPoll(status, body);
  if (outcome.kind !== "tokens") {
    const description =
      outcome.kind === "failed" ? `${outcome.error}: ${outcome.description}` : `unexpected ${outcome.kind}`;
    // invalid_grant = the refresh token is dead (revoked, password change, 90 days idle).
    const dead = outcome.kind === "failed" && outcome.error === "invalid_grant";
    await setStatus(email, { ...(dead ? { status: "expired" } : {}), lastError: description.slice(0, 500) });
    throw new MicrosoftOAuthError(`token refresh for ${email} failed: ${description}`, outcome.kind === "failed" ? outcome.error : null);
  }
  await setStatus(email, { status: "authorized", refreshToken: outcome.refreshToken, lastRefreshedAt: new Date(), lastError: null });
  cacheAccessToken(email, outcome.accessToken, outcome.expiresInSeconds);
  return outcome.accessToken;
}

/** Every mailbox we can read now. */
export async function loadAuthorizedMicrosoftReceivers(): Promise<string[]> {
  const rows = await db
    .select({ email: microsoftSeedMailboxes.email })
    .from(microsoftSeedMailboxes)
    .where(eq(microsoftSeedMailboxes.status, "authorized"));
  return rows.map((r) => r.email).sort();
}

/**
 * Every authorized Microsoft receiver with the ESP code it grades under:
 * Outlook.com (consumer) and a Microsoft 365 tenant (business) filter apart, so
 * they are scored apart (SENDER-HEALTH-1019). A receiver whose kind is unknown
 * is skipped loudly: grading it under either code would claim a filter we did
 * not measure.
 */
export async function loadAuthorizedMicrosoftReceiverEsps(): Promise<{ email: string; recipientEsp: number }[]> {
  const rows = await db
    .select({ email: microsoftSeedMailboxes.email, accountKind: microsoftSeedMailboxes.accountKind })
    .from(microsoftSeedMailboxes)
    .where(eq(microsoftSeedMailboxes.status, "authorized"));
  const out: { email: string; recipientEsp: number }[] = [];
  for (const r of rows) {
    const recipientEsp = espForMicrosoftReceiver(r.accountKind);
    if (recipientEsp === null) {
      console.warn(`[ms-seed-oauth] ${r.email}: account kind unknown, not used as a receiver`);
      continue;
    }
    out.push({ email: r.email, recipientEsp });
  }
  return out.sort((a, b) => (a.email < b.email ? -1 : 1));
}

/** Staff read: state of every Microsoft seed mailbox, never a token. */
export async function listMicrosoftSeedMailboxes() {
  const rows = await db.select().from(microsoftSeedMailboxes);
  return rows
    .map((r) => ({
      email: r.email,
      status: r.status as MicrosoftSeedMailboxStatus,
      accountKind: r.accountKind,
      pendingCode:
        r.deviceCode && r.deviceCodeExpiresAt && r.deviceCodeExpiresAt.getTime() > Date.now()
          ? { userCode: r.userCode, verificationUri: r.verificationUri, expiresAt: r.deviceCodeExpiresAt.toISOString() }
          : null,
      authorizedAt: r.authorizedAt?.toISOString() ?? null,
      lastRefreshedAt: r.lastRefreshedAt?.toISOString() ?? null,
      lastError: r.lastError,
    }))
    .sort((a, b) => a.email.localeCompare(b.email));
}

/**
 * Refresh every authorized mailbox once. Run after each seed sync, so a refresh
 * token is exercised daily and never reaches Microsoft's inactivity expiry.
 */
export async function keepMicrosoftReceiversAlive(): Promise<{ refreshed: number; failed: number }> {
  let refreshed = 0;
  let failed = 0;
  for (const email of await loadAuthorizedMicrosoftReceivers()) {
    try {
      await refreshMicrosoftToken(email);
      refreshed += 1;
    } catch (error) {
      failed += 1;
      console.error(`[ms-seed-oauth] keepalive ${email}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  return { refreshed, failed };
}
