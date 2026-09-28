/**
 * Stop re-trying a mailbox login Google has already REJECTED with the same
 * password.
 *
 * Every IMAP opener in this service (self-send poll, inbox watcher connect and
 * its fallback read, warmup poll, seed-placement sync) re-resolves the
 * credential and logs in again on its own cadence. For a mailbox whose app
 * password Google refuses (`NO [ALERT] Invalid credentials`), that is ~265
 * rejected logins a day, each also leaking a half-open socket that times out a
 * minute later — measured 2026-09-28 on `bailey@fuseconnectio.com`, whose
 * Primeforge-issued app password has been refused since 2026-09-03 while its
 * two siblings on the same domain log in fine. Hammering a Google account with
 * bad credentials is also the one thing that can make its state worse.
 *
 * So after an AUTHENTICATION failure (ImapFlow's `authenticationFailed`, never a
 * timeout or a network error — those say nothing about the password) the
 * `(login, password)` pair is held, and every opener refuses it WITHOUT
 * connecting until either:
 *
 *   - the password the vendor serves CHANGES (a reissued app password is picked
 *     up on the very next attempt — the credential is re-resolved per attempt
 *     upstream, so the fingerprint comparison is what makes recovery immediate);
 *   - or {@link AUTH_QUARANTINE_RETRY_MS} has passed, when ONE real login is
 *     allowed through, so a fix made on the Google side without a new password
 *     is still noticed within that bound.
 *
 * The refusal is an ERROR, not a skip: callers keep counting the mailbox as
 * failed and keep logging it, with a message naming the quarantine and since
 * when. A mailbox nobody can read must stay loud; what goes away is the login.
 *
 * In-memory on purpose: a restart costs one real login per quarantined mailbox,
 * which is the cheapest possible re-check, and there is no row to go stale.
 */

import { createHash } from "node:crypto";

/** One real login per quarantined mailbox per this window (unless the password changes). */
export const AUTH_QUARANTINE_RETRY_MS = 6 * 60 * 60 * 1000;

interface QuarantineEntry {
  fingerprint: string;
  since: number;
  lastAttemptAt: number;
}

const entries = new Map<string, QuarantineEntry>();

function key(login: string): string {
  return login.trim().toLowerCase();
}

function fingerprint(password: string): string {
  return createHash("sha256").update(password).digest("hex");
}

/** ImapFlow sets `authenticationFailed: true` on a rejected LOGIN/AUTHENTICATE. */
export function isImapAuthFailure(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { authenticationFailed?: unknown }).authenticationFailed === true
  );
}

export class ImapAuthQuarantinedError extends Error {
  readonly since: Date;
  readonly nextAttemptAt: Date;
  constructor(login: string, since: Date, nextAttemptAt: Date) {
    super(
      `auth quarantined: Google rejected the app password for ${login} since ${since.toISOString()}; no login attempted, next attempt ${nextAttemptAt.toISOString()} or as soon as the credential changes`,
    );
    this.name = "ImapAuthQuarantinedError";
    this.since = since;
    this.nextAttemptAt = nextAttemptAt;
  }
}

/**
 * Throws {@link ImapAuthQuarantinedError} when this exact `(login, password)`
 * was rejected and the retry window has not elapsed. Otherwise returns, and —
 * when it lets a quarantined pair through for its periodic retry — stamps the
 * attempt so concurrent openers do not all retry at once.
 */
export function assertNotAuthQuarantined(
  login: string,
  password: string,
  now: number = Date.now(),
): void {
  const entry = entries.get(key(login));
  if (!entry) return;
  if (entry.fingerprint !== fingerprint(password)) {
    // The vendor now serves a different password: try it straight away.
    entries.delete(key(login));
    return;
  }
  const nextAttemptAt = entry.lastAttemptAt + AUTH_QUARANTINE_RETRY_MS;
  if (now < nextAttemptAt) {
    throw new ImapAuthQuarantinedError(login, new Date(entry.since), new Date(nextAttemptAt));
  }
  entry.lastAttemptAt = now;
}

/** Record the outcome of a real login attempt. Non-auth errors change nothing. */
export function recordImapLoginOutcome(
  login: string,
  password: string,
  error: unknown | null,
  now: number = Date.now(),
): void {
  if (error === null) {
    entries.delete(key(login));
    return;
  }
  if (!isImapAuthFailure(error)) return;
  const fp = fingerprint(password);
  const existing = entries.get(key(login));
  entries.set(key(login), {
    fingerprint: fp,
    since: existing && existing.fingerprint === fp ? existing.since : now,
    lastAttemptAt: now,
  });
}

/** Tests only. */
export function resetImapAuthQuarantine(): void {
  entries.clear();
}
