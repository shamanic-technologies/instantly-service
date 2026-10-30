/**
 * Mailbox credentials for the self-hosted sender.
 *
 * The mailboxes we send from are real Google Workspace accounts provisioned by
 * Primeforge, and Primeforge's REST API hands back a 16-character Google app
 * password per mailbox. That single credential authenticates BOTH the SMTP
 * dispatch and the IMAP poll, so no Workspace admin action and no OAuth grant
 * is involved — verified live against three separate domains (2026-08-16).
 *
 * Use `imap.gmail.com` / `smtp.gmail.com`. `imap.google.com` / `smtp.google.com`
 * are the wrong hosts and fail to connect, and the vendor's `password` field is
 * the interactive login password, which these endpoints reject.
 *
 * Not every mailbox is on Primeforge, though. The legacy Gandi and Mailforge
 * fleets are real senders whose vendors do NOT hand an existing mailbox's
 * password back over an API, so a MANUAL layer sits in front: a key-service
 * platform key holding hand-entered credentials, hosts included (the whole point
 * of the layer is that these mailboxes are not on Google). Manual wins, so an
 * operator can override a vendor-reported password without waiting on the
 * vendor.
 *
 * This resolution is the ONE source of truth for "how do I authenticate as this
 * mailbox". The seed harness reads it too — it used to carry its own copy that
 * knew about the manual layer while the SENDER did not, so a Gandi mailbox could
 * be measured and not sent from. Two implementations of one concept drift, and
 * the one nobody remembers to update is the one that breaks.
 *
 * Fail loud throughout: a mailbox we cannot authenticate must abort the send,
 * never degrade to some other sender. A VENDOR that refuses to list our
 * mailboxes (Primeforge 402) is not a mailbox outage: its mailboxes keep their
 * credential from the vendor's last stored listing, loudly (see
 * `loadPrimeforgeMailboxes`), and with no stored listing only ITS mailboxes leave
 * the fleet maps, so the other vendors' mailboxes keep sending.
 */

import { errorMessage } from "../db-error.js";
import { KeyServiceError, resolvePlatformKey, type CallerInfo } from "../key-client";
import {
  listPrimeforgeRawMailboxes,
  type PrimeforgeRawMailbox,
} from "../providers/primeforge-client";
import { loadLastPrimeforgeListing } from "./primeforge-last-listing";

export const GMAIL_SMTP_HOST = "smtp.gmail.com";
export const GMAIL_IMAP_HOST = "imap.gmail.com";

/**
 * Submission port, STARTTLS — NOT 465.
 *
 * ⚠️ The host blocks outbound 465 and 25 (the usual anti-spam policy on a cloud
 * VPS) and leaves 587 open. Measured from the box itself: 465 and 25 time out,
 * 587 and 993 connect. A laptop can reach 465 fine, which is exactly how this
 * was missed — the credential was proven from a machine that is not the one the
 * code runs on.
 *
 * STARTTLS is REQUIRED by the transport (`requireTLS`), not merely offered: on
 * 587 an unencrypted session is syntactically valid, and a silent downgrade
 * would put the mailbox password on the wire in clear text.
 */
export const GMAIL_SMTP_PORT = 587;
export const GMAIL_IMAP_PORT = 993;

export class MailboxCredentialError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MailboxCredentialError";
  }
}

export interface MailboxCredential {
  address: string;
  appPassword: string;
  smtpHost: string;
  imapHost: string;
  /**
   * SMTP/IMAP login, when it differs from the address.
   *
   * A Gandi domain is typically ONE mailbox plus several aliases, and Instantly
   * holds an account per alias — so the account `klourd@pressbeat.ai` sends as
   * itself but authenticates as `kevin@pressbeat.ai`, the only real mailbox on
   * that domain. Measured: 175 Instantly IMAP accounts sit on 44 Gandi
   * mailboxes. Conflating the two makes every alias unauthenticable.
   *
   * Absent means the address IS the login, which is the Primeforge case.
   */
  authUser?: string;
}

/** The SMTP/IMAP username for a credential — the alias-aware login. */
export function loginFor(credential: MailboxCredential): string {
  return credential.authUser ?? credential.address;
}

/**
 * Pure selection of one mailbox's credential out of a vendor mailbox list.
 *
 * Addresses are compared case-insensitively because the vendor stores whatever
 * casing the mailbox was created with while we key on the lowercased address
 * everywhere else.
 */
export function selectMailboxCredential(
  email: string,
  mailboxes: readonly Pick<PrimeforgeRawMailbox, "address" | "appPassword">[],
): MailboxCredential {
  const wanted = email.trim().toLowerCase();

  const mailbox = mailboxes.find(
    (m) => m.address?.trim().toLowerCase() === wanted,
  );

  if (!mailbox) {
    throw new MailboxCredentialError(
      `No Primeforge mailbox found for ${wanted} — cannot authenticate a send from it`,
    );
  }

  const appPassword = String(mailbox.appPassword ?? "").replace(/\s/g, "");

  if (!appPassword) {
    throw new MailboxCredentialError(
      `Primeforge mailbox ${wanted} carries no app password — cannot authenticate a send from it`,
    );
  }

  return {
    address: wanted,
    appPassword,
    smtpHost: GMAIL_SMTP_HOST,
    imapHost: GMAIL_IMAP_HOST,
  };
}

/**
 * Resolve one mailbox's live credential: the manual list first, then Primeforge.
 *
 * Manual wins so that a non-Primeforge mailbox (Gandi, Mailforge) resolves at
 * all, and so an operator can override a vendor-reported password without
 * waiting on the vendor. Both keys are PLATFORM keys (global, org-less) — the
 * mailbox fleet is ours, not a customer's.
 *
 * Throws when neither source knows the mailbox. That is the right behaviour for
 * the SEND path: a mailbox we cannot authenticate must abort its send rather
 * than degrade to another sender. (The seed harness wants the opposite for the
 * same fact — an unmeasurable mailbox is skipped, not fatal — so it wraps this
 * resolution in its own batch loader.)
 */
export async function resolveMailboxCredential(
  email: string,
  caller: CallerInfo,
): Promise<MailboxCredential> {
  const manual = await loadManualCredentials(caller);
  const override = selectManualCredential(email, manual);
  if (override) return override;

  // Through the vendor-refusal path: a Primeforge 402 must not stop the
  // dispatch, IMAP poll or reply of a mailbox whose password still works.
  const mailboxes = await loadPrimeforgeMailboxes(caller, "skip");
  return selectMailboxCredential(email, mailboxes);
}

/**
 * Every mailbox we can authenticate as, from both sources at once.
 *
 * The send path resolves ONE mailbox at a time and throws when it cannot, which
 * is right for dispatching. Deciding whether a mailbox is ELIGIBLE for the
 * self-send pipe is a different question asked on the request path, where an
 * exception is the wrong answer and a per-send vendor pagination is the wrong
 * cost. So this reads both sources once and returns the addresses; the caller
 * treats absence as "not eligible", never as an error.
 */
export async function loadCredentialedMailboxes(
  caller: CallerInfo,
): Promise<Set<string>> {
  return new Set((await loadMailboxLogins(caller)).keys());
}

/**
 * Every sending address we hold a credential for, mapped to the REAL MAILBOX it
 * authenticates as.
 *
 * ⚠️ The value is the grain a provider's daily limit is enforced at, and it is
 * NOT the key. Five Gandi aliases resolve to one login, one relay account and
 * one reputation — the relay says so itself, refusing with
 * `450 4.7.1 Too many mail per day for sasl <user>`, per SASL USER. Anything
 * that budgets a day's sending has to group by the value, or it hands one
 * mailbox five times its quota (`growthagency.forum`: three aliases in
 * production at 50/day each against a single 50/day mailbox).
 *
 * For a Primeforge mailbox the address IS the login, so the map is an identity
 * and the grouping costs nothing.
 *
 * ONE loader, two shapes: {@link loadCredentialedMailboxes} is this map's key
 * set. Two loaders would drift, which is the mistake this file already carries a
 * warning about.
 */
export async function loadMailboxLogins(
  caller: CallerInfo,
): Promise<Map<string, string>> {
  const entries = await loadMailboxLoginEntries(caller);
  return new Map([...entries].map(([address, e]) => [address, e.login]));
}

/** Which source holds a mailbox's password. */
export type CredentialSource = "manual" | "primeforge";

export interface MailboxLoginEntry {
  /** The real mailbox (SASL login) this address authenticates as. */
  login: string;
  source: CredentialSource;
}

/**
 * The same map as {@link loadMailboxLogins}, carrying WHERE each credential came
 * from. The mailbox sync persists that provenance; the dispatch path does not
 * care and reads the narrower shape. One loader, three shapes — a fourth loader
 * would drift.
 */
export async function loadMailboxLoginEntries(
  caller: CallerInfo,
  options: { onVendorRefusal?: "skip" | "throw" } = {},
): Promise<Map<string, MailboxLoginEntry>> {
  const manual = await loadManualCredentials(caller);
  const mailboxes = await loadPrimeforgeMailboxes(
    caller,
    options.onVendorRefusal ?? "skip",
  );

  const logins = new Map<string, MailboxLoginEntry>();

  // Manual first, then Primeforge WITHOUT overwriting — the manual layer wins
  // everywhere else in this file, so it wins here too.
  for (const entry of manual) {
    logins.set(entry.address, {
      login: (entry.authUser ?? entry.address).trim().toLowerCase(),
      source: "manual",
    });
  }
  for (const mailbox of mailboxes) {
    const address = String(mailbox.address ?? "").trim().toLowerCase();
    const appPassword = String(mailbox.appPassword ?? "").replace(/\s/g, "");
    if (address && appPassword && !logins.has(address)) {
      logins.set(address, { login: address, source: "primeforge" });
    }
  }
  return logins;
}

// ─── Vendor refusals (SEND-PRIMEFORGE-402-1030) ─────────────────────────────

/**
 * A mailbox vendor whose API refused to list our mailboxes, as last seen by
 * THIS replica. Cleared the moment the vendor answers again.
 */
export interface VendorRefusal {
  vendor: "primeforge";
  /** The vendor's answer, verbatim (e.g. `402 - {"message":"active subscription required"}`). */
  error: string;
  /** First refusal of the current streak. */
  since: string;
  /** Latest refusal. */
  lastSeenAt: string;
  /**
   * When the stored listing its mailboxes are sending on was fetched (ISO), or
   * null when we hold none and its mailboxes are out of the pool.
   */
  lastListingAt: string | null;
  /** How many of its mailboxes that stored listing still credentials. */
  lastListingMailboxes: number;
}

const vendorRefusals = new Map<VendorRefusal["vendor"], VendorRefusal>();

/** Every vendor currently refusing us (empty = every vendor answered on its last read). */
export function currentVendorRefusals(): VendorRefusal[] {
  return [...vendorRefusals.values()];
}

/** Test seam: forget every recorded refusal. */
export function resetVendorRefusals(): void {
  vendorRefusals.clear();
}

/**
 * Primeforge's mailbox list; when Primeforge refuses us, the list it last gave us.
 *
 * ⚠️ A vendor refusing its API (402 "active subscription required") is not its
 * mailboxes dying: they are Google Workspace accounts whose app passwords keep
 * working (IMAP LOGIN OK on 2026-10-30, a day into the 402). So a refusal serves
 * the last listing stored by the infra sync (`primeforge-last-listing.ts`,
 * SEND-POOL-VENDOR-402-1030): before it, the 13 in-production Primeforge
 * mailboxes, the WHOLE send pool, left it and no org sent anything. Only with no
 * stored listing do its mailboxes leave the pool.
 *
 * ⚠️ One vendor refusing us (402, 401, 5xx, timeout) must never take out more
 * than ITS mailboxes, never the whole fleet. Prod
 * 2026-10-29 ~15:00 UTC: Primeforge answered 402, this read threw, and with it
 * every `POST /orgs/send` and every dispatch tick, so the 42 Gandi mailboxes
 * (manual credentials, cheapest fill rank) sent nothing either, for every
 * client. A Primeforge mailbox missing from the map is simply not credentialed:
 * it gets no capacity row, takes no new sequence, and its queued first emails
 * are rehomed (dispatch rule). The refusal stays LOUD: `console.error` on every
 * read and `currentVendorRefusals()` served on the Monitoring infra read.
 *
 * `throw` is for a caller that WRITES the credential provenance (the mailbox
 * sync): there a missing vendor would rewrite every Primeforge row as
 * credential-less.
 */
export async function loadPrimeforgeMailboxes(
  caller: CallerInfo,
  onRefusal: "skip" | "throw",
): Promise<PrimeforgeRawMailbox[]> {
  try {
    const key = await resolvePlatformKey("primeforge", caller);
    const mailboxes = await listPrimeforgeRawMailboxes(key);
    vendorRefusals.delete("primeforge");
    return mailboxes;
  } catch (error) {
    const now = new Date().toISOString();
    const message = errorMessage(error);
    const record = (lastListingAt: string | null, lastListingMailboxes: number) =>
      vendorRefusals.set("primeforge", {
        vendor: "primeforge",
        error: message,
        since: vendorRefusals.get("primeforge")?.since ?? now,
        lastSeenAt: now,
        lastListingAt,
        lastListingMailboxes,
      });
    if (onRefusal === "throw") {
      record(vendorRefusals.get("primeforge")?.lastListingAt ?? null, vendorRefusals.get("primeforge")?.lastListingMailboxes ?? 0);
      throw error;
    }
    let last: Awaited<ReturnType<typeof loadLastPrimeforgeListing>>;
    try {
      last = await loadLastPrimeforgeListing();
    } catch (readError) {
      record(null, 0);
      console.error(
        `[instantly-service] mailbox-credentials: Primeforge REFUSED us (${message}) and its stored listing is unreadable (${errorMessage(readError)}): its mailboxes are out of the send pool, every other vendor sends on`,
      );
      return [];
    }
    record(last.fetchedAt, last.mailboxes.length);
    console.error(
      last.mailboxes.length > 0
        ? `[instantly-service] mailbox-credentials: Primeforge REFUSED us, its ${last.mailboxes.length} mailboxes send on the listing stored ${last.fetchedAt}: ${message}`
        : `[instantly-service] mailbox-credentials: Primeforge REFUSED us and we hold no stored listing, its mailboxes are out of the send pool, every other vendor sends on: ${message}`,
    );
    return last.mailboxes;
  }
}

// ─── Manual credentials (non-Primeforge mailboxes) ──────────────────────────

/** key-service platform provider holding the hand-entered mailbox credentials. */
export const MANUAL_CREDENTIALS_PROVIDER = "mailbox-credentials";

export interface ManualMailboxEntry {
  address: string;
  appPassword: string;
  smtpHost: string;
  imapHost: string;
  /**
   * SMTP/IMAP login when it differs from the address (the Gandi alias case).
   * Optional — omitted means the address is the login.
   */
  authUser?: string;
}

/**
 * Parse the manual-credential payload.
 *
 * Fail loud on a shape we do not recognise: a typo in the stored JSON must not
 * silently degrade to "no manual mailboxes", which would look identical to the
 * key being absent and would send the caller down the Primeforge path for a
 * mailbox Primeforge has never heard of.
 */
export function parseManualCredentials(raw: string): ManualMailboxEntry[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new MailboxCredentialError(
      `${MANUAL_CREDENTIALS_PROVIDER} platform key is not valid JSON: ${
        errorMessage(error)
      }`,
    );
  }

  if (!Array.isArray(parsed)) {
    throw new MailboxCredentialError(
      `${MANUAL_CREDENTIALS_PROVIDER} platform key must be a JSON array of mailbox credentials`,
    );
  }

  return parsed.map((entry, index) => {
    const e = entry as Partial<ManualMailboxEntry> | null;
    const address = String(e?.address ?? "").trim().toLowerCase();
    const appPassword = String(e?.appPassword ?? "").replace(/\s/g, "");
    const smtpHost = String(e?.smtpHost ?? "").trim();
    const imapHost = String(e?.imapHost ?? "").trim();
    const authUser = String(e?.authUser ?? "").trim().toLowerCase();

    if (!address || !appPassword || !smtpHost || !imapHost) {
      throw new MailboxCredentialError(
        `${MANUAL_CREDENTIALS_PROVIDER}[${index}] must carry address, appPassword, smtpHost and imapHost`,
      );
    }

    return {
      address,
      appPassword,
      smtpHost,
      imapHost,
      ...(authUser && authUser !== address ? { authUser } : {}),
    };
  });
}

/** Pure lookup of one address in the manual list. Null when it is not listed. */
export function selectManualCredential(
  email: string,
  entries: readonly ManualMailboxEntry[],
): MailboxCredential | null {
  const wanted = email.trim().toLowerCase();
  const entry = entries.find((e) => e.address === wanted);
  if (!entry) return null;

  return {
    address: entry.address,
    appPassword: entry.appPassword,
    smtpHost: entry.smtpHost,
    imapHost: entry.imapHost,
    ...(entry.authUser ? { authUser: entry.authUser } : {}),
  };
}

/**
 * Read the manual credential list. An absent key means "none configured".
 *
 * The 404 carve-out is deliberately narrow: only a missing key yields an empty
 * list. Every other failure (401, 5xx, a malformed payload) throws, because
 * those mean we could not READ the credentials, which is a different thing from
 * there being none — and treating them the same would send a Gandi mailbox down
 * the Primeforge path that cannot possibly know it.
 */
export async function loadManualCredentials(
  caller: CallerInfo,
): Promise<ManualMailboxEntry[]> {
  try {
    const raw = await resolvePlatformKey(MANUAL_CREDENTIALS_PROVIDER, caller);
    return parseManualCredentials(raw);
  } catch (error) {
    if (error instanceof KeyServiceError && error.statusCode === 404) return [];
    throw error;
  }
}
