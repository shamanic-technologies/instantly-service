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
 * never degrade to some other sender.
 */

import { KeyServiceError, resolvePlatformKey, type CallerInfo } from "../key-client";
import {
  listPrimeforgeRawMailboxes,
  type PrimeforgeRawMailbox,
} from "../providers/primeforge-client";

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

  const key = await resolvePlatformKey("primeforge", caller);
  const mailboxes = await listPrimeforgeRawMailboxes(key);
  return selectMailboxCredential(email, mailboxes);
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
        error instanceof Error ? error.message : String(error)
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
