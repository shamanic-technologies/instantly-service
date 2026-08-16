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
 * Fail loud throughout: a mailbox we cannot authenticate must abort the send,
 * never degrade to some other sender.
 */

import { resolvePlatformKey, type CallerInfo } from "../key-client";
import {
  listPrimeforgeRawMailboxes,
  type PrimeforgeRawMailbox,
} from "../providers/primeforge-client";

export const GMAIL_SMTP_HOST = "smtp.gmail.com";
export const GMAIL_IMAP_HOST = "imap.gmail.com";

/** Implicit TLS. No STARTTLS negotiation to get wrong. */
export const GMAIL_SMTP_PORT = 465;
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
 * Resolve one mailbox's live credential: platform Primeforge key from
 * key-service, then the vendor mailbox list, then the pure selection above.
 *
 * The Primeforge key is a PLATFORM key (global, org-less) — the mailbox fleet is
 * ours, not a customer's, so this is the same resolution path the provider-infra
 * sync already uses.
 */
export async function resolveMailboxCredential(
  email: string,
  caller: CallerInfo,
): Promise<MailboxCredential> {
  const key = await resolvePlatformKey("primeforge", caller);
  const mailboxes = await listPrimeforgeRawMailboxes(key);
  return selectMailboxCredential(email, mailboxes);
}
