/**
 * The ONE door every email to a person leaves through (POSTMARK-COLD-1117,
 * SES-COLD-1119).
 *
 * ONE pipe per sending DOMAIN, decided here and nowhere else (`pipeForSender`):
 *   1. `ses` = the domain is live in `ses_sending_domains` (identity, Easy DKIM
 *      and custom MAIL FROM verified on the marketing SES account) AND the kill
 *      switch `SES_COLD_SENDING_DISABLED` is not "true";
 *   2. `postmark` = the domain is live in `postmark_sending_domains` and NOT live
 *      on SES (kept as a per-domain choice; since SES-COLD-1119 no domain uses it);
 *   3. else `smtp`, the mailbox's own relay.
 * The kill switch is read at EVERY send: "true" puts every SES domain back on its
 * mailbox's SMTP, no deploy (edit the env file, recreate the container).
 * Same built message on every pipe (From, signature, threading headers, opt-out
 * header, attachments); replies still land in the mailbox (MX untouched), so
 * the poll, the Unibox and threading are unchanged. A refusal on any pipe is an
 * `SmtpDispatchError` in SMTP words, so every caller's bounce / sender-fault
 * handling reads it unchanged. No silent fallback: an SES domain whose SES
 * account cannot be read fails the send (the mail waits), never leaves by SMTP.
 *
 * Callers: the dispatch worker (sequence steps), reply-to-lead (answers) and the
 * weekly seed test (the seed IS the email a prospect gets). Warmup stays on SMTP.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import type { CallerInfo } from "../key-client";
import { dispatchMessage, SmtpDispatchError, type DispatchResult } from "./smtp";
import {
  MailboxCredentialError,
  resolveMailboxCredential,
  type MailboxCredential,
} from "./mailbox-credentials";
import type { BuiltMessage } from "./message";
import {
  PostmarkDispatchError,
  loadPostmarkSeedServerCached,
  sendThroughPostmark,
} from "../seed-placement/postmark-transport";
import { sendThroughSes } from "./ses-transport";

export type OutboundTransport = "smtp" | "postmark" | "ses";

/** Kill switch, read at each send: "true" = every SES domain sends over SMTP again. */
export { SES_KILL_SWITCH_ENV, sesColdSendingDisabled } from "./ses-kill-switch";
import { sesColdSendingDisabled } from "./ses-kill-switch";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/outbound" };
const LIVE_CACHE_MS = 60_000;
let liveCache: { at: number; pipes: LivePipes } | null = null;

/** The sending address inside a From header (pure). */
export function senderAddressOf(from: string): string {
  const match = /<([^>]+)>/.exec(from);
  return (match ? match[1] : from).trim().toLowerCase();
}

export function domainOfAddress(address: string): string {
  return address.slice(address.lastIndexOf("@") + 1).toLowerCase();
}

export interface LivePipes {
  ses: ReadonlySet<string>;
  postmark: ReadonlySet<string>;
}

/** Domains live on each pipe, held 60 s. Fails loud. */
export async function loadLivePipes(): Promise<LivePipes> {
  if (liveCache && Date.now() - liveCache.at < LIVE_CACHE_MS) return liveCache.pipes;
  const result = await db.execute(sql`
    SELECT 'ses' AS pipe, domain FROM ses_sending_domains WHERE live_since IS NOT NULL
    UNION ALL
    SELECT 'postmark', domain FROM postmark_sending_domains WHERE live_since IS NOT NULL
  `);
  const rows = ((result as { rows?: unknown[] }).rows ?? []) as { pipe: string; domain: string }[];
  const pipes: LivePipes = {
    ses: new Set(rows.filter((r) => r.pipe === "ses").map((r) => r.domain.toLowerCase())),
    postmark: new Set(rows.filter((r) => r.pipe === "postmark").map((r) => r.domain.toLowerCase())),
  };
  liveCache = { at: Date.now(), pipes };
  return pipes;
}

/** Drop the cache (after an onboarding flips a domain live; tests). */
export function clearLivePipesCache(): void {
  liveCache = null;
}

/** Which pipe a sending address uses right now (pure). The one decision. */
export function pipeForSender(
  senderEmail: string,
  live: LivePipes,
  sesDisabled: boolean = sesColdSendingDisabled(),
): OutboundTransport {
  const domain = domainOfAddress(senderEmail.trim().toLowerCase());
  if (live.ses.has(domain)) return sesDisabled ? "smtp" : "ses";
  if (live.postmark.has(domain)) return "postmark";
  return "smtp";
}

/**
 * The credential a SEND from this address needs (PF-CLOUDFLARE-1120): the
 * mailbox's own login, or null when the address sends through SES and we hold
 * no login for it (a domain whose mailboxes are gone, sending only through SES:
 * the Primeforge domains after 2026-11-29). An address with neither throws
 * exactly as `resolveMailboxCredential` does: never sent from anywhere else.
 */
export async function resolveSendCredential(
  address: string,
  caller: CallerInfo,
): Promise<MailboxCredential | null> {
  try {
    return await resolveMailboxCredential(address, caller);
  } catch (error) {
    if (error instanceof MailboxCredentialError && pipeForSender(address, await loadLivePipes()) === "ses") return null;
    throw error;
  }
}

export async function sendFromMailbox(
  credential: MailboxCredential | null,
  message: BuiltMessage,
): Promise<DispatchResult & { transport: OutboundTransport }> {
  const sender = senderAddressOf(message.from);
  const transport = pipeForSender(sender, await loadLivePipes());
  if (transport === "smtp") {
    // A send-only address (no login) whose SES pipe is off: it has no way out.
    if (!credential) throw new MailboxCredentialError(`no credential for ${sender} and its domain is not live on SES: cannot send from it`);
    return { ...(await dispatchMessage(credential, message)), transport };
  }
  if (transport === "ses") return { ...(await sendThroughSes(sender, message)), transport };

  const server = await loadPostmarkSeedServerCached(CALLER);
  try {
    const sent = await sendThroughPostmark(server, message);
    return {
      messageId: sent.messageId,
      response: sent.response,
      accepted: [message.to],
      rejected: [],
      rawMessage: sent.rawMessage,
      transport,
    };
  } catch (error) {
    if (error instanceof PostmarkDispatchError) {
      throw new SmtpDispatchError(error.kind, error.responseCode, error.response, error.response);
    }
    throw error;
  }
}
