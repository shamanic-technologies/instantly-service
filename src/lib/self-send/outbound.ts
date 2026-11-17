/**
 * The ONE door every email to a person leaves through (POSTMARK-COLD-1117).
 *
 * A mailbox whose DOMAIN is live on Postmark (`postmark_sending_domains.live_since`,
 * DKIM + Return-Path verified) sends through the Postmark cold server (never the
 * transactional one); every other mailbox sends over its own SMTP as before.
 * Same built message either way (From, signature, threading headers, opt-out
 * header, attachments); replies still land in the mailbox (MX untouched), so the
 * poll, the Unibox and threading are unchanged. A Postmark refusal is thrown as
 * an `SmtpDispatchError` in SMTP words, so every caller's bounce / sender-fault
 * handling reads it unchanged.
 *
 * Callers: the dispatch worker (sequence steps), reply-to-lead (answers) and the
 * weekly seed test (the seed IS the email a prospect gets). Warmup stays on SMTP.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import type { CallerInfo } from "../key-client";
import { dispatchMessage, SmtpDispatchError, type DispatchResult } from "./smtp";
import type { MailboxCredential } from "./mailbox-credentials";
import type { BuiltMessage } from "./message";
import {
  PostmarkDispatchError,
  loadPostmarkSeedServerCached,
  sendThroughPostmark,
} from "../seed-placement/postmark-transport";

export type OutboundTransport = "smtp" | "postmark";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/outbound" };
const LIVE_CACHE_MS = 60_000;
let liveCache: { at: number; domains: Set<string> } | null = null;

/** The sending address inside a From header (pure). */
export function senderAddressOf(from: string): string {
  const match = /<([^>]+)>/.exec(from);
  return (match ? match[1] : from).trim().toLowerCase();
}

export function domainOfAddress(address: string): string {
  return address.slice(address.lastIndexOf("@") + 1).toLowerCase();
}

/** Domains whose mail leaves through Postmark, held 60 s. Fails loud. */
export async function loadPostmarkLiveDomains(): Promise<Set<string>> {
  if (liveCache && Date.now() - liveCache.at < LIVE_CACHE_MS) return liveCache.domains;
  const result = await db.execute(sql`SELECT domain FROM postmark_sending_domains WHERE live_since IS NOT NULL`);
  const rows = ((result as { rows?: unknown[] }).rows ?? []) as { domain: string }[];
  const domains = new Set(rows.map((r) => r.domain.toLowerCase()));
  liveCache = { at: Date.now(), domains };
  return domains;
}

/** Drop the cache (after an onboarding flips a domain live; tests). */
export function clearPostmarkLiveDomainsCache(): void {
  liveCache = null;
}

/** Which pipe a sending address uses right now (pure over the live set). */
export function transportForSender(senderEmail: string, liveDomains: ReadonlySet<string>): OutboundTransport {
  return liveDomains.has(domainOfAddress(senderEmail.trim().toLowerCase())) ? "postmark" : "smtp";
}

export async function sendFromMailbox(
  credential: MailboxCredential,
  message: BuiltMessage,
): Promise<DispatchResult & { transport: OutboundTransport }> {
  const transport = transportForSender(senderAddressOf(message.from), await loadPostmarkLiveDomains());
  if (transport === "smtp") return { ...(await dispatchMessage(credential, message)), transport };

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
