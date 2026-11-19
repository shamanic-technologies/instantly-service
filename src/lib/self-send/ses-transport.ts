/**
 * Send one built message through Amazon SES (SES-COLD-1119).
 *
 * SES SMTP interface (`email-smtp.<region>.amazonaws.com:587`, STARTTLS
 * required), authenticated with the marketing account's IAM key and its
 * derived SMTP password. Same `dispatchMessage` as our mailboxes, so the built
 * message, the byte tee and every failure classification are the ones the
 * mailbox path already has: a `454 Throttling failure` is 4xx = transient (the
 * mail waits, never a bounce); `554 Message rejected` (identity, paused
 * account) carries no recipient code = a SENDER fault, the lead untouched.
 *
 * ⚠️ SES REWRITES THE MESSAGE-ID. What recipients see is `<id@email.amazonses.com>`
 * where `id` is the one SES answers in `250 Ok <id>`. That id is what we
 * return (and every caller stores in `smtp_dispatch_raw.message_id`, the seeds
 * in `seed_placement_dispatches`, the send record): replies name it in
 * In-Reply-To, the next step threads on it, the seed sync matches on it, and
 * SES's bounce / complaint events name it. Our own minted id never reaches anyone.
 *
 * Rate: SES allows 14 sends/s for the WHOLE account, shared with the newsletter.
 * Every cold send waits its slot here (`SES_COLD_MAX_PER_SECOND`, default 5 per
 * process); a throttle SES still answers is transient.
 */
import type { CallerInfo } from "../key-client";
import { loadSesAccountCached, SES_COLD_CONFIGURATION_SET, type SesAccount } from "../aws/ses-account";
import { dispatchMessage, type DispatchResult } from "./smtp";
import type { MailboxCredential } from "./mailbox-credentials";
import type { BuiltMessage } from "./message";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/outbound/ses" };
export const SES_MESSAGE_ID_DOMAIN = "email.amazonses.com";
export const SES_DEFAULT_MAX_PER_SECOND = 5;

/** The SES message id inside `250 Ok <id>`, or null (pure). */
export function sesIdFromResponse(response: string): string | null {
  const match = /\b250\s+Ok\s+([0-9A-Za-z-]+)/i.exec(response ?? "");
  return match ? match[1] : null;
}

/** The Message-Id recipients see for an SES id (pure, us-east-1 form). */
export function sesRfcMessageId(sesId: string): string {
  return `<${sesId}@${SES_MESSAGE_ID_DOMAIN}>`;
}

/** The SES id inside a stored `<id@email.amazonses.com>` (pure), or null. */
export function sesIdFromRfcMessageId(messageId: string): string | null {
  const match = /^<([^@>]+)@email\.amazonses\.com>$/i.exec(messageId.trim());
  return match ? match[1] : null;
}

/**
 * The raw bytes as SES carried them: our composed message with its Message-ID
 * header replaced by SES's (pure; header block only, folded header included).
 */
export function withMessageId(raw: Buffer, messageId: string): Buffer {
  const text = raw.toString("latin1");
  const split = /\r?\n\r?\n/.exec(text);
  const headEnd = split ? split.index : text.length;
  const head = text.slice(0, headEnd);
  const replaced = /^message-id:[^\r\n]*(?:\r?\n[ \t][^\r\n]*)*/im.test(head)
    ? head.replace(/^message-id:[^\r\n]*(?:\r?\n[ \t][^\r\n]*)*/im, `Message-ID: ${messageId}`)
    : `Message-ID: ${messageId}\r\n${head}`;
  return Buffer.from(replaced + text.slice(headEnd), "latin1");
}

/** Sends per second this process allows itself (env read at each send). */
export function sesMaxPerSecond(): number {
  const n = Number(process.env.SES_COLD_MAX_PER_SECOND);
  return Number.isFinite(n) && n > 0 ? Math.min(n, 14) : SES_DEFAULT_MAX_PER_SECOND;
}

let nextSlotAt = 0;

/** Wait for this send's slot: at most `sesMaxPerSecond()` starts per second. */
export async function waitForSesSlot(
  now: () => number = Date.now,
  sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
): Promise<void> {
  const gap = 1000 / sesMaxPerSecond();
  const t = now();
  const slot = Math.max(t, nextSlotAt);
  nextSlotAt = slot + gap;
  if (slot > t) await sleep(slot - t);
}

export function resetSesSlots(): void {
  nextSlotAt = 0;
}

/** The SMTP login SES takes, in the shape `dispatchMessage` reads (pure). */
export function sesSmtpCredential(account: SesAccount, senderAddress: string): MailboxCredential {
  return {
    address: senderAddress,
    appPassword: account.smtpPassword,
    smtpHost: account.smtpHost,
    imapHost: "",
    authUser: account.smtpUser,
  } as MailboxCredential;
}

export async function sendThroughSes(
  senderAddress: string,
  message: BuiltMessage,
  deps: { account?: SesAccount; dispatch?: typeof dispatchMessage } = {},
): Promise<DispatchResult> {
  const account = deps.account ?? (await loadSesAccountCached(CALLER));
  await waitForSesSlot();
  const sent = await (deps.dispatch ?? dispatchMessage)(sesSmtpCredential(account, senderAddress), {
    ...message,
    headers: { ...message.headers, "X-SES-CONFIGURATION-SET": SES_COLD_CONFIGURATION_SET },
  });
  const sesId = sesIdFromResponse(sent.response);
  if (!sesId) {
    // The email left; only SES's answer is unreadable. Keep what we sent, say so loudly.
    console.error(`[instantly-service] ses: no id in SES answer "${sent.response}" for ${senderAddress} -> ${message.to}; keeping ${sent.messageId}`);
    return sent;
  }
  const messageId = sesRfcMessageId(sesId);
  return { ...sent, messageId, rawMessage: withMessageId(sent.rawMessage, messageId) };
}
