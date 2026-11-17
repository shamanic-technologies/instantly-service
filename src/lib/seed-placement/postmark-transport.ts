/**
 * Postmark as a seed SENDING path (POSTMARK-PLACEMENT-1117).
 *
 * Kevin is weighing moving cold sending from Google Workspace mailboxes to
 * Postmark. Before that, the same email has to be measured leaving both ways
 * to the same seed receivers. This module is the Postmark half: it takes the
 * SAME `BuiltMessage` a mailbox would send (`buildMessage`: From name,
 * signature, List-Unsubscribe) and hands it to the Postmark API instead of SMTP.
 *
 * ⚠️ NEVER THE TRANSACTIONAL SERVER. Signups, invoices and every product email
 * leave through Postmark server 18354400 (key-service `postmark`). A complaint
 * on cold-looking mail must never touch that server's reputation, so the seed
 * path reads its OWN server token (`postmark-cold-placement`) and refuses to
 * send when that token is the transactional one or resolves to its server id.
 *
 * Correlation: the receivers match seeds on the RFC 5322 Message-Id. We ask
 * Postmark to carry ours, then read the message back from Postmark's dump and
 * keep the Message-Id it really sent (the dump is the truth, our header a wish).
 */

import type { BuiltMessage } from "../self-send/message";
import { KeyServiceError, resolvePlatformKey, type CallerInfo } from "../key-client";

/** key-service platform key holding the cold-placement server's Server API token. */
export const POSTMARK_SEED_PROVIDER = "postmark-cold-placement";
/** key-service platform key of the TRANSACTIONAL server (product mail). Read only to refuse it. */
export const TRANSACTIONAL_POSTMARK_PROVIDER = "postmark";
/** The transactional server (distribute.you product mail). Never a seed sender. */
export const TRANSACTIONAL_POSTMARK_SERVER_ID = 18354400;

const POSTMARK_API = "https://api.postmarkapp.com";

export interface PostmarkSeedServer {
  token: string;
  serverId: number;
  serverName: string;
}

export class PostmarkSeedServerError extends Error {}

export class PostmarkDispatchError extends Error {
  constructor(
    public readonly kind: "permanent" | "transient",
    public readonly response: string,
  ) {
    super(response);
  }
}

async function postmarkJson(
  path: string,
  token: string,
  init: { method?: string; body?: unknown } = {},
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${POSTMARK_API}${path}`, {
    method: init.method ?? "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Postmark-Server-Token": token,
    },
    ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}),
  });
  const text = await response.text();
  let body: Record<string, unknown> = {};
  try {
    body = text ? (JSON.parse(text) as Record<string, unknown>) : {};
  } catch {
    body = { Message: text };
  }
  return { status: response.status, body };
}

/**
 * The cold-placement server, proven NOT to be the transactional one.
 * Throws (never falls back) when the key is missing or points at product mail.
 */
export async function loadPostmarkSeedServer(caller: CallerInfo): Promise<PostmarkSeedServer> {
  let token: string;
  try {
    token = await resolvePlatformKey(POSTMARK_SEED_PROVIDER, caller);
  } catch (error) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      throw new PostmarkSeedServerError(
        `no Postmark cold-placement server configured (key-service platform key "${POSTMARK_SEED_PROVIDER}")`,
      );
    }
    throw error;
  }
  const transactional = await resolvePlatformKey(TRANSACTIONAL_POSTMARK_PROVIDER, caller).catch((error) => {
    if (error instanceof KeyServiceError && error.statusCode === 404) return null;
    throw error;
  });
  if (transactional !== null && transactional.trim() === token.trim()) {
    throw new PostmarkSeedServerError("the cold-placement token IS the transactional server's token: refused");
  }
  const server = await postmarkJson("/server", token);
  if (server.status !== 200 || typeof server.body.ID !== "number") {
    throw new PostmarkSeedServerError(`Postmark GET /server answered ${server.status}: ${JSON.stringify(server.body)}`);
  }
  assertNotTransactional(server.body.ID);
  return { token, serverId: server.body.ID, serverName: String(server.body.Name ?? "") };
}

export function assertNotTransactional(serverId: number): void {
  if (serverId === TRANSACTIONAL_POSTMARK_SERVER_ID) {
    throw new PostmarkSeedServerError(
      `Postmark server ${serverId} carries our transactional email: never a seed sender`,
    );
  }
}

/** Domain part of an address, for the Message-Id we mint. */
function domainOfAddress(from: string): string {
  const match = /<([^>]+)>/.exec(from);
  const address = (match ? match[1] : from).trim();
  return address.slice(address.lastIndexOf("@") + 1).toLowerCase();
}

export function mintPostmarkMessageId(from: string): string {
  return `<${crypto.randomUUID()}@${domainOfAddress(from)}>`;
}

/**
 * The Postmark `/email` body for a built message (pure). Same From, To,
 * Subject, HTML and headers as the SMTP path; no Postmark open or link
 * tracking (our mailboxes track neither), so only the pipe differs.
 */
export function postmarkPayload(message: BuiltMessage, messageId: string): Record<string, unknown> {
  return {
    From: message.from,
    To: message.to,
    Subject: message.subject,
    HtmlBody: message.html,
    Headers: [
      { Name: "Message-ID", Value: messageId },
      ...Object.entries(message.headers).map(([Name, Value]) => ({ Name, Value })),
    ],
    TrackOpens: false,
    TrackLinks: "None",
    MessageStream: "outbound",
  };
}

/** The Message-Id header of a raw RFC 5322 message, or null. */
export function messageIdFromRaw(raw: string): string | null {
  const head = raw.split(/\r?\n\r?\n/, 1)[0] ?? "";
  const unfolded = head.replace(/\r?\n[ \t]+/g, " ");
  const match = /^message-id:\s*(<[^>\s]+>)/im.exec(unfolded);
  return match ? match[1] : null;
}

const DUMP_ATTEMPTS = 6;
const DUMP_WAIT_MS = 2_000;

/**
 * Send one built message through the cold-placement server. Returns the
 * Message-Id the receivers will see.
 */
export async function sendThroughPostmark(
  server: PostmarkSeedServer,
  message: BuiltMessage,
  wait: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
): Promise<{ messageId: string; response: string }> {
  assertNotTransactional(server.serverId);
  const wished = mintPostmarkMessageId(message.from);
  const sent = await postmarkJson("/email", server.token, {
    method: "POST",
    body: postmarkPayload(message, wished),
  });
  const errorCode = typeof sent.body.ErrorCode === "number" ? sent.body.ErrorCode : -1;
  if (sent.status !== 200 || errorCode !== 0) {
    throw new PostmarkDispatchError(
      sent.status >= 500 ? "transient" : "permanent",
      `Postmark ${sent.status} ErrorCode=${errorCode}: ${String(sent.body.Message ?? "")}`,
    );
  }
  const postmarkId = String(sent.body.MessageID);
  for (let attempt = 0; attempt < DUMP_ATTEMPTS; attempt += 1) {
    const dump = await postmarkJson(`/messages/outbound/${postmarkId}/dump`, server.token);
    const raw = typeof dump.body.Body === "string" ? dump.body.Body : "";
    const actual = raw ? messageIdFromRaw(raw) : null;
    if (actual) return { messageId: actual, response: `postmark:${postmarkId}` };
    await wait(DUMP_WAIT_MS);
  }
  // The email left; only its dump is late. Our requested header is what
  // Postmark was asked to carry: keep it, say so.
  console.warn(`[seed-placement] postmark dump of ${postmarkId} unreadable, keeping requested Message-Id ${wished}`);
  return { messageId: wished, response: `postmark:${postmarkId} (dump unread)` };
}
