/**
 * SMTP dispatch from our own mailboxes.
 *
 * One connection per dispatch rather than a pooled transport. The fleet sends at
 * most 50/day per mailbox across dozens of mailboxes, so connection setup is
 * nowhere near the bottleneck, and a pool keyed by mailbox would hold sockets
 * open across the long idle gaps between sends — exactly the long-idle socket
 * that gets closed underneath you and throws (the same failure this repo already
 * documents for its DB connections).
 *
 * Errors are classified by SMTP REPLY CLASS, which is the discriminator this
 * codebase already settled on for account health: 5xx is permanent (a real
 * rejection — a block, a dead recipient domain, a hard throttle) and must stop
 * the step; 4xx is transient (greylisting, a rate limit, a momentary refusal)
 * and is safe to retry later. Nothing is swallowed: an unclassifiable error is
 * treated as transient and surfaced, never as a silent success.
 */

import nodemailer from "nodemailer";

import { GMAIL_SMTP_PORT, loginFor, type MailboxCredential } from "./mailbox-credentials";
import type { BuiltMessage } from "./message";

export interface DispatchResult {
  /** RFC 5322 Message-Id the server accepted, used to thread the next step. */
  messageId: string;
  /** The server's raw reply line — mirrored verbatim into bronze. */
  response: string;
  accepted: string[];
  rejected: string[];
}

export type DispatchFailureKind = "permanent" | "transient";

export class SmtpDispatchError extends Error {
  constructor(
    public readonly kind: DispatchFailureKind,
    /** SMTP reply code when the server gave one, else null. */
    public readonly responseCode: number | null,
    public readonly response: string,
    message: string,
  ) {
    super(message);
    this.name = "SmtpDispatchError";
  }
}

/**
 * Pull an SMTP reply code out of whatever the client threw.
 *
 * nodemailer surfaces `responseCode` on a server rejection, but a socket-level
 * failure carries none — so the raw response text is also scanned for a leading
 * 3-digit reply, and for an RFC 3463 enhanced status code, before giving up.
 */
export function extractReplyCode(error: unknown): number | null {
  const err = error as { responseCode?: unknown; response?: unknown } | null;

  if (typeof err?.responseCode === "number" && Number.isFinite(err.responseCode)) {
    return err.responseCode;
  }

  const response = typeof err?.response === "string" ? err.response : "";
  const basic = /(?:^|\s)([45]\d\d)(?:[\s-]|$)/.exec(response);
  if (basic?.[1]) return Number(basic[1]);

  // Hyphen as well as whitespace: SMTP's multiline continuation form writes the
  // enhanced code straight after the basic one (`550-5.4.5 ...`). The basic
  // branch above already catches that shape, but keeping the two separators
  // consistent means a reply carrying ONLY an enhanced code still classifies.
  const enhanced = /(?:^|[\s-])([45])\.\d+\.\d+/.exec(response);
  if (enhanced?.[1]) return Number(enhanced[1]) * 100;

  return null;
}

/**
 * Classify a dispatch failure.
 *
 * Unknown defaults to TRANSIENT deliberately. Calling an unclassified failure
 * permanent would cancel a lead's remaining steps — and a hold — on what may
 * have been a dropped socket; treating it as transient costs one retry and, if
 * it really is permanent, the server says so again on the next attempt.
 */
export function classifyDispatchFailure(error: unknown): DispatchFailureKind {
  const code = extractReplyCode(error);
  if (code === null) return "transient";
  return code >= 500 && code < 600 ? "permanent" : "transient";
}

/**
 * Send one message from one mailbox.
 *
 * Throws `SmtpDispatchError` on any failure — including a server that accepts
 * the connection but rejects the recipient. A partial success (`rejected`
 * non-empty) is a failure here: one message goes to exactly one prospect, so a
 * rejected recipient means nothing was delivered, and reporting it as a send
 * would fabricate an `email_sent` event.
 */
export async function dispatchMessage(
  credential: MailboxCredential,
  message: BuiltMessage,
): Promise<DispatchResult> {
  const transport = nodemailer.createTransport({
    host: credential.smtpHost,
    port: GMAIL_SMTP_PORT,
    // 587 starts in the clear and upgrades. `secure: false` selects that
    // handshake; `requireTLS` makes the upgrade MANDATORY, so a server that
    // fails to offer STARTTLS aborts the send rather than silently putting the
    // mailbox password on the wire in clear text.
    secure: false,
    requireTLS: true,
    auth: { user: loginFor(credential), pass: credential.appPassword },
  });

  try {
    const info = await transport.sendMail({
      from: message.from,
      to: message.to,
      subject: message.subject,
      html: message.html,
      headers: message.headers,
      ...(message.inReplyTo ? { inReplyTo: message.inReplyTo } : {}),
      ...(message.references ? { references: message.references } : {}),
    });

    const rejected = (info.rejected ?? []).map(String);
    if (rejected.length > 0 || (info.accepted ?? []).length === 0) {
      throw new SmtpDispatchError(
        "permanent",
        null,
        info.response ?? "",
        `SMTP accepted no recipient for ${message.to} (rejected: ${rejected.join(", ") || "none"})`,
      );
    }

    return {
      messageId: info.messageId,
      response: info.response ?? "",
      accepted: (info.accepted ?? []).map(String),
      rejected,
    };
  } catch (error) {
    if (error instanceof SmtpDispatchError) throw error;

    const kind = classifyDispatchFailure(error);
    const code = extractReplyCode(error);
    const response =
      typeof (error as { response?: unknown })?.response === "string"
        ? ((error as { response: string }).response)
        : "";

    throw new SmtpDispatchError(
      kind,
      code,
      response,
      `SMTP dispatch from ${credential.address} to ${message.to} failed (${kind}): ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  } finally {
    transport.close();
  }
}
