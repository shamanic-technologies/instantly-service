/**
 * Reading what comes BACK — pure classification, no IO.
 *
 * Once we dispatch ourselves, nobody tells us a prospect replied or a message
 * bounced: it arrives as mail in the sending mailbox, and we have to read it.
 * Three things can land there and they must not be confused, because two of them
 * end a sequence and one of them must not.
 *
 * The correlation key is our own `Message-Id`. A real reply carries it in
 * `In-Reply-To`/`References`; a bounce carries it inside the delivery-status
 * report. Matching on the recipient address instead would be wrong — the same
 * prospect can legitimately be in two sequences — and matching on subject is
 * hopeless once a client rewrites it.
 */

/** Header names are case-insensitive on the wire; callers pass them lowercased. */
export type InboundHeaders = Readonly<Record<string, string | undefined>>;

export type InboundKind = "reply" | "auto_reply" | "bounce" | "unrelated";

export interface InboundClassification {
  kind: InboundKind;
  /** Our Message-Ids this message refers to, most specific first. */
  referencedMessageIds: string[];
}

/** Pull `<...>` tokens out of a header that may list several. */
export function parseMessageIdList(value: string | undefined): string[] {
  if (!value) return [];
  return [...value.matchAll(/<[^<>@\s]+@[^<>\s]+>/g)].map((m) => m[0]);
}

/**
 * Is this an automatic response rather than a human reply?
 *
 * **Load-bearing.** `reply_received` is in `SEQUENCE_STOP_EVENTS`: it halts the
 * sequence and cancels the lead's remaining holds. An out-of-office autoresponder
 * treated as a reply would therefore end the outreach — and refund the spend —
 * for a prospect who has not actually engaged and will be back at their desk next
 * week. Silver already has the right vocabulary for this: `auto_reply_received`
 * classifies neutral and is deliberately NOT a stop event.
 *
 * RFC 3834 `Auto-Submitted` is the standard signal and the only one that is
 * normative; the rest are the widely-deployed conventions that predate it
 * (Microsoft's suppression header, the `Precedence` family, vendor `X-Auto*`).
 * Any one of them is enough — an autoresponder that announces itself in only one
 * way is still an autoresponder.
 */
export function isAutoReply(headers: InboundHeaders): boolean {
  const autoSubmitted = headers["auto-submitted"]?.trim().toLowerCase();
  if (autoSubmitted && autoSubmitted !== "no") return true;

  if (headers["x-autoreply"] || headers["x-autorespond"]) return true;
  if (headers["x-auto-response-suppress"]) return true;

  const precedence = headers["precedence"]?.trim().toLowerCase();
  if (precedence === "auto_reply" || precedence === "bulk" || precedence === "junk") {
    return true;
  }

  // Microsoft and Google both stamp vacation replies with this.
  const autoreply = headers["x-auto-reply"]?.trim().toLowerCase();
  return autoreply === "yes" || autoreply === "true";
}

/**
 * Is this a delivery-status notification (an async bounce)?
 *
 * The MIME type is authoritative (RFC 3462) and is checked first. The
 * `MAILER-DAEMON` / `postmaster` envelope is the fallback for the many mail
 * systems that still send a plain-text bounce with no report part, and the
 * null return-path is the RFC 5321 marker that a message is itself a
 * notification and must not be replied to.
 */
export function isDeliveryStatusNotification(headers: InboundHeaders): boolean {
  const contentType = headers["content-type"]?.toLowerCase() ?? "";
  if (contentType.includes("report-type=delivery-status")) return true;

  if (headers["return-path"]?.trim() === "<>") return true;

  const from = headers["from"]?.toLowerCase() ?? "";
  return from.includes("mailer-daemon") || from.includes("postmaster@");
}

/**
 * Recover the Message-Id of the message a bounce is about.
 *
 * A DSN quotes the original message, so its own `In-Reply-To` is unreliable —
 * some systems set it, many do not. The quoted headers in the body are what
 * actually carry the id, so both places are searched and everything found is
 * returned; the caller matches against ids we know we sent, which makes a false
 * positive from an unrelated quoted id harmless.
 */
export function extractBouncedMessageIds(
  headers: InboundHeaders,
  body: string,
): string[] {
  const fromHeaders = [
    ...parseMessageIdList(headers["in-reply-to"]),
    ...parseMessageIdList(headers["references"]),
  ];

  // `Original-Message-ID` is the RFC 3464 field; a quoted `Message-ID:` line is
  // what everyone else leaves behind.
  const fromBody = [
    ...parseMessageIdList(/original-message-id:\s*([^\n]+)/i.exec(body)?.[1]),
    ...parseMessageIdList(body.match(/^\s*message-id:\s*[^\n]+/gim)?.join("\n")),
  ];

  return [...new Set([...fromHeaders, ...fromBody])];
}

/**
 * Classify one inbound message against the ids we know we sent.
 *
 * ORDER MATTERS. A bounce is checked before an auto-reply because some systems
 * stamp DSNs with `Auto-Submitted: auto-replied` — reading that first would file
 * a hard bounce as a harmless vacation notice and leave the sequence running at
 * a dead address. An auto-reply is then checked before a plain reply, so a
 * vacation message never stops a sequence.
 *
 * Anything that references none of our ids is `unrelated` and touched by nothing:
 * these are real mailboxes that also receive ordinary mail, and acting on a
 * message we cannot tie to a send would attribute a stranger's email to a lead.
 */
export function classifyInbound(
  headers: InboundHeaders,
  body: string,
  knownMessageIds: ReadonlySet<string>,
): InboundClassification {
  if (isDeliveryStatusNotification(headers)) {
    const referenced = extractBouncedMessageIds(headers, body).filter((id) =>
      knownMessageIds.has(id),
    );
    return {
      kind: referenced.length > 0 ? "bounce" : "unrelated",
      referencedMessageIds: referenced,
    };
  }

  const referenced = [
    ...parseMessageIdList(headers["in-reply-to"]),
    ...parseMessageIdList(headers["references"]),
  ].filter((id) => knownMessageIds.has(id));

  if (referenced.length === 0) {
    return { kind: "unrelated", referencedMessageIds: [] };
  }

  return {
    kind: isAutoReply(headers) ? "auto_reply" : "reply",
    referencedMessageIds: referenced,
  };
}

/** The silver event type each classification promotes. `unrelated` promotes none. */
export function eventTypeForInbound(kind: InboundKind): string | null {
  switch (kind) {
    case "reply":
      return "reply_received";
    // Neutral, and deliberately NOT a sequence-stop event — see `isAutoReply`.
    case "auto_reply":
      return "auto_reply_received";
    case "bounce":
      return "email_bounced";
    default:
      return null;
  }
}
