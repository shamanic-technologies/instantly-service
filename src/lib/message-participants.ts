/**
 * EVERY address on a message is a person (EVERY-PERSON-ON-THE-THREAD-1109,
 * owner 2026-11-09).
 *
 * Real case: our cold email went to Thomas (our lead); Thomas forwarded it
 * internally and his colleague Markus answered in the same thread. Bronze kept
 * Markus as the sender, but every reader projected the message onto the
 * thread's lead (Thomas), so Markus existed nowhere and the AI's answer went to
 * Thomas only. The fix is not a rule about forwards or colleagues: every
 * reader states, for every message, EVERY address on it with its role, read
 * from what the message itself carried. Who those people are (lead, client,
 * colleague, about the brand or not) is judged downstream (crm-service, Jev),
 * never here.
 *
 * Pure: parses the shapes our three stores hold.
 *  - IMAP bronze (`imap_messages_raw.payload.headers.*`): each address header is
 *    a JSON STRING of mailparser's `{value:[{address,name}], text, html}`;
 *    `references` a JSON string array.
 *  - our dispatch log (`smtp_dispatch_raw.payload`): `cc` / `bcc` / `to` as one
 *    comma-separated string (what nodemailer and Instantly take).
 *  - the Instantly mirror: `from_address_email`, `to_address_email_list`,
 *    `cc_address_email_list`, `bcc_address_email_list` (comma-separated).
 */

export type ParticipantRole = "from" | "to" | "cc" | "bcc";

export interface MessageParticipant {
  /** Lowercased bare address. */
  email: string;
  role: ParticipantRole;
}

export interface MessageAddress {
  /** Lowercased bare address. */
  address: string;
  name: string | null;
}

const ADDRESS_RE = /[^\s<>(),;:"']+@[^\s<>(),;:"']+\.[^\s<>(),;:"']+/g;

function cleanAddress(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const match = value.trim().match(ADDRESS_RE);
  if (!match) return null;
  return match[0].replace(/\.+$/, "").toLowerCase();
}

function cleanName(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const name = value.trim().replace(/^"+|"+$/g, "").trim();
  return name === "" ? null : name;
}

function dedupe(list: MessageAddress[]): MessageAddress[] {
  const seen = new Set<string>();
  const out: MessageAddress[] = [];
  for (const a of list) {
    if (seen.has(a.address)) continue;
    seen.add(a.address);
    out.push(a);
  }
  return out;
}

/**
 * One address header, in whatever shape it was stored: a mailparser object (or
 * its JSON string), an array of them, `"Name" <a@b>` text, or a comma list.
 * Never throws; what carries no address yields nothing.
 */
export function parseAddressHeader(raw: unknown): MessageAddress[] {
  if (raw === null || raw === undefined) return [];
  if (Array.isArray(raw)) return dedupe(raw.flatMap((r) => parseAddressHeader(r)));
  if (typeof raw === "object") {
    const obj = raw as Record<string, unknown>;
    if (Array.isArray(obj.value)) {
      return dedupe(
        obj.value.flatMap((v) => {
          const entry = (v ?? {}) as Record<string, unknown>;
          // A group (`undisclosed-recipients:`) nests its members.
          if (Array.isArray(entry.group)) return parseAddressHeader({ value: entry.group });
          const address = cleanAddress(entry.address);
          return address ? [{ address, name: cleanName(entry.name) }] : [];
        }),
      );
    }
    if (typeof obj.address === "string") {
      const address = cleanAddress(obj.address);
      return address ? [{ address, name: cleanName(obj.name) }] : [];
    }
    if (typeof obj.text === "string") return parseAddressHeader(obj.text);
    return [];
  }
  if (typeof raw !== "string") return [];
  const text = raw.trim();
  if (text === "") return [];
  if (text.startsWith("{") || text.startsWith("[")) {
    try {
      return parseAddressHeader(JSON.parse(text));
    } catch {
      // not JSON after all: read it as header text below
    }
  }
  // Header text: split on commas/semicolons that sit outside quotes.
  const parts: string[] = [];
  let current = "";
  let quoted = false;
  for (const ch of text) {
    if (ch === '"') quoted = !quoted;
    if ((ch === "," || ch === ";") && !quoted) {
      parts.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  parts.push(current);
  return dedupe(
    parts.flatMap((part) => {
      const address = cleanAddress(part);
      if (!address) return [];
      const angle = part.indexOf("<");
      const name = angle > 0 ? cleanName(part.slice(0, angle)) : null;
      return [{ address, name }];
    }),
  );
}

/** A Message-Id list header (References), in any stored shape, oldest first. */
export function parseMessageIdList(raw: unknown): string[] {
  if (raw === null || raw === undefined) return [];
  if (Array.isArray(raw)) return raw.flatMap((r) => parseMessageIdList(r));
  if (typeof raw !== "string") return [];
  const text = raw.trim();
  if (text === "") return [];
  if (text.startsWith("[")) {
    try {
      return parseMessageIdList(JSON.parse(text));
    } catch {
      // fall through to the token read
    }
  }
  const ids = text.match(/<[^<>\s]+>/g);
  if (ids) return ids;
  return text.split(/\s+/).filter((t) => t.length > 0);
}

/** One Message-Id header value (In-Reply-To, Message-ID), or null. */
export function parseMessageId(raw: unknown): string | null {
  const ids = parseMessageIdList(raw);
  return ids.length > 0 ? ids[0] : null;
}

/** The thread a message belongs to: the RFC root, References[0] ?? In-Reply-To ?? its own id. */
export function threadKeyOf(
  references: string[],
  inReplyTo: string | null,
  messageId: string | null,
): string | null {
  return references[0] ?? inReplyTo ?? messageId ?? null;
}

/** The role-tagged list every reader serves (one entry per address per role). */
export function participantsOf(parts: {
  from: MessageAddress | null;
  to: MessageAddress[];
  cc: MessageAddress[];
  bcc: MessageAddress[];
}): MessageParticipant[] {
  const out: MessageParticipant[] = [];
  const push = (role: ParticipantRole, list: MessageAddress[]) => {
    const seen = new Set<string>();
    for (const a of list) {
      if (seen.has(a.address)) continue;
      seen.add(a.address);
      out.push({ email: a.address, role });
    }
  };
  push("from", parts.from ? [parts.from] : []);
  push("to", parts.to);
  push("cc", parts.cc);
  push("bcc", parts.bcc);
  return out;
}

export interface MessageEnvelope {
  from: MessageAddress | null;
  to: MessageAddress[];
  cc: MessageAddress[];
  bcc: MessageAddress[];
  messageId: string | null;
  inReplyTo: string | null;
  references: string[];
  threadKey: string | null;
  participants: MessageParticipant[];
}

function envelope(e: Omit<MessageEnvelope, "threadKey" | "participants">): MessageEnvelope {
  return {
    ...e,
    threadKey: threadKeyOf(e.references, e.inReplyTo, e.messageId),
    participants: participantsOf(e),
  };
}

function headerOf(headers: Record<string, unknown>, name: string): unknown {
  return headers[name];
}

/**
 * A message our IMAP poll (or a mirror recovery) stored. `fromAddress` and
 * `accountEmail` are the row's own columns; the headers win when present.
 */
export function imapEnvelope(row: {
  accountEmail: string;
  fromAddress: string | null;
  messageId: string | null;
  payload: unknown;
}): MessageEnvelope {
  const payload = (row.payload ?? {}) as Record<string, unknown>;
  const headers = (payload.headers ?? {}) as Record<string, unknown>;
  const from =
    parseAddressHeader(headerOf(headers, "from"))[0] ??
    parseAddressHeader(payload.from)[0] ??
    parseAddressHeader(row.fromAddress)[0] ??
    null;
  let to = parseAddressHeader(headerOf(headers, "to"));
  if (to.length === 0) to = parseAddressHeader(payload.to);
  if (to.length === 0) {
    const own = cleanAddress(row.accountEmail);
    to = own ? [{ address: own, name: null }] : [];
  }
  const cc = parseAddressHeader(headerOf(headers, "cc"));
  const bcc = parseAddressHeader(headerOf(headers, "bcc"));
  const references = parseMessageIdList(headerOf(headers, "references"));
  return envelope({
    from,
    to,
    cc,
    bcc,
    messageId: parseMessageId(headerOf(headers, "message-id")) ?? row.messageId,
    inReplyTo: parseMessageId(headerOf(headers, "in-reply-to")),
    references:
      references.length > 0 ? references : parseMessageIdList(payload.referencedMessageIds),
  });
}

/**
 * An email WE dispatched (`smtp_dispatch_raw`). A sequence step went to the
 * lead alone; an answer carries its own `to` (when the caller chose one) and
 * its `cc` / `bcc`.
 */
export function dispatchEnvelope(row: {
  accountEmail: string;
  accountName?: string | null;
  leadEmail: string;
  messageId: string | null;
  payload: unknown;
}): MessageEnvelope {
  const payload = (row.payload ?? {}) as Record<string, unknown>;
  const own = cleanAddress(row.accountEmail);
  let to = parseAddressHeader(payload.to);
  if (to.length === 0) to = parseAddressHeader(row.leadEmail);
  const references = parseMessageIdList(payload.references);
  const inReplyTo = parseMessageId(payload.inReplyTo);
  return envelope({
    from: own ? { address: own, name: cleanName(row.accountName ?? null) } : null,
    to,
    cc: parseAddressHeader(payload.cc),
    bcc: parseAddressHeader(payload.bcc),
    messageId: row.messageId,
    inReplyTo,
    references,
  });
}

/** One row of the Instantly Unibox mirror (`instantly_emails_raw.payload`). */
export function instantlyEnvelope(payload: unknown): MessageEnvelope {
  const p = (payload ?? {}) as Record<string, unknown>;
  const from = parseAddressHeader(p.from_address_email)[0] ?? null;
  return envelope({
    from,
    to: parseAddressHeader(p.to_address_email_list),
    cc: parseAddressHeader(p.cc_address_email_list),
    bcc: parseAddressHeader(p.bcc_address_email_list),
    messageId: parseMessageId(p.message_id),
    inReplyTo: null,
    references: [],
  });
}
