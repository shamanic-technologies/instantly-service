/**
 * Ask a blocklist to remove a listed sending domain, at most once a week per
 * (domain, list) (owner 2026-10-16: "Automate the auto form filling in the
 * backend with a 1 week cool pause, to not spam them").
 *
 * SURBL only, and BY EMAIL to whitelist@surbl.org, the channel SURBL's own
 * removal page offers ("If you can't use the web form, then please send all
 * information required above by plaintext email to: whitelist@surbl.org").
 * Its web form is NOT ours to drive: the site forbids automated use and
 * refuses a post "submitted faster than a person can fill it in" (measured
 * 2026-10-16, every one of 11 posts refused). The email carries every field
 * the form asks for, is sent from the company's own mailbox (`company.email`,
 * so SURBL's answer reaches the owner) and blind-copies that address so the
 * owner holds what was sent. Spamhaus and URIBL need an owner account: their
 * alert keeps the link.
 *
 * Every attempt is stored (owner: "Report this form filling success or failure
 * in BSG event storing"): bronze `domain_blocklist_removal_attempts` keeps what
 * we sent and the SMTP answer, silver is the event history
 * (`removal_requested` when SMTP accepted it, `removal_failed` otherwise), gold
 * is the daily `removalRequests` series of the staff read.
 *
 * What we tell SURBL is true and only that: who we are (read from the owner's
 * own record, kevinlourd.com, never guessed), a real email of ours as sent, our
 * real sending practice and the anti-spam clause our Terms publish.
 */

import nodemailer from "nodemailer";
import { sql } from "drizzle-orm";

import { db } from "../db";
import { domainBlocklistEvents, domainBlocklistRemovalAttempts } from "../db/schema";
import type { BlocklistKey } from "./domain-blocklist";

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

/** One request per (domain, list) per this many days, whatever the list answered. */
export const REMOVAL_COOLDOWN_DAYS = 7;
const MS_DAY = 86_400_000;

/** SURBL's documented removal address (on its lookup answer page). */
export const SURBL_REMOVAL_ADDRESS = "whitelist@surbl.org";

/** The person and company filing, as the owner's own record states them. */
export interface RemovalContact {
  fullName: string;
  email: string;
  phone: string;
  organization: string;
  /** Street, postal code, city, country on one line. */
  address: string;
  website: string;
}

export interface SampleMessage {
  headers: string;
  body: string;
}

/** `refused` = the receiving server permanently refused it; `not_listed` is no longer produced (kept for rows of 2026-10-16). */
export type FilingOutcome = "sent" | "refused" | "not_listed" | "failed";

export interface FilingResult {
  outcome: FilingOutcome;
  /** SMTP reply code when the server answered. */
  httpStatus: number | null;
  /** The SMTP server's answer. */
  responseExcerpt: string | null;
  error: string | null;
  /** What we sent (fields + the email), stored in bronze. */
  request: Record<string, string> | null;
}

/** The email that carries a removal request. */
export interface RemovalEmail {
  from: string;
  to: string;
  bcc: string;
  subject: string;
  text: string;
}

/** What sending one removal email answered. Throws on failure, with `responseCode` when the server answered. */
export type SendRemovalEmail = (email: RemovalEmail) => Promise<{ response: string; code: number | null }>;

// ─── Pure ────────────────────────────────────────────────────────────────────

/** True when no request for this pair reached the list within the cooldown. Pure. */
export function removalDue(lastSentAt: Date | null, now: Date): boolean {
  return lastSentAt === null || now.getTime() - lastSentAt.getTime() >= REMOVAL_COOLDOWN_DAYS * MS_DAY;
}

/** The form's fields. Pure: only facts we hold, in plain ASCII as SURBL asks. */
export function removalFormFields(input: {
  domain: string;
  contact: RemovalContact;
  sample: SampleMessage;
  sendingServers: string;
  stoppedNewContactsOn: string;
}): Record<string, string> {
  const { domain, contact } = input;
  const ascii = (s: string) => s.normalize("NFKD").replace(/[^\x09\x0A\x0D\x20-\x7E]/g, "");
  return {
    name: ascii(contact.fullName),
    email: ascii(contact.email),
    org: ascii(contact.organization),
    address1: ascii(contact.address),
    phone: ascii(contact.phone),
    fp: domain,
    servers: ascii(input.sendingServers),
    headers: ascii(input.sample.headers).slice(0, 10_000),
    body: ascii(input.sample.body).slice(0, 100_000),
    describe: ascii(
      `${contact.organization} runs ${contact.website}, a B2B sales platform. ${domain} is one of its sending domains: ` +
        "it sends one-to-one business emails on behalf of the platform's business clients. Nobody else sends from it.",
    ),
    justify: ascii(
      `Every email from ${domain} is a one-to-one message to a business address, written for that recipient. ` +
        "Every address is checked by an email verifier before we send; addresses found invalid or risky are never emailed. " +
        "Every email carries a one-click unsubscribe (RFC 8058 List-Unsubscribe), honoured at once, and any reply stops the sequence. " +
        `When we learned this domain was listed, it stopped taking new contacts (${input.stoppedNewContactsOn}); ` +
        "only follow-ups to people already in a conversation continue. No affiliate or third party sends from this domain.",
    ),
    policy_link: `${contact.website.replace(/\/$/, "")}/terms`,
    policy:
      "Terms of Service, section 9 (Acceptable Use): You will not, and will not attempt to: Use the Service to send unsolicited bulk email in violation of any anti-spam law. " +
      "Contact recipients on suppression lists, who have unsubscribed, or who have not provided a lawful basis to be contacted.",
  };
}

/** The plaintext email carrying every field SURBL's form asks for, in its order. Pure. */
export function removalEmail(fields: Record<string, string>): RemovalEmail {
  const text = [
    `SURBL blacklist removal request for ${fields.fp}`,
    "",
    `1. Your name: ${fields.name}`,
    `2. Your email address: ${fields.email}`,
    `3. Organization name: ${fields.org}`,
    `4. Street address: ${fields.address1}`,
    `5. Telephone number: ${fields.phone}`,
    `6. Web site domain name: ${fields.fp}`,
    `7. Sending IP addresses: ${fields.servers}`,
    "",
    "8. Full headers of a typical, normal message:",
    fields.headers,
    "",
    "Full source of the typical, normal message body:",
    fields.body,
    "",
    "9. Description of web site and organization:",
    fields.describe,
    "",
    "10. Explanation:",
    fields.justify,
    "",
    `11. Link to the organization's published policy against unsolicited messages: ${fields.policy_link}`,
    "Text of that policy:",
    fields.policy,
    "",
    fields.name,
    fields.org,
  ].join("\n");
  return {
    from: `"${fields.name.replace(/"/g, "")}" <${fields.email}>`,
    to: SURBL_REMOVAL_ADDRESS,
    bcc: fields.email,
    subject: `Removal request: ${fields.fp}`,
    text,
  };
}

/** Sent, refused for good (5xx), or failed (anything else). Pure. */
export function filingFromError(error: unknown, request: Record<string, string>): FilingResult {
  const e = error as { responseCode?: unknown; response?: unknown; message?: unknown };
  const code = typeof e?.responseCode === "number" ? e.responseCode : null;
  return {
    outcome: code !== null && code >= 500 && code < 600 ? "refused" : "failed",
    httpStatus: code,
    responseExcerpt: typeof e?.response === "string" ? e.response.slice(0, 2000) : null,
    error: error instanceof Error ? error.message : String(error),
    request,
  };
}

// ─── Send (SURBL, by email) ──────────────────────────────────────────────────

/** Send through the company mailbox's own SMTP login (the mailbox-credentials store). */
export function smtpSender(fromAddress: string): SendRemovalEmail {
  return async (email) => {
    const { resolveMailboxCredential, loginFor, GMAIL_SMTP_PORT } = await import("./self-send/mailbox-credentials");
    const credential = await resolveMailboxCredential(fromAddress, {
      method: "POST",
      path: "/internal/domain-blocklist/removals",
    });
    const transport = nodemailer.createTransport({
      host: credential.smtpHost,
      port: GMAIL_SMTP_PORT,
      secure: false,
      requireTLS: true,
      auth: { user: loginFor(credential), pass: credential.appPassword },
    });
    try {
      const info = await transport.sendMail(email);
      const accepted = (info.accepted ?? []).map((a) => String(a).toLowerCase());
      if (!accepted.includes(email.to.toLowerCase())) {
        throw Object.assign(new Error(`SMTP did not accept ${email.to}`), { response: info.response ?? "" });
      }
      return { response: info.response ?? "", code: Number(/^(\d{3})/.exec(info.response ?? "")?.[1] ?? NaN) || null };
    } finally {
      transport.close();
    }
  };
}

/** Build and send one domain's removal request. Never throws: a failure is an outcome, stored like any other. */
export async function fileSurblRemoval(input: {
  domain: string;
  contact: RemovalContact;
  sample: SampleMessage;
  sendingServers: string;
  stoppedNewContactsOn: string;
  send?: SendRemovalEmail;
}): Promise<FilingResult> {
  const fields = removalFormFields(input);
  const email = removalEmail(fields);
  const request = { ...fields, emailTo: email.to, emailSubject: email.subject };
  try {
    const answer = await (input.send ?? smtpSender(input.contact.email))(email);
    return { outcome: "sent", httpStatus: answer.code, responseExcerpt: answer.response.slice(0, 2000), error: null, request };
  } catch (error: unknown) {
    return filingFromError(error, request);
  }
}

// ─── IO: sample email, servers, the weekly pass ──────────────────────────────

/**
 * A real first email this domain sent, rebuilt as it left (headers + HTML), the
 * recipient's address redacted. Null when the domain has sent nothing we hold.
 */
export async function loadSampleMessage(domain: string): Promise<SampleMessage | null> {
  const result = await db.execute(sql`
    SELECT d.account_email AS "accountEmail", d.message_id AS "messageId", d.dispatched_at AS "sentAt",
           s.subject AS "subject", s.body_html AS "bodyHtml",
           a.first_name AS "firstName", a.last_name AS "lastName"
    FROM smtp_dispatch_raw d
    JOIN sequence_steps s ON s.instantly_campaign_id = d.instantly_campaign_id AND s.step = d.step
    LEFT JOIN instantly_accounts a ON lower(a.email) = lower(d.account_email)
    WHERE d.outcome = 'sent' AND d.step = 1
      AND lower(d.account_email) LIKE ${`%@${domain}`}
      AND d.dispatched_at > now() - interval '60 days'
    ORDER BY d.dispatched_at DESC
    LIMIT 1
  `);
  const row = rowsOf<{
    accountEmail: string; messageId: string | null; sentAt: Date | string; subject: string | null;
    bodyHtml: string | null; firstName: string | null; lastName: string | null;
  }>(result)[0];
  if (!row || !row.bodyHtml) return null;
  const { buildMessage } = await import("./self-send/message");
  const built = buildMessage({
    account: { email: row.accountEmail, first_name: row.firstName ?? undefined, last_name: row.lastName ?? undefined } as never,
    leadEmail: "recipient@redacted.invalid",
    subject: row.subject ?? "",
    bodyHtml: row.bodyHtml,
    step: 1,
    identity: { instantlyCampaignId: "redacted", leadEmail: "recipient@redacted.invalid" } as never,
    unsubscribeUrl: "https://link.distribute.you/u/redacted",
    trackingOrigin: null,
  });
  const sentAt = row.sentAt instanceof Date ? row.sentAt : new Date(`${String(row.sentAt).replace(" ", "T")}Z`);
  const headers = [
    `From: ${built.from}`,
    `To: ${built.to}`,
    `Subject: ${built.subject}`,
    `Date: ${sentAt.toUTCString()}`,
    ...(row.messageId ? [`Message-ID: ${row.messageId}`] : []),
    "MIME-Version: 1.0",
    "Content-Type: text/html; charset=utf-8",
    ...Object.entries(built.headers).map(([k, v]) => `${k}: ${v}`),
  ].join("\n");
  return { headers, body: built.html };
}

/** Who carries the domain's mail, from the vendor we bought it from. */
export async function sendingServersFor(domain: string): Promise<string> {
  const result = await db.execute(sql`SELECT provider FROM infra_domains WHERE domain = ${domain}`);
  const providers = rowsOf<{ provider: string }>(result).map((r) => r.provider);
  if (providers.some((p) => p === "primeforge" || p === "instantly-dfy")) {
    return "Google Workspace outbound servers (smtp.gmail.com). We run no mail server of our own.";
  }
  if (providers.includes("gandi")) return "Gandi mail relay (mail.gandi.net). We run no mail server of our own.";
  if (providers.includes("mailforge")) return "Mailforge mail servers. We run no mail server of our own.";
  return "Our mailbox provider's outbound servers. We run no mail server of our own.";
}

/** Lists we can file a removal for ourselves. */
const FILERS: Partial<Record<BlocklistKey, typeof fileSurblRemoval>> = { surbl: fileSurblRemoval };

export function filesRemovalItself(list: BlocklistKey): boolean {
  return FILERS[list] !== undefined;
}

export interface RemovalPassSummary {
  due: number;
  sent: number;
  refused: number;
  notListed: number;
  failed: number;
  skippedCoolingDown: number;
}

/**
 * File every due removal. One request per (domain, list) per week: the clock is
 * the last request that reached the list (sent or refused); a failure before
 * it (owner record unreadable, network) does not start it, so the next daily
 * pass tries again. Requests go out one at a time.
 */
export async function fileDueRemovals(options: {
  now?: Date;
  loadContact: () => Promise<RemovalContact>;
  /** Injected in tests; default = the company mailbox's SMTP login. */
  send?: SendRemovalEmail;
  /** Pause between two requests (a person's pace). */
  spacingMs?: number;
}): Promise<RemovalPassSummary> {
  const now = options.now ?? new Date();
  const summary: RemovalPassSummary = { due: 0, sent: 0, refused: 0, notListed: 0, failed: 0, skippedCoolingDown: 0 };

  const result = await db.execute(sql`
    SELECT s.domain, s.list, s.listed_since AS "listedSince",
           -- Any request that reached the list starts the week, accepted or
           -- refused by its server. A failure before it does not.
           (SELECT MAX(a.attempted_at) FROM domain_blocklist_removal_attempts a
             WHERE a.domain = s.domain AND a.list = s.list AND a.outcome IN ('sent', 'refused')) AS "lastSentAt"
    FROM domain_blocklist_status s
    WHERE s.listed = true
    ORDER BY s.domain, s.list
  `);
  const listed = rowsOf<{ domain: string; list: BlocklistKey; listedSince: Date | string | null; lastSentAt: Date | string | null }>(result)
    .filter((r) => filesRemovalItself(r.list));

  const toDate = (v: Date | string | null): Date | null =>
    v === null ? null : v instanceof Date ? v : new Date(String(v).replace(" ", "T").replace(/([+-]\d{2})$/, "$1:00"));

  const due = listed.filter((r) => {
    const ok = removalDue(toDate(r.lastSentAt), now);
    if (!ok) summary.skippedCoolingDown += 1;
    return ok;
  });
  summary.due = due.length;
  if (due.length === 0) return summary;

  let contact: RemovalContact | null = null;
  let contactError: string | null = null;
  try {
    contact = await options.loadContact();
  } catch (error: unknown) {
    contactError = `owner identity unavailable: ${error instanceof Error ? error.message : String(error)}`;
  }

  for (const [i, row] of due.entries()) {
    if (i > 0 && (options.spacingMs ?? 0) > 0) await new Promise((r) => setTimeout(r, options.spacingMs));
    let filing: FilingResult;
    if (!contact) {
      filing = { outcome: "failed", httpStatus: null, responseExcerpt: null, error: contactError, request: null };
    } else {
      const sample = await loadSampleMessage(row.domain);
      if (!sample) {
        filing = { outcome: "failed", httpStatus: null, responseExcerpt: null, error: "no recent email from this domain to show", request: null };
      } else {
        filing = await FILERS[row.list]!({
          domain: row.domain,
          contact,
          sample,
          sendingServers: await sendingServersFor(row.domain),
          stoppedNewContactsOn: (toDate(row.listedSince) ?? now).toISOString().slice(0, 10),
          send: options.send,
        });
      }
    }
    await recordAttempt(row.domain, row.list, filing, now);
    if (filing.outcome === "sent") summary.sent += 1;
    else if (filing.outcome === "refused") summary.refused += 1;
    else if (filing.outcome === "not_listed") summary.notListed += 1;
    else summary.failed += 1;
  }
  return summary;
}

/** Bronze row + silver event for one attempt, in one transaction. */
async function recordAttempt(domain: string, list: BlocklistKey, filing: FilingResult, at: Date): Promise<void> {
  await db.transaction(async (tx) => {
    const [attempt] = await tx
      .insert(domainBlocklistRemovalAttempts)
      .values({
        domain,
        list,
        attemptedAt: at,
        outcome: filing.outcome,
        httpStatus: filing.httpStatus,
        request: filing.request,
        responseExcerpt: filing.responseExcerpt,
        error: filing.error,
      })
      .returning({ id: domainBlocklistRemovalAttempts.id });
    // `not_listed`: SURBL's own page already reads the domain clean; the daily
    // check delists it on its next fresh answer. No request, nothing to report.
    if (filing.outcome === "not_listed") return;
    await tx.insert(domainBlocklistEvents).values({
      domain,
      list,
      eventType: filing.outcome === "sent" ? "removal_requested" : "removal_failed",
      detail: {
        method: "automated",
        attemptId: attempt!.id,
        outcome: filing.outcome,
        httpStatus: filing.httpStatus,
        error: filing.error,
        answer: filing.responseExcerpt?.slice(0, 300) ?? null,
      },
      occurredAt: at,
    });
  });
}
