/**
 * SES bounces and complaints of cold mail, filed like the DSNs our mailboxes
 * receive (SES-COLD-1119).
 *
 * Through SES a DSN goes to SES's MAIL FROM (`ses.<domain>`), never to our
 * mailbox, so the poll would never see it. SES publishes the event on OUR
 * configuration set to OUR SNS topic, which posts to `POST /webhooks/ses`:
 *   - the SNS signature is verified (certificate fetched only from
 *     `sns.<region>.amazonaws.com`, SignatureVersion 1 = SHA1, 2 = SHA256) and
 *     the TopicArn must be the pinned cold topic, else 403 and nothing is read;
 *   - a SubscriptionConfirmation is confirmed by fetching its SubscribeURL
 *     (same host rule);
 *   - a Notification is filed once (`ses_events_raw`, keyed on the SNS id):
 *       Bounce Permanent (OnAccountSuppressionList included: the address
 *       bounced or complained before, a RECIPIENT-side refusal, never ours)
 *         = an `imap_messages_raw` `bounce` row with the report's words, which
 *           the DSN judgment sweep decides like any DSN (never a code verdict);
 *       Bounce Transient / Undetermined, DeliveryDelay = a `delay` row;
 *       Complaint = the person's opt-out (`lead_unsubscribed`);
 *       Reject / anything else = recorded only.
 *     The send is found by the Message-Id SES gave it (`<id@email.amazonses.com>`,
 *     stored as `smtp_dispatch_raw.message_id`); a seed or a test has no sequence:
 *     its bounce row is filed on the sending mailbox with no campaign (what a
 *     DSN on a seed looks like when the mailbox receives it).
 */
import { createVerify } from "node:crypto";
import { sql } from "drizzle-orm";

import { db } from "../../db";
import { promoteEvent } from "../silver-promote";
import { senderAddressOf } from "./outbound";
import { sesRfcMessageId } from "./ses-transport";

export interface SnsEnvelope {
  Type: string;
  MessageId: string;
  TopicArn: string;
  Message: string;
  Timestamp: string;
  SignatureVersion: string;
  Signature: string;
  SigningCertURL: string;
  Subject?: string;
  SubscribeURL?: string;
  Token?: string;
}

/** The exact string SNS signs for a message (pure). */
export function snsStringToSign(m: SnsEnvelope): string {
  const keys =
    m.Type === "Notification"
      ? ["Message", "MessageId", ...(m.Subject !== undefined && m.Subject !== null ? ["Subject"] : []), "Timestamp", "TopicArn", "Type"]
      : ["Message", "MessageId", "SubscribeURL", "Timestamp", "Token", "TopicArn", "Type"];
  return keys.map((k) => `${k}\n${(m as unknown as Record<string, string>)[k]}\n`).join("");
}

/** An SNS-owned https URL (cert or subscribe link), never anything else (pure). */
export function isSnsUrl(raw: string | undefined, kind: "cert" | "subscribe"): boolean {
  if (!raw) return false;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return false;
  }
  if (url.protocol !== "https:" || !/^sns\.[a-z0-9-]+\.amazonaws\.com$/.test(url.hostname)) return false;
  return kind === "cert" ? url.pathname.endsWith(".pem") : true;
}

const certCache = new Map<string, string>();

export async function verifySnsSignature(
  m: SnsEnvelope,
  fetchCert: (url: string) => Promise<string> = async (url) => {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`SNS certificate ${url}: ${r.status}`);
    return r.text();
  },
): Promise<boolean> {
  if (!isSnsUrl(m.SigningCertURL, "cert")) return false;
  const algorithm = m.SignatureVersion === "1" ? "RSA-SHA1" : m.SignatureVersion === "2" ? "RSA-SHA256" : null;
  if (!algorithm || !m.Signature) return false;
  let cert = certCache.get(m.SigningCertURL);
  if (!cert) {
    cert = await fetchCert(m.SigningCertURL);
    certCache.set(m.SigningCertURL, cert);
  }
  const verifier = createVerify(algorithm);
  verifier.update(snsStringToSign(m), "utf8");
  return verifier.verify(cert, m.Signature, "base64");
}

export type SesFiling = "bounce" | "delay" | "unsubscribe" | "ignored";

export interface SesEvent {
  eventType?: string;
  notificationType?: string;
  mail?: { messageId?: string; source?: string; destination?: string[]; commonHeaders?: { subject?: string } };
  bounce?: {
    bounceType?: string;
    bounceSubType?: string;
    timestamp?: string;
    reportingMTA?: string;
    bouncedRecipients?: { emailAddress?: string; action?: string; status?: string; diagnosticCode?: string }[];
  };
  complaint?: { timestamp?: string; complaintFeedbackType?: string; complainedRecipients?: { emailAddress?: string }[] };
  deliveryDelay?: { timestamp?: string; delayType?: string; delayedRecipients?: { emailAddress?: string; status?: string; diagnosticCode?: string }[] };
}

export function eventTypeOf(event: SesEvent): string {
  return String(event.eventType ?? event.notificationType ?? "Unknown");
}

/** What an SES event becomes (pure). */
export function filingForSesEvent(event: SesEvent): SesFiling {
  const type = eventTypeOf(event);
  if (type === "Bounce") return event.bounce?.bounceType === "Permanent" ? "bounce" : "delay";
  if (type === "DeliveryDelay") return "delay";
  if (type === "Complaint") return "unsubscribe";
  return "ignored";
}

/** The report's words, as a DSN would carry them, for the judgment sweep (pure). */
export function sesEventWords(event: SesEvent): string {
  const lines: string[] = [];
  if (event.bounce) {
    const b = event.bounce;
    lines.push(`Amazon SES delivery failure: ${b.bounceType ?? "?"} / ${b.bounceSubType ?? "?"}`);
    if (b.bounceSubType === "OnAccountSuppressionList") {
      lines.push(
        "Amazon SES did not send this message: the recipient address is on the account's suppression list " +
          "because it hard-bounced or complained before (the address refused our mail; not a problem with the sender).",
      );
    }
    for (const r of b.bouncedRecipients ?? []) {
      lines.push(`Final-Recipient: ${r.emailAddress ?? "?"}`, `Action: ${r.action ?? "?"}`, `Status: ${r.status ?? "?"}`);
      if (r.diagnosticCode) lines.push(`Diagnostic-Code: ${r.diagnosticCode}`);
    }
    if (b.reportingMTA) lines.push(`Reporting-MTA: ${b.reportingMTA}`);
  }
  if (event.deliveryDelay) {
    const d = event.deliveryDelay;
    lines.push(`Amazon SES delivery delayed: ${d.delayType ?? "?"}`);
    for (const r of d.delayedRecipients ?? []) {
      lines.push(`Final-Recipient: ${r.emailAddress ?? "?"}`, `Status: ${r.status ?? "?"}`);
      if (r.diagnosticCode) lines.push(`Diagnostic-Code: ${r.diagnosticCode}`);
    }
  }
  return lines.join("\n").slice(0, 4000);
}

function rowsOf<T>(result: unknown): T[] {
  return (((result as { rows?: unknown[] }).rows ?? []) as T[]);
}

export interface SesEventFiled {
  filedAs: SesFiling;
  duplicate: boolean;
  instantlyCampaignId: string | null;
}

/** File one SNS notification carrying an SES event. Idempotent on the SNS id. */
export async function fileSesNotification(snsMessageId: string, event: SesEvent, receivedAt: Date = new Date()): Promise<SesEventFiled> {
  const [known] = rowsOf<{ filed: string; campaign: string | null }>(
    await db.execute(sql`SELECT filed_as AS filed, instantly_campaign_id AS campaign FROM ses_events_raw WHERE sns_message_id = ${snsMessageId}`),
  );
  if (known) return { filedAs: known.filed as SesFiling, duplicate: true, instantlyCampaignId: known.campaign };

  const sesId = event.mail?.messageId ?? null;
  const messageId = sesId ? sesRfcMessageId(sesId) : null;
  const [send] = messageId
    ? rowsOf<{ id: string; campaign: string; step: number; lead: string; account: string }>(
        await db.execute(sql`
          SELECT id, instantly_campaign_id AS campaign, step, lead_email AS lead, account_email AS account
          FROM smtp_dispatch_raw WHERE message_id = ${messageId} AND outcome = 'sent'
          ORDER BY dispatched_at DESC LIMIT 1
        `),
      )
    : [];
  const recipient =
    event.bounce?.bouncedRecipients?.[0]?.emailAddress ??
    event.complaint?.complainedRecipients?.[0]?.emailAddress ??
    event.deliveryDelay?.delayedRecipients?.[0]?.emailAddress ??
    event.mail?.destination?.[0] ??
    null;
  let filing = filingForSesEvent(event);
  const at = new Date(event.bounce?.timestamp ?? event.complaint?.timestamp ?? event.deliveryDelay?.timestamp ?? receivedAt.toISOString());
  const accountEmail = send?.account ?? (event.mail?.source ? senderAddressOf(event.mail.source) : null);

  if (filing === "bounce" || filing === "delay") {
    if (!accountEmail) filing = "ignored";
    else {
      await db.execute(sql`
        INSERT INTO imap_messages_raw
          (id, account_email, message_id, from_address, subject, kind, instantly_campaign_id, step, payload, received_at)
        VALUES (${crypto.randomUUID()}, ${accountEmail}, ${`<ses-event-${snsMessageId}@amazonses.com>`},
                ${"mailer-daemon@amazonses.com"}, ${event.mail?.commonHeaders?.subject ?? null}, ${filing},
                ${send?.campaign ?? null}, ${send?.step ?? null},
                ${JSON.stringify({ textSnippet: sesEventWords(event), recoveredFrom: "ses_event", inReplyTo: messageId, sesEvent: { type: eventTypeOf(event), bounceType: event.bounce?.bounceType ?? null, bounceSubType: event.bounce?.bounceSubType ?? null, recipient } })}::jsonb,
                ${at})
        ON CONFLICT (account_email, message_id) DO NOTHING
      `);
    }
  } else if (filing === "unsubscribe") {
    if (!send) filing = "ignored";
    else {
      await promoteEvent({
        eventType: "lead_unsubscribed",
        instantlyCampaignId: send.campaign,
        leadEmail: send.lead,
        accountEmail: send.account,
        step: null,
        variant: null,
        timestamp: at,
        source: "self_send",
        sourceRowId: send.id,
      });
    }
  }

  await db.execute(sql`
    INSERT INTO ses_events_raw
      (sns_message_id, event_type, ses_message_id, recipient, payload, filed_as, instantly_campaign_id, step)
    VALUES (${snsMessageId}, ${eventTypeOf(event)}, ${sesId}, ${recipient}, ${JSON.stringify(event)}::jsonb, ${filing},
            ${send?.campaign ?? null}, ${send?.step ?? null})
    ON CONFLICT (sns_message_id) DO NOTHING
  `);
  return { filedAs: filing, duplicate: false, instantlyCampaignId: send?.campaign ?? null };
}
