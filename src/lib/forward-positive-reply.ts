/**
 * Forward a positive reply's full email thread to the agency inbox.
 *
 * The agency (distribute) runs cold outreach from Instantly. Instantly now
 * paywalls its Unibox/CRM, so a positive reply can no longer be seen or acted on
 * from the Instantly UI without paying. But the V2 API still exposes the replies
 * AND Instantly's OWN qualification of them. This side effect closes the loop:
 * when Instantly ITSELF marks an inbound reply positive/interested, we fetch the
 * whole conversation thread from the V2 API and email it to kevin@distribute.you
 * via the production transactional-email path (Postmark under the hood) — zero
 * dependency on the paid Instantly CRM.
 *
 * We trust Instantly's qualification as-is — NO separate sentiment classifier.
 * "Positive" here is `POSITIVE_REPLY_KINDS`, i.e. what is worth READING. It is
 * deliberately NOT the same set as the 'positive' entries of
 * REPLY_CLASSIFICATION_MAP any more: `lead_referral` is forwarded (a name to
 * talk to is worth reading) while it reports as `neutral` (it is not this
 * person's buying interest). See the divergence note on
 * REPLY_KIND_CLASSIFICATION in lib/reply-kind.
 *
 * Placement: fired as a fail-soft side effect from `promoteEvent` in
 * silver-promote.ts, on REAL (non-inferred) events only. Both the webhook path
 * (event_type=lead_interested…) and the reconcile poll path (lt_interest_status
 * → the same event types) converge here.
 *
 * Idempotency: exactly-once via an atomic claim on
 * `instantly_campaigns.positive_reply_forwarded_at` (migration 0028). The first
 * positive event for a lead claims the column (UPDATE … WHERE … IS NULL
 * RETURNING) and sends; every later positive event (webhook retry, reconcile
 * re-poll, re-qualification interested→meeting_booked→closed) finds it non-null
 * and no-ops. On a send FAILURE the claim is released back to NULL so a later
 * retry re-attempts — at-most-once send, biased against duplicates (the explicit
 * no-go) while still self-healing a transient failure.
 *
 * Fail-soft: never throws into the webhook promote path (a 5xx would make
 * Instantly auto-pause the webhook). Any brand/key/Instantly/email error is
 * swallowed + logged; the claim is released so the forward is retried later.
 */

import { and, eq, isNull } from "drizzle-orm";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import type { EmailRecord } from "./instantly-client";
import { sendEmail } from "./email-client";
import { isEscalatedReplyKind, POSITIVE_REPLY_KINDS } from "./reply-kind";
import { agencyInbox } from "./agency-inbox";
import { salesRepCopyList } from "./sales-rep-copy";
import { isStaffSender } from "./staff-senders";

/**
 * The reply kinds that mean "worth forwarding to the agency inbox". We forward
 * ONLY on these; negative / automated replies never trigger a forward.
 *
 * All four positive distinctions forward: someone who wants to know more, or
 * points us at the right buyer, is exactly as worth reading as someone asking
 * for a call. Deal progress is absent because it is no longer a reply kind at
 * all — a booked meeting is recorded by the lead-outcomes service, and it is
 * not evidence that a reply just arrived.
 *
 * ⚠️ `lead_referral` is NOT forwarded here any more: it is ESCALATED instead
 * (`ESCALATED_REPLY_KINDS`, lib/escalate-off-topic-reply), which forwards the
 * same thread with a "a person must follow this up" lead — one email per reply.
 * Historical note: it used to forward while reporting `neutral`. Forwarding answers "is this
 * worth a human's eyes"; the coarse map answers "was this a buying signal we
 * should price and count". A unit test asserts exactly that relationship — do
 * not restore an equality assertion between the two.
 */
export const POSITIVE_QUALIFICATION_EVENT_TYPES = new Set<string>(
  POSITIVE_REPLY_KINDS.filter((k) => !isEscalatedReplyKind(k)),
);

/** True iff this event is a positive reply kind. */
export function isPositiveQualification(eventType: string): boolean {
  return POSITIVE_QUALIFICATION_EVENT_TYPES.has(eventType);
}

/** The subset of a campaign row this side effect needs. */
export interface ForwardPositiveReplyCampaign {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  brandIds?: string[] | null;
  /**
   * The caller campaign whose WHOLE conversation the email tells. Defaults to
   * `campaignId`. The escalation passes it separately because it deliberately
   * leaves `campaignId` null (run attribution, see escalate-reply).
   */
  conversationCampaignId?: string | null;
}

/** One rendered message in the conversation thread. */
export interface ThreadMessage {
  /** 'outbound' = sent by us (ue_type 1/3), 'inbound' = reply from the lead (2). */
  direction: "outbound" | "inbound";
  from: string;
  to: string;
  date: string;
  subject: string;
  bodyText: string;
}

/**
 * Collapse an Instantly email HTML body to readable plain text. Deliberately
 * light — enough to strip markup for an internal ops email, not a full parser.
 * Removes <style>/<script>, turns <br> and block-close tags into newlines,
 * strips all remaining tags, decodes the few common entities, and collapses
 * runaway blank lines.
 *
 * ⚠️ A PARAGRAPH ends in an EMPTY LINE (`</p>`, `</h1-6>`, `</table>` → "\n\n"),
 * a `<br>` / `</div>` / `</li>` / `</tr>` in ONE newline. The text this returns
 * is what a customer reads as "the email we sent" (lead timeline), and our mail
 * is one `<p>` per paragraph, so the prospect sees blank lines between them. A
 * single newline per `</p>` made every sent email read as one dense block.
 * `</div>` stays single on purpose: mail clients (Gmail) write one `<div>` per
 * LINE, so doubling it would space out every line of a prospect's reply.
 */
export function htmlToText(html: string): string {
  return html
    .replace(/<\s*(style|script)[^>]*>[\s\S]*?<\s*\/\s*\1\s*>/gi, "")
    .replace(/<\s*br\s*\/?\s*>/gi, "\n")
    .replace(/<\s*\/\s*(p|h[1-6]|table)\s*>/gi, "\n\n")
    .replace(/<\s*\/\s*(div|tr|li)\s*>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/** Prefer the plain-text body; fall back to a stripped HTML body. */
function bodyToText(record: EmailRecord): string {
  const text = record.body?.text?.trim();
  if (text) return text;
  const html = record.body?.html?.trim();
  if (html) return htmlToText(html);
  return "(no body)";
}

/**
 * Normalize the raw Instantly email records of one campaign into an ordered
 * conversation thread. 1 Instantly campaign = 1 lead, so `GET /emails?campaign_id`
 * returns exactly this lead's thread. Includes real messages (ue_type 1 sent,
 * 2 received, 3 manual-sent); skips scheduled-but-unsent (ue_type 4). Sorted
 * oldest → newest by the email timestamp so the reader follows the exchange in
 * order.
 */
export function selectThreadMessages(records: EmailRecord[]): ThreadMessage[] {
  return records
    .filter((r) => r.ue_type === 1 || r.ue_type === 2 || r.ue_type === 3)
    .slice()
    .sort(
      (a, b) =>
        new Date(a.timestamp_email).getTime() -
        new Date(b.timestamp_email).getTime(),
    )
    .map((r) => ({
      // An inbound message one of our own people wrote (a staff answer CC'd to
      // the sending mailbox) is OUR side of the conversation — see staff-senders.
      direction:
        r.ue_type === 2 && !isStaffSender(r.from_address_email, r.lead) ? "inbound" : "outbound",
      from: r.from_address_email || r.eaccount || "(unknown)",
      to: r.to_address_email_list || r.lead || "(unknown)",
      date: r.timestamp_email,
      subject: r.subject || "(no subject)",
      bodyText: bodyToText(r),
    }));
}

/** Format an ISO timestamp as a readable email date, e.g. "Jul 13, 2026, 5:57 PM UTC". */
export function formatThreadDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return (
    d.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
      timeZone: "UTC",
    }) + " UTC"
  );
}

/** The conversation subject = the newest message's subject (what a reply carries). */
export function threadSubject(messages: ThreadMessage[]): string {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].subject && messages[i].subject !== "(no subject)") {
      return messages[i].subject;
    }
  }
  return messages.length > 0 ? messages[messages.length - 1].subject : "(no subject)";
}

/**
 * Forward the prospect's WHOLE history to the agency inbox — every email we
 * sent, every reply, every action they took (visits, bounces, unsubscribes),
 * oldest first (see lib/prospect-history) — as a CLEAN, client-forwardable
 * email. Shared by the positive-reply forward, both escalations and the manual
 * re-forward endpoint. Returns the message count.
 *
 * ⚠️ It no longer starts at the prospect's first reply: a reply does not always
 * quote what it answers, so the reader could not see what we had sent. A part
 * of the history that cannot be read is STATED in the email and never stops
 * it; only the send itself throws (the caller decides whether to swallow it).
 */
export interface ThreadForwardTemplate {
  /**
   * The transactional-email template to render. Defaults to the positive-reply
   * forward; the escalation path names its own, because "your prospect said
   * something good" and "the responder could not answer this" are different
   * things to walk into an inbox.
   */
  eventType?: string;
  /** Extra template variables, merged over `subject` / `thread`. */
  metadata?: Record<string, string>;
}

export async function sendThreadForward(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  template: ThreadForwardTemplate = {},
): Promise<number> {
  if (!campaign.orgId) {
    throw new Error("forward-thread requires an org-scoped campaign (orgId is null)");
  }
  // Dynamic import: prospect-history reads the thread through modules that
  // import this one, so a static import would be a load-time cycle.
  const { loadProspectHistory, renderProspectHistory } = await import("./prospect-history");
  const history = await loadProspectHistory(campaign, leadEmail);
  const messages = history.messages;

  // The client's own rep is copied on the thread their prospect just wrote,
  // seconds before their phone rings. VISIBLY, never blind: a blind-copied rep
  // receives a message addressed to the agency, which reads as mis-sent, and a
  // reply-all from them would reach nobody on our side.
  //
  // Resolved HERE from the campaign's own brand, never handed in by a caller —
  // the same reasoning that makes the one-to-one reply resolve its sending
  // identity rather than accept one. A brand that stated no rep, or a rep with
  // no email, sends exactly what it sent before this existed.
  const ccEmails = await salesRepCopyList(campaign.brandIds?.[0], campaign.orgId);

  await sendEmail(
    {
      appId: "instantly-service",
      eventType: template.eventType ?? "positive-reply-forward",
      recipientEmail: agencyInbox(),
      ...(ccEmails.length > 0 ? { ccEmails } : {}),
      metadata: {
        subject: threadSubject(messages),
        thread: renderProspectHistory(history, leadEmail),
        ...(template.metadata ?? {}),
      },
    },
    {
      orgId: campaign.orgId,
      userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
      runId: campaign.runId || undefined,
      tracking: {
        campaignId: campaign.campaignId ?? undefined,
        brandId: campaign.brandIds?.[0],
      },
    },
  );
  console.log(
    `[instantly-service] ${template.eventType ?? "forward-positive-reply"}: sent history (${messages.length} msg, ${history.items.length - messages.length} action${history.notes.length > 0 ? `, ${history.notes.length} unreadable part(s)` : ""}) for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} → ${agencyInbox()}${ccEmails.length > 0 ? ` cc=${ccEmails.join(",")}` : ""}`,
  );
  return messages.length;
}

/**
 * Atomically claim the positive-reply forward for a campaign. Returns true iff
 * THIS call won the claim (the column was NULL and is now set). A losing caller
 * (already claimed / already forwarded) gets false and must not send.
 */
async function claimForward(instantlyCampaignId: string): Promise<boolean> {
  const claimed = await db
    .update(instantlyCampaigns)
    .set({ positiveReplyForwardedAt: new Date(), updatedAt: new Date() })
    .where(
      and(
        eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId),
        isNull(instantlyCampaigns.positiveReplyForwardedAt),
      ),
    )
    .returning({ id: instantlyCampaigns.id });
  return claimed.length > 0;
}

/** Release a claim (send failed) so a later retry re-attempts the forward. */
async function releaseForward(instantlyCampaignId: string): Promise<void> {
  await db
    .update(instantlyCampaigns)
    .set({ positiveReplyForwardedAt: null, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
}

/**
 * Forward the full thread of a positively-qualified reply to the agency inbox.
 * No-op unless `eventType` is a positive qualification and the campaign is
 * org-scoped. Fully fail-soft — never throws.
 *
 * Platform sends (orgId null — e.g. journalist price-requests) are out of scope
 * for v1: they are a different product flow with a distinct key path. A positive
 * reply there is a documented follow-up, not handled here.
 */
export async function maybeForwardPositiveReply(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  eventType: string,
): Promise<void> {
  if (!isPositiveQualification(eventType)) return;
  if (!campaign.orgId) return;

  // Exactly-once: claim before any external side effect. A loser (already
  // forwarded / claimed by a concurrent positive event) stops here.
  let claimed: boolean;
  try {
    claimed = await claimForward(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] forward-positive-reply: claim failed for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; will retry on next positive signal`,
    );
    return;
  }
  if (!claimed) return;

  try {
    await sendThreadForward(campaign, leadEmail);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    // Release the claim so a later webhook retry / reconcile re-poll re-attempts.
    await releaseForward(campaign.instantlyCampaignId).catch(() => {});
    console.warn(
      `[instantly-service] forward-positive-reply: no-op for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; claim released, will retry`,
    );
  }
}
