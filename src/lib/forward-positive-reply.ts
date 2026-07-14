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
 * "Positive" is exactly the set of lead_* events that flip
 * `reply_classification` to 'positive' in silver-promote's
 * REPLY_CLASSIFICATION_MAP (lead_interested / lead_meeting_booked / lead_closed).
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
import { resolveInstantlyApiKey } from "./key-client";
import { listEmails, type EmailRecord } from "./instantly-client";
import { sendEmail } from "./email-client";

/** The agency inbox that receives forwarded positive replies. */
const AGENCY_INBOX = process.env.ADMIN_NOTIFICATION_EMAIL || "kevin@distribute.you";

/**
 * The Instantly qualification events that mean "positive reply". Kept in lockstep
 * with the 'positive' entries of silver-promote's REPLY_CLASSIFICATION_MAP — a
 * unit test asserts the two never drift. We forward ONLY on these; negative /
 * neutral / non-qualified replies never trigger a forward.
 */
export const POSITIVE_QUALIFICATION_EVENT_TYPES = new Set<string>([
  "lead_interested",
  "lead_meeting_booked",
  "lead_closed",
]);

/** True iff Instantly has qualified this event as a positive reply. */
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
 */
export function htmlToText(html: string): string {
  return html
    .replace(/<\s*(style|script)[^>]*>[\s\S]*?<\s*\/\s*\1\s*>/gi, "")
    .replace(/<\s*br\s*\/?\s*>/gi, "\n")
    .replace(/<\s*\/\s*(p|div|tr|li|h[1-6]|table)\s*>/gi, "\n")
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
      direction: r.ue_type === 2 ? "inbound" : "outbound",
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
 * Render the ordered thread as a CLEAN, client-forwardable email conversation —
 * no instantly-service branding, no notes, no labels, no metadata. Each message
 * is a standard From/To/Date/Subject header block + its full body, oldest →
 * newest, so the recipient can forward it as-is to the client. Plain text drops
 * into the template's `<pre>` (rendered with an inherited font + wrapping, so it
 * reads like a normal email, not monospace).
 */
export function renderThreadText(messages: ThreadMessage[]): string {
  if (messages.length === 0) return "(conversation unavailable)";
  return messages
    .map((m) =>
      [
        `From: ${m.from}`,
        `To: ${m.to}`,
        `Date: ${formatThreadDate(m.date)}`,
        `Subject: ${m.subject}`,
        ``,
        m.bodyText,
      ].join("\n"),
    )
    .join("\n\n─────────────────────────────────────────\n\n");
}

/**
 * Fetch a campaign's full Instantly thread and forward it — as a CLEAN,
 * client-forwardable email (subject = the conversation's real subject; body =
 * the plain conversation, no branding) — to the agency inbox. Shared by the
 * positive-reply webhook side effect AND the manual re-forward endpoint. Returns
 * the message count. Throws on any failure (the caller decides whether to
 * swallow it).
 */
export async function sendThreadForward(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
): Promise<number> {
  if (!campaign.orgId) {
    throw new Error("forward-thread requires an org-scoped campaign (orgId is null)");
  }
  const { key } = await resolveInstantlyApiKey(campaign.orgId, "system", {
    method: "POST",
    path: "/internal/forward-positive-reply",
  });
  const records = await listEmails(key, { campaignId: campaign.instantlyCampaignId });
  const messages = selectThreadMessages(records);

  await sendEmail(
    {
      appId: "instantly-service",
      eventType: "positive-reply-forward",
      recipientEmail: AGENCY_INBOX,
      metadata: {
        subject: threadSubject(messages),
        thread: renderThreadText(messages),
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
    `[instantly-service] forward-positive-reply: sent thread (${messages.length} msg) for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} → ${AGENCY_INBOX}`,
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
