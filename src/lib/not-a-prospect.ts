/**
 * A reply from somebody who is NOT a prospect for this brand at all — they
 * already buy from the client (`lead_already_customer`) or they ARE the client
 * (`lead_is_client`). See NOT_A_PROSPECT_REPLY_KINDS in lib/reply-kind.
 *
 * Measured 2026-09-30: dr.k@kineticchiropracticutah.com replied "I actually am
 * a Shockwave Centers of America clinic. I have the OTG unit. Is this email
 * meant for those who don't have shockwave units?" It was filed as a positive
 * reply, the client was sent a "Good news" celebration about their own
 * customer, and nothing stopped the brand from writing to him again.
 *
 * Three things happen, and the order is the point:
 *  1. Every OTHER live sequence of this brand to the person stops (the reply
 *     already stopped this one). Same org, same brand, same address.
 *  2. lead-service is told it is a won sale our outreach did not cause, when it
 *     is `lead_already_customer` (`recordExistingCustomerByEmail`).
 *  3. The prospect is answered NOW, in their thread, with a short apology that
 *     claims nothing about them (`REASSURANCE_SYSTEM_PROMPT`), so they are not
 *     left wondering why they were pitched. Exactly once per thread (the
 *     escalation claim, migration 0062), and afterwards the responder never
 *     writes on that thread again.
 *
 * ⚠️ A HAND-STATED qualification (`source = 'manual'`) does 1 and 2 but NEVER 3:
 * a person who states it is handling the conversation themselves, and the one
 * case this was written for (Andrew) had already been answered by hand.
 *
 * And the send gate: `findNotAProspect` refuses any NEW send of the brand to an
 * address whose standing reply kind says it is not a prospect.
 *
 * Fail-soft everywhere on the ingestion path (it runs off Instantly's webhook);
 * the send gate fails LOUD (a gate that cannot read its history must not wave a
 * send through).
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { agencyInbox } from "./agency-inbox";
import { brandContextOrNull } from "./celebrate-positive-reply";
import { orgComplete } from "./chat-client";
import {
  claimEscalation,
  handoffTextToHtml,
  recordHandedTo,
  releaseEscalation,
  renderQuotedHistory,
} from "./escalate-reply";
import type { ThreadMessage } from "./forward-positive-reply";
import { recordExistingCustomerByEmail } from "./lead-client";
import { loadHistoryWithLatestReply, REPLY_WAIT_BACKGROUND_MS } from "./prospect-history";
import { normalizeLeadEmail } from "./recontact-window";
import { isNotAProspectReplyKind, NOT_A_PROSPECT_REPLY_KINDS } from "./reply-kind";
import { replyToLead } from "./reply-to-lead";
import { stripQuotedHistory } from "./self-send/qualify-reply";
import { stopLeadSequence } from "./stop-lead-sequence";

/** The campaign row fields this needs. */
export interface NotAProspectCampaign {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  brandIds?: string[] | null;
}

/** What `escalation_handed_to` records once the prospect was reassured. */
export const REASSURED_MARKER = "reassured";

/**
 * The reassurance draft. The hard rule is the second bullet list: we hold NO
 * data on this person and we are not the client, so the email may not state or
 * imply anything about who they are or what they own — not even by repeating
 * what they told us. Owner, verbatim: it must "NEVER state or imply facts about
 * the prospect as if we knew them".
 */
export const REASSURANCE_SYSTEM_PROMPT = `You write one short email reply on behalf of the person who sent a cold email.
Situation: the recipient replied that the email was not meant for someone like them. You apologize for the mix-up and say they will not receive any more of these emails.
Write like a busy, competent person typing in Gmail. Plain, warm, human.
- 25 to 60 words. No subject line. Plain text only.
- Start with "Hi <first name or how they signed>," on its own line.
- Apologize and thank them for flagging it.
- Say the email was not meant for them and that you have taken them off the list, so they will not get any more of these emails.
- End with "Best," on its own line and nothing after it: the sender's name and signature are added automatically.
Hard rules, never broken:
- Never state or imply any fact about them, their company, what they own, what they buy, or who they work with. We hold no information about them. Do not repeat or confirm what they told us about themselves.
- Never speak for the company the email was about, and never claim to know its customers.
- Do not sell, do not ask a question, do not offer anything, do not promise anything else.
Banned: em-dashes, "great question", "loop in", "reach out", "happy to", "don't hesitate", "I hope", "delve", "seamless", exclamation marks, bullet points.`;

/** Phrases the draft must never contain: each restates a fact about the person. */
const CLAIM_PATTERNS: RegExp[] = [
  /\byou(?:'re| are) (?:already )?(?:a |an |one of |part of |in )/i,
  /\byou already\b/i,
  /\byour (?:unit|clinic|company|team|account)\b/i,
  /\bas (?:a|an) (?:existing |current )?(?:customer|client|member)\b/i,
];

/**
 * Pure: the draft is usable. Throws on a draft that would put a fact about the
 * prospect into our mouth, or an em-dash, rather than sending it.
 */
export function assertReassuranceDraft(text: string): string {
  const trimmed = text.trim();
  if (!trimmed) throw new Error("empty reassurance draft");
  for (const pattern of CLAIM_PATTERNS) {
    if (pattern.test(trimmed)) {
      throw new Error(`reassurance draft states a fact about the prospect (${pattern}): ${trimmed}`);
    }
  }
  return trimmed;
}

async function draftReassurance(
  reply: ThreadMessage,
  identity: { orgId: string; userId: string; runId: string },
): Promise<string> {
  const words = stripQuotedHistory(reply.bodyText).trim() || reply.bodyText;
  const message = [
    `Their reply, verbatim (from ${reply.from}):`,
    `"""`,
    words,
    `"""`,
  ].join("\n");
  // Two attempts: a draft that slips a claim about them is refused, not sent,
  // and a second draft almost always complies. Two refusals throw.
  let lastError: unknown;
  for (let attempt = 0; attempt < 2; attempt++) {
    const result = await orgComplete(
      {
        message,
        systemPrompt: REASSURANCE_SYSTEM_PROMPT,
        provider: "anthropic",
        model: "opus",
        maxTokens: 400,
      },
      identity,
    );
    try {
      return assertReassuranceDraft(result.content ?? "");
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}

/**
 * Every OTHER live sequence this org holds for the address under one of the
 * given brands — the ones a "not a prospect" reply on one thread must stop too.
 */
async function siblingSequences(
  orgId: string,
  leadEmail: string,
  brandIds: string[],
  exceptInstantlyCampaignId: string,
): Promise<string[]> {
  if (brandIds.length === 0) return [];
  const brandList = sql.join(
    brandIds.map((b) => sql`${b}`),
    sql`, `,
  );
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS id
    FROM instantly_campaigns c
    WHERE c.org_id = ${orgId}
      AND lower(c.lead_email) = ${normalizeLeadEmail(leadEmail)}
      AND c.status = 'active'
      AND c.instantly_campaign_id <> ${exceptInstantlyCampaignId}
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND c.brand_ids && ARRAY[${brandList}]::text[]
  `);
  return ((result as { rows?: Record<string, unknown>[] }).rows ?? []).map((r) => String(r.id));
}

/**
 * The prospect hears back once, in their thread. Returns what happened, for the
 * log. Never throws.
 */
async function reassureOnce(campaign: NotAProspectCampaign, leadEmail: string): Promise<string> {
  if (!campaign.orgId || !campaign.userId || !campaign.runId || !campaign.campaignId) {
    return "skipped: the campaign row carries no org/user/run/campaign to answer on";
  }
  if (!(await claimEscalation(campaign.instantlyCampaignId))) {
    return "skipped: this thread was already escalated or answered";
  }

  try {
    const { history, latestReply } = await loadHistoryWithLatestReply(
      {
        instantlyCampaignId: campaign.instantlyCampaignId,
        campaignId: null,
        orgId: campaign.orgId,
        userId: campaign.userId,
        runId: campaign.runId,
        brandIds: campaign.brandIds ?? null,
        conversationCampaignId: campaign.campaignId,
      },
      leadEmail,
      { waitsMs: REPLY_WAIT_BACKGROUND_MS },
    );
    if (!latestReply) throw new Error("their reply could not be read, so nothing is drafted against it");

    const text = await draftReassurance(latestReply, {
      orgId: campaign.orgId,
      userId: campaign.userId,
      runId: campaign.runId,
    });
    const agency = agencyInbox();
    await replyToLead({
      orgId: campaign.orgId,
      userId: campaign.userId,
      campaignId: campaign.campaignId,
      leadEmail,
      bodyHtml: handoffTextToHtml(text),
      sentBy: "automation",
      handoff: true,
      copy: { cc: [], bcc: [agency] },
      quotedHtml: renderQuotedHistory(history.messages),
    });
    await recordHandedTo(campaign.instantlyCampaignId, REASSURED_MARKER);
    return "reassured";
  } catch (error) {
    // Nothing went out: give the claim back so a person (or a retry) can act.
    await releaseEscalation(campaign.instantlyCampaignId).catch(() => {});
    return `NOT reassured: ${error instanceof Error ? error.message : String(error)}`;
  }
}

/**
 * The side effect, fired from `promoteEvent` on a real not-a-prospect kind.
 * Never throws.
 */
export async function maybeHandleNotAProspect(
  campaign: NotAProspectCampaign,
  leadEmail: string,
  eventType: string,
  source: string,
): Promise<void> {
  if (!isNotAProspectReplyKind(eventType)) return;
  if (!campaign.orgId) return;
  const orgId = campaign.orgId;

  try {
    const siblings = await siblingSequences(
      orgId,
      leadEmail,
      campaign.brandIds ?? [],
      campaign.instantlyCampaignId,
    );
    let stopped = 0;
    for (const id of siblings) {
      const ok = await stopLeadSequence({
        orgId,
        instantlyCampaignId: id,
        leadEmail,
        reason: `${eventType} on ${campaign.instantlyCampaignId}: not a prospect for this brand`,
        caller: { method: "POST", path: "/internal/not-a-prospect" },
      });
      if (ok) stopped += 1;
    }

    // A person who already buys from the client is a won sale, just not ours.
    // Told on every source (a hand-stated one included); never fatal.
    let won = "n/a";
    if (eventType === "lead_already_customer" && campaign.campaignId) {
      try {
        won = (
          await recordExistingCustomerByEmail({ orgId, campaignId: campaign.campaignId, email: leadEmail })
        ).status;
      } catch (error) {
        won = `FAILED: ${error instanceof Error ? error.message : String(error)}`;
      }
    }

    const reassurance =
      source === "manual"
        ? "skipped: stated by a person, who handles the conversation"
        : await reassureOnce(campaign, leadEmail);

    console.log(
      `[instantly-service] not-a-prospect: ${eventType} campaign=${campaign.instantlyCampaignId} lead=${leadEmail} source=${source} siblingsStopped=${stopped}/${siblings.length} wonNotOurs=${won} reassurance=${reassurance}`,
    );
  } catch (error) {
    console.error(
      `[instantly-service] not-a-prospect FAILED for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

// ─── The send gate ──────────────────────────────────────────────────────────

/** Machine-readable refusal code on the 409 body. */
export const NOT_A_PROSPECT_REFUSAL_CODE = "lead_not_a_prospect";

export interface NotAProspectRecord {
  brandId: string;
  replyKind: string;
}

/**
 * Does this address carry a standing not-a-prospect reply kind for one of the
 * brands of this send? Read off gold `instantly_lead_status_current.reply_kind`,
 * which already applies the manual-statement precedence and skips a withdrawn
 * statement, so a person who corrects the kind releases the gate.
 */
export async function findNotAProspect(
  leadEmail: string,
  brandIds: string[],
): Promise<NotAProspectRecord | null> {
  if (brandIds.length === 0) return null;
  const normalized = normalizeLeadEmail(leadEmail);
  if (!normalized) return null;
  const brandList = sql.join(
    brandIds.map((b) => sql`${b}`),
    sql`, `,
  );
  const kinds = sql.join(
    NOT_A_PROSPECT_REPLY_KINDS.map((k) => sql`${k}`),
    sql`, `,
  );
  const result = await db.execute(sql`
    SELECT b.brand_id AS brand_id, g.reply_kind AS reply_kind
    FROM instantly_lead_status_current g
    CROSS JOIN LATERAL unnest(g.brand_ids) AS b(brand_id)
    WHERE lower(g.lead_email) = ${normalized}
      AND g.reply_kind IN (${kinds})
      AND b.brand_id IN (${brandList})
    LIMIT 1
  `);
  const row = ((result as { rows?: Record<string, unknown>[] }).rows ?? [])[0];
  return row ? { brandId: String(row.brand_id), replyKind: String(row.reply_kind) } : null;
}

export function notAProspectRefusal(leadEmail: string, record: NotAProspectRecord) {
  return {
    error: "Not a prospect for this brand",
    code: NOT_A_PROSPECT_REFUSAL_CODE,
    details:
      `${leadEmail} answered a previous email for brand ${record.brandId} saying they are not a prospect ` +
      `(${record.replyKind}). No email was sent and nothing was billed.`,
    brandId: record.brandId,
    replyKind: record.replyKind,
  };
}
