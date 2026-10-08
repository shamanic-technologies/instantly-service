/**
 * A reply the qualification fallback could not classify, told to a person.
 *
 * ⚠️ AN UNCLASSIFIED REPLY IS INVISIBLE. Every gate (opt-out, forward, rep
 * call, follow-up) hangs off the reply KIND, so a reply the sweep cannot read
 * or cannot label sits there, is re-selected every tick, and ages out of the
 * 7-day window without a single line saying so. Prod 2026-10-01: a bottom-posted
 * "STOP!" stripped to an empty body that way, and its opt-out stayed unrecorded
 * for a week until staff found it by hand.
 *
 * So once a reply has waited `UNCLASSIFIED_ALERT_AFTER_MS` past arrival and the
 * sweep still has no kind for it, the agency inbox gets ONE email per reply
 * (the claim is the reply's own timestamp on the campaign row's metadata, so
 * a newer reply on the same thread is a new alert).
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { agencyInbox } from "./agency-inbox";
import { sendEmail } from "./email-client";

/**
 * One hour: past the 15-minute grace and a handful of ticks, so a mirror that
 * was merely late (the common `no_body` cause) has had its retries, and still
 * early enough that a stop request is honoured the same day.
 */
export const UNCLASSIFIED_ALERT_AFTER_MS = 60 * 60 * 1000;

export interface UnclassifiedReplyAlertInput {
  instantlyCampaignId: string;
  leadEmail: string;
  orgId: string | null;
  userId: string | null;
  repliedAt: Date;
  reason: "no_body" | "unqualified";
  /** What we could read of the message, if anything. */
  bodyText: string | null;
}

export type UnclassifiedReplyAlertResult = "sent" | "too_early" | "already_sent";

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

const WHY: Record<UnclassifiedReplyAlertInput["reason"], string> = {
  no_body: "We could not read the message: Instantly's mirror holds no body for it.",
  unqualified: "We read the message but could not label it (nothing of theirs left once our quoted email is removed, or no usable answer from the classifier).",
};

/**
 * Throws when the email cannot be sent (the claim is released first, so the
 * next tick retries). The caller logs; it must never stop the sweep.
 */
export async function alertUnclassifiedReply(
  input: UnclassifiedReplyAlertInput,
  asOf: Date = new Date(),
): Promise<UnclassifiedReplyAlertResult> {
  if (asOf.getTime() - input.repliedAt.getTime() < UNCLASSIFIED_ALERT_AFTER_MS) return "too_early";

  const repliedAt = input.repliedAt.toISOString();
  const entry = JSON.stringify({ repliedAt, reason: input.reason, alertedAt: asOf.toISOString() });
  const claimed = rowsOf(
    await db.execute(sql`
      UPDATE instantly_campaigns
      SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{unclassifiedReplyAlert}', ${entry}::jsonb)
      WHERE instantly_campaign_id = ${input.instantlyCampaignId}
        AND (metadata->'unclassifiedReplyAlert'->>'repliedAt') IS DISTINCT FROM ${repliedAt}
      RETURNING instantly_campaign_id
    `),
  );
  if (claimed.length === 0) return "already_sent";

  try {
    await sendEmail(
      {
        appId: "instantly-service",
        eventType: "reply-unclassified",
        recipientEmail: agencyInbox(),
        metadata: {
          leadEmail: input.leadEmail,
          instantlyCampaignId: input.instantlyCampaignId,
          repliedAt,
          why: WHY[input.reason],
          body: input.bodyText?.trim().slice(0, 4000) || "(no body)",
        },
      },
      { orgId: input.orgId ?? "system", userId: input.userId ?? "system" },
    );
  } catch (error) {
    await db.execute(sql`
      UPDATE instantly_campaigns
      SET metadata = metadata - 'unclassifiedReplyAlert'
      WHERE instantly_campaign_id = ${input.instantlyCampaignId}
        AND metadata->'unclassifiedReplyAlert'->>'repliedAt' = ${repliedAt}
    `);
    throw error;
  }
  return "sent";
}
