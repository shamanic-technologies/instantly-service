// Per-brand re-contact window — a lookup of what THIS service already sent.
//
// A brand's prospect must not receive two cold emails from us inside three
// months. That rule is also enforced UPSTREAM, at the moment a person is
// SERVED (before we pay to reveal their email), and it works: zero re-serves
// inside the window since it shipped. But a serve and an actual email are
// separated by the sending queue, which has run up to three months behind — a
// lead handed over in May leaves in August. So a perfectly compliant serve
// still produces an email days (measured: 25 seconds) after the previous one
// landed in the prospect's inbox.
//
// Nothing in the fleet knows what was actually SENT except this service: it
// performed the terminal action against the vendor. The upstream guard
// structurally cannot answer it — it reasons about serves, and the queue
// decouples the two. So the send-time half of the rule lives here, and it
// reads OUR OWN silver event log. Do NOT ask another service for this: the
// send evidence is ours, and asking anyone else re-creates the bug.
//
// This is a LOOKUP, not a detector: one row, one timestamp, one comparison.
// There is no ratio, no threshold, no heuristic.

import { sql } from "drizzle-orm";
import { db } from "../db";

/**
 * The window, byte-identical in meaning to the serve path's
 * (human-service `suppression.ts` → `now() - interval '3 months'`).
 * Calendar-accurate and evaluated by Postgres at query time, NOT a
 * 90-day approximation — the two halves of one rule must not drift.
 */
export const RECONTACT_WINDOW_INTERVAL = "3 months";

/** Machine-readable refusal code on the 409 body. */
export const RECONTACT_REFUSAL_CODE = "recent_brand_contact";

/**
 * Same normalization the serve path applies to an identity key: trim +
 * lowercase. A prospect written `Joe@X.com` on one send and `joe@x.com` on the
 * next is the same inbox, and matching case-sensitively would let exactly that
 * pair through.
 */
export function normalizeLeadEmail(email: string): string {
  return email.trim().toLowerCase();
}

export interface RecentBrandContact {
  /** The brand of THIS send that the prospect was already emailed for. */
  brandId: string;
  /** When the most recent real email to them for that brand went out. */
  lastEmailedAt: Date;
}

/**
 * The most recent real email this service sent to `leadEmail` for any of
 * `brandIds`, inside the window. `null` = never, or outside the window.
 *
 * Evidence is a REAL (`inferred = false`) `email_sent` silver event. An
 * inferred row is a synthetic predecessor projected from a downstream event —
 * it asserts a send we never observed, so gating a live send on one would
 * refuse on a fact nobody witnessed.
 *
 * Both transports converge here: the Instantly webhook path and the self-send
 * dispatch worker both promote `email_sent` through `promoteEvent`, so there
 * is one source of send truth regardless of which pipe carried the mail.
 *
 * Fail loud — a DB error propagates. A gate that cannot read its own history
 * must not silently wave the send through.
 */
export async function findRecentBrandContact(
  leadEmail: string,
  brandIds: string[],
): Promise<RecentBrandContact | null> {
  // No brand on this send (platform sends carry none) → the window is per
  // brand, so there is no window to enforce. Not a fallback: there is
  // genuinely nothing to compare against.
  if (brandIds.length === 0) return null;

  const normalized = normalizeLeadEmail(leadEmail);
  if (!normalized) return null;

  const brandList = sql.join(
    brandIds.map((b) => sql`${b}`),
    sql`, `,
  );

  // Driven from `instantly_campaigns` (a handful of rows for one person) and
  // only then joined to the event log, so the scan is bounded by the person,
  // not by the fleet's ~88k events. `lower(lead_email)` is served by the
  // expression index added in migration 0040.
  const result = await db.execute(sql`
    SELECT b.brand_id AS brand_id,
           MAX(e.timestamp) AS last_emailed_at
    FROM instantly_campaigns c
    CROSS JOIN LATERAL unnest(c.brand_ids) AS b(brand_id)
    JOIN instantly_events e
      ON e.campaign_id = c.instantly_campaign_id
     AND e.event_type = 'email_sent'
     AND e.inferred = false
    WHERE lower(c.lead_email) = ${normalized}
      AND b.brand_id IN (${brandList})
      AND e.timestamp > now() - ${sql.raw(`interval '${RECONTACT_WINDOW_INTERVAL}'`)}
    GROUP BY b.brand_id
    ORDER BY MAX(e.timestamp) DESC
    LIMIT 1
  `);

  const row = (result.rows as { brand_id: string; last_emailed_at: string | Date }[])[0];
  if (!row) return null;

  return {
    brandId: row.brand_id,
    lastEmailedAt: new Date(row.last_emailed_at),
  };
}

/**
 * The refusal body. Explicit and distinguishable: a caller can tell it from a
 * success (200), from the other 409 on this route (lead-id conflict, a
 * different `code`), and from a transport failure (500). It is never a silent
 * drop and never a fake success.
 */
export function recontactRefusal(leadEmail: string, contact: RecentBrandContact) {
  return {
    error: "Recently contacted for this brand",
    code: RECONTACT_REFUSAL_CODE,
    details:
      `${leadEmail} was already emailed for brand ${contact.brandId} at ` +
      `${contact.lastEmailedAt.toISOString()}, inside the ${RECONTACT_WINDOW_INTERVAL} re-contact window. ` +
      `No email was sent and nothing was billed.`,
    brandId: contact.brandId,
    lastEmailedAt: contact.lastEmailedAt.toISOString(),
    windowInterval: RECONTACT_WINDOW_INTERVAL,
  };
}
