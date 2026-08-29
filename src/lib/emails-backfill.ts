/**
 * Mirroring the whole Instantly Unibox into bronze, once, before it is deleted.
 *
 * Cancelling an Instantly plan — or a single DFY inbox — permanently deletes
 * every conversation those mailboxes sent or received, replies included, and
 * Instantly says so on the confirmation dialog. Our silver event log records
 * THAT a lead replied; the words they wrote live only in `instantly_emails_raw`,
 * and that table was only ever filled by the reconcile poll, which fetches
 * `/emails?campaign_id=` for campaigns it happens to re-poll. Whole domains have
 * no rows at all. This sweep closes that gap.
 *
 * It reads `/emails` with NO campaign filter, which returns the workspace
 * newest-first through one cursor. That is exhaustive by construction: it needs
 * no account list to be correct, so it also captures mail from mailboxes that
 * have since been deleted — precisely the ones about to be cancelled.
 *
 * Bronze only. Nothing is promoted to silver and no event is synthesized: this
 * is a photocopy of what Instantly holds, not a new source of facts.
 */
import { inArray } from "drizzle-orm";

import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { insertEmailsBatch } from "./bronze";
import { listEmailsPage, type EmailRecord } from "./instantly-client";

/** Rows per `/emails` page. Instantly returns an empty list above 100. */
const PAGE_SIZE = 100;

/**
 * Campaign ids per `IN (...)` chunk when resolving orgs. Drizzle expands a
 * large JS array into a ROW expression, which trips Postgres' 1664-entry limit,
 * so the lookup is chunked even though one page can only carry 100 campaigns.
 */
const ORG_LOOKUP_CHUNK = 500;

/** How often to log progress, in pages. */
const PROGRESS_EVERY_PAGES = 25;

export interface EmailsBackfillSummary {
  /** Pages read from Instantly. */
  pages: number;
  /** Emails returned across those pages. */
  emailsRead: number;
  /** Rows newly written to bronze (already-mirrored emails are not counted). */
  emailsStored: number;
  /** Of `emailsRead`, how many were INBOUND (a reply, a bounce, an auto-reply). */
  inboundRead: number;
  /** Of `emailsRead`, how many carried no campaign id. */
  campaignlessRead: number;
  /** Whether the sweep reached the end of the list (false when `maxPages` cut it). */
  exhausted: boolean;
}

/**
 * Group a page's emails by their campaign id, `null` for mail attached to none.
 *
 * Pure, so the grouping is testable without a database. Insertion order is
 * preserved within each group, which keeps a re-run's bronze rows in the same
 * order as the first run for anyone diffing them.
 */
export function groupByCampaign(
  emails: readonly EmailRecord[],
): Map<string | null, EmailRecord[]> {
  const groups = new Map<string | null, EmailRecord[]>();
  for (const email of emails) {
    const rawCampaignId = email.campaign_id;
    const campaignId = typeof rawCampaignId === "string" && rawCampaignId.length > 0
      ? rawCampaignId
      : null;
    const existing = groups.get(campaignId);
    if (existing) existing.push(email);
    else groups.set(campaignId, [email]);
  }
  return groups;
}

/** An email Instantly did not send from us — a reply, a bounce, an auto-reply. */
function isInbound(email: EmailRecord): boolean {
  return String(email.ue_type) !== "1";
}

/**
 * Resolve the owning org for each campaign id we have a local row for.
 *
 * A campaign we never stored (or a platform send) simply has no org, and the
 * bronze row carries `null` rather than a guessed one — the payload is the fact,
 * the org is only an index.
 */
async function resolveOrgIds(campaignIds: string[]): Promise<Map<string, string | null>> {
  const byCampaign = new Map<string, string | null>();
  for (let i = 0; i < campaignIds.length; i += ORG_LOOKUP_CHUNK) {
    const chunk = campaignIds.slice(i, i + ORG_LOOKUP_CHUNK);
    const rows = await db
      .select({
        instantlyCampaignId: instantlyCampaigns.instantlyCampaignId,
        orgId: instantlyCampaigns.orgId,
      })
      .from(instantlyCampaigns)
      .where(inArray(instantlyCampaigns.instantlyCampaignId, chunk));
    for (const row of rows) byCampaign.set(row.instantlyCampaignId, row.orgId ?? null);
  }
  return byCampaign;
}

/**
 * Walk the whole workspace and mirror every email into bronze.
 *
 * Idempotent: the insert conflicts on `instantly_email_id` and does nothing, so
 * a re-run re-reads pages but writes only what is new. Resumable in the only
 * sense that matters — each page is persisted as it arrives, so an interrupted
 * sweep keeps everything it had already read and a re-run picks the rest up.
 *
 * Fail-loud: an error on any page propagates. A sweep that silently skipped a
 * page would report a clean run over a Unibox it had not finished copying, and
 * the copy is the entire point.
 *
 * `maxPages` bounds the walk for a probe; without it the sweep runs to the end
 * of the list (~1,200 pages at 3.5s of mandated pacing ≈ 75 minutes).
 */
export async function backfillEmails(
  apiKey: string,
  options: { maxPages?: number } = {},
): Promise<EmailsBackfillSummary> {
  const { maxPages } = options;
  const summary: EmailsBackfillSummary = {
    pages: 0,
    emailsRead: 0,
    emailsStored: 0,
    inboundRead: 0,
    campaignlessRead: 0,
    exhausted: false,
  };

  let startingAfter: string | undefined;
  for (;;) {
    if (maxPages !== undefined && summary.pages >= maxPages) break;

    const page = await listEmailsPage(apiKey, { startingAfter, limit: PAGE_SIZE });
    summary.pages += 1;
    summary.emailsRead += page.items.length;
    summary.inboundRead += page.items.filter(isInbound).length;

    const groups = groupByCampaign(page.items);
    const campaignIds = [...groups.keys()].filter((id): id is string => id !== null);
    const orgByCampaign = await resolveOrgIds(campaignIds);

    for (const [campaignId, emails] of groups) {
      summary.emailsStored += (
        await insertEmailsBatch(
          campaignId,
          campaignId === null ? null : (orgByCampaign.get(campaignId) ?? null),
          emails,
        )
      ).length;
      if (campaignId === null) summary.campaignlessRead += emails.length;
    }

    if (summary.pages % PROGRESS_EVERY_PAGES === 0) {
      console.log(
        `[emails-backfill] progress ${JSON.stringify({
          pages: summary.pages,
          emailsRead: summary.emailsRead,
          emailsStored: summary.emailsStored,
          inboundRead: summary.inboundRead,
        })}`,
      );
    }

    // Terminate on an exhausted cursor OR an empty page — a page that returns
    // nothing while still handing back a cursor would otherwise loop forever.
    if (page.nextStartingAfter === null || page.items.length === 0) {
      summary.exhausted = true;
      break;
    }
    startingAfter = page.nextStartingAfter;
  }

  return summary;
}
