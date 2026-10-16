/**
 * The ONE staff alert per new sending-domain listing (DOMAIN-BLOCKLIST-1016).
 *
 * A `listed` event is written `alert_status = 'pending'` in the same
 * transaction as the listing; this module delivers it and flips it to `sent`.
 * A listing that persists writes no new event, so the owner is told once, never
 * daily. A failed delivery stays pending and is retried on the next worker tick
 * (the event id rides the request as its dedup key, so a retry after a lost
 * answer does not send twice).
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { blocklistByKey, type BlocklistKey } from "./domain-blocklist";
import { sendDomainBlocklistedAlert, type DomainBlocklistedAlert } from "./email-client";

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

export interface PendingAlertRow {
  id: string;
  domain: string;
  list: BlocklistKey;
  answer: string | null;
  labels: string[] | null;
  mailboxes: string[] | null;
  detail: { firstBlocklistBounceAt?: string | null } | null;
  occurredAt: Date | string;
}

/** The alert's words, from the event alone. Pure. */
export function buildBlocklistAlert(row: PendingAlertRow): DomainBlocklistedAlert {
  const def = blocklistByKey(row.list);
  if (!def) throw new Error(`unknown list ${row.list} on event ${row.id}`);
  const occurredAt = row.occurredAt instanceof Date ? row.occurredAt : new Date(String(row.occurredAt));
  const mailboxes = row.mailboxes ?? [];
  const firstBounce = row.detail?.firstBlocklistBounceAt ?? null;
  const labels = (row.labels ?? []).join(", ");
  return {
    eventId: row.id,
    domain: row.domain,
    list: def.name,
    listedSince: occurredAt.toISOString().slice(0, 10),
    listAnswer: labels ? `${labels} (${row.answer ?? "?"})` : (row.answer ?? "listed"),
    mailboxes: mailboxes.length > 0 ? mailboxes.join(", ") : "none",
    actionTaken:
      `Its ${mailboxes.length} mailbox${mailboxes.length === 1 ? "" : "es"} take no new prospects from now on. ` +
      "Follow-ups already planned from them still go out. They come back by themselves the day a daily check reads the domain clean." +
      (firstBounce ? ` First bounce citing a blocklist from this domain: ${firstBounce.slice(0, 10)}.` : ""),
    removalUrl: def.removalUrl(row.domain),
  };
}

export async function deliverPendingBlocklistAlerts(
  send: (alert: DomainBlocklistedAlert) => Promise<void> = sendDomainBlocklistedAlert,
): Promise<{ attempted: number; sent: number; failed: number }> {
  const result = await db.execute(sql`
    SELECT id, domain, list, answer, labels, mailboxes, detail, occurred_at AS "occurredAt"
    FROM domain_blocklist_events
    WHERE alert_status = 'pending'
    ORDER BY occurred_at, id
    LIMIT 50
  `);
  const rows = rowsOf<PendingAlertRow>(result);
  let sent = 0;
  let failed = 0;
  for (const row of rows) {
    try {
      await send(buildBlocklistAlert(row));
      await db.execute(sql`
        UPDATE domain_blocklist_events
        SET alert_status = 'sent', alerted_at = now(), alert_attempts = alert_attempts + 1, alert_last_error = NULL
        WHERE id = ${row.id} AND alert_status = 'pending'
      `);
      sent += 1;
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      await db.execute(sql`
        UPDATE domain_blocklist_events
        SET alert_attempts = alert_attempts + 1, alert_last_error = ${message.slice(0, 1000)}
        WHERE id = ${row.id}
      `);
      console.error(`[instantly-service] domain-blocklist: alert NOT sent for ${row.domain} on ${row.list} (event ${row.id}): ${message}`);
      failed += 1;
    }
  }
  return { attempted: rows.length, sent, failed };
}
