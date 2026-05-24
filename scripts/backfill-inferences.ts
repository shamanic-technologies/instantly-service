/**
 * One-shot CLI to backfill silver inference rows on top of an existing
 * `instantly_events` table. Runs the deterministic inference rules
 * (`opened ⇒ sent`, `clicked ⇒ opened+sent`, `replied/bounced/unsub ⇒ sent`,
 * `sent step N ⇒ sent 1..N-1`) for every existing trigger event.
 *
 * Idempotent — re-running is a no-op (predecessors dedup via silver indexes).
 *
 * Usage:
 *   npm run backfill:inferences
 *   npm run backfill:inferences -- --dry-run
 *
 * MUST NOT be wired into boot (port-bind hazard on Railway). Manual CLI only.
 */
import { db, closeDb } from "../src/db";
import { instantlyEvents } from "../src/db/schema";
import { eq, and, isNull, sql } from "drizzle-orm";
import { backfillInferenceForEvent } from "../src/lib/silver-promote";

const TRIGGER_EVENT_TYPES = [
  "email_opened",
  "email_link_clicked",
  "reply_received",
  "email_bounced",
  "lead_unsubscribed",
  "email_sent",
] as const;

interface CliArgs {
  dryRun: boolean;
  batchSize: number;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const sizeIdx = args.indexOf("--batch-size");
  const batchSize =
    sizeIdx >= 0 && args[sizeIdx + 1] ? parseInt(args[sizeIdx + 1], 10) : 500;
  return { dryRun, batchSize };
}

async function countSilverRows(): Promise<{ total: number; inferred: number }> {
  const [totalRow] = await db
    .select({ count: sql<number>`count(*)::int` })
    .from(instantlyEvents);
  const [inferredRow] = await db
    .select({ count: sql<number>`count(*)::int` })
    .from(instantlyEvents)
    .where(eq(instantlyEvents.inferred, true));
  return {
    total: Number(totalRow?.count ?? 0),
    inferred: Number(inferredRow?.count ?? 0),
  };
}

async function fetchTriggerEvents(eventType: string): Promise<
  Array<{
    id: string;
    campaignId: string | null;
    leadEmail: string | null;
    accountEmail: string | null;
    step: number | null;
    timestamp: Date;
  }>
> {
  return db
    .select({
      id: instantlyEvents.id,
      campaignId: instantlyEvents.campaignId,
      leadEmail: instantlyEvents.leadEmail,
      accountEmail: instantlyEvents.accountEmail,
      step: instantlyEvents.step,
      timestamp: instantlyEvents.timestamp,
    })
    .from(instantlyEvents)
    .where(
      and(
        eq(instantlyEvents.eventType, eventType),
        eq(instantlyEvents.inferred, false),
      ),
    )
    .orderBy(instantlyEvents.createdAt);
}

async function main(): Promise<void> {
  const { dryRun, batchSize } = parseArgs();
  console.log(
    `[backfill-inferences] starting${dryRun ? " (DRY RUN)" : ""} batchSize=${batchSize}`,
  );

  const before = await countSilverRows();
  console.log(
    `[backfill-inferences] before: total=${before.total} inferred=${before.inferred}`,
  );

  let totalProcessed = 0;

  for (const eventType of TRIGGER_EVENT_TYPES) {
    const rows = await fetchTriggerEvents(eventType);
    console.log(
      `[backfill-inferences] event_type=${eventType} candidates=${rows.length}`,
    );
    if (rows.length === 0) continue;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      for (const row of batch) {
        if (!row.campaignId) continue;
        if (row.step == null) continue;

        if (dryRun) {
          totalProcessed++;
          continue;
        }

        await backfillInferenceForEvent({
          silverEventId: row.id,
          eventType,
          instantlyCampaignId: row.campaignId,
          leadEmail: row.leadEmail,
          accountEmail: row.accountEmail,
          step: row.step,
          timestamp: row.timestamp,
        });
        totalProcessed++;
      }
      console.log(
        `[backfill-inferences] event_type=${eventType} batch ${i / batchSize + 1}/${Math.ceil(rows.length / batchSize)} done`,
      );
    }
  }

  const after = await countSilverRows();
  console.log(
    `[backfill-inferences] after: total=${after.total} inferred=${after.inferred}`,
  );
  console.log(
    `[backfill-inferences] done. processed=${totalProcessed} new_inferred=${after.inferred - before.inferred}`,
  );

  await closeDb();
}

main().catch((err) => {
  console.error("[backfill-inferences] fatal:", err);
  process.exit(1);
});
