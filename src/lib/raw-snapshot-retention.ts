/**
 * Retention for the RAW tables that pile up duplicate copies of one item
 * (BOX-DISK-1101, owner-approved 2026-11-02: keep the latest copy of each item).
 *
 * The box disk had ~30 days left; three bronze tables were 8.2 GB of it, almost
 * all of it repeated copies:
 *
 *  - `instantly_campaigns_config_raw` (2.8 GB, 1.05M rows): one GET /campaigns/{id}
 *    snapshot per campaign per reconcile cycle, ~20 per campaign (max 139) over
 *    54k campaigns. Writer = `reconcile.ts` phase 0 (no row since 2026-10-11).
 *    Readers want the LATEST config (schedule timezone, step delays, footer
 *    sweeps), `outreach-facts.ts` the FIRST (`open_tracking` as the campaign was
 *    created), `reply-verdicts.ts` ANY row with an `org_id`. Kept per campaign:
 *    latest, first, and the latest carrying an `org_id`.
 *  - `instantly_analytics_raw` (1.5 GB, 1.0M rows): one /campaigns/analytics
 *    aggregate per campaign per cycle. Nothing reads it back (the drift check
 *    compares the fresh response in memory); kept per campaign: latest.
 *  - `imap_messages_raw` (3.8 GB, 945k rows): unique per (mailbox address,
 *    Message-Id), but one Workspace mailbox is polled under each of its ~5
 *    aliases, so every message it receives lands ~5 times (99.8 % of rows are
 *    `unrelated`, mostly our own warmup traffic). Only `unrelated` copies older
 *    than `IMAP_DUPLICATE_MIN_AGE_DAYS` go, and only when a newer copy of the same
 *    Message-Id stays. The age floor sits past every per-address window that
 *    reads this table (poll 3 d, orphan sweep 7 d, inbound blindness 14 d): inside
 *    it an alias losing its copy would re-insert it on the next poll or read as a
 *    mailbox nobody reads. `unrelated` rows are projected nowhere (messages-sync
 *    excludes them); correlated kinds (reply, bounce...) are never touched.
 *
 * Mechanics: chunked DELETEs (one short autocommit statement each, a pause
 * between), each on an index: the per-campaign ones on
 * `(instantly_campaign_id, fetched_at)` / `(instantly_campaign_id)` after a loose
 * index scan of the campaign ids, the IMAP one walking `polled_at` with a
 * `(polled_at, id)` keyset and probing the newer copy on `message_id`
 * (migration 0102). The space freed is reused by new rows; giving it back to the
 * filesystem is a VACUUM FULL run by ops, not this job.
 */
import { sql, type SQL } from "drizzle-orm";
import { db } from "../db";
import { errorMessage } from "./db-error.js";

export const RAW_RETENTION_FIRST_TICK_MS = 60 * 1000;
export const RAW_RETENTION_INTERVAL_MS = 24 * 60 * 60 * 1000;
/** Campaigns per DELETE: ~20 copies each (max ~150), so ~5k rows a statement. */
export const CAMPAIGNS_PER_CHUNK = 250;
/** IMAP rows examined per DELETE. */
export const IMAP_ROWS_PER_CHUNK = 5_000;
export const CHUNK_PAUSE_MS = 250;
export const IMAP_DUPLICATE_MIN_AGE_DAYS = 30;

type ExecResult = { rows: Record<string, unknown>[]; rowCount: number | null };
export type Execute = (query: SQL) => Promise<ExecResult>;
const defaultExecute: Execute = (q) => db.execute(q) as never;
const defaultPause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** The two per-campaign snapshot tables and which copies each one keeps. */
export const SNAPSHOT_TABLES = {
  instantly_campaigns_config_raw: { keepFirst: true, keepLatestWithOrg: true },
  instantly_analytics_raw: { keepFirst: false, keepLatestWithOrg: false },
} as const;
export type SnapshotTable = keyof typeof SNAPSHOT_TABLES;

export interface TableTrim {
  table: string;
  deleted: number;
  statements: number;
}

/** Next campaign ids after `cursor`, one index probe each (loose index scan). */
export function nextCampaignIdsQuery(table: SnapshotTable, cursor: string, limit: number): SQL {
  const t = sql.raw(table);
  return sql`
    WITH RECURSIVE c AS (
      (SELECT instantly_campaign_id AS id FROM ${t}
        WHERE instantly_campaign_id > ${cursor}
        ORDER BY instantly_campaign_id LIMIT 1)
      UNION ALL
      SELECT (SELECT instantly_campaign_id FROM ${t}
               WHERE instantly_campaign_id > c.id
               ORDER BY instantly_campaign_id LIMIT 1)
      FROM c WHERE c.id IS NOT NULL
    )
    SELECT id FROM c WHERE id IS NOT NULL LIMIT ${limit}`;
}

/** Delete every copy of these campaigns except the ones the table keeps. */
export function trimSnapshotsQuery(table: SnapshotTable, campaignIds: string[]): SQL {
  const t = sql.raw(table);
  const { keepFirst, keepLatestWithOrg } = SNAPSHOT_TABLES[table];
  const keepers: SQL[] = [sql`r.newest > 1`];
  if (keepFirst) keepers.push(sql`r.oldest > 1`);
  if (keepLatestWithOrg) keepers.push(sql`r.newest_with_org > 1`);
  return sql`
    DELETE FROM ${t} WHERE id IN (
      SELECT r.id FROM (
        SELECT id,
               row_number() OVER (PARTITION BY instantly_campaign_id ORDER BY fetched_at DESC, id DESC) AS newest,
               row_number() OVER (PARTITION BY instantly_campaign_id ORDER BY fetched_at ASC, id ASC) AS oldest,
               row_number() OVER (PARTITION BY instantly_campaign_id
                                  ORDER BY (org_id IS NOT NULL) DESC, fetched_at DESC, id DESC) AS newest_with_org
        FROM ${t}
        WHERE instantly_campaign_id = ANY(${sql.param(campaignIds)}::text[])
      ) r
      WHERE ${sql.join(keepers, sql` AND `)}
    )`;
}

export async function trimSnapshotTable(
  table: SnapshotTable,
  execute: Execute = defaultExecute,
  pause: (ms: number) => Promise<void> = defaultPause,
): Promise<TableTrim> {
  let cursor = "";
  let deleted = 0;
  let statements = 0;
  for (;;) {
    const ids = (await execute(nextCampaignIdsQuery(table, cursor, CAMPAIGNS_PER_CHUNK))).rows.map((r) =>
      String(r.id),
    );
    if (ids.length === 0) break;
    const res = await execute(trimSnapshotsQuery(table, ids));
    statements++;
    deleted += res.rowCount ?? 0;
    cursor = ids[ids.length - 1];
    if (ids.length < CAMPAIGNS_PER_CHUNK) break;
    await pause(CHUNK_PAUSE_MS);
  }
  return { table, deleted, statements };
}

/**
 * One keyset window of old `imap_messages_raw` rows: delete the `unrelated`
 * ones that have a NEWER copy of the same Message-Id (any address, any kind),
 * and return where the window ended. The cursor timestamp travels as TEXT so
 * its microseconds survive the round trip (a JS Date keeps milliseconds).
 */
export function trimImapWindowQuery(cutoff: Date, cursorPolledAt: string, cursorId: string, limit: number): SQL {
  return sql`
    WITH batch AS (
      SELECT id, message_id, polled_at FROM imap_messages_raw
      WHERE polled_at < ${cutoff.toISOString()}::timestamptz AT TIME ZONE 'UTC'
        -- The plain range is what lets the planner walk imap_messages_raw_polled_at_idx;
        -- the row compare only breaks ties inside one timestamp.
        AND polled_at >= ${cursorPolledAt}::timestamp
        AND (polled_at, id) > (${cursorPolledAt}::timestamp, ${cursorId})
      ORDER BY polled_at, id
      LIMIT ${limit}
    ), gone AS (
      DELETE FROM imap_messages_raw m
      USING batch b
      WHERE m.id = b.id
        AND m.kind = 'unrelated'
        AND EXISTS (
          SELECT 1 FROM imap_messages_raw n
          WHERE n.message_id = b.message_id
            AND (n.polled_at, n.id) > (b.polled_at, b.id)
        )
      RETURNING m.id
    ), last AS (
      SELECT polled_at::text AS polled_at, id FROM batch ORDER BY polled_at DESC, id DESC LIMIT 1
    )
    SELECT (SELECT count(*) FROM gone)::int AS deleted,
           (SELECT count(*) FROM batch)::int AS scanned,
           (SELECT polled_at FROM last) AS last_polled_at,
           (SELECT id FROM last) AS last_id`;
}

export async function trimImapDuplicates(
  now: Date = new Date(),
  execute: Execute = defaultExecute,
  pause: (ms: number) => Promise<void> = defaultPause,
): Promise<TableTrim> {
  const cutoff = new Date(now.getTime() - IMAP_DUPLICATE_MIN_AGE_DAYS * 24 * 60 * 60 * 1000);
  let cursorPolledAt = "-infinity";
  let cursorId = "";
  let deleted = 0;
  let statements = 0;
  for (;;) {
    const [row] = (await execute(trimImapWindowQuery(cutoff, cursorPolledAt, cursorId, IMAP_ROWS_PER_CHUNK)))
      .rows;
    statements++;
    deleted += Number(row?.deleted ?? 0);
    const scanned = Number(row?.scanned ?? 0);
    if (scanned < IMAP_ROWS_PER_CHUNK || !row?.last_polled_at) break;
    cursorPolledAt = String(row.last_polled_at);
    cursorId = String(row.last_id);
    await pause(CHUNK_PAUSE_MS);
  }
  return { table: "imap_messages_raw", deleted, statements };
}

/** One full sweep: each table on its own, a failure on one never skips the others. */
export async function trimRawDuplicates(
  now: Date = new Date(),
  execute: Execute = defaultExecute,
  pause: (ms: number) => Promise<void> = defaultPause,
): Promise<TableTrim[]> {
  const jobs: Array<() => Promise<TableTrim>> = [
    () => trimSnapshotTable("instantly_campaigns_config_raw", execute, pause),
    () => trimSnapshotTable("instantly_analytics_raw", execute, pause),
    () => trimImapDuplicates(now, execute, pause),
  ];
  const done: TableTrim[] = [];
  for (const job of jobs) {
    const started = Date.now();
    try {
      const r = await job();
      done.push(r);
      console.log(
        `[instantly-service] raw-retention: ${r.table} deleted ${r.deleted} duplicate rows ` +
          `(${r.statements} statements, ${Date.now() - started}ms)`,
      );
    } catch (error: unknown) {
      console.error(`[instantly-service] raw-retention: FAILED: ${errorMessage(error)}`);
    }
  }
  return done;
}

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function rawRetentionTick(now: Date = new Date()): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = trimRawDuplicates(now)
    .then(() => undefined)
    .finally(() => {
      inFlight = null;
    });
  return inFlight;
}

export function startRawRetentionWorker(): void {
  if (timer) return;
  setTimeout(() => void rawRetentionTick(), RAW_RETENTION_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void rawRetentionTick(), RAW_RETENTION_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] raw-retention worker armed (first pass in ${RAW_RETENTION_FIRST_TICK_MS}ms, then every ${RAW_RETENTION_INTERVAL_MS}ms)`,
  );
}
