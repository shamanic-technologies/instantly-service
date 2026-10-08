import { describe, it, expect } from "vitest";
import fs from "fs";
import path from "path";

const drizzleDir = path.join(__dirname, "..", "..", "drizzle");

describe("drizzle migration journal", () => {
  it("should have a journal entry for every migration SQL file", () => {
    const sqlFiles = fs
      .readdirSync(drizzleDir)
      .filter((f) => f.endsWith(".sql"))
      .map((f) => f.replace(".sql", ""))
      .sort();

    const journal = JSON.parse(
      fs.readFileSync(path.join(drizzleDir, "meta", "_journal.json"), "utf-8")
    );
    const journalTags: string[] = journal.entries.map(
      (e: { tag: string }) => e.tag
    );

    const missingFromJournal = sqlFiles.filter(
      (f) => !journalTags.includes(f)
    );

    expect(missingFromJournal).toEqual([]);
  });

  it("should have sequential idx values in the journal", () => {
    const journal = JSON.parse(
      fs.readFileSync(path.join(drizzleDir, "meta", "_journal.json"), "utf-8")
    );

    journal.entries.forEach((entry: { idx: number }, i: number) => {
      expect(entry.idx).toBe(i);
    });
  });
});

describe("instantly_emails_raw per-mailbox index (migration 0065)", () => {
  const read = (rel: string) => fs.readFileSync(path.join(__dirname, "..", "..", rel), "utf-8");

  it("indexes the exact expression the per-mailbox readers filter on", () => {
    const migration = read("drizzle/0065_emails_raw_eaccount_idx.sql");
    expect(migration).toContain(
      "CREATE INDEX IF NOT EXISTS instantly_emails_raw_eaccount_idx\n  ON instantly_emails_raw ((payload->>'eaccount'));",
    );
    // Without it, every IMAP poll and inbox-watcher arrival seq-scanned the
    // whole bronze table (~0.9 s of 3 workers each on prod, 2026-10-08).
    expect(read("src/lib/self-send/imap-poller.ts")).toContain("m.payload->>'eaccount' = ${accountEmail}");
    expect(read("src/lib/self-send/orphan-reply-sweep.ts")).toContain("payload->>'eaccount' = ${accountEmail}");
  });
});

describe("instantly_events index-only stats reads (migration 0066)", () => {
  const read = (rel: string) => fs.readFileSync(path.join(__dirname, "..", "..", rel), "utf-8");
  const migration = read("drizzle/0066_events_stats_index_only.sql");
  const analytics = read("src/routes/analytics.ts");

  it("indexes the bounce sets on the exact predicate BOUNCE_JOINS filters on", () => {
    expect(migration).toContain(
      "CREATE INDEX IF NOT EXISTS instantly_events_bounced_idx\n  ON instantly_events (campaign_id, lead_email, step)\n  WHERE event_type = 'email_bounced';",
    );
    // Both DISTINCT bounce sets select these columns under this predicate; a
    // different literal would leave the partial index unusable.
    const bounceJoins = analytics.slice(analytics.indexOf("const BOUNCE_JOINS"), analytics.indexOf("const BOUNCED_STEP"));
    expect(bounceJoins).toContain("SELECT DISTINCT campaign_id, lead_email, step FROM instantly_events\n        WHERE event_type = 'email_bounced'");
    expect(bounceJoins).toContain("SELECT DISTINCT campaign_id, lead_email FROM instantly_events\n        WHERE event_type = 'email_bounced'");
  });

  it("covers the columns the stats events side reads, keeping the old key columns", () => {
    expect(migration).toContain(
      "ON instantly_events (campaign_id, event_type, lead_email, step)\n  INCLUDE (account_email, \"timestamp\");",
    );
    // internalExclusionClause reads account_email; the day bucket reads timestamp.
    expect(analytics).toContain("(e.account_email IS NULL OR e.lead_email != e.account_email)");
    expect(analytics).toContain("localDayKey(sql`e.timestamp`, timezone)");
  });

  it("builds the replacement before dropping the old index (short exclusive lock)", () => {
    const create = migration.indexOf("CREATE INDEX IF NOT EXISTS instantly_events_stats_covering_incl_idx");
    const drop = migration.indexOf("DROP INDEX IF EXISTS instantly_events_stats_covering_idx;");
    const rename = migration.indexOf("ALTER INDEX instantly_events_stats_covering_incl_idx RENAME TO instantly_events_stats_covering_idx;");
    expect(create).toBeGreaterThan(-1);
    expect(drop).toBeGreaterThan(create);
    expect(rename).toBeGreaterThan(drop);
  });

  it("keeps the visibility map current so the scans stay index-only", () => {
    expect(migration).toContain("ALTER TABLE instantly_events SET (autovacuum_vacuum_insert_scale_factor = 0.02);");
  });
});
