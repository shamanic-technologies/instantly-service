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
