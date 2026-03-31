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
