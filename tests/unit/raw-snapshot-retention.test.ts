import { describe, it, expect, vi } from "vitest";
import fs from "fs";
import path from "path";

vi.mock("../../src/db", () => ({ db: { execute: vi.fn() } }));

import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";
import {
  trimSnapshotTable,
  trimImapDuplicates,
  trimRawDuplicates,
  trimSnapshotsQuery,
  trimImapWindowQuery,
  CAMPAIGNS_PER_CHUNK,
  IMAP_ROWS_PER_CHUNK,
  IMAP_DUPLICATE_MIN_AGE_DAYS,
  type Execute,
} from "../../src/lib/raw-snapshot-retention";

const dialect = new PgDialect();
const render = (q: SQL) => dialect.sqlToQuery(q);
const noPause = async () => {};
const read = (rel: string) => fs.readFileSync(path.join(__dirname, "..", "..", rel), "utf-8");

describe("trimSnapshotsQuery: which copies each table keeps", () => {
  it("config keeps latest, first and latest-with-org per campaign", () => {
    const { sql } = render(trimSnapshotsQuery("instantly_campaigns_config_raw", ["a"]));
    expect(sql).toContain("DELETE FROM instantly_campaigns_config_raw");
    expect(sql).toContain("r.newest > 1 AND r.oldest > 1 AND r.newest_with_org > 1");
  });

  it("analytics keeps only the latest per campaign", () => {
    const { sql } = render(trimSnapshotsQuery("instantly_analytics_raw", ["a"]));
    expect(sql).toContain("DELETE FROM instantly_analytics_raw");
    expect(sql).toMatch(/WHERE r\.newest > 1\s*\)/);
    expect(sql).not.toContain("r.oldest > 1");
  });

  it("binds the campaign ids, never inlines them", () => {
    const q = render(trimSnapshotsQuery("instantly_analytics_raw", ["x'; DROP", "b"]));
    expect(q.sql).not.toContain("DROP");
    expect(q.params).toContainEqual(["x'; DROP", "b"]);
  });
});

describe("trimSnapshotTable: walks campaigns in chunks", () => {
  it("pages campaign ids by cursor, sums deletes, stops on a short page", async () => {
    const full = Array.from({ length: CAMPAIGNS_PER_CHUNK }, (_, i) => ({ id: `c${String(i).padStart(4, "0")}` }));
    const calls: string[] = [];
    let idPages = 0;
    const execute: Execute = async (q) => {
      const { sql, params } = render(q);
      if (sql.includes("WITH RECURSIVE")) {
        calls.push(`ids>${params[0]}`);
        idPages++;
        return { rows: idPages === 1 ? full : [{ id: "z1" }], rowCount: null };
      }
      calls.push("delete");
      return { rows: [], rowCount: 100 };
    };
    const r = await trimSnapshotTable("instantly_analytics_raw", execute, noPause);
    expect(r).toEqual({ table: "instantly_analytics_raw", deleted: 200, statements: 2 });
    expect(calls).toEqual(["ids>", "delete", `ids>${full[full.length - 1].id}`, "delete"]);
  });

  it("does nothing on an empty table", async () => {
    const execute: Execute = async () => ({ rows: [], rowCount: 0 });
    expect(await trimSnapshotTable("instantly_campaigns_config_raw", execute, noPause)).toEqual({
      table: "instantly_campaigns_config_raw",
      deleted: 0,
      statements: 0,
    });
  });
});

describe("trimImapDuplicates: old unrelated copies with a newer twin", () => {
  it("only unrelated rows, only past the age floor, only when a newer copy of the Message-Id stays", () => {
    const now = new Date("2026-11-02T00:00:00Z");
    const q = render(trimImapWindowQuery(now, "-infinity", "", 10));
    expect(q.sql).toContain("m.kind = 'unrelated'");
    expect(q.sql).toContain("n.message_id = b.message_id");
    expect(q.sql).toContain("(n.polled_at, n.id) > (b.polled_at, b.id)");
    expect(q.sql).toContain("polled_at >= $");
    expect(q.params[0]).toBe("2026-11-02T00:00:00.000Z");
  });

  it("the age floor sits past every per-address window that reads the table", () => {
    // poll 3 d, orphan sweep 7 d, inbound blindness 14 d.
    expect(IMAP_DUPLICATE_MIN_AGE_DAYS).toBeGreaterThan(14);
    expect(read("src/lib/self-send/inbound-blindness.ts")).toContain("INBOUND_BLIND_WINDOW_DAYS = 14;");
    expect(read("src/lib/self-send/orphan-reply-sweep.ts")).toContain("ROUTINE_WINDOW_DAYS = 7;");
  });

  it("advances the keyset cursor as text and stops on a short window", async () => {
    const now = new Date("2026-11-02T00:00:00Z");
    const seen: unknown[][] = [];
    let n = 0;
    const execute: Execute = async (q) => {
      const { params } = render(q);
      seen.push(params);
      n++;
      return n === 1
        ? { rows: [{ deleted: 4000, scanned: IMAP_ROWS_PER_CHUNK, last_polled_at: "2026-09-01 10:00:00.123456", last_id: "id-9" }], rowCount: null }
        : { rows: [{ deleted: 7, scanned: 12, last_polled_at: "2026-09-02 00:00:00", last_id: "id-z" }], rowCount: null };
    };
    const r = await trimImapDuplicates(now, execute, noPause);
    expect(r).toEqual({ table: "imap_messages_raw", deleted: 4007, statements: 2 });
    const cutoff = new Date(now.getTime() - IMAP_DUPLICATE_MIN_AGE_DAYS * 86_400_000).toISOString();
    expect(seen[0]).toEqual([cutoff, "-infinity", "-infinity", "", IMAP_ROWS_PER_CHUNK]);
    expect(seen[1]).toEqual([cutoff, "2026-09-01 10:00:00.123456", "2026-09-01 10:00:00.123456", "id-9", IMAP_ROWS_PER_CHUNK]);
  });
});

describe("trimRawDuplicates: one table failing never skips the others", () => {
  it("logs a deleted line per table and carries on past a failure", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    const execute: Execute = async (q) => {
      const { sql } = render(q);
      if (sql.includes("instantly_campaigns_config_raw")) throw new Error("boom");
      if (sql.includes("WITH RECURSIVE")) return { rows: [{ id: "c1" }], rowCount: null };
      if (sql.includes("imap_messages_raw")) return { rows: [{ deleted: 3, scanned: 3, last_polled_at: "x", last_id: "y" }], rowCount: null };
      return { rows: [], rowCount: 5 };
    };
    const r = await trimRawDuplicates(new Date(), execute, noPause);
    expect(r.map((t) => [t.table, t.deleted])).toEqual([
      ["instantly_analytics_raw", 5],
      ["imap_messages_raw", 3],
    ]);
    expect(err.mock.calls[0][0]).toContain("raw-retention: FAILED: boom");
    const lines = log.mock.calls.map((c) => String(c[0]));
    expect(lines.some((l) => l.includes("raw-retention: instantly_analytics_raw deleted 5 duplicate rows"))).toBe(true);
    expect(lines.some((l) => l.includes("raw-retention: imap_messages_raw deleted 3 duplicate rows"))).toBe(true);
    log.mockRestore();
    err.mockRestore();
  });
});

describe("wiring", () => {
  it("index.ts arms the worker and migration 0102 indexes message_id", () => {
    expect(read("src/index.ts")).toContain("startRawRetentionWorker()");
    expect(read("drizzle/0102_imap_message_id_idx.sql")).toContain(
      "CREATE INDEX IF NOT EXISTS imap_messages_raw_message_id_idx\n  ON imap_messages_raw (message_id);",
    );
  });
});
