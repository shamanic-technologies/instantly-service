import { describe, it, expect, vi, beforeEach } from "vitest";

// Minimal drizzle stand-in: records every insert, serves the stored sync state.
const inserts: Array<{ table: string; values: unknown }> = [];
let state: { cursor: string | null; caughtUpAt: Date | null } | null = null;

vi.mock("../../src/db/schema", () => ({
  emailVerificationHistory: { __name: "history", verificationId: "verification_id" },
  emailVerificationHistorySync: { __name: "sync", feed: "feed", cursor: "cursor", caughtUpAt: "caught_up_at" },
}));

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({
      from: () => ({ where: () => ({ limit: async () => (state ? [state] : []) }) }),
    }),
    insert: (table: { __name: string }) => ({
      values: (values: any) => ({
        onConflictDoUpdate: async ({ set }: { set: any }) => {
          inserts.push({ table: table.__name, values });
          if (table.__name === "sync") {
            state = {
              cursor: values.cursor,
              caughtUpAt: set.caughtUpAt ?? state?.caughtUpAt ?? values.caughtUpAt ?? null,
            };
          }
        },
      }),
    }),
  },
}));

const mockFetchPage = vi.fn();
vi.mock("../../src/lib/apollo-client", () => ({
  fetchVerificationHistoryPage: (cursor: string | null) => mockFetchPage(cursor),
}));

import {
  syncVerificationHistory,
  readMirrorCaughtUpAt,
  CAUGHT_UP_LAG_MS,
} from "../../src/lib/verification-history-mirror";

const row = (id: string, email: string, verdict: string | null) => ({ id, email, verdict, verifiedAt: "2026-10-01T00:00:00.000Z" });

describe("verification-history mirror", () => {
  beforeEach(() => {
    inserts.length = 0;
    state = null;
    mockFetchPage.mockReset();
  });

  it("pages to the end of the feed, keeps the cursor after each page, and marks the copy complete up to pass start minus the settle lag", async () => {
    mockFetchPage
      .mockResolvedValueOnce({ rows: [row("a", " Ann@X.com ", "valid")], nextCursor: "1", hasMore: true })
      .mockResolvedValueOnce({ rows: [row("b", "bob@y.com", null)], nextCursor: "2", hasMore: false });
    const before = Date.now();
    const out = await syncVerificationHistory();
    expect(out).toMatchObject({ pages: 2, rows: 2, caughtUp: true, cursor: "2" });
    expect(mockFetchPage.mock.calls.map((c) => c[0])).toEqual([null, "1"]);

    const history = inserts.filter((i) => i.table === "history").flatMap((i) => i.values as any[]);
    expect(history.map((h) => [h.verificationId, h.email, h.verdict])).toEqual([
      ["a", "ann@x.com", "valid"],
      ["b", "bob@y.com", null], // no verdict stays no verdict
    ]);
    const caughtUp = await readMirrorCaughtUpAt();
    expect(caughtUp).not.toBeNull();
    expect(caughtUp!.getTime()).toBeLessThanOrEqual(before - CAUGHT_UP_LAG_MS + 50);
  });

  it("resumes from the stored cursor", async () => {
    state = { cursor: "41", caughtUpAt: new Date("2026-10-14T00:00:00Z") };
    mockFetchPage.mockResolvedValueOnce({ rows: [], nextCursor: "41", hasMore: false });
    await syncVerificationHistory();
    expect(mockFetchPage).toHaveBeenCalledWith("41");
  });

  it("a page that fails leaves the copy where it was: the cursor of the last stored page, never marked complete", async () => {
    mockFetchPage
      .mockResolvedValueOnce({ rows: [row("a", "a@x.com", "valid")], nextCursor: "1", hasMore: true })
      .mockRejectedValueOnce(new Error("apollo 502"));
    await expect(syncVerificationHistory()).rejects.toThrow(/apollo 502/);
    expect(state?.cursor).toBe("1");
    expect(await readMirrorCaughtUpAt()).toBeNull();
  });
});
