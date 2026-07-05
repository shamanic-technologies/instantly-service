import { describe, it, expect } from "vitest";
import {
  aggregatePlacementRows,
  blendEspRows,
  selectDailyTestBatch,
  type LatestEspRow,
} from "../../src/lib/placement-promote";
import type { InboxPlacementAnalyticsRow } from "../../src/lib/instantly-client";

function received(
  sender: string,
  recipient: string,
  esp: number,
  isSpam: boolean,
  auth: Partial<Pick<InboxPlacementAnalyticsRow, "spf_pass" | "dkim_pass" | "dmarc_pass">> = {},
): InboxPlacementAnalyticsRow {
  return {
    id: `${sender}-${recipient}`,
    test_id: "t1",
    is_spam: isSpam,
    sender_email: sender,
    sender_esp: 1,
    recipient_email: recipient,
    recipient_esp: esp,
    spf_pass: auth.spf_pass ?? true,
    dkim_pass: auth.dkim_pass ?? true,
    dmarc_pass: auth.dmarc_pass ?? true,
    record_type: 2,
  };
}

const TESTED = new Date("2026-06-30T09:00:00.000Z");

describe("aggregatePlacementRows", () => {
  it("counts inbox vs spam per (account, ESP) and computes percentages", () => {
    // acct A, Gmail(1): 3 inbox + 1 spam = 4 seed → 75/25/0
    const rows = [
      received("a@x.com", "s1@g", 1, false),
      received("a@x.com", "s2@g", 1, false),
      received("a@x.com", "s3@g", 1, false),
      received("a@x.com", "s4@g", 1, true),
    ];
    const [r] = aggregatePlacementRows(rows, "t1", TESTED);
    expect(r).toMatchObject({
      testId: "t1",
      accountEmail: "a@x.com",
      recipientEsp: 1,
      seedTotal: 4,
      inboxCount: 3,
      spamCount: 1,
      missingCount: 0,
      inboxPct: 75,
      spamPct: 25,
      missingPct: 0,
    });
    expect(r.testedAt).toEqual(TESTED);
  });

  it("derives missing from seeds sent but never received", () => {
    // 2 received (1 inbox, 1 spam) + a sent-only seed with no received row → missing 1
    const rows = [
      received("a@x.com", "s1@g", 1, false),
      received("a@x.com", "s2@g", 1, true),
      {
        id: "sent-only",
        test_id: "t1",
        is_spam: null, // not received
        sender_email: "a@x.com",
        sender_esp: 1,
        recipient_email: "s3@g",
        recipient_esp: 1,
        spf_pass: null,
        dkim_pass: null,
        dmarc_pass: null,
        record_type: 1,
      } as InboxPlacementAnalyticsRow,
    ];
    const [r] = aggregatePlacementRows(rows, "t1", TESTED);
    expect(r.seedTotal).toBe(3);
    expect(r.inboxCount).toBe(1);
    expect(r.spamCount).toBe(1);
    expect(r.missingCount).toBe(1);
    expect(r.inboxPct).toBe(33);
    expect(r.missingPct).toBe(33);
  });

  it("splits into separate rows per ESP (Gmail vs Outlook)", () => {
    const rows = [
      received("a@x.com", "g1@g", 1, true), // Gmail spam
      received("a@x.com", "o1@o", 2, false), // Outlook inbox
    ];
    const out = aggregatePlacementRows(rows, "t1", TESTED).sort(
      (x, y) => x.recipientEsp - y.recipientEsp,
    );
    expect(out).toHaveLength(2);
    expect(out[0]).toMatchObject({ recipientEsp: 1, spamPct: 100, inboxPct: 0 });
    expect(out[1]).toMatchObject({ recipientEsp: 2, inboxPct: 100, spamPct: 0 });
  });

  it("skips rows missing sender_email or recipient_esp (unattributable)", () => {
    const rows = [
      received("a@x.com", "s1@g", 1, false),
      { ...received("", "s2@g", 1, false) }, // no sender
      { ...received("a@x.com", "s3@g", 1, false), recipient_esp: null }, // no esp
    ];
    const out = aggregatePlacementRows(rows, "t1", TESTED);
    expect(out).toHaveLength(1);
    expect(out[0].seedTotal).toBe(1);
  });

  it("auth flags AND-fold: true only when all received rows pass", () => {
    const rows = [
      received("a@x.com", "s1@g", 1, false, { dkim_pass: true }),
      received("a@x.com", "s2@g", 1, false, { dkim_pass: false }),
    ];
    const [r] = aggregatePlacementRows(rows, "t1", TESTED);
    expect(r.spfPass).toBe(true);
    expect(r.dkimPass).toBe(false); // one failed
  });

  it("returns [] for no rows", () => {
    expect(aggregatePlacementRows([], "t1", TESTED)).toEqual([]);
  });
});

describe("blendEspRows", () => {
  const row = (o: Partial<LatestEspRow>): LatestEspRow => ({
    inboxCount: 0,
    spamCount: 0,
    missingCount: 0,
    seedTotal: 0,
    testedAt: TESTED,
    ...o,
  });

  it("sums counts across ESPs and recomputes pooled percentages", () => {
    // Gmail: 0 inbox / 4 spam ; Outlook: 4 inbox / 0 spam → pooled 4/4/0 of 8
    const blended = blendEspRows([
      row({ inboxCount: 0, spamCount: 4, seedTotal: 4 }),
      row({ inboxCount: 4, spamCount: 0, seedTotal: 4 }),
    ]);
    expect(blended).toEqual({
      inboxPct: 50,
      spamPct: 50,
      missingPct: 0,
      testedAt: TESTED.toISOString(),
    });
  });

  it("takes the newest testedAt across rows", () => {
    const newer = new Date("2026-07-01T00:00:00.000Z");
    const blended = blendEspRows([
      row({ inboxCount: 1, seedTotal: 1, testedAt: TESTED }),
      row({ inboxCount: 1, seedTotal: 1, testedAt: newer }),
    ]);
    expect(blended?.testedAt).toBe(newer.toISOString());
  });

  it("returns null for empty input or zero pooled seed (never fabricates 0%)", () => {
    expect(blendEspRows([])).toBeNull();
    expect(blendEspRows([row({ seedTotal: 0 })])).toBeNull();
  });
});

describe("selectDailyTestBatch", () => {
  const d = (iso: string) => new Date(iso);

  it("empty pool → []", () => {
    expect(selectDailyTestBatch([], new Map())).toEqual([]);
  });

  it("N = ceil(pool / 7)", () => {
    const pool = Array.from({ length: 14 }, (_, i) => `a${i}@x.com`);
    expect(selectDailyTestBatch(pool, new Map())).toHaveLength(2);
    expect(selectDailyTestBatch(pool.slice(0, 10), new Map())).toHaveLength(2); // ceil(10/7)=2
    expect(selectDailyTestBatch(pool.slice(0, 7), new Map())).toHaveLength(1);
    expect(selectDailyTestBatch(pool.slice(0, 3), new Map())).toHaveLength(1);
  });

  it("never-tested accounts sort FIRST (highest priority)", () => {
    const pool = ["tested@x.com", "never@x.com"];
    const last = new Map<string, Date | null>([["tested@x.com", d("2026-07-05T00:00:00Z")]]);
    // N = ceil(2/7) = 1 → the never-tested one wins.
    expect(selectDailyTestBatch(pool, last)).toEqual(["never@x.com"]);
  });

  it("least-recently-tested first among tested accounts", () => {
    const pool = ["fresh@x.com", "stale@x.com", "mid@x.com"];
    const last = new Map<string, Date | null>([
      ["fresh@x.com", d("2026-07-05T00:00:00Z")],
      ["stale@x.com", d("2026-07-01T00:00:00Z")],
      ["mid@x.com", d("2026-07-03T00:00:00Z")],
    ]);
    // N = ceil(3/7) = 1 → the stalest.
    expect(selectDailyTestBatch(pool, last)).toEqual(["stale@x.com"]);
  });

  it("ties (same last-tested / both null) break deterministically by email", () => {
    const pool = ["b@x.com", "a@x.com"];
    expect(selectDailyTestBatch(pool, new Map())).toEqual(["a@x.com"]);
  });

  it("covers the whole pool within 7 rounds (rotation, no starvation)", () => {
    const pool = Array.from({ length: 14 }, (_, i) => `a${String(i).padStart(2, "0")}@x.com`);
    const last = new Map<string, Date | null>(); // all never-tested
    const seen = new Set<string>();
    let clock = 0;
    for (let day = 0; day < 7; day++) {
      const batch = selectDailyTestBatch(pool, last);
      for (const email of batch) {
        seen.add(email);
        last.set(email, d(`2026-07-${String(10 + clock++).padStart(2, "0")}T00:00:00Z`));
      }
    }
    expect(seen.size).toBe(pool.length); // every account tested once across the week
  });
});
