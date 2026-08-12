import { describe, it, expect } from "vitest";
import {
  aggregatePlacementRows,
  summarizeEspRows,
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

describe("summarizeEspRows — ONE pooled score across every ESP", () => {
  const row = (o: Partial<LatestEspRow>): LatestEspRow => ({
    recipientEsp: 1,
    inboxCount: 0,
    spamCount: 0,
    missingCount: 0,
    seedTotal: 0,
    testedAt: TESTED,
    ...o,
  });

  it("pools inbox/seeds across ESPs into a single percentage", () => {
    // Gmail 23/25, Outlook 13/13 → 36/38 = 94.7% → 95.
    const summary = summarizeEspRows([
      row({ recipientEsp: 1, inboxCount: 23, spamCount: 2, seedTotal: 25 }),
      row({ recipientEsp: 2, inboxCount: 13, seedTotal: 13 }),
    ]);
    expect(summary?.inboxPct).toBe(95);
    expect(summary?.spamPct).toBe(5);
    expect(summary?.missingPct).toBe(0);
  });

  it("pools a Gmail-dead account down, so the score reflects the bad leg", () => {
    // Gmail 0/20 + Outlook 10/10 → 10/30 = 33%. Nowhere near the 90 bar.
    const summary = summarizeEspRows([
      row({ recipientEsp: 2, inboxCount: 10, seedTotal: 10 }),
      row({ recipientEsp: 1, inboxCount: 0, spamCount: 20, seedTotal: 20 }),
    ]);
    expect(summary?.inboxPct).toBe(33);
    expect(summary?.spamPct).toBe(67);
  });

  it("counts an under-seeded leg like any other — no seed floor", () => {
    // Gmail 28/30 + a 1-of-2 'other' bucket → 29/32 = 90.6% → 91.
    const summary = summarizeEspRows([
      row({ recipientEsp: 1, inboxCount: 28, spamCount: 2, seedTotal: 30 }),
      row({ recipientEsp: 999, inboxCount: 1, spamCount: 1, seedTotal: 2 }),
    ]);
    expect(summary?.inboxPct).toBe(91);
  });

  it("grades a tiny test rather than returning null (sample size is not a gate)", () => {
    // The prod case: emily@fuseconnectio.com, 2 Gmail + 3 Outlook seeds, all inbox.
    // The old seed floor returned null here, which read as delivery-unknown and
    // trapped the account in in_recovery forever.
    const summary = summarizeEspRows([
      row({ recipientEsp: 1, inboxCount: 2, seedTotal: 2 }),
      row({ recipientEsp: 2, inboxCount: 3, seedTotal: 3 }),
    ]);
    expect(summary?.inboxPct).toBe(100);
  });

  it("takes the newest testedAt across rows", () => {
    const newer = new Date("2026-07-01T00:00:00.000Z");
    const summary = summarizeEspRows([
      row({ recipientEsp: 1, inboxCount: 10, seedTotal: 10, testedAt: TESTED }),
      row({ recipientEsp: 2, inboxCount: 10, seedTotal: 10, testedAt: newer }),
    ]);
    expect(summary?.testedAt).toBe(newer.toISOString());
  });

  it("returns null for empty input or zero-seed rows (never fabricates 0%)", () => {
    expect(summarizeEspRows([])).toBeNull();
    expect(summarizeEspRows([row({ seedTotal: 0 })])).toBeNull();
  });
});
