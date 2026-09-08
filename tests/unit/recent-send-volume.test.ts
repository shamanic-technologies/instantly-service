import { describe, it, expect } from "vitest";
import {
  sustainedFor,
  sustainedForMailbox,
  type DailyVolume,
} from "../../src/lib/recent-send-volume";

const volume = (entries: [string, Record<string, number>][]): DailyVolume =>
  new Map(entries.map(([email, days]) => [email, new Map(Object.entries(days))]));

// ─── Why the SECOND-highest day and not the highest ──────────────────────────
//
// ⚠️ The weekly seed placement test dispatches the whole fleet in ONE burst
// (1,780 seeds on 2026-09-05), so every mailbox's busiest day IS that burst.
// Measured at mailbox grain in prod, the highest day averaged 78 and the second
// 51 — reading the highest would have handed 41 of 43 mailboxes their full cap
// on the strength of one artificial day, i.e. the 0-to-50 jump the ramp exists
// to prevent, arriving through the back door.

describe("sustainedFor — one address", () => {
  it("takes the SECOND-highest day, so a single spike does not earn a cap", () => {
    const v = volume([["a@x.com", { "2026-09-01": 4, "2026-09-02": 41, "2026-09-03": 7 }]]);
    expect(sustainedFor(v, "a@x.com")).toBe(7);
  });

  it("credits a volume the address reached TWICE", () => {
    const v = volume([["a@x.com", { "2026-09-01": 20, "2026-09-02": 20, "2026-09-03": 3 }]]);
    expect(sustainedFor(v, "a@x.com")).toBe(20);
  });

  it("reads a single day of history as 0 — one day is not a sustained rate", () => {
    // Not a trap: the mailbox still sends at the floor, which gives it a second
    // day tomorrow, and it climbs from there.
    const v = volume([["a@x.com", { "2026-09-01": 40 }]]);
    expect(sustainedFor(v, "a@x.com")).toBe(0);
  });

  it("reads an address that sent nothing as 0, never as unknown", () => {
    // The ramp floors a 0 at RAMP_FLOOR_PER_DAY, which is exactly a cold mailbox.
    // Returning null/undefined here would invite a caller to grant a full cap.
    expect(sustainedFor(volume([]), "silent@x.com")).toBe(0);
  });

  it("normalises the address, so a mixed-case row still matches", () => {
    const v = volume([["a@x.com", { "2026-09-01": 3, "2026-09-02": 3 }]]);
    expect(sustainedFor(v, "  A@X.com ")).toBe(3);
  });
});

describe("sustainedForMailbox — several aliases, one relay login, one quota", () => {
  // ⚠️ The reason the loader returns per-DAY volume at all. Combining each
  // alias's own figure would over-state a mailbox whose aliases were busy on
  // different days, and over-stating is the one direction a quota ramp must not
  // err in — it is what the relay answers with `450 4.7.1 Too many mail per day
  // for sasl <user>`, per SASL user and not per alias.
  it("totals the mailbox's DAYS, not its aliases' individual figures", () => {
    const v = volume([
      ["kevin@ga.forum", { "2026-09-01": 10, "2026-09-02": 2, "2026-09-03": 6 }],
      ["klourd@ga.forum", { "2026-09-01": 3, "2026-09-02": 9, "2026-09-03": 4 }],
    ]);
    // Daily totals are 13, 11, 10 ⇒ sustained 11. Combining per-alias figures
    // (6 + 4, or 10 + 9) would answer something the mailbox never sent.
    expect(sustainedForMailbox(v, ["kevin@ga.forum", "klourd@ga.forum"])).toBe(11);
  });

  it("adds up aliases that were busy on the SAME day", () => {
    const v = volume([
      ["kevin@ga.forum", { "2026-09-01": 10, "2026-09-02": 10 }],
      ["klourd@ga.forum", { "2026-09-01": 9, "2026-09-02": 9 }],
    ]);
    expect(sustainedForMailbox(v, ["kevin@ga.forum", "klourd@ga.forum"])).toBe(19);
  });

  it("ignores the mailbox's one big day, exactly as the per-address form does", () => {
    const v = volume([
      ["kevin@ga.forum", { "2026-09-01": 41, "2026-09-02": 8 }],
      ["klourd@ga.forum", { "2026-09-01": 37, "2026-09-02": 5 }],
    ]);
    // The seed-test day is 78; the mailbox actually sustains 13.
    expect(sustainedForMailbox(v, ["kevin@ga.forum", "klourd@ga.forum"])).toBe(13);
  });

  it("reads a mailbox whose aliases all sent nothing as 0", () => {
    expect(sustainedForMailbox(volume([]), ["a@x.com", "b@x.com"])).toBe(0);
  });

  it("equals the address's own figure for a mailbox with a single address", () => {
    // The Primeforge case: the address IS the login, so the grouping is a no-op.
    const v = volume([["solo@primeforge.com", { "2026-09-01": 8, "2026-09-02": 14 }]]);
    expect(sustainedForMailbox(v, ["solo@primeforge.com"])).toBe(
      sustainedFor(v, "solo@primeforge.com"),
    );
  });
});
