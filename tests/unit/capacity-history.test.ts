import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

import { fetchCapacityHistory } from "../../src/lib/capacity-history";
import { RAMP_FLOOR_PER_DAY } from "../../src/lib/account-lifecycle";

/** A UTC day key N days before today, which is what the series is anchored on. */
const dayAgo = (n: number) =>
  new Date(Date.parse(`${new Date().toISOString().slice(0, 10)}T00:00:00Z`) - n * 86_400_000)
    .toISOString()
    .slice(0, 10);

/** Queue the two reads in order: the volume loader, then the day/account rows. */
function primeReads(volumeRows: unknown[], accountRows: unknown[]) {
  mockExecute
    .mockResolvedValueOnce({ rows: volumeRows })
    .mockResolvedValueOnce({ rows: accountRows });
}

beforeEach(() => {
  mockExecute.mockReset();
});

// ─── The series shows what the fleet could REALLY send ───────────────────────
//
// ⚠️ It used to be `Σ daily_limit` over the in_production pool, which is the
// theoretical ceiling: it ignored the ramp AND counted per address when several
// aliases share one relay login and therefore one quota. Measured 2026-09-08 the
// live point read 2,980/day against roughly 1,100-1,400 with both applied.

describe("fetchCapacityHistory", () => {
  it("applies the ramp — a mailbox still climbing does not contribute its stated limit", async () => {
    const today = dayAgo(0);
    primeReads(
      [
        // Sustained 12/day ⇒ may attempt 18, well under its stated 50.
        { accountEmail: "ramping@x.com", day: dayAgo(2), n: 30 },
        { accountEmail: "ramping@x.com", day: dayAgo(1), n: 12 },
      ],
      [{ date: today, accountEmail: "ramping@x.com", status: "in_production", dailyLimit: 50 }],
    );

    const series = await fetchCapacityHistory(1);
    expect(series).toEqual([{ date: today, inProductionCount: 1, dailyCapacity: 18 }]);
  });

  it("counts a mailbox ONCE however many aliases sit on it", async () => {
    const today = dayAgo(0);
    const aliases = ["kevin@ga.forum", "klourd@ga.forum", "kevinl@ga.forum"];
    primeReads(
      aliases.flatMap((email) => [
        { accountEmail: email, day: dayAgo(2), n: 20 },
        { accountEmail: email, day: dayAgo(1), n: 20 },
      ]),
      aliases.map((email) => ({
        date: today,
        accountEmail: email,
        status: "in_production",
        dailyLimit: 50,
      })),
    );

    const series = await fetchCapacityHistory(
      1,
      new Map(aliases.map((e) => [e, "kevin@ga.forum"])),
    );
    // One mailbox: its three aliases summed to 60/day, so it may attempt 50 (its
    // limit binds). Per address this would have read 150.
    expect(series[0]!.dailyCapacity).toBe(50);
    // The ACCOUNT count answers a different question and stays 3.
    expect(series[0]!.inProductionCount).toBe(3);
  });

  it("takes the LOWEST operator limit across a mailbox's aliases", async () => {
    const today = dayAgo(0);
    primeReads(
      [
        { accountEmail: "kevin@ga.forum", day: dayAgo(2), n: 40 },
        { accountEmail: "kevin@ga.forum", day: dayAgo(1), n: 40 },
      ],
      [
        { date: today, accountEmail: "kevin@ga.forum", status: "in_production", dailyLimit: 50 },
        { date: today, accountEmail: "klourd@ga.forum", status: "in_production", dailyLimit: 12 },
      ],
    );
    const series = await fetchCapacityHistory(
      1,
      new Map([
        ["kevin@ga.forum", "kevin@ga.forum"],
        ["klourd@ga.forum", "kevin@ga.forum"],
      ]),
    );
    expect(series[0]!.dailyCapacity).toBe(12);
  });

  it("evaluates the ramp AS-OF each day, so the series climbs instead of showing today everywhere", async () => {
    // ⚠️ The whole point of a windowed statistic. Painting today's ramp over the
    // past would claim the fleet always had the capacity it has now.
    primeReads(
      [
        { accountEmail: "a@x.com", day: dayAgo(4), n: 4 },
        { accountEmail: "a@x.com", day: dayAgo(3), n: 8 },
        { accountEmail: "a@x.com", day: dayAgo(2), n: 20 },
        { accountEmail: "a@x.com", day: dayAgo(1), n: 24 },
      ],
      [3, 2, 1, 0].map((n) => ({
        date: dayAgo(n),
        accountEmail: "a@x.com",
        status: "in_production",
        dailyLimit: 50,
      })),
    );

    const series = await fetchCapacityHistory(4);
    // Second-highest as of each day: 4, 8, 20, then 20 again (nothing was sent
    // today yet) ⇒ 6, 12, 30, 30. The series CLIMBS — the point of evaluating
    // the window as-of the day rather than painting today's ramp over the past.
    expect(series.map((p) => p.dailyCapacity)).toEqual([6, 12, 30, 30]);
  });

  it("emits EVERY day in the window, including days the fleet had nothing in production", async () => {
    // A row-per-account query returns nothing for such a day; dropping it would
    // leave a hole in the series that reads as missing data rather than as zero.
    primeReads([], [{ date: dayAgo(0), accountEmail: "a@x.com", status: "in_production", dailyLimit: 50 }]);
    const series = await fetchCapacityHistory(3);
    expect(series.map((p) => p.date)).toEqual([dayAgo(2), dayAgo(1), dayAgo(0)]);
    expect(series.slice(0, 2)).toEqual([
      { date: dayAgo(2), inProductionCount: 0, dailyCapacity: 0 },
      { date: dayAgo(1), inProductionCount: 0, dailyCapacity: 0 },
    ]);
  });

  it("reports the FLOOR for a production mailbox nobody has measured, never its stated limit", async () => {
    primeReads([], [{ date: dayAgo(0), accountEmail: "cold@x.com", status: "in_production", dailyLimit: 50 }]);
    const series = await fetchCapacityHistory(1);
    expect(series[0]!.dailyCapacity).toBe(RAMP_FLOOR_PER_DAY);
  });

  it("clamps the window to [1, 365]", async () => {
    primeReads([], []);
    expect(await fetchCapacityHistory(0)).toHaveLength(1);
    primeReads([], []);
    expect(await fetchCapacityHistory(9999)).toHaveLength(365);
  });
});
