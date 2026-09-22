import { describe, it, expect, vi, beforeEach } from "vitest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

// Mock the DB — every fn under test bottoms out at db.execute(sql`...`).
const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

// The capacity snapshot resolves the alias map so its figures land at real-mailbox
// grain. Default: nobody shares a login, which is the 1:1 world every pre-existing
// case in this file assumes — so their expectations are unchanged by construction.
const mockLoadMailboxLogins = vi.fn(async () => new Map<string, string>());
vi.mock("../../src/lib/self-send/mailbox-credentials", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  loadMailboxLogins: (...args: unknown[]) => mockLoadMailboxLogins(...args),
}));

import {
  aggregateCapacityByMailbox,
  fetchQueueSizeByAccount,
  fetchSentYesterdayByAccount,
  fetchQueueBreakdownByAccount,
  fetchAccountCapacity,
  fetchAccountCapacityCached,
} from "../../src/lib/account-sending-stats";
import { clearStatsCache } from "../../src/lib/stats-cache";

const dialect = new PgDialect();
/** Compile the SQL object handed to the Nth db.execute call into raw text. */
function executedSqlText(callIndex: number): string {
  const arg = mockExecute.mock.calls[callIndex]?.[0] as SQL;
  return dialect.sqlToQuery(arg).sql;
}

beforeEach(() => {
  mockExecute.mockReset();
  mockLoadMailboxLogins.mockReset();
  mockLoadMailboxLogins.mockResolvedValue(new Map<string, string>());
  // The capacity snapshot now makes THREE reads (sentToday, the queued-sequence
  // loader, and the ramp's volume query). A default keeps the tests that only
  // care about the first two from having to queue a third value.
  mockExecute.mockResolvedValue([]);
  clearStatsCache();
});

describe("fetchQueueSizeByAccount — persisted-account attribution", () => {
  it("attributes queue via COALESCE(persisted account_email, observed) with a LEFT JOIN", async () => {
    mockExecute.mockResolvedValueOnce([]);
    await fetchQueueSizeByAccount();
    const text = executedSqlText(0).toLowerCase();
    // Persisted column drives attribution, falling back to the observed send.
    expect(text).toContain("persisted_account");
    expect(text).toContain("coalesce");
    // LEFT JOIN keeps a persisted-but-not-yet-sent campaign in the result
    // (the INNER JOIN used to drop it — the over-concentration gap).
    expect(text).toContain("left join");
    expect(text).not.toMatch(/\binner join\b/);
  });
});

describe("fetchSentYesterdayByAccount — previous full UTC day", () => {
  it("counts real email_sent events bounded to [prev-midnight, today-midnight)", async () => {
    mockExecute.mockResolvedValueOnce([
      { account_email: "a@x.com", count: 7 },
    ]);
    const map = await fetchSentYesterdayByAccount();
    expect(map.get("a@x.com")).toBe(7);

    const text = executedSqlText(0).toLowerCase();
    // Same provenance/filters as sentToday — real dispatches only.
    expect(text).toContain("event_type");
    expect(text).toContain("'email_sent'");
    expect(text).toContain("inferred = false");
    // Window: >= yesterday-midnight AND < today-midnight (excludes today + older).
    expect(text).toContain("date_trunc('day'");
    expect(text).toContain("interval '1 day'");
    expect(text).toMatch(/timestamp\s+<\s+date_trunc/);
  });

  it("returns an empty map (honest 0 upstream) when no account sent yesterday", async () => {
    mockExecute.mockResolvedValueOnce([]);
    const map = await fetchSentYesterdayByAccount();
    expect(map.size).toBe(0);
  });
});

describe("fetchQueueBreakdownByAccount — per-STEP partition", () => {
  it("chains real bronze delays across every step, attributes via COALESCE, partitions per account", async () => {
    const asOf = new Date("2026-07-11T12:00:00.000Z");
    const DAY = 86_400_000;
    mockExecute.mockResolvedValueOnce([
      // never-sent, 2 un-sent steps → both firstUnsent
      {
        account_email: "a@x.com",
        last_sent_step: null,
        last_sent_at: null,
        provisioned_steps: [1, 2],
        step_config: null,
      },
      // sent 3d ago at step 1; steps 2,3 queued; delays [3,7]:
      // step2 = +3 → today; step3 = +10 → later.
      {
        account_email: "a@x.com",
        last_sent_step: 1,
        last_sent_at: new Date(asOf.getTime() - 3 * DAY).toISOString(),
        provisioned_steps: [2, 3],
        step_config: [{ delay: 3 }, { delay: 7 }],
      },
      // b@x.com: sent today at step 2; step 3 queued; delay steps[1]=9 → later.
      {
        account_email: "b@x.com",
        last_sent_step: 2,
        last_sent_at: new Date(asOf.getTime()).toISOString(),
        provisioned_steps: [3],
        step_config: [{ delay: 1 }, { delay: 9 }],
      },
    ]);

    const map = await fetchQueueBreakdownByAccount(asOf);
    // a@x.com: 2 sequences, 4 steps → 2 firstUnsent + 1 today + 0 tomorrow + 1 later.
    expect(map.get("a@x.com")).toEqual({
      sequences: 2,
      steps: 4,
      firstUnsent: 2,
      // one never-contacted SEQUENCE (its 2 un-sent steps = one first email due)
      firstUnsentSequences: 1,
      nextToday: 1,
      nextTomorrow: 0,
      nextOverdue: 0,
      nextLater: 1,
    });
    // Invariant: the four buckets sum to STEPS (not sequences).
    const a = map.get("a@x.com")!;
    expect(a.firstUnsent + a.nextToday + a.nextTomorrow + a.nextLater).toBe(a.steps);
    expect(map.get("b@x.com")).toEqual({
      sequences: 1,
      steps: 1,
      firstUnsent: 0,
      firstUnsentSequences: 0,
      nextToday: 0,
      nextTomorrow: 0,
      nextOverdue: 0,
      nextLater: 1,
    });

    const text = executedSqlText(0).toLowerCase();
    // Same queued gate + COALESCE attribution as fetchQueueSizeByAccount.
    expect(text).toContain("coalesce");
    expect(text).toContain("delivery_status in ('contacted', 'sent')");
    // Loads the distinct provisioned step numbers (same set queueSize counts).
    expect(text).toContain("array_agg(distinct sc.step) filter (where sc.status = 'provisioned')");
    // Full per-step delay array from the latest bronze sequence config (chained).
    expect(text).toContain("instantly_campaigns_config_raw");
    expect(text).toContain("'sequences'");
    expect(text).toContain("->'steps'");
    expect(text).toContain("order by r.fetched_at desc");
  });
});

describe("fetchAccountCapacity — merge sentToday with the per-day booked map", () => {
  it("books every un-sent step onto the UTC day it will really leave, per lead timezone", async () => {
    // Monday 2026-08-31, 09:00 in Chicago.
    const asOf = new Date("2026-08-31T14:00:00.000Z");
    const DAY = 86_400_000;
    // Array order: fetchSentTodayByAccount() first (db.execute #0), then the
    // queued-sequence loader (#1), then the ramp's volume query (#2).
    mockExecute
      .mockResolvedValueOnce([{ account_email: "a@x.com", count: 5 }]) // sentToday
      .mockResolvedValueOnce([
        // a@x.com never contacted, 2 un-sent steps, delay 3 → today + Thursday.
        {
          account_email: "a@x.com",
          last_sent_step: null,
          last_sent_at: null,
          provisioned_steps: [1, 2],
          step_config: [{ delay: 3 }],
          lead_timezone: "America/Chicago",
        },
        // a@x.com contacted 3d ago at step 1; steps 2,3; delays [3,7]:
        // step 2 due today, step 3 a week out.
        {
          account_email: "a@x.com",
          last_sent_step: 1,
          last_sent_at: new Date(asOf.getTime() - 3 * DAY).toISOString(),
          provisioned_steps: [2, 3],
          step_config: [{ delay: 3 }, { delay: 7 }],
          lead_timezone: "America/Chicago",
        },
        // b@x.com in Auckland: its local Tuesday morning is still MONDAY here,
        // so it books the same UTC day the US leads do.
        {
          account_email: "b@x.com",
          last_sent_step: null,
          last_sent_at: null,
          provisioned_steps: [1],
          step_config: null,
          lead_timezone: "Pacific/Auckland",
        },
        // c@x.com carries no timezone — the fleet default applies, never a guess.
        {
          account_email: "c@x.com",
          last_sent_step: null,
          last_sent_at: null,
          provisioned_steps: [1],
          step_config: null,
          lead_timezone: null,
        },
      ]);

    const cap = await fetchAccountCapacity(asOf);
    expect(cap.get("a@x.com")).toEqual({
      sentToday: 5,
      recentSustainedDaily: 0, // nothing measured in this fixture ⇒ the ramp floors it
      byDay: {
        "2026-08-31": 2, // the never-contacted first email + the due followup
        "2026-09-03": 1, // the never-contacted sequence's step 2
        "2026-09-07": 1, // the contacted sequence's step 3
      },
    });
    expect(cap.get("b@x.com")).toEqual({
      sentToday: 0, // absent from sentToday ⇒ honest 0
      recentSustainedDaily: 0,
      byDay: { "2026-08-31": 1 },
    });
    expect(cap.get("c@x.com")).toEqual({
      sentToday: 0,
      recentSustainedDaily: 0,
      byDay: { "2026-08-31": 1 },
    });
  });

  it("reads the lead timezone from the persisted column, falling back to bronze", async () => {
    mockExecute.mockResolvedValueOnce([]).mockResolvedValueOnce([]);
    await fetchAccountCapacity(new Date("2026-08-31T14:00:00.000Z"));
    const text = executedSqlText(1).toLowerCase();
    expect(text).toContain("min(c.timezone)");
    expect(text).toContain("'campaign_schedule'");
    expect(text).toContain("as lead_timezone");
  });
});

describe("fetchAccountCapacity — the volume the ramp reads", () => {
  it("carries each account's sustained daily volume, 0 when it has sent nothing", async () => {
    // The cap ramps on this, so the snapshot the selector reads has to hold it —
    // otherwise selection would have to make its own DB call and the ops table
    // and the selector could disagree about the same mailbox.
    mockExecute
      .mockResolvedValueOnce([]) // sentToday
      .mockResolvedValueOnce([]) // queued rows
      .mockResolvedValueOnce({
        rows: [
          // Per DAY. The figure is the SECOND-highest, so a one-day spike (the
          // weekly seed test) cannot earn a mailbox its full cap.
          { accountEmail: "busy@x.com", day: "2026-08-28", n: 60 },
          { accountEmail: "busy@x.com", day: "2026-08-29", n: 31 },
          { accountEmail: "busy@x.com", day: "2026-08-30", n: 12 },
          { accountEmail: "QUIET@X.com", day: "2026-08-28", n: 2 },
          { accountEmail: "QUIET@X.com", day: "2026-08-29", n: 2 },
        ],
      });

    const cap = await fetchAccountCapacity(new Date("2026-08-31T14:00:00.000Z"));
    expect(cap.get("busy@x.com")?.recentSustainedDaily).toBe(31);
    // Addresses are normalised, so a mixed-case row still matches its account.
    expect(cap.get("quiet@x.com")?.recentSustainedDaily).toBe(2);
  });
});

describe("aggregateCapacityByMailbox — the quota belongs to the LOGIN, not the address", () => {
  const vol = (rows: [string, string, number][]) => {
    const v = new Map<string, Map<string, number>>();
    for (const [email, day, n] of rows) {
      const days = v.get(email) ?? new Map<string, number>();
      days.set(day, n);
      v.set(email, days);
    }
    return v;
  };

  // `eric@salesmolt.com` is a REAL prod mailbox: one Gandi login, five aliases,
  // two of them `in_production` at 50/day. Read per address the fleet offered it
  // 100/day against ~50, and `accountFillOrder` sorts a domain's aliases
  // adjacent, so the waterfall walked from one straight onto the other.
  const ALIAS_A = "kevinl@salesmolt.com";
  const ALIAS_B = "klourd@salesmolt.com";
  const LOGIN = "eric@salesmolt.com";
  const aliasMap = new Map([
    [ALIAS_A, LOGIN],
    [ALIAS_B, LOGIN],
  ]);

  it("hands both aliases their MAILBOX's sentToday, not their own", () => {
    const out = aggregateCapacityByMailbox(
      [ALIAS_A, ALIAS_B],
      new Map([
        [ALIAS_A, 30],
        [ALIAS_B, 12],
      ]),
      new Map(),
      vol([]),
      aliasMap,
    );
    // 42 came out of ONE relay quota, so that is what each alias has spent.
    expect(out.get(ALIAS_A)?.sentToday).toBe(42);
    expect(out.get(ALIAS_B)?.sentToday).toBe(42);
  });

  it("merges the booked-work map across aliases, per day", () => {
    const out = aggregateCapacityByMailbox(
      [ALIAS_A, ALIAS_B],
      new Map(),
      new Map([
        [ALIAS_A, { byDay: { "2026-09-22": 8, "2026-09-25": 3 } }],
        [ALIAS_B, { byDay: { "2026-09-22": 5 } }],
      ]),
      vol([]),
      aliasMap,
    );
    expect(out.get(ALIAS_A)?.byDay).toEqual({ "2026-09-22": 13, "2026-09-25": 3 });
    expect(out.get(ALIAS_B)?.byDay).toEqual({ "2026-09-22": 13, "2026-09-25": 3 });
  });

  it("ramps on the mailbox's DAILY TOTALS, never on the sum of per-alias figures", () => {
    // Each alias alone sustains 4 (its second-highest day). Summing those would
    // read 8; the mailbox actually sustained 12 and 9, so the honest figure is 9.
    // Per-day-first is the direction that cannot OVER-state a relay quota.
    const out = aggregateCapacityByMailbox(
      [ALIAS_A, ALIAS_B],
      new Map(),
      new Map(),
      vol([
        [ALIAS_A, "2026-09-18", 8],
        [ALIAS_A, "2026-09-19", 4],
        [ALIAS_B, "2026-09-18", 4],
        [ALIAS_B, "2026-09-19", 5],
      ]),
      aliasMap,
    );
    expect(out.get(ALIAS_A)?.recentSustainedDaily).toBe(9);
    expect(out.get(ALIAS_B)?.recentSustainedDaily).toBe(9);
  });

  it("treats an address the login map does not know as its OWN mailbox", () => {
    // Primeforge / Instantly-DFY: the address IS the login, so the grouping is a
    // no-op and these accounts select byte-identically to before this change.
    const out = aggregateCapacityByMailbox(
      ["solo@primeforge.com", "other@primeforge.com"],
      new Map([
        ["solo@primeforge.com", 30],
        ["other@primeforge.com", 12],
      ]),
      new Map([["solo@primeforge.com", { byDay: { "2026-09-22": 4 } }]]),
      vol([
        ["solo@primeforge.com", "2026-09-18", 40],
        ["solo@primeforge.com", "2026-09-19", 20],
      ]),
      new Map(),
    );
    expect(out.get("solo@primeforge.com")).toEqual({
      sentToday: 30,
      recentSustainedDaily: 20,
      byDay: { "2026-09-22": 4 },
    });
    expect(out.get("other@primeforge.com")).toEqual({
      sentToday: 12,
      recentSustainedDaily: 0,
      byDay: {},
    });
  });

  it("matches the login map on the normalised address", () => {
    const out = aggregateCapacityByMailbox(
      ["KevinL@Salesmolt.com", ALIAS_B],
      new Map([["KevinL@Salesmolt.com", 30]]),
      new Map(),
      vol([]),
      aliasMap,
    );
    expect(out.get(ALIAS_B)?.sentToday).toBe(30);
  });
});

describe("fetchAccountCapacity — folds the snapshot to mailbox grain", () => {
  it("reads the alias map and applies it to the three fleet reads", async () => {
    mockLoadMailboxLogins.mockResolvedValue(
      new Map([
        ["a@salesmolt.com", "eric@salesmolt.com"],
        ["b@salesmolt.com", "eric@salesmolt.com"],
      ]),
    );
    mockExecute
      .mockResolvedValueOnce([
        { account_email: "a@salesmolt.com", count: 30 },
        { account_email: "b@salesmolt.com", count: 12 },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce({ rows: [] });

    const cap = await fetchAccountCapacity(new Date("2026-09-22T14:00:00.000Z"));
    expect(cap.get("a@salesmolt.com")?.sentToday).toBe(42);
    expect(cap.get("b@salesmolt.com")?.sentToday).toBe(42);
  });

  it("fails loud when the alias map cannot be read", async () => {
    // A snapshot we cannot build must not degrade into one that claims every
    // mailbox is its own — that is the over-booking this change removes.
    mockLoadMailboxLogins.mockRejectedValue(new Error("key-service unavailable"));
    await expect(
      fetchAccountCapacity(new Date("2026-09-22T14:00:00.000Z")),
    ).rejects.toThrow("key-service unavailable");
  });
});

describe("fetchAccountCapacityCached — 60s TTL cache", () => {
  it("collapses a burst to a single capacity snapshot within the window", async () => {
    mockExecute
      .mockResolvedValueOnce([{ account_email: "a@x.com", count: 4 }]) // sent
      .mockResolvedValueOnce([]); // queued rows

    const first = await fetchAccountCapacityCached();
    const second = await fetchAccountCapacityCached();

    expect(first.get("a@x.com")?.sentToday).toBe(4);
    expect(second.get("a@x.com")?.sentToday).toBe(4);
    // Two db.execute calls total (sent + queued), NOT four — the second cached
    // read hits the in-memory snapshot, not the DB.
    expect(mockExecute.mock.calls.length).toBe(3);
  });

  it("re-fetches after the cache is cleared", async () => {
    mockExecute.mockResolvedValue([]);
    await fetchAccountCapacityCached();
    clearStatsCache();
    await fetchAccountCapacityCached();
    expect(mockExecute.mock.calls.length).toBe(6); // 3 per uncached snapshot
  });
});

describe("fetchAccountCapacityCached — effective-day scoping", () => {
  it("does NOT share a snapshot across two different effective days", async () => {
    mockExecute.mockResolvedValue([]);
    // A weekend caller measures Monday; a Monday caller measures Monday too —
    // same key, one snapshot. A Friday caller measures Friday — different key.
    await fetchAccountCapacityCached(new Date("2026-08-17T00:00:00.000Z"));
    await fetchAccountCapacityCached(new Date("2026-08-17T09:30:00.000Z"));
    expect(mockExecute.mock.calls.length).toBe(3); // one uncached snapshot

    await fetchAccountCapacityCached(new Date("2026-08-21T09:30:00.000Z"));
    expect(mockExecute.mock.calls.length).toBe(6); // a second, separate snapshot
  });

  it("threads the effective day into the queued-bucket projection", async () => {
    const DAY = 86_400_000;
    const monday = new Date("2026-08-17T00:00:00.000Z");
    // A followup nominally due on Saturday 08-15. Measured for Saturday it is
    // due "today"; measured for Monday it is ALSO in the today-or-overdue
    // bucket — which is the point: Monday is what has to dispatch it.
    mockExecute.mockResolvedValueOnce([]).mockResolvedValueOnce([
      {
        account_email: "a@x.com",
        last_sent_step: 1,
        last_sent_at: new Date(monday.getTime() - 5 * DAY).toISOString(), // Wed 08-12
        provisioned_steps: [2],
        step_config: [{ delay: 3 }], // due Sat 08-15
      },
    ]);

    const cap = await fetchAccountCapacity(monday);
    // Monday is a sending day in Chicago too, so the overdue Saturday step books
    // on Monday itself — never on the Saturday it was owed.
    expect(cap.get("a@x.com")?.byDay).toEqual({ "2026-08-17": 1 });
  });
});
