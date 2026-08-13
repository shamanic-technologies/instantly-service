import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock DB
const mockDbWhere = vi.fn();
const mockDbReturning = vi.fn();
const mockDbInsertValues = vi.fn();
const mockDbDelete = vi.fn();
const mockOnConflictDoUpdate = vi.fn();
const mockRefreshLeadStatusCurrent = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: (table: unknown) => ({ where: (...args: unknown[]) => { const result = mockDbWhere(...args); return Object.assign(result, { limit: () => result }); } }) }),
    insert: () => ({ values: (v: unknown) => {
      mockDbInsertValues(v);
      return {
        // Reservation upsert (onConflictDoUpdate) and the lead insert
        // (onConflictDoNothing) both resolve via the shared returning queue.
        onConflictDoUpdate: (cfg: unknown) => { mockOnConflictDoUpdate(cfg); return { returning: mockDbReturning }; },
        onConflictDoNothing: () => ({ returning: mockDbReturning }),
        returning: mockDbReturning,
      };
    }}),
    update: () => ({ set: () => ({ where: vi.fn().mockResolvedValue([{}]) }) }),
    delete: () => ({ where: (...args: unknown[]) => { mockDbDelete(...args); return Promise.resolve([]); } }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    instantlyCampaignId: "instantly_campaign_id",
    runId: "run_id",
    status: "status",
  },
  instantlyLeads: { instantlyCampaignId: "instantly_campaign_id", email: "email" },
  sequenceCosts: {},
}));

// Mock key-client
const mockResolveInstantlyApiKey = vi.fn();

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
  KeyServiceError: class KeyServiceError extends Error {
    statusCode: number;
    constructor(statusCode: number, message: string) {
      super(message);
      this.name = "KeyServiceError";
      this.statusCode = statusCode;
    }
  },
}));

// Mock instantly-client
const mockAddLeads = vi.fn();
const mockUpdateCampaignStatus = vi.fn();
const mockCreateCampaign = vi.fn();
const mockUpdateCampaign = vi.fn();
const mockGetCampaign = vi.fn();
const mockListAccounts = vi.fn();
// Spy over the silver in_production pool read so tests can assert which feature
// pool a send drew from (the feature carve-out). Delegates to mockListAccounts
// so every existing POST /send test keeps its seeded pool.
const mockFetchInProductionAccounts = vi.fn(
  (_featureSlug?: string | null) => mockListAccounts(),
);

vi.mock("../../src/lib/instantly-client", () => ({
  addLeads: (...args: unknown[]) => mockAddLeads(...args),
  createCampaign: (...args: unknown[]) => mockCreateCampaign(...args),
  updateCampaign: (...args: unknown[]) => mockUpdateCampaign(...args),
  getCampaign: (...args: unknown[]) => mockGetCampaign(...args),
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
  listAccounts: (...args: unknown[]) => mockListAccounts(...args),
}));

// Mock runs-client
const mockCreateRun = vi.fn();
const mockUpdateRun = vi.fn();
const mockAddCosts = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...args: unknown[]) => mockCreateRun(...args),
  updateRun: (...args: unknown[]) => mockUpdateRun(...args),
  addCosts: (...args: unknown[]) => mockAddCosts(...args),
}));

// Mock billing-client
const mockAuthorizeCreditSpend = vi.fn();

vi.mock("../../src/lib/billing-client", () => ({
  authorizeCreditSpend: (...args: unknown[]) => mockAuthorizeCreditSpend(...args),
}));

vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshLeadStatusCurrent(...args),
}));

// The live-send pool is the silver `in_production` lifecycle set, read via
// fetchInProductionAccounts. Reuse mockListAccounts as the pool source so the
// existing POST /send tests that seed accounts via mockListAccounts keep working
// (both return Account[]); a test can still override it per-case.
vi.mock("../../src/lib/account-lifecycle-sync", () => ({
  fetchInProductionAccounts: (...args: unknown[]) =>
    mockFetchInProductionAccounts(...args),
}));

// Per-account capacity snapshot feeding capacity-aware selection. Defaults to an
// empty map (all accounts all-zeros ⇒ room today ⇒ uniform random) so the
// existing send flow tests keep passing; a test overrides it per-case.
import type { AccountCapacity } from "../../src/lib/account-sending-stats";
const mockFetchAccountCapacity = vi.fn(
  async () => new Map<string, AccountCapacity>(),
);
vi.mock("../../src/lib/account-sending-stats", () => ({
  fetchAccountCapacityCached: (...args: unknown[]) =>
    mockFetchAccountCapacity(...args),
}));

import {
  autolinkifyHtml,
  buildEmailBodyWithSignature,
  pickSequentialFillAccount,
  accountFillOrder,
  providerFillRank,
  UNKNOWN_PROVIDER_FILL_RANK,
  buildSequenceSteps,
  stripAccountSignature,
  sendLeadToInstantly,
  UNSUBSCRIBE_FOOTER_HTML,
} from "../../src/lib/send-lead";
import { requireOrgId } from "../../src/middleware/requireOrgId";
import type { Account } from "../../src/lib/instantly-client";
import request from "supertest";
import express from "express";

async function createSendApp() {
  const sendRouter = (await import("../../src/routes/send")).default;
  const app = express();
  app.use(express.json());
  app.use("/send", requireOrgId, sendRouter);
  return app;
}

const identityHeadersObj = {
  "x-org-id": "org-1",
  "x-user-id": "user-1",
  "x-run-id": "run-1",
  "x-campaign-id": "camp-1",
  "x-brand-id": "brand-1",
};

const validBody = {
  to: "test@example.com",
  firstName: "Test",
  lastName: "User",
  company: "TestCo",
  subject: "Hello",
  sequence: [
    { step: 1, bodyHtml: "<p>First email</p>", daysSinceLastStep: 0 },
    { step: 2, bodyHtml: "<p>Follow up</p>", daysSinceLastStep: 3 },
    { step: 3, bodyHtml: "<p>Last chance</p>", daysSinceLastStep: 7 },
  ],
  leadId: "lead-1",
};

function acct(
  overrides: Partial<Account> & { infraProvider?: string | null } = {},
): Account & { infraProvider?: string | null } {
  return { email: "a@test.com", warmup_status: 1, status: 1, ...overrides };
}

/**
 * Helper: set up mocks for a new campaign creation flow (happy path).
 * One getCampaign call per attempt (verify-after-PATCH). NSS is no longer
 * checked post-activate.
 */
function mockNewCampaignFlow() {
  mockDbWhere.mockReset();
  mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check (no conflict)

  mockCreateCampaign.mockResolvedValue({ id: "inst-camp-new", status: "draft" });
  mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH

  mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-new" }]); // RESERVE upsert → winner
  mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
  mockUpdateCampaignStatus.mockResolvedValue({});
}

describe("pickSequentialFillAccount", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  /** Build a capacity map from partial per-account overrides. */
  const caps = (
    entries: [string, Partial<AccountCapacity>][],
  ): Map<string, AccountCapacity> =>
    new Map(
      entries.map(([email, o]) => [
        email,
        { sentToday: 0, q0first: 0, q0next: 0, q1next: 0, totalQueue: 0, ...o },
      ]),
    );

  const asOf = new Date("2026-07-22T00:00:00Z");
  const created = (daysOld: number) =>
    new Date(asOf.getTime() - daysOld * 24 * 60 * 60 * 1000).toISOString();

  it("should throw when no accounts are available", () => {
    expect(() => pickSequentialFillAccount([], caps([]))).toThrow(
      "No accounts available",
    );
  });

  it("should return the only account when only one is available", () => {
    const a = acct({ email: "only@x.com", daily_limit: 50 });
    expect(
      pickSequentialFillAccount([a], caps([["only@x.com", { sentToday: 42 }]])),
    ).toBe(a);
  });

  // ── The vendor tier (primary sort key) ───────────────────────────────────────

  it("ranks the infrastructure vendors gandi < mailforge < primeforge < instantly-dfy", () => {
    expect(providerFillRank("gandi")).toBe(0);
    expect(providerFillRank("mailforge")).toBe(1);
    expect(providerFillRank("primeforge")).toBe(2);
    expect(providerFillRank("instantly-dfy")).toBe(3);
    // Anything we cannot attribute sorts behind every known vendor.
    expect(providerFillRank(null)).toBe(UNKNOWN_PROVIDER_FILL_RANK);
    expect(providerFillRank(undefined)).toBe(UNKNOWN_PROVIDER_FILL_RANK);
    expect(providerFillRank("some-new-vendor")).toBe(UNKNOWN_PROVIDER_FILL_RANK);
  });

  it("puts the vendor AHEAD of age: a younger gandi account beats an older primeforge one", () => {
    const accounts = [
      acct({
        email: "old-primeforge@x.com",
        infraProvider: "primeforge",
        timestamp_created: created(365),
      }),
      acct({
        email: "young-gandi@x.com",
        infraProvider: "gandi",
        timestamp_created: created(1),
      }),
    ];
    expect(accountFillOrder(accounts).map((a) => a.email)).toEqual([
      "young-gandi@x.com",
      "old-primeforge@x.com",
    ]);
  });

  it("drains the four vendors in order, oldest-first WITHIN each vendor", () => {
    const accounts = [
      acct({ email: "dfy@x.com", infraProvider: "instantly-dfy", timestamp_created: created(400) }),
      acct({ email: "pf-young@x.com", infraProvider: "primeforge", timestamp_created: created(10) }),
      acct({ email: "pf-old@x.com", infraProvider: "primeforge", timestamp_created: created(60) }),
      acct({ email: "mf@x.com", infraProvider: "mailforge", timestamp_created: created(5) }),
      acct({ email: "gandi@x.com", infraProvider: "gandi", timestamp_created: created(2) }),
    ];
    expect(accountFillOrder(accounts).map((a) => a.email)).toEqual([
      "gandi@x.com",
      "mf@x.com",
      "pf-old@x.com",
      "pf-young@x.com",
      "dfy@x.com",
    ]);
  });

  it("sorts an account with no known vendor LAST, never first", () => {
    // A domain missing from infra_domains cannot be honestly placed in the
    // vendor sequence — the tail is the position that risks the least. Note it
    // lands behind instantly-dfy even though it is the OLDEST account here.
    const accounts = [
      acct({ email: "unattributed@x.com", timestamp_created: created(999) }),
      acct({ email: "dfy@x.com", infraProvider: "instantly-dfy", timestamp_created: created(1) }),
      acct({ email: "gandi@x.com", infraProvider: "gandi", timestamp_created: created(1) }),
    ];
    expect(accountFillOrder(accounts).map((a) => a.email)).toEqual([
      "gandi@x.com",
      "dfy@x.com",
      "unattributed@x.com",
    ]);
  });

  it("fills the earlier vendor while it has room, even when a later vendor is emptier", () => {
    const accounts = [
      acct({
        email: "gandi@x.com",
        infraProvider: "gandi",
        daily_limit: 45,
        timestamp_created: created(90),
      }),
      acct({
        email: "primeforge@x.com",
        infraProvider: "primeforge",
        daily_limit: 45,
        timestamp_created: created(90),
      }),
    ];
    const byEmail = caps([
      ["gandi@x.com", { sentToday: 44 }],
      ["primeforge@x.com", { sentToday: 0 }],
    ]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "gandi@x.com",
    );
  });

  it("cascades to the next vendor once the earlier one is at cap", () => {
    const accounts = [
      acct({
        email: "gandi@x.com",
        infraProvider: "gandi",
        daily_limit: 45,
        timestamp_created: created(90),
      }),
      acct({
        email: "mailforge@x.com",
        infraProvider: "mailforge",
        daily_limit: 45,
        timestamp_created: created(90),
      }),
      acct({
        email: "primeforge@x.com",
        infraProvider: "primeforge",
        daily_limit: 45,
        timestamp_created: created(90),
      }),
    ];
    // gandi full → mailforge.
    expect(
      pickSequentialFillAccount(
        accounts,
        caps([["gandi@x.com", { sentToday: 45 }]]),
        asOf,
      ).email,
    ).toBe("mailforge@x.com");
    // gandi AND mailforge full → primeforge.
    expect(
      pickSequentialFillAccount(
        accounts,
        caps([
          ["gandi@x.com", { sentToday: 45 }],
          ["mailforge@x.com", { sentToday: 45 }],
        ]),
        asOf,
      ).email,
    ).toBe("primeforge@x.com");
  });

  // ── The fixed order ──────────────────────────────────────────────────────────

  it("orders by timestamp_created ascending, email ascending on a tie", () => {
    const accounts = [
      acct({ email: "young@x.com", timestamp_created: created(1) }),
      acct({ email: "b@x.com", timestamp_created: created(90) }),
      acct({ email: "a@x.com", timestamp_created: created(90) }), // same instant as b
      acct({ email: "middle@x.com", timestamp_created: created(30) }),
    ];
    expect(accountFillOrder(accounts).map((a) => a.email)).toEqual([
      "a@x.com",
      "b@x.com",
      "middle@x.com",
      "young@x.com",
    ]);
  });

  it("sorts an account with no (or unparseable) timestamp_created LAST", () => {
    const accounts = [
      acct({ email: "undated@x.com" }),
      acct({ email: "garbage@x.com", timestamp_created: "not-a-date" }),
      acct({ email: "dated@x.com", timestamp_created: created(1) }),
    ];
    // Both undatable accounts land behind the dated one, ordered between
    // themselves by email.
    expect(accountFillOrder(accounts).map((a) => a.email)).toEqual([
      "dated@x.com",
      "garbage@x.com",
      "undated@x.com",
    ]);
  });

  it("adding a NEWER account never displaces the head of the order", () => {
    const existing = [
      acct({ email: "first@x.com", timestamp_created: created(90) }),
      acct({ email: "second@x.com", timestamp_created: created(60) }),
    ];
    const withNewcomer = [
      acct({ email: "aaa-newcomer@x.com", timestamp_created: created(1) }),
      ...existing,
    ];
    expect(accountFillOrder(withNewcomer).map((a) => a.email)).toEqual([
      "first@x.com",
      "second@x.com",
      "aaa-newcomer@x.com",
    ]);
  });

  // ── The waterfall ────────────────────────────────────────────────────────────

  it("fills the FIRST account while it has room, even when a later one is emptier", () => {
    // This is the whole policy: the fill-ratio rule it replaces would have picked
    // `idle` (0/45) over `first` (44/45). Concentrating on the head is the point —
    // it lets the tail go quiet and become safe to cancel.
    const accounts = [
      acct({ email: "first@x.com", daily_limit: 45, timestamp_created: created(90) }),
      acct({ email: "idle@x.com", daily_limit: 45, timestamp_created: created(60) }),
    ];
    const byEmail = caps([
      ["first@x.com", { sentToday: 44 }],
      ["idle@x.com", { sentToday: 0 }],
    ]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "first@x.com",
    );
  });

  it("moves to the SECOND account once the first reaches its cap", () => {
    const accounts = [
      acct({ email: "first@x.com", daily_limit: 45, timestamp_created: created(90) }),
      acct({ email: "second@x.com", daily_limit: 45, timestamp_created: created(60) }),
    ];
    const byEmail = caps([
      ["first@x.com", { sentToday: 45 }], // exactly at cap ⇒ full
      ["second@x.com", { sentToday: 0 }],
    ]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "second@x.com",
    );
  });

  it("cascades to the THIRD account once the first two are full", () => {
    const accounts = [
      acct({ email: "first@x.com", daily_limit: 45, timestamp_created: created(90) }),
      acct({ email: "second@x.com", daily_limit: 45, timestamp_created: created(60) }),
      acct({ email: "third@x.com", daily_limit: 45, timestamp_created: created(30) }),
    ];
    const byEmail = caps([
      ["first@x.com", { sentToday: 45 }],
      ["second@x.com", { sentToday: 50 }], // over cap
      ["third@x.com", { sentToday: 44 }],
    ]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "third@x.com",
    );
  });

  it("counts sentToday + Q0-first + Q0-next together against the cap", () => {
    // 20 dispatched + 15 never-contacted sequences + 10 followups due today = 45,
    // i.e. FULL — a followup queued days ago but due today occupies today's cap.
    const accounts = [
      acct({ email: "first@x.com", daily_limit: 45, timestamp_created: created(90) }),
      acct({ email: "second@x.com", daily_limit: 45, timestamp_created: created(60) }),
    ];
    const full = caps([
      ["first@x.com", { sentToday: 20, q0first: 15, q0next: 10 }],
      ["second@x.com", { sentToday: 0 }],
    ]);
    expect(pickSequentialFillAccount(accounts, full, asOf).email).toBe(
      "second@x.com",
    );

    // One below the cap and the head still wins.
    const nearlyFull = caps([
      ["first@x.com", { sentToday: 20, q0first: 15, q0next: 9 }],
      ["second@x.com", { sentToday: 0 }],
    ]);
    expect(pickSequentialFillAccount(accounts, nearlyFull, asOf).email).toBe(
      "first@x.com",
    );
  });

  it("treats an account absent from the map as all-zeros (has full room)", () => {
    const accounts = [
      acct({ email: "first@x.com", daily_limit: 45, timestamp_created: created(90) }),
      acct({ email: "second@x.com", daily_limit: 45, timestamp_created: created(60) }),
    ];
    // `first` is absent ⇒ load 0 ⇒ it is the pick, unchanged.
    expect(
      pickSequentialFillAccount(accounts, caps([["second@x.com", {}]]), asOf).email,
    ).toBe("first@x.com");
  });

  it("is deterministic — repeated calls on the same input pick the same account", () => {
    const accounts = [
      acct({ email: "first@x.com", daily_limit: 45, timestamp_created: created(90) }),
      acct({ email: "second@x.com", daily_limit: 45, timestamp_created: created(60) }),
      acct({ email: "third@x.com", daily_limit: 45, timestamp_created: created(30) }),
    ];
    const byEmail = caps([]);
    const picks = Array.from(
      { length: 20 },
      () => pickSequentialFillAccount(accounts, byEmail, asOf).email,
    );
    expect(new Set(picks)).toEqual(new Set(["first@x.com"]));
  });

  it("skips an account pinned to daily_limit 0 entirely", () => {
    const accounts = [
      acct({ email: "zero@x.com", daily_limit: 0, timestamp_created: created(90) }),
      acct({ email: "next@x.com", daily_limit: 45, timestamp_created: created(60) }),
    ];
    const byEmail = caps([["zero@x.com", { sentToday: 0 }]]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "next@x.com",
    );
  });

  it("falls back to the least-overloaded account when every one is full", () => {
    const accounts = [
      acct({ email: "first@x.com", daily_limit: 50, timestamp_created: created(90) }), // 60/50 = 1.2
      acct({ email: "second@x.com", daily_limit: 50, timestamp_created: created(60) }), // 55/50 = 1.1
    ];
    const byEmail = caps([
      ["first@x.com", { sentToday: 60 }],
      ["second@x.com", { sentToday: 55 }],
    ]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "second@x.com",
    );
  });

  it("breaks an all-full tie toward the EARLIER account in the order", () => {
    const accounts = [
      acct({ email: "second@x.com", daily_limit: 50, timestamp_created: created(60) }),
      acct({ email: "first@x.com", daily_limit: 50, timestamp_created: created(90) }),
    ];
    const byEmail = caps([
      ["first@x.com", { sentToday: 55 }],
      ["second@x.com", { sentToday: 55 }],
    ]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "first@x.com",
    );
  });

  // ── AGE: the cap is age-scaled; the ORDER is unaffected by age ────────────────

  it("fills a fresh HEAD account only to its age-scaled cap, then moves on", () => {
    // The head is 14 days old ⇒ cap 25 (half of the 50 base), NOT 45. At 25 it is
    // full even though its Instantly daily_limit says 45 — this is what keeps a
    // young Google mailbox under Gmail's per-user quota (550-5.4.5).
    const accounts = [
      acct({ email: "fresh@x.com", daily_limit: 45, timestamp_created: created(14) }),
      acct({ email: "mature@x.com", daily_limit: 45, timestamp_created: created(1) }),
    ];
    expect(
      pickSequentialFillAccount(accounts, caps([["fresh@x.com", { sentToday: 24 }]]), asOf)
        .email,
    ).toBe("fresh@x.com");
    expect(
      pickSequentialFillAccount(accounts, caps([["fresh@x.com", { sentToday: 25 }]]), asOf)
        .email,
    ).toBe("mature@x.com");
  });

  it("caps a day-old HEAD account at the ramp floor, not at its daily_limit", () => {
    const accounts = [
      acct({ email: "dayold@x.com", daily_limit: 45, timestamp_created: created(1) }),
      acct({ email: "next@x.com", daily_limit: 45 }),
    ];
    // RAMP_FLOOR_PER_DAY = 5 ⇒ full at 5 despite daily_limit 45.
    expect(
      pickSequentialFillAccount(accounts, caps([["dayold@x.com", { sentToday: 4 }]]), asOf)
        .email,
    ).toBe("dayold@x.com");
    expect(
      pickSequentialFillAccount(accounts, caps([["dayold@x.com", { sentToday: 5 }]]), asOf)
        .email,
    ).toBe("next@x.com");
  });

  it("does NOT double-scale once Instantly's own daily_limit is already ramped", () => {
    // lifecycle-limits-sync writes the ramped value onto Instantly, so a 14d
    // account arrives here with daily_limit 23 — already its age cap. Scaling THAT
    // by age again would give 12 and cut the head's share in half every sweep.
    const accounts = [
      acct({ email: "ramped@x.com", daily_limit: 23, timestamp_created: created(14) }),
      acct({ email: "next@x.com", daily_limit: 45, timestamp_created: created(1) }),
    ];
    // At a double-scaled cap of 12 a load of 20 would read as full and hand the
    // send to `next`; the true cap is 23, so the head keeps it.
    expect(
      pickSequentialFillAccount(accounts, caps([["ramped@x.com", { sentToday: 20 }]]), asOf)
        .email,
    ).toBe("ramped@x.com");
  });

  it("still honours an operator-set daily_limit BELOW the age cap", () => {
    // ramp(14d) = 23, but the operator pinned 10 → the lower one binds.
    const accounts = [
      acct({ email: "pinned@x.com", daily_limit: 10, timestamp_created: created(14) }),
      acct({ email: "next@x.com", daily_limit: 45, timestamp_created: created(1) }),
    ];
    expect(
      pickSequentialFillAccount(accounts, caps([["pinned@x.com", { sentToday: 10 }]]), asOf)
        .email,
    ).toBe("next@x.com");
  });

  it("an undatable account keeps its FULL daily_limit as cap", () => {
    // Undatable sorts LAST, so make it the only candidate with room: a missing
    // timestamp must never trap an account at the ramp floor.
    const accounts = [
      acct({ email: "dated@x.com", daily_limit: 45, timestamp_created: created(90) }),
      acct({ email: "undated@x.com", daily_limit: 45 }),
    ];
    const byEmail = caps([
      ["dated@x.com", { sentToday: 45 }],
      ["undated@x.com", { sentToday: 44 }], // full only if its cap were the floor
    ]);
    expect(pickSequentialFillAccount(accounts, byEmail, asOf).email).toBe(
      "undated@x.com",
    );
  });
});

describe("send gate — only in_production accounts (lifecycle)", () => {
  afterEach(() => vi.restoreAllMocks());

  const seq = [{ step: 1, bodyHtml: "<p>x</p>", daysSinceLastStep: 0 }];
  const lead = { email: "lead@x.com", firstName: "L" } as any;

  it("returns no_healthy_accounts_available when the in_production pool is empty", async () => {
    mockListAccounts.mockResolvedValueOnce([]);
    const res = await sendLeadToInstantly({
      apiKey: "k",
      campaignName: "c",
      subject: "s",
      sortedSequence: seq,
      lead,
    });
    expect(res).toEqual({ ok: false, reason: "no_healthy_accounts_available" });
  });

  it("picks an account from the in_production pool", async () => {
    mockListAccounts.mockResolvedValueOnce([
      acct({ email: "prod@good.com", stat_warmup_score: 100 }),
    ]);
    mockCreateCampaign.mockResolvedValue({ id: "ic", status: "draft" });
    mockUpdateCampaign.mockResolvedValue({});
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["prod@good.com"], not_sending_status: null });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockUpdateCampaignStatus.mockResolvedValue({});
    const res = await sendLeadToInstantly({
      apiKey: "k",
      campaignName: "c",
      subject: "s",
      sortedSequence: seq,
      lead,
    });
    expect(res.ok).toBe(true);
    if (res.ok) expect(res.value.account.email).toBe("prod@good.com");
  });

  it("keeps filling the oldest account while it still has room today", async () => {
    // `older` was created first, so it heads the fill order and takes the send
    // even though `newer` is far emptier — the tail is meant to go quiet.
    mockListAccounts.mockResolvedValueOnce([
      acct({
        email: "newer@good.com",
        stat_warmup_score: 100,
        daily_limit: 50,
        timestamp_created: "2026-05-01T00:00:00Z",
      }),
      acct({
        email: "older@good.com",
        stat_warmup_score: 100,
        daily_limit: 50,
        timestamp_created: "2026-01-01T00:00:00Z",
      }),
    ]);
    mockFetchAccountCapacity.mockResolvedValueOnce(
      new Map<string, AccountCapacity>([
        ["older@good.com", { sentToday: 37, q0first: 0, q0next: 0, q1next: 0, totalQueue: 37 }],
        ["newer@good.com", { sentToday: 2, q0first: 0, q0next: 0, q1next: 0, totalQueue: 2 }],
      ]),
    );
    mockCreateCampaign.mockResolvedValue({ id: "ic", status: "draft" });
    mockUpdateCampaign.mockResolvedValue({});
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["older@good.com"], not_sending_status: null });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockUpdateCampaignStatus.mockResolvedValue({});
    const res = await sendLeadToInstantly({
      apiKey: "k",
      campaignName: "c",
      subject: "s",
      sortedSequence: seq,
      lead,
    });
    expect(res.ok).toBe(true);
    if (res.ok) expect(res.value.account.email).toBe("older@good.com");
    expect(mockFetchAccountCapacity).toHaveBeenCalled();
  });

  it("moves to the next account in the order once the oldest is at its cap", async () => {
    mockListAccounts.mockResolvedValueOnce([
      acct({
        email: "newer@good.com",
        stat_warmup_score: 100,
        daily_limit: 50,
        timestamp_created: "2026-05-01T00:00:00Z",
      }),
      acct({
        email: "older@good.com",
        stat_warmup_score: 100,
        daily_limit: 50,
        timestamp_created: "2026-01-01T00:00:00Z",
      }),
    ]);
    mockFetchAccountCapacity.mockResolvedValueOnce(
      new Map<string, AccountCapacity>([
        ["older@good.com", { sentToday: 50, q0first: 0, q0next: 0, q1next: 0, totalQueue: 50 }],
        ["newer@good.com", { sentToday: 2, q0first: 0, q0next: 0, q1next: 0, totalQueue: 2 }],
      ]),
    );
    mockCreateCampaign.mockResolvedValue({ id: "ic", status: "draft" });
    mockUpdateCampaign.mockResolvedValue({});
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["newer@good.com"], not_sending_status: null });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockUpdateCampaignStatus.mockResolvedValue({});
    const res = await sendLeadToInstantly({
      apiKey: "k",
      campaignName: "c",
      subject: "s",
      sortedSequence: seq,
      lead,
    });
    expect(res.ok).toBe(true);
    if (res.ok) expect(res.value.account.email).toBe("newer@good.com");
  });

  it("passes featureSlug to fetchInProductionAccounts (pool carve-out)", async () => {
    mockListAccounts.mockResolvedValueOnce([]);
    await sendLeadToInstantly({
      apiKey: "k",
      campaignName: "c",
      subject: "s",
      sortedSequence: seq,
      lead,
      featureSlug: "sales-crm-email-outreach",
    });
    expect(mockFetchInProductionAccounts).toHaveBeenCalledWith(
      "sales-crm-email-outreach",
    );
  });

  it("defaults featureSlug to null when absent", async () => {
    mockListAccounts.mockResolvedValueOnce([]);
    await sendLeadToInstantly({
      apiKey: "k",
      campaignName: "c",
      subject: "s",
      sortedSequence: seq,
      lead,
    });
    expect(mockFetchInProductionAccounts).toHaveBeenCalledWith(null);
  });
});

describe("autolinkifyHtml", () => {
  it("wraps plain https URL in anchor tag", () => {
    const out = autolinkifyHtml("<p>visit https://pressbeat.io now</p>");
    expect(out).toContain('<a href="https://pressbeat.io"');
    expect(out).toContain(">https://pressbeat.io</a>");
  });

  it("wraps bare domain with https default protocol", () => {
    const out = autolinkifyHtml("<p>over at pressbeat.io okay</p>");
    expect(out).toContain('href="https://pressbeat.io"');
    expect(out).toContain(">pressbeat.io</a>");
  });

  it("leaves existing <a href> untouched (no double wrap)", () => {
    const out = autolinkifyHtml('<p><a href="https://x.com">x.com</a> and https://y.com</p>');
    expect((out.match(/<a /g) || []).length).toBe(2);
    expect(out).toContain('<a href="https://x.com">x.com</a>');
    expect(out).toContain('href="https://y.com"');
  });

  it("preserves mustache placeholders even when they look domain-like", () => {
    const out = autolinkifyHtml("<p>Hi {{firstName}}, {{user.email}}, see https://z.com</p>");
    expect(out).toContain("{{firstName}}");
    expect(out).toContain("{{user.email}}");
    expect(out).toContain('href="https://z.com"');
  });

  it("returns input unchanged when no URLs and no domains present", () => {
    expect(autolinkifyHtml("<p>Hello world</p>")).toBe("<p>Hello world</p>");
  });

  it("strips trailing punctuation from URL match", () => {
    const out = autolinkifyHtml("<p>(over at pressbeat.io)</p>");
    expect(out).toContain('href="https://pressbeat.io"');
    expect(out).toContain(">pressbeat.io</a>)");
  });

  it("strips the query string from the display text but keeps it in href (UTM)", () => {
    const url =
      "https://opsfolio.com/lp/cmmc/level-1-free-assessment/?utm_source=landing_page&utm_medium=email&utm_id=distribute";
    const out = autolinkifyHtml(`<p>see ${url} now</p>`);
    // href keeps the full URL (tracking + destination intact)
    expect(out).toContain(`href="${url}"`);
    // display text is the path only — no utm_* noise shown to the recipient
    expect(out).toContain(">https://opsfolio.com/lp/cmmc/level-1-free-assessment/</a>");
    expect(out).not.toContain(">https://opsfolio.com/lp/cmmc/level-1-free-assessment/?utm");
  });

  it("leaves a query-less URL display unchanged", () => {
    const out = autolinkifyHtml("<p>visit https://pressbeat.io/pricing now</p>");
    expect(out).toContain('href="https://pressbeat.io/pricing"');
    expect(out).toContain(">https://pressbeat.io/pricing</a>");
  });
});

describe("buildEmailBodyWithSignature", () => {
  const sig = "<p>Best,<br>John Doe</p>";

  it("should append HTML <p>--</p> separator + signature to body", () => {
    const result = buildEmailBodyWithSignature("<p>Hello</p>", acct({ signature: sig }));
    expect(result).toBe(`<p>Hello</p><p>--</p>${sig}${UNSUBSCRIBE_FOOTER_HTML}`);
  });

  it("should replace {{accountSignature}} placeholder with HTML separator + signature", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, acct({ signature: sig }));
    expect(result).toBe(`Hello\n\n<p>--</p>${sig}${UNSUBSCRIBE_FOOTER_HTML}`);
  });

  it("falls back to the canonical Distribute.you signature when account.signature is empty (with placeholder)", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, acct({ email: "kevinl@growthagency.dev", signature: "" }));
    expect(result).toContain("<p>--</p>");
    expect(result).toContain("Kevin Lourd");
    expect(result).not.toContain("Founder");
    expect(result).toContain("Distribute.you | Marketing Agency");
    expect((result.match(/<p>--<\/p>/g) ?? []).length).toBe(1);
  });

  it("falls back to the canonical Distribute.you signature when account.signature is empty (no placeholder)", () => {
    // Brand line is plain text — NOT auto-linkified into an <a>.
    const result = buildEmailBodyWithSignature("Hello", acct({ email: "kevinl@growthagency.dev" }));
    expect(result).toBe(
      `Hello<p>--</p><p>Kevin Lourd<br>Distribute.you | Marketing Agency</p>${UNSUBSCRIBE_FOOTER_HTML}`,
    );
    // The brand line stays plain text; the ONLY anchor is the unsubscribe link.
    expect((result.match(/<a /g) ?? []).length).toBe(1);
    expect(result).toContain('href="{unsubscribe_link}"');
  });

  it("uses the same Distribute.you brand line regardless of sending account domain", () => {
    const a = buildEmailBodyWithSignature("Hello", acct({ email: "kevin@marketingagency.forum" }));
    const b = buildEmailBodyWithSignature("Hello", acct({ email: "x@unknownbrand.io" }));
    expect(a).toContain("Distribute.you | Marketing Agency");
    expect(b).toContain("Distribute.you | Marketing Agency");
    // Only the unsubscribe anchor — the brand line is never auto-linkified.
    expect((a.match(/<a /g) ?? []).length).toBe(1);
    expect((b.match(/<a /g) ?? []).length).toBe(1);
  });

  it("signs with the account's OWN name so From-name and signature agree (multi-persona)", () => {
    const result = buildEmailBodyWithSignature(
      "Hello",
      acct({ email: "amy@gildcultivatecoil.com", first_name: "Amy", last_name: "Moore" }),
    );
    expect(result).toBe(
      `Hello<p>--</p><p>Amy Moore<br>Distribute.you | Marketing Agency</p>${UNSUBSCRIBE_FOOTER_HTML}`,
    );
    expect(result).not.toContain("Kevin Lourd");
    expect(result).not.toContain("Founder");
  });

  it("autolinkifies URLs in the body but NOT in the appended signature", () => {
    const sigWithLink = '<p>Best,<br>John — see https://example.com</p>';
    const result = buildEmailBodyWithSignature("<p>hey https://z.com</p>", acct({ signature: sigWithLink }));
    expect(result).toContain('href="https://z.com"');
    // Signature is appended verbatim — its URL stays plain text, no <a> wrap.
    expect(result).not.toContain('href="https://example.com"');
    expect(result).toContain("see https://example.com");
  });

  it("leaves mustache vars in body intact even after autolinkify", () => {
    const result = buildEmailBodyWithSignature(
      "<p>Hi {{firstName}}, visit https://z.com</p>",
      acct({ signature: "<p>--</p>" }),
    );
    expect(result).toContain("{{firstName}}");
    expect(result).toContain('href="https://z.com"');
  });

  it("strips a pre-existing plain-text signature before appending the new one (no cumulative stacking)", () => {
    const newSig = "<p>Best,<br>Jane</p>";
    const body = `<p>Hello world</p>\n\n--\n<p>Old signature from Bob</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).toBe(`<p>Hello world</p><p>--</p>${newSig}${UNSUBSCRIBE_FOOTER_HTML}`);
    expect(result.match(/--/g)?.length).toBe(1);
  });

  it("strips a pre-existing HTML <p>--</p> signature marker before appending", () => {
    const newSig = "<p>Best,<br>Jane</p>";
    const body = `<p>Hello world</p><p>--</p><p>Old signature</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Old signature");
    expect(result).toContain(newSig);
    expect((result.match(/Old signature/g) ?? []).length).toBe(0);
  });

  it("strips a pre-existing <br>--<br> signature marker before appending", () => {
    const newSig = "<p>Jane</p>";
    const body = `<p>Hello world</p><br>--<br><p>Old signature</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Old signature");
    expect(result).toContain(newSig);
  });

  it("collapses 3 stacked signatures into exactly 1 signature (no cumulative)", () => {
    const newSig = "<p>Jane</p>";
    const sig1 = "<p>Sig One</p>";
    const sig2 = "<p>Sig Two</p>";
    const sig3 = "<p>Sig Three</p>";
    const body = `<p>Hello</p>\n\n--\n${sig1}\n\n--\n${sig2}\n\n--\n${sig3}`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Sig One");
    expect(result).not.toContain("Sig Two");
    expect(result).not.toContain("Sig Three");
    expect(result).toContain(newSig);
    expect(result).toBe(`<p>Hello</p><p>--</p>${newSig}${UNSUBSCRIBE_FOOTER_HTML}`);
  });

  it("collapses stacked HTML signatures into exactly 1 signature", () => {
    const newSig = "<p>Jane</p>";
    const body = `<p>Hello</p><p>--</p><p>Sig One</p><p>--</p><p>Sig Two</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Sig One");
    expect(result).not.toContain("Sig Two");
    expect(result).toContain(newSig);
  });

  it("idempotent: f(f(x)) === f(x)", () => {
    const newSig = "<p>Jane</p>";
    const body = "<p>Hello world</p>";
    const once = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    const twice = buildEmailBodyWithSignature(once, acct({ signature: newSig }));
    expect(twice).toBe(once);
  });

  it("appends a visible unsubscribe footer with Instantly's {unsubscribe_link} merge var below the signature", () => {
    const result = buildEmailBodyWithSignature("<p>Hi</p>", acct({ email: "kevinl@growthagency.dev" }));
    // Single-brace merge var — resolved per-lead by Instantly at send time.
    expect(result).toContain('href="{unsubscribe_link}"');
    expect(result).not.toContain("{{unsubscribe_link}}");
    expect(result).toContain(">unsubscribe</a>");
    expect(result).toContain("Don't want to hear from me again?");
    // Footer comes AFTER the signature block.
    expect(result.indexOf("Distribute.you")).toBeLessThan(result.indexOf("unsubscribe</a>"));
  });

  it("separates the signature from the unsubscribe footer with a blank-line spacer paragraph", () => {
    const result = buildEmailBodyWithSignature("<p>Hi</p>", acct({ email: "kevinl@growthagency.dev" }));
    // The <p>&nbsp;</p> spacer sits between the brand line and the unsubscribe line.
    expect(result).toContain("Distribute.you | Marketing Agency</p><p>&nbsp;</p>");
  });

  it("idempotent WITH the unsubscribe footer: a re-sent body keeps exactly one footer", () => {
    const account = acct({ email: "kevinl@growthagency.dev", signature: "" });
    const once = buildEmailBodyWithSignature("<p>Hi</p>", account);
    const twice = buildEmailBodyWithSignature(once, account);
    expect(twice).toBe(once);
    expect((twice.match(/unsubscribe<\/a>/g) ?? []).length).toBe(1);
    expect((twice.match(/&nbsp;/g) ?? []).length).toBe(1);
  });

  it("account.signature wins over per-account default signature when present", () => {
    const result = buildEmailBodyWithSignature("<p>Hello</p>", acct({ signature: "<p>Account Sig</p>" }));
    expect(result).toBe(`<p>Hello</p><p>--</p><p>Account Sig</p>${UNSUBSCRIBE_FOOTER_HTML}`);
    expect(result).not.toContain("Kevin Lourd");
  });

  it("canonical fallback also strips stacked sigs (idempotence preserved)", () => {
    const body = "<p>Hello</p>\n\n--\n<p>Old Sig 1</p>\n\n--\n<p>Old Sig 2</p>";
    const result = buildEmailBodyWithSignature(body, acct({ email: "kevinl@growthagency.dev", signature: "" }));
    expect(result).toContain("<p>Hello</p><p>--</p><p>Kevin Lourd<br>Distribute.you | Marketing Agency</p>");
    expect(result).not.toContain("Old Sig 1");
    expect(result).not.toContain("Old Sig 2");
  });
});

describe("stripAccountSignature", () => {
  it("returns body unchanged when no marker present", () => {
    expect(stripAccountSignature("<p>Hello world</p>")).toBe("<p>Hello world</p>");
    expect(stripAccountSignature("Just plain text")).toBe("Just plain text");
  });

  it("strips first plain-text `\\n\\n--\\n` marker and everything after", () => {
    const body = "<p>Hello</p>\n\n--\n<p>Bob signature</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips HTML `<p>--</p>` marker (paragraph form) and everything after", () => {
    const body = "<p>Hello</p><p>--</p><p>Bob signature</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips HTML `<br>--<br>` marker (line-break form) and everything after", () => {
    const body = "<p>Hello world</p><br>--<br><p>Sig</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello world</p>");
  });

  it("strips HTML `<div>--</div>` marker and everything after", () => {
    const body = "<div>Hello</div><div>--</div><div>Sig</div>";
    expect(stripAccountSignature(body)).toBe("<div>Hello</div>");
  });

  it("strips marker with `-- ` (trailing space, RFC 3676)", () => {
    const body = "<p>Hello</p>\n\n-- \n<p>Sig</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips marker with `&nbsp;` adjacent (HTML non-breaking space)", () => {
    const body = "<p>Hello</p><p>--&nbsp;</p><p>Sig</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips all of 3 stacked plain-text signatures via first occurrence", () => {
    const body = "<p>Hello</p>\n\n--\nSig1\n\n--\nSig2\n\n--\nSig3";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips all of 3 stacked HTML signatures via first occurrence", () => {
    const body = "<p>Hello</p><p>--</p><p>Sig1</p><p>--</p><p>Sig2</p><p>--</p><p>Sig3</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("idempotent: f(f(x)) === f(x)", () => {
    const inputs = [
      "<p>Hello</p>\n\n--\n<p>Sig</p>",
      "<p>Hello</p><p>--</p><p>Sig</p>",
      "<p>Hello</p>",
      "<p>Hello</p><br>--<br><p>Sig</p>",
    ];
    for (const input of inputs) {
      const once = stripAccountSignature(input);
      const twice = stripAccountSignature(once);
      expect(twice).toBe(once);
    }
  });
});

describe("buildSequenceSteps", () => {
  it("should inject signature into every step bodyHtml", () => {
    const sig = "<p>Cheers</p>";
    const sequence = [
      { step: 1, bodyHtml: "<p>First</p>", daysSinceLastStep: 0 },
      { step: 2, bodyHtml: "<p>Second</p>", daysSinceLastStep: 3 },
    ];
    const steps = buildSequenceSteps("Subject", sequence, acct({ signature: sig }));
    expect(steps).toHaveLength(2);
    expect(steps[0].bodyHtml).toContain(sig);
    expect(steps[1].bodyHtml).toContain(sig);
    expect(steps[0].subject).toBe("Subject");
    expect(steps[1].subject).toBe("Subject");
    expect(steps[0].daysSinceLastStep).toBe(0);
    expect(steps[1].daysSinceLastStep).toBe(3);
  });

  it("should sort steps by step number", () => {
    const sequence = [
      { step: 3, bodyHtml: "C", daysSinceLastStep: 7 },
      { step: 1, bodyHtml: "A", daysSinceLastStep: 0 },
      { step: 2, bodyHtml: "B", daysSinceLastStep: 3 },
    ];
    const steps = buildSequenceSteps("Subject", sequence, acct());
    expect(steps[0].bodyHtml).toMatch(/^A\b/);
    expect(steps[1].bodyHtml).toMatch(/^B\b/);
    expect(steps[2].bodyHtml).toMatch(/^C\b/);
  });
});

describe("POST /send", () => {
  let runCounter: number;

  beforeEach(() => {
    vi.clearAllMocks();
    runCounter = 0;

    mockResolveInstantlyApiKey.mockResolvedValue({ key: "test-instantly-key", keySource: "platform" });
    mockAuthorizeCreditSpend.mockResolvedValue({ sufficient: true, balance_cents: 1000 });

    mockCreateRun.mockImplementation(() => {
      runCounter++;
      return Promise.resolve({ id: `step-run-${runCounter}` });
    });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockAddCosts.mockImplementation((runId: string, items: { costName: string }[]) => {
      return Promise.resolve({
        costs: items.map((item, i) => ({ id: `cost-${runId}-${item.costName}`, costName: item.costName })),
      });
    });
    mockUpdateRun.mockResolvedValue({});
    mockListAccounts.mockResolvedValue([{ email: "sender@example.com", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Best,<br>Sender</p>" }]);
    mockUpdateCampaign.mockResolvedValue({});
    mockGetCampaign.mockResolvedValue({ email_list: [], bcc_list: [], not_sending_status: null, status: "active" });
    mockDbReturning.mockResolvedValue([{ id: "lead-1" }]);
    mockDbInsertValues.mockReset();
    mockRefreshLeadStatusCurrent.mockResolvedValue(undefined);
  });

  it("sends from the in_production pool (silver-gated, pre-filtered)", async () => {
    // The send path reads the pre-derived in_production pool from silver
    // (fetchInProductionAccounts). It does NOT re-filter by status/warmup/domain
    // at send time — that eligibility is owned by the lifecycle reconcile.
    mockListAccounts.mockResolvedValue([
      { email: "prod@example.com", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
    ]);
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ email_list: ["prod@example.com"] }),
    );
  });

  it("draws the account pool from the x-feature-slug (feature carve-out)", async () => {
    // A send carrying x-feature-slug picks the pool reserved for that feature:
    // send.ts → sendLeadToInstantly → fetchInProductionAccounts(featureSlug).
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set({ ...identityHeadersObj, "x-feature-slug": "sales-crm-email-outreach" })
      .send(validBody);

    expect(mockFetchInProductionAccounts).toHaveBeenCalledWith(
      "sales-crm-email-outreach",
    );
  });

  it("draws from the default pool (null slug) when x-feature-slug absent", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    // identityHeadersObj carries no x-feature-slug.
    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockFetchInProductionAccounts).toHaveBeenCalledWith(null);
  });

  it("sets bcc_list on the campaign PATCH when bcc is provided", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set(identityHeadersObj)
      .send({ ...validBody, bcc: ["a@x.com", "b@x.com"] });

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ bcc_list: ["a@x.com", "b@x.com"] }),
    );
  });

  it("omits bcc_list from the campaign PATCH when bcc absent", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    const patchArg = mockUpdateCampaign.mock.calls[0][2] as Record<string, unknown>;
    expect(patchArg).not.toHaveProperty("bcc_list");
  });

  it("omits bcc_list from the campaign PATCH when bcc is an empty array", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set(identityHeadersObj)
      .send({ ...validBody, bcc: [] });

    const patchArg = mockUpdateCampaign.mock.calls[0][2] as Record<string, unknown>;
    expect(patchArg).not.toHaveProperty("bcc_list");
  });

  it("refreshes the Gold status row after attaching the real Instantly campaign id", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(mockRefreshLeadStatusCurrent).toHaveBeenCalledWith(
      "inst-camp-new",
      validBody.to,
    );
  });

  it("should return 500 when the in_production pool is empty", async () => {
    mockListAccounts.mockResolvedValue([]); // no in_production accounts
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.details).toContain("No active Instantly accounts available");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
  });

  it("should reject the old email format", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      subject: undefined,
      sequence: undefined,
      email: { subject: "Hello", body: "World" },
    });

    expect(res.status).toBe(400);
  });

  it("should create campaign with multi-step sequence", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockCreateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      {
        name: "Campaign camp-1",
        steps: expect.arrayContaining([
          expect.objectContaining({ subject: "Hello", daysSinceLastStep: 0 }),
          expect.objectContaining({ subject: "Hello", daysSinceLastStep: 3 }),
          expect.objectContaining({ subject: "Hello", daysSinceLastStep: 7 }),
        ]),
      },
    );
  });

  it("should inject signature into all step bodies", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // [0] = apiKey, [1] = params
    const createCall = mockCreateCampaign.mock.calls[0][1];
    for (const step of createCall.steps) {
      expect(step.bodyHtml).toContain("<p>Best,<br>Sender</p>");
    }
  });

  it("should enable stop_on_reply when patching campaign", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({
        stop_on_reply: true,
        email_list: ["sender@example.com"],
      }),
    );
  });

  it("should store leadId and deliveryStatus in campaign insert", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.campaignId === "camp-1" && v.leadEmail === "test@example.com",
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0]).toMatchObject({
      leadId: "lead-1",
      deliveryStatus: "contacted",
    });
  });

  it("should create per-step runs with correct cost items: contact upload (step 1 only) + 2 email costs per step", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockCreateRun).toHaveBeenCalledTimes(3);
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-1" }), expect.objectContaining({ orgId: "org-1", userId: "user-1" }));
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-2" }), expect.objectContaining({ orgId: "org-1", userId: "user-1" }));
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-3" }), expect.objectContaining({ orgId: "org-1", userId: "user-1" }));

    expect(mockAddCosts).toHaveBeenCalledTimes(3);
    // Step 1: 2 email costs (provisioned — only actualized on webhook email_sent)
    // + contact upload (actual — lead is uploaded regardless of dispatch)
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-1", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-contact-uploaded", quantity: 1, costSource: "platform", status: "actual" },
    ], expect.objectContaining({ orgId: "org-1" }));
    // Steps 2-3: 2 email costs (provisioned), no contact upload
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-2", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
    ], expect.objectContaining({ orgId: "org-1" }));
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-3", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
    ], expect.objectContaining({ orgId: "org-1" }));
  });


  it("should store per-step email cost IDs in sequence_costs table (2 per step, excluding contact upload)", async () => {
    mockNewCampaignFlow();
    // Reset to track sequence_costs inserts
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-new" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]); // for sequence_costs inserts

    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // Check that sequence_costs were inserted for ALL steps with 2 rows per step (account + domain)
    // Contact upload cost should NOT be in sequence_costs
    const insertCalls = mockDbInsertValues.mock.calls;
    const sequenceCostInserts = insertCalls.filter(
      ([v]: [any]) => v.costId && v.step,
    );
    expect(sequenceCostInserts).toHaveLength(6); // 2 costs × 3 steps
    // Step 1: 2 provisioned costs (flipped to actual on webhook email_sent)
    expect(sequenceCostInserts[0][0]).toMatchObject({ step: 1, runId: "step-run-1", status: "provisioned" });
    expect(sequenceCostInserts[1][0]).toMatchObject({ step: 1, runId: "step-run-1", status: "provisioned" });
    // Step 2: 2 provisioned costs
    expect(sequenceCostInserts[2][0]).toMatchObject({ step: 2, runId: "step-run-2", status: "provisioned" });
    expect(sequenceCostInserts[3][0]).toMatchObject({ step: 2, runId: "step-run-2", status: "provisioned" });
    // Step 3: 2 provisioned costs
    expect(sequenceCostInserts[4][0]).toMatchObject({ step: 3, runId: "step-run-3", status: "provisioned" });
    expect(sequenceCostInserts[5][0]).toMatchObject({ step: 3, runId: "step-run-3", status: "provisioned" });
  });

  it("should work with a single-step sequence (1 run, 3 costs: account + domain + contact upload)", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const singleStep = {
      ...validBody,
      sequence: [{ step: 1, bodyHtml: "<p>Only email</p>", daysSinceLastStep: 0 }],
    };

    const res = await request(app).post("/send").set(identityHeadersObj).send(singleStep);

    expect(res.status).toBe(200);
    expect(mockCreateRun).toHaveBeenCalledTimes(1);
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-1" }), expect.objectContaining({ orgId: "org-1" }));
    expect(mockAddCosts).toHaveBeenCalledTimes(1);
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-1", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-contact-uploaded", quantity: 1, costSource: "platform", status: "actual" },
    ], expect.objectContaining({ orgId: "org-1" }));
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "completed", expect.objectContaining({ orgId: "org-1" }));
  });

  it("should skip Instantly API call and step runs when same lead already processed for campaign", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    // RESERVE upsert loses the claim (row already committed) → empty RETURNING.
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.duplicate).toBe(true);
    expect(res.body.added).toBe(0);
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    expect(mockAddLeads).not.toHaveBeenCalled();
    // No step runs or costs should be created for duplicates
    expect(mockCreateRun).not.toHaveBeenCalled();
    expect(mockAddCosts).not.toHaveBeenCalled();
    // AC2: no Instantly campaign created before the reservation is won.
    expect(mockDbDelete).not.toHaveBeenCalled(); // nothing to release (we never reserved)
  });

  it("should create separate campaigns for different leads in the same campaign", async () => {
    const app = await createSendApp();

    // Send 1: Lead A
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-A", status: "draft" });
    mockGetCampaign.mockReset();
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-A", campaignId: "camp-1", instantlyCampaignId: "inst-camp-A" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-A" }]);
    mockDbReturning.mockResolvedValue([]);

    const res1 = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      to: "alice@example.com",
      sequence: [{ step: 1, bodyHtml: "<p>Hi Alice</p>", daysSinceLastStep: 0 }],
    });
    expect(res1.status).toBe(200);

    // Send 2: Lead B (same campaignId, different lead)
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-B", status: "draft" });
    mockGetCampaign.mockReset();
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-B", campaignId: "camp-1", instantlyCampaignId: "inst-camp-B" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-B" }]);
    mockDbReturning.mockResolvedValue([]);

    const res2 = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      to: "bob@example.com",
      sequence: [{ step: 1, bodyHtml: "<p>Hi Bob</p>", daysSinceLastStep: 0 }],
    });
    expect(res2.status).toBe(200);

    expect(mockCreateCampaign).toHaveBeenCalledTimes(2);
    // [0] = apiKey, [1] = params
    const call1 = mockCreateCampaign.mock.calls[0][1];
    const call2 = mockCreateCampaign.mock.calls[1][1];
    expect(call1.steps[0].bodyHtml).toContain("Hi Alice");
    expect(call2.steps[0].bodyHtml).toContain("Hi Bob");
  });

  it("should ignore not_sending_status post-activate (no retry, no failure)", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    // One-shot create + verify-PATCH (NSS=4 on verify but no longer error-signal).
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-1", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({
      email_list: ["sender@example.com"],
      not_sending_status: 4, // daily limit hit on the chosen account — pacing only
    });

    mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-1" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(mockCreateCampaign).toHaveBeenCalledTimes(1);
    expect(mockCreateRun).toHaveBeenCalledTimes(3); // 3 step runs created
    expect(mockUpdateRun).toHaveBeenCalledTimes(3); // all steps completed immediately
  });

  it("should complete all step runs immediately (not just step 1)", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // All 3 step runs should be completed immediately
    expect(mockUpdateRun).toHaveBeenCalledTimes(3);
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "completed", expect.objectContaining({ orgId: "org-1" }));
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-2", "completed", expect.objectContaining({ orgId: "org-1" }));
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-3", "completed", expect.objectContaining({ orgId: "org-1" }));
  });

  it("should return 200 idempotent duplicate (NOT 409) when a concurrent request already claimed the lead", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check

    // RESERVE upsert loses the atomic claim — concurrent peer holds it. Empty
    // RETURNING ⇒ idempotent 200 duplicate, NOT a fatal 409.
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.duplicate).toBe(true);
    expect(res.body.added).toBe(0);
    // AC2: the loser creates NO Instantly campaign (claim lost before dispatch).
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    // No step runs/costs for the losing request, and nothing to release.
    expect(mockCreateRun).not.toHaveBeenCalled();
    expect(mockDbDelete).not.toHaveBeenCalled();
  });

  it("should return stepRuns array in response", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.stepRuns).toHaveLength(3);
    expect(res.body.stepRuns[0]).toMatchObject({ step: 1, runId: "step-run-1" });
    expect(res.body.stepRuns[1]).toMatchObject({ step: 2, runId: "step-run-2" });
    expect(res.body.stepRuns[2]).toMatchObject({ step: 3, runId: "step-run-3" });
  });

  it("should read brandIds and workflowSlug from headers only", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set({
        ...identityHeadersObj,
        "x-brand-id": "header-brand",
        "x-workflow-slug": "header-workflow",
      })
      .send(validBody);

    // Campaign insert should use header values
    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0].brandIds).toEqual(["header-brand"]);
    expect(campaignInsert![0].workflowSlug).toBe("header-workflow");
  });

  it("should parse multi-brand CSV header into brandIds array", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set({
        ...identityHeadersObj,
        "x-brand-id": "brand-a,brand-b,brand-c",
      })
      .send(validBody);

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0].brandIds).toEqual(["brand-a", "brand-b", "brand-c"]);
  });

  it("should forward tracking headers to runs-service", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set({
        ...identityHeadersObj,
        "x-brand-id": "header-brand",
        "x-campaign-id": "header-camp",
        "x-workflow-slug": "header-wf",
        "x-goal": "signup",
        "x-brand-profile-id": "brand-profile-1",
        "x-audience-id": "audience-1",
      })
      .send(validBody);

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0].metadata).toEqual({
      goal: "signup",
      brandProfileId: "brand-profile-1",
      audienceId: "audience-1",
    });

    // createRun should receive tracking in identity context
    expect(mockCreateRun).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        tracking: expect.objectContaining({
          brandId: "header-brand",
          campaignId: "header-camp",
          workflowSlug: "header-wf",
          goal: "signup",
          brandProfileId: "brand-profile-1",
          audienceId: "audience-1",
        }),
      }),
    );
  });

  it("should accept platform sends without campaign context and not pass campaignId to runs-service", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-platform", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null });
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-platform", campaignId: null, instantlyCampaignId: "inst-camp-platform" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]);

    const app = await createSendApp();
    const res = await request(app)
      .post("/send")
      .set({
        "x-org-id": "org-1",
        "x-user-id": "user-1",
        "x-run-id": "run-1",
        "x-brand-id": "brand-1",
      })
      .send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.campaignId).toBeNull();
    expect(mockDbWhere).toHaveBeenCalledTimes(1);
    expect(mockCreateRun).toHaveBeenCalledTimes(3);
    for (const [params, identity] of mockCreateRun.mock.calls) {
      expect(params.campaignId).toBeUndefined();
      expect(identity.tracking?.campaignId).toBeUndefined();
    }

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert![0].campaignId).toBeNull();

    const sequenceCostInserts = mockDbInsertValues.mock.calls.filter(
      ([v]: [any]) => v.costId && v.step,
    );
    expect(sequenceCostInserts).toHaveLength(6);
    for (const [value] of sequenceCostInserts) {
      expect(value.campaignId).toBeNull();
    }
  });

  // ── Platform-send idempotency (campaignId NULL) ─────────────────────────────
  // A platform send carries no x-campaign-id, so campaignId is null. The
  // reservation must collide on a retry via the (run_id, lead_email) partial
  // unique index — NOT (campaign_id, lead_email), which never collides on null
  // campaign (Postgres NULLs are DISTINCT). Guards the 2026-06-27 dup incident.
  const platformHeaders = {
    "x-org-id": "org-1",
    "x-user-id": "user-1",
    "x-run-id": "run-1",
    "x-brand-id": "brand-1",
  };

  function mockPlatformWinnerFlow() {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check (no conflict)
    mockCreateCampaign.mockResolvedValue({ id: "inst-camp-plat", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null });
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-plat", campaignId: null, instantlyCampaignId: "inst-camp-plat" }]); // RESERVE → winner
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
    mockDbReturning.mockResolvedValue([]);
    mockUpdateCampaignStatus.mockResolvedValue({});
  }

  it("platform send (campaignId null) reserves on the (run_id, lead_email) arbiter", async () => {
    mockPlatformWinnerFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(platformHeaders).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.campaignId).toBeNull();
    const cfg = mockOnConflictDoUpdate.mock.calls[0][0];
    expect(cfg.target).toEqual(["run_id", "lead_email"]);
    expect(cfg.targetWhere).toBeDefined(); // partial index predicate (campaign_id IS NULL AND status='active')
  });

  it("campaign send (campaignId present) reserves on the (campaign_id, lead_email) arbiter", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    const cfg = mockOnConflictDoUpdate.mock.calls[0][0];
    expect(cfg.target).toEqual(["campaign_id", "lead_email"]);
    expect(cfg.targetWhere).toBeUndefined(); // full unique index, no partial predicate
  });

  it("platform retry (same leadEmail + same runId, campaignId null) is an idempotent duplicate — no 2nd campaign", async () => {
    const app = await createSendApp();

    // 1st send: wins the reservation, creates the Instantly campaign.
    mockPlatformWinnerFlow();
    const res1 = await request(app).post("/send").set(platformHeaders).send(validBody);
    expect(res1.status).toBe(200);
    expect(res1.body.duplicate).toBeUndefined();
    expect(mockCreateCampaign).toHaveBeenCalledTimes(1);

    // 2nd send (the timeout-retry): the (run_id, lead_email) reservation
    // collides → empty RETURNING → idempotent 200 duplicate, NO 2nd campaign.
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([]); // RESERVE upsert loses the claim

    const res2 = await request(app).post("/send").set(platformHeaders).send(validBody);
    expect(res2.status).toBe(200);
    expect(res2.body.duplicate).toBe(true);
    expect(res2.body.added).toBe(0);
    expect(mockCreateCampaign).toHaveBeenCalledTimes(1); // still ONE — no duplicate campaign
  });

  it("should return 402 when credit authorization fails for platform keySource", async () => {
    mockAuthorizeCreditSpend.mockResolvedValue({ sufficient: false, balance_cents: 2, required_cents: 15 });
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(402);
    expect(res.body.error).toBe("Insufficient credits");
    expect(res.body.balance_cents).toBe(2);
    expect(res.body.required_cents).toBe(15);
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    expect(mockCreateRun).not.toHaveBeenCalled();
  });

  it("should call authorizeCreditSpend with 3 cost items (contact + account + domain)", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockAuthorizeCreditSpend).toHaveBeenCalledWith(
      [
        { costName: "instantly-contact-uploaded", quantity: 1 },
        { costName: "instantly-account-email-sent", quantity: 3 },
        { costName: "instantly-domain-email-sent", quantity: 3 },
      ],
      "instantly-send",
      expect.objectContaining({
        orgId: "org-1",
        userId: "user-1",
        runId: "run-1",
      }),
    );
  });

  it("should skip credit authorization when keySource is org (BYOK)", async () => {
    mockResolveInstantlyApiKey.mockResolvedValue({ key: "org-key", keySource: "org" });
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockAuthorizeCreditSpend).not.toHaveBeenCalled();
    expect(mockCreateCampaign).toHaveBeenCalled();
  });

  it("should return 409 when email already exists with a different leadId", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ leadId: "existing-lead-99" }]); // lead_id conflict found

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      leadId: "different-lead-1",
    });

    expect(res.status).toBe(409);
    expect(res.body.error).toBe("Lead ID conflict");
    expect(res.body.details).toContain("existing-lead-99");
    expect(res.body.details).toContain("different-lead-1");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
  });

  // ── Reservation idempotency (DIS-148) ──────────────────────────────────────

  it("AC2: reserves the (campaignId, leadEmail) row with a reserving:<uuid> sentinel BEFORE any Instantly call", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // First instantly_campaigns insert is the RESERVE — it carries the sentinel
    // and must precede createCampaign (the external side-effect).
    const reserveInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.campaignId === "camp-1" && v.leadEmail === "test@example.com",
    );
    expect(reserveInsert).toBeDefined();
    expect(reserveInsert![0].instantlyCampaignId).toMatch(/^reserving:/);
    expect(reserveInsert![0].deliveryStatus).toBe("contacted");

    // Ordering: the reserve insert was recorded before createCampaign fired.
    const reserveInsertOrder = mockDbInsertValues.mock.invocationCallOrder[0];
    const createCampaignOrder = mockCreateCampaign.mock.invocationCallOrder[0];
    expect(reserveInsertOrder).toBeLessThan(createCampaignOrder);
  });

  it("winner: overwrites the sentinel with the real Instantly campaign id (phase-2) and 200s", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(mockCreateCampaign).toHaveBeenCalled();
    // Winner did NOT release its reservation.
    expect(mockDbDelete).not.toHaveBeenCalled();
  });

  it("AC4: releases the reservation (delete) when sendLeadToInstantly finds no accounts", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "reserved-1", campaignId: "camp-1", instantlyCampaignId: "reserving:x" }]); // RESERVE → winner
    // Empty in_production pool → sendLeadToInstantly returns { ok: false }.
    mockListAccounts.mockResolvedValue([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.details).toContain("No active Instantly accounts available");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    // Reservation released so a later legit retry can re-claim.
    expect(mockDbDelete).toHaveBeenCalledTimes(1);
  });

  it("AC4: releases the reservation when a later step throws after the campaign was dispatched", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "reserved-2", campaignId: "camp-1", instantlyCampaignId: "reserving:y" }]); // RESERVE → winner
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
    mockDbReturning.mockResolvedValue([]);
    mockCreateCampaign.mockResolvedValue({ id: "inst-camp-throw", status: "draft" });
    mockGetCampaign.mockResolvedValue({ email_list: ["sender@example.com"], not_sending_status: null });
    // Step-run creation throws → handler unwinds into the catch.
    mockCreateRun.mockReset();
    mockCreateRun.mockRejectedValue(new Error("runs-service down"));

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    // releaseReservation runs in the catch (no-op at DB level once phase-2 ran,
    // but the handler still attempts it for any still-open reservation).
    expect(mockDbDelete).toHaveBeenCalledTimes(1);
  });
});
