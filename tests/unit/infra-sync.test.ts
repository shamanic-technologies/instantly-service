import { describe, it, expect, vi, beforeEach } from "vitest";

// ── Mock the DB. Inserts resolve; onConflictDoUpdate is chained off values(). ──
const mockExecute = vi.fn(async () => [] as unknown[]);
const mockInsertValues = vi.fn(async () => undefined);

vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({
      values: (...a: unknown[]) => {
        const promise = mockInsertValues(...a) as Promise<undefined> & {
          onConflictDoUpdate?: (...args: unknown[]) => Promise<undefined>;
        };
        promise.onConflictDoUpdate = () => Promise.resolve(undefined);
        return promise;
      },
    }),
  },
}));

// ── Mock key resolution ───────────────────────────────────────────────────────
const mockResolvePlatformKey = vi.fn(async (provider: string) => `key-${provider}`);
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformKey: (...a: [string, unknown]) => mockResolvePlatformKey(...a),
}));

// ── Mock every provider fetch ─────────────────────────────────────────────────
const emptyInventory = { domains: [], mailboxes: [], accountScopes: [] };
const mockGandi = vi.fn(async () => emptyInventory);
const mockMailforge = vi.fn(async () => emptyInventory);
const mockPrimeforge = vi.fn(async () => emptyInventory);
const mockDfy = vi.fn(async () => emptyInventory);

vi.mock("../../src/lib/providers/gandi-client", () => ({
  fetchGandiInventory: (...a: unknown[]) => mockGandi(...(a as [])),
}));
vi.mock("../../src/lib/providers/mailforge-client", () => ({
  fetchMailforgeInventory: (...a: unknown[]) => mockMailforge(...(a as [])),
}));
vi.mock("../../src/lib/providers/primeforge-client", () => ({
  fetchPrimeforgeInventory: (...a: unknown[]) => mockPrimeforge(...(a as [])),
}));
vi.mock("../../src/lib/providers/instantly-dfy-client", () => ({
  fetchInstantlyDfyInventory: (...a: unknown[]) => mockDfy(...(a as [])),
}));

import {
  buildProviderTasks,
  emptySummary,
  syncProviderInfra,
  GANDI_KEY_PROVIDERS,
} from "../../src/lib/infra-sync";

const caller = { method: "POST", path: "/internal/infra/sync" };

beforeEach(() => {
  vi.resetAllMocks();
  mockExecute.mockResolvedValue([]);
  mockInsertValues.mockResolvedValue(undefined);
  mockResolvePlatformKey.mockImplementation(async (provider: string) => `key-${provider}`);
  mockGandi.mockResolvedValue(emptyInventory);
  mockMailforge.mockResolvedValue(emptyInventory);
  mockPrimeforge.mockResolvedValue(emptyInventory);
  mockDfy.mockResolvedValue(emptyInventory);
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
});

describe("buildProviderTasks", () => {
  it("emits one task per Gandi organisation plus the three single-tenant vendors", () => {
    const tasks = buildProviderTasks();

    expect(tasks).toHaveLength(GANDI_KEY_PROVIDERS.length + 3);
    expect(tasks.filter((t) => t.provider === "gandi")).toHaveLength(3);
    expect(tasks.map((t) => t.keyProvider)).toEqual([
      "gandi-org1",
      "gandi-org2",
      "gandi-org3",
      "mailforge",
      "primeforge",
      "instantly",
    ]);
  });

  it("keeps all three Gandi organisations under the single `gandi` silver provider", () => {
    const gandi = buildProviderTasks().filter((t) => t.provider === "gandi");
    expect(new Set(gandi.map((t) => t.provider))).toEqual(new Set(["gandi"]));
  });

  it("reads the DFY inventory with the Instantly platform key, not a DFY-specific one", () => {
    const dfy = buildProviderTasks().find((t) => t.provider === "instantly-dfy");
    expect(dfy?.keyProvider).toBe("instantly");
  });
});

describe("emptySummary", () => {
  it("starts every counter at zero so a partial run cannot read as a full one", () => {
    const summary = emptySummary(6);
    expect(summary).toEqual({
      providersAttempted: 6,
      providersSucceeded: 0,
      providersFailed: 0,
      domainsUpserted: 0,
      mailboxesUpserted: 0,
      accountScopesRecorded: 0,
      domainsMarkedAbsent: 0,
      mailboxesMarkedAbsent: 0,
      failures: [],
    });
  });
});

describe("syncProviderInfra", () => {
  it("resolves one platform key per provider task", async () => {
    await syncProviderInfra(caller);

    expect(mockResolvePlatformKey.mock.calls.map((c) => c[0])).toEqual([
      "gandi-org1",
      "gandi-org2",
      "gandi-org3",
      "mailforge",
      "primeforge",
      "instantly",
    ]);
  });

  it("writes bronze and silver for each domain the vendor reports", async () => {
    mockMailforge.mockResolvedValue({
      domains: [
        {
          provider: "mailforge" as const,
          providerAccount: "wks_1",
          externalId: "dom_1",
          domain: "joindistribute.com",
          role: "mailbox" as const,
          status: "active",
          createdAtProvider: null,
          expiresAt: null,
          autorenew: null,
          deletionScheduled: false,
          cancelledAt: null,
          priceCents: 1400,
          priceCurrency: "USD",
          payload: { sld: "joindistribute" },
        },
      ],
      mailboxes: [],
      accountScopes: [],
    });

    const summary = await syncProviderInfra(caller);

    expect(summary.domainsUpserted).toBe(1);
    // One bronze insert + one silver upsert for the single domain.
    expect(mockInsertValues).toHaveBeenCalledTimes(2);
  });

  it("counts a failing vendor without stopping the others", async () => {
    mockPrimeforge.mockRejectedValue(new Error("Primeforge GET /domains failed: 401"));

    const summary = await syncProviderInfra(caller);

    expect(summary.providersFailed).toBe(1);
    expect(summary.providersSucceeded).toBe(5);
    expect(summary.failures).toEqual([
      { provider: "primeforge", message: "Primeforge GET /domains failed: 401" },
    ]);
    expect(mockDfy).toHaveBeenCalledTimes(1);
  });

  it("counts a missing platform key as that vendor's failure, not a fatal run error", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) => {
      if (provider === "mailforge") throw new Error("Platform key not found: mailforge");
      return `key-${provider}`;
    });

    const summary = await syncProviderInfra(caller);

    expect(summary.providersFailed).toBe(1);
    expect(summary.failures[0].provider).toBe("mailforge");
  });

  it("throws when EVERY vendor failed — a run that ingested nothing is not green", async () => {
    mockResolvePlatformKey.mockRejectedValue(new Error("key-service down"));

    await expect(syncProviderInfra(caller)).rejects.toThrow(/every provider failed/);
  });

  it("sweeps absences only for the vendors whose fetch succeeded", async () => {
    mockGandi.mockRejectedValue(new Error("Gandi 503"));

    await syncProviderInfra(caller);

    const swept = mockExecute.mock.calls
      .map((call) => JSON.stringify(call[0]))
      .filter((text) => text.includes("absent_since"));

    // 3 vendors succeeded (mailforge, primeforge, instantly-dfy) × domains+mailboxes.
    expect(swept).toHaveLength(6);
    expect(swept.some((text) => text.includes("gandi"))).toBe(false);
  });

  it("scopes the Gandi absence sweep to the reporting organisation", async () => {
    await syncProviderInfra(caller);

    const sweepText = mockExecute.mock.calls
      .map((call) => JSON.stringify(call[0]))
      .filter((text) => text.includes("absent_since") && text.includes("provider_account"));

    // Both sweeps (domains + mailboxes) for each of the three organisations.
    expect(sweepText).toHaveLength(6);
  });

  it("does not sweep absences with a NOT IN list — it would hit the bind-parameter ceiling", async () => {
    await syncProviderInfra(caller);

    const sweeps = mockExecute.mock.calls
      .map((call) => JSON.stringify(call[0]))
      .filter((text) => text.includes("absent_since"));

    for (const sweep of sweeps) {
      expect(sweep).toContain("last_seen_at");
      expect(sweep).not.toContain("NOT IN");
    }
  });
});
