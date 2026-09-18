import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockInsertValues = vi.fn();
const mockOnConflict = vi.fn(async () => undefined);
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({
      values: (...a: unknown[]) => {
        mockInsertValues(...a);
        return { onConflictDoUpdate: (...b: unknown[]) => mockOnConflict(...b) };
      },
    }),
  },
}));

const mockLoadEntries = vi.fn();
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  loadMailboxLoginEntries: (...a: unknown[]) => mockLoadEntries(...a),
}));

import {
  deriveMailboxes,
  primaryProviderByDomain,
  syncMailboxes,
  type MailboxSyncInput,
} from "../../src/lib/mailboxes-sync";

/** What node-postgres actually returns — never a bare array. */
function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

const D = (s: string) => new Date(s);

const BASE: MailboxSyncInput = {
  credentials: [
    { address: "kevin@marketingagency.life", login: "kevin@marketingagency.life", source: "manual" },
    { address: "kevinl@marketingagency.life", login: "kevin@marketingagency.life", source: "manual" },
    { address: "amy@boostdistribute.com", login: "amy@boostdistribute.com", source: "primeforge" },
  ],
  accounts: [
    { email: "kevin@marketingagency.life", timestampCreated: D("2026-05-02"), vendorPrewarmedAt: null, absentSince: null },
    { email: "kevinl@marketingagency.life", timestampCreated: D("2026-04-01"), vendorPrewarmedAt: null, absentSince: null },
    { email: "amy@boostdistribute.com", timestampCreated: D("2026-07-07"), vendorPrewarmedAt: D("2026-06-01"), absentSince: null },
    { email: "lou@dfydomain.com", timestampCreated: D("2026-08-01"), vendorPrewarmedAt: null, absentSince: null },
  ],
  vendorMailboxes: [
    { provider: "gandi", email: "kevin@marketingagency.life", createdAtProvider: D("2026-03-15"), absentSince: null },
    { provider: "gandi", email: "unused@marketingagency.life", createdAtProvider: D("2026-03-15"), absentSince: null },
    { provider: "primeforge", email: "amy@boostdistribute.com", createdAtProvider: D("2026-06-01"), absentSince: null },
  ],
  domainProviders: [
    { domain: "marketingagency.life", provider: "gandi" },
    { domain: "boostdistribute.com", provider: "primeforge" },
    { domain: "dfydomain.com", provider: "instantly-dfy" },
  ],
};

describe("deriveMailboxes — the address → real-mailbox grouping", () => {
  it("groups Gandi aliases under their login and links every address", () => {
    const { mailboxes, accountLogins } = deriveMailboxes(BASE);
    const gandi = mailboxes.find((m) => m.login === "kevin@marketingagency.life")!;
    expect(gandi.provider).toBe("gandi");
    expect(gandi.poolType).toBe("gandi-relay");
    expect(gandi.subscription).toBe("standard");
    expect(gandi.credentialSource).toBe("manual");
    expect(accountLogins).toContainEqual({ email: "kevinl@marketingagency.life", login: "kevin@marketingagency.life" });
    expect(accountLogins).toContainEqual({ email: "kevin@marketingagency.life", login: "kevin@marketingagency.life" });
  });

  it("takes the EARLIEST import across aliases and the vendor's own creation date", () => {
    const gandi = deriveMailboxes(BASE).mailboxes.find((m) => m.login === "kevin@marketingagency.life")!;
    expect(gandi.importedAt).toEqual(D("2026-04-01"));
    expect(gandi.vendorCreatedAt).toEqual(D("2026-03-15"));
  });

  it("an address the credential map does not know is its OWN mailbox, never folded", () => {
    const { mailboxes, accountLogins } = deriveMailboxes(BASE);
    const dfy = mailboxes.find((m) => m.login === "lou@dfydomain.com")!;
    expect(dfy.credentialSource).toBe("none");
    expect(dfy.poolType).toBe("dfy-google");
    expect(dfy.subscription).toBe("dfy");
    expect(accountLogins).toContainEqual({ email: "lou@dfydomain.com", login: "lou@dfydomain.com" });
  });

  it("a Primeforge mailbox the vendor warmed is `prewarmed`, on google-workspace", () => {
    const amy = deriveMailboxes(BASE).mailboxes.find((m) => m.login === "amy@boostdistribute.com")!;
    expect(amy.subscription).toBe("prewarmed");
    expect(amy.poolType).toBe("google-workspace");
    expect(amy.credentialSource).toBe("primeforge");
    expect(amy.vendorPrewarmedAt).toEqual(D("2026-06-01"));
  });

  it("a vendor mailbox carrying no Instantly address still gets a row", () => {
    const unused = deriveMailboxes(BASE).mailboxes.find((m) => m.login === "unused@marketingagency.life")!;
    expect(unused).toBeDefined();
    expect(unused.credentialSource).toBe("none");
    expect(unused.importedAt).toBeNull();
    expect(unused.provider).toBe("gandi");
  });

  it("a domain no vendor reports carries null provider / pool / subscription — never a guess", () => {
    const out = deriveMailboxes({
      ...BASE,
      accounts: [{ email: "x@unknown.io", timestampCreated: null, vendorPrewarmedAt: null, absentSince: null }],
      credentials: [],
      vendorMailboxes: [],
    });
    const x = out.mailboxes.find((m) => m.login === "x@unknown.io")!;
    expect(x.provider).toBeNull();
    expect(x.poolType).toBeNull();
    expect(x.subscription).toBeNull();
  });

  it("is absent only when EVERY alias is absent (and the vendor row, when there is one)", () => {
    const oneAliasLive = deriveMailboxes({
      ...BASE,
      accounts: [
        { email: "kevin@marketingagency.life", timestampCreated: null, vendorPrewarmedAt: null, absentSince: D("2026-08-02") },
        { email: "kevinl@marketingagency.life", timestampCreated: null, vendorPrewarmedAt: null, absentSince: null },
      ],
    });
    expect(oneAliasLive.mailboxes.find((m) => m.login === "kevin@marketingagency.life")!.absentSince).toBeNull();

    const allAbsentVendorLive = deriveMailboxes({
      ...BASE,
      accounts: [
        { email: "kevin@marketingagency.life", timestampCreated: null, vendorPrewarmedAt: null, absentSince: D("2026-08-02") },
        { email: "kevinl@marketingagency.life", timestampCreated: null, vendorPrewarmedAt: null, absentSince: D("2026-08-03") },
      ],
    });
    // The vendor still hosts it → the mailbox exists, its addresses merely left Instantly.
    expect(allAbsentVendorLive.mailboxes.find((m) => m.login === "kevin@marketingagency.life")!.absentSince).toBeNull();

    const allAbsentNoVendor = deriveMailboxes({
      ...BASE,
      accounts: [
        { email: "kevin@marketingagency.life", timestampCreated: null, vendorPrewarmedAt: null, absentSince: D("2026-08-02") },
        { email: "kevinl@marketingagency.life", timestampCreated: null, vendorPrewarmedAt: null, absentSince: D("2026-08-03") },
      ],
      vendorMailboxes: [],
    });
    expect(allAbsentNoVendor.mailboxes.find((m) => m.login === "kevin@marketingagency.life")!.absentSince).toEqual(D("2026-08-02"));
  });

  it("lower-cases addresses so casing never splits one mailbox in two", () => {
    const out = deriveMailboxes({
      ...BASE,
      accounts: [{ email: "Amy@BoostDistribute.com", timestampCreated: null, vendorPrewarmedAt: null, absentSince: null }],
    });
    expect(out.mailboxes.filter((m) => m.login === "amy@boostdistribute.com")).toHaveLength(1);
  });

  it("is deterministic — same input, same order", () => {
    expect(deriveMailboxes(BASE)).toEqual(deriveMailboxes(BASE));
  });
});

describe("primaryProviderByDomain — a domain two vendors report resolves to the one that fills earliest", () => {
  it("gandi beats primeforge, the selector's own tie-break", () => {
    const m = primaryProviderByDomain([
      { domain: "x.com", provider: "primeforge" },
      { domain: "x.com", provider: "gandi" },
    ]);
    expect(m.get("x.com")).toBe("gandi");
  });
});

describe("syncMailboxes — IO", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockOnConflict.mockResolvedValue(undefined);
    mockLoadEntries.mockResolvedValue(
      new Map([
        ["kevin@marketingagency.life", { login: "kevin@marketingagency.life", source: "manual" }],
        ["kevinl@marketingagency.life", { login: "kevin@marketingagency.life", source: "manual" }],
      ]),
    );
    mockExecute
      .mockResolvedValueOnce(
        pgResult([
          { email: "kevin@marketingagency.life", timestamp_created: "2026-05-02T00:00:00Z", vendor_prewarmed_at: null, absent_since: null },
          { email: "kevinl@marketingagency.life", timestamp_created: "2026-04-01T00:00:00Z", vendor_prewarmed_at: null, absent_since: null },
          { email: "lou@dfydomain.com", timestamp_created: null, vendor_prewarmed_at: null, absent_since: null },
        ]),
      )
      .mockResolvedValueOnce(pgResult([{ provider: "gandi", email: "kevin@marketingagency.life", created_at_provider: "2026-03-15T00:00:00Z", absent_since: null }]))
      .mockResolvedValueOnce(pgResult([{ domain: "marketingagency.life", provider: "gandi" }, { domain: "dfydomain.com", provider: "instantly-dfy" }]))
      .mockResolvedValue(pgResult([]));
  });

  it("reads through the QueryResult shape, upserts one row per mailbox and links every address", async () => {
    const summary = await syncMailboxes({ method: "POST", path: "/t" });
    expect(summary).toEqual({ mailboxes: 2, addresses: 3, addressesWithoutCredential: 1, vendorOnlyMailboxes: 0 });
    expect(mockInsertValues).toHaveBeenCalledTimes(2);
    const inserted = mockInsertValues.mock.calls.map((c) => (c[0] as { login: string }).login).sort();
    expect(inserted).toEqual(["kevin@marketingagency.life", "lou@dfydomain.com"]);
    // 3 reads + 3 UPDATEs (one per address)
    expect(mockExecute).toHaveBeenCalledTimes(6);
  });

  it("fails loud when the credential map cannot be read", async () => {
    mockLoadEntries.mockRejectedValueOnce(new Error("key-service 503"));
    await expect(syncMailboxes({ method: "POST", path: "/t" })).rejects.toThrow("key-service 503");
    expect(mockInsertValues).not.toHaveBeenCalled();
  });
});
