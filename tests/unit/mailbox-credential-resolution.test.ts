import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

// `vi.mock` factories are hoisted above every top-level const/class, so the
// error class the factory returns must come from `vi.hoisted` — a plain class
// declaration here is not initialised yet when the factory runs.
const { KeyServiceError } = vi.hoisted(() => {
  class KeyServiceError extends Error {
    constructor(public readonly statusCode: number, message: string) {
      super(message);
      this.name = "KeyServiceError";
    }
  }
  return { KeyServiceError };
});

const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformKey: (...a: unknown[]) => mockResolvePlatformKey(...a),
  KeyServiceError,
}));

const mockListPrimeforgeRawMailboxes = vi.fn();
vi.mock("../../src/lib/providers/primeforge-client", () => ({
  listPrimeforgeRawMailboxes: (...a: unknown[]) => mockListPrimeforgeRawMailboxes(...a),
}));

import {
  MANUAL_CREDENTIALS_PROVIDER,
  MailboxCredentialError,
  resolveMailboxCredential,
  loginFor,
  loadMailboxLogins,
  loadCredentialedMailboxes,
  loadMailboxLoginEntries,
  currentVendorRefusals,
  resetVendorRefusals,
} from "../../src/lib/self-send/mailbox-credentials";

const CALLER = { method: "POST", path: "/test" } as const;

const GANDI_ALIAS = {
  address: "kevinl@marketingagency.life",
  appPassword: "gandi-secret",
  smtpHost: "mail.gandi.net",
  imapHost: "mail.gandi.net",
  authUser: "kevin@marketingagency.life",
};

function manualKey(entries: unknown[]): string {
  return JSON.stringify(entries);
}

beforeEach(() => {
  vi.resetAllMocks();
  resetVendorRefusals();
  mockListPrimeforgeRawMailboxes.mockResolvedValue([
    { address: "kevin@boostdistribute.com", appPassword: "abcd efgh ijkl mnop" },
  ]);
});

describe("resolveMailboxCredential — one resolution for sending and measuring", () => {
  it("resolves a manually-listed mailbox, carrying its own hosts and alias login", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER ? manualKey([GANDI_ALIAS]) : "pf-key",
    );

    const credential = await resolveMailboxCredential(
      "kevinl@marketingagency.life",
      CALLER,
    );

    // A Gandi mailbox is NOT on Google, so the hosts come from the entry.
    expect(credential.smtpHost).toBe("mail.gandi.net");
    expect(credential.appPassword).toBe("gandi-secret");
    // The alias sends as itself but authenticates as the real mailbox.
    expect(loginFor(credential)).toBe("kevin@marketingagency.life");
  });

  it("falls through to Primeforge for a mailbox the manual list does not carry", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER ? manualKey([GANDI_ALIAS]) : "pf-key",
    );

    const credential = await resolveMailboxCredential(
      "kevin@boostdistribute.com",
      CALLER,
    );

    expect(credential.appPassword).toBe("abcdefghijklmnop");
    expect(credential.smtpHost).toBe("smtp.gmail.com");
    expect(loginFor(credential)).toBe("kevin@boostdistribute.com");
  });

  it("prefers the manual entry when both sources carry the mailbox", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER
        ? manualKey([
            {
              address: "kevin@boostdistribute.com",
              appPassword: "operator-override",
              smtpHost: "smtp.gmail.com",
              imapHost: "imap.gmail.com",
            },
          ])
        : "pf-key",
    );

    const credential = await resolveMailboxCredential(
      "kevin@boostdistribute.com",
      CALLER,
    );

    // An operator can correct a vendor-reported password without the vendor.
    expect(credential.appPassword).toBe("operator-override");
    expect(mockListPrimeforgeRawMailboxes).not.toHaveBeenCalled();
  });

  it("still resolves via Primeforge when no manual key is configured (404)", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) => {
      if (provider === MANUAL_CREDENTIALS_PROVIDER) {
        throw new KeyServiceError(404, "not found");
      }
      return "pf-key";
    });

    const credential = await resolveMailboxCredential(
      "kevin@boostdistribute.com",
      CALLER,
    );

    expect(credential.appPassword).toBe("abcdefghijklmnop");
  });

  it("throws when neither source knows the mailbox — a send must abort, not switch sender", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER ? manualKey([]) : "pf-key",
    );

    await expect(
      resolveMailboxCredential("stranger@nowhere.test", CALLER),
    ).rejects.toBeInstanceOf(MailboxCredentialError);
  });

  it("propagates a non-404 key-service failure instead of reading it as 'none configured'", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) => {
      if (provider === MANUAL_CREDENTIALS_PROVIDER) {
        throw new KeyServiceError(500, "key-service down");
      }
      return "pf-key";
    });

    // Could-not-read is not the same fact as there-is-none: degrading here would
    // send a Gandi mailbox down the Primeforge path, which cannot know it.
    await expect(
      resolveMailboxCredential("kevinl@marketingagency.life", CALLER),
    ).rejects.toBeInstanceOf(KeyServiceError);
    expect(mockListPrimeforgeRawMailboxes).not.toHaveBeenCalled();
  });

  it("rejects a malformed manual payload rather than silently ignoring it", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER ? "not json" : "pf-key",
    );

    await expect(
      resolveMailboxCredential("kevin@boostdistribute.com", CALLER),
    ).rejects.toThrow(/valid JSON/);
  });
});

// ─── The alias→mailbox map, which is what a day's quota is spent against ─────
//
// 154 sending accounts sit on 44 real Gandi mailboxes. Budgeting per ADDRESS
// hands one mailbox five times its quota, and the relay answers
// `450 4.7.1 Too many mail per day for sasl <user>` — per SASL USER. So the
// dispatcher groups capacity by this map's VALUE, never by its key.

describe("loadMailboxLogins", () => {
  const SIBLING_ALIAS = {
    address: "klourd@marketingagency.life",
    appPassword: "gandi-secret",
    smtpHost: "mail.gandi.net",
    imapHost: "mail.gandi.net",
    authUser: "kevin@marketingagency.life",
  };

  beforeEach(() => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER
        ? manualKey([GANDI_ALIAS, SIBLING_ALIAS])
        : "pf-key",
    );
  });

  it("collapses several aliases onto the ONE mailbox they authenticate as", async () => {
    const logins = await loadMailboxLogins(CALLER);

    expect(logins.get("kevinl@marketingagency.life")).toBe("kevin@marketingagency.life");
    expect(logins.get("klourd@marketingagency.life")).toBe("kevin@marketingagency.life");
    // Two addresses, one mailbox — the whole point.
    expect(new Set(logins.values()).size).toBe(2);
  });

  it("maps a Primeforge mailbox to itself — the address IS the login", async () => {
    const logins = await loadMailboxLogins(CALLER);
    expect(logins.get("kevin@boostdistribute.com")).toBe("kevin@boostdistribute.com");
  });

  it("keeps the manual entry when both sources carry the same address", async () => {
    mockListPrimeforgeRawMailboxes.mockResolvedValue([
      { address: "kevinl@marketingagency.life", appPassword: "pf-secret" },
    ]);

    const logins = await loadMailboxLogins(CALLER);
    // Manual wins everywhere else in this module, so it wins here too — the
    // alias must not collapse back onto itself.
    expect(logins.get("kevinl@marketingagency.life")).toBe("kevin@marketingagency.life");
  });

  it("is the same set loadCredentialedMailboxes returns — ONE loader, two shapes", async () => {
    const logins = await loadMailboxLogins(CALLER);
    const addresses = await loadCredentialedMailboxes(CALLER);
    expect(addresses).toEqual(new Set(logins.keys()));
  });
});

// SEND-PRIMEFORGE-402-1030 — prod 2026-10-29 ~15:00 UTC: Primeforge answered
// 402 "active subscription required", this loader threw, and every send of every
// client failed, Gandi mailboxes included. A vendor refusing us takes out ITS
// mailboxes only, loudly.
describe("a mailbox vendor refusing us (SEND-PRIMEFORGE-402-1030)", () => {
  const PRIMEFORGE_402 = new Error(
    'Primeforge GET /mailboxes?limit=100&offset=0 failed: 402 - {"message":"active subscription required"}',
  );

  beforeEach(() => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER ? manualKey([GANDI_ALIAS]) : "pf-key",
    );
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  it("keeps every manual (Gandi/Mailforge) mailbox and drops only Primeforge's", async () => {
    mockListPrimeforgeRawMailboxes.mockRejectedValue(PRIMEFORGE_402);

    const logins = await loadMailboxLogins(CALLER);

    expect([...logins.keys()]).toEqual(["kevinl@marketingagency.life"]);
    expect(logins.has("kevin@boostdistribute.com")).toBe(false);
  });

  it("stays loud: the refusal is logged and served verbatim with when it began", async () => {
    mockListPrimeforgeRawMailboxes.mockRejectedValue(PRIMEFORGE_402);

    await loadMailboxLogins(CALLER);
    const first = currentVendorRefusals();
    await loadMailboxLogins(CALLER);
    const second = currentVendorRefusals();

    expect(console.error).toHaveBeenCalledWith(expect.stringContaining("Primeforge REFUSED us"));
    expect(second).toHaveLength(1);
    expect(second[0].vendor).toBe("primeforge");
    expect(second[0].error).toContain('402 - {"message":"active subscription required"}');
    // The streak keeps its start.
    expect(second[0].since).toBe(first[0].since);
  });

  it("a key-service failure for the Primeforge key is the same: that vendor only", async () => {
    mockResolvePlatformKey.mockImplementation(async (provider: string) => {
      if (provider === MANUAL_CREDENTIALS_PROVIDER) return manualKey([GANDI_ALIAS]);
      throw new KeyServiceError(503, "key-service unavailable");
    });

    const logins = await loadMailboxLogins(CALLER);
    expect([...logins.keys()]).toEqual(["kevinl@marketingagency.life"]);
    expect(currentVendorRefusals()).toHaveLength(1);
  });

  it("clears the refusal the moment the vendor answers again", async () => {
    mockListPrimeforgeRawMailboxes.mockRejectedValueOnce(PRIMEFORGE_402);
    await loadMailboxLogins(CALLER);
    expect(currentVendorRefusals()).toHaveLength(1);

    mockListPrimeforgeRawMailboxes.mockResolvedValue([
      { address: "kevin@boostdistribute.com", appPassword: "abcd efgh ijkl mnop" },
    ]);
    const logins = await loadMailboxLogins(CALLER);
    expect(logins.has("kevin@boostdistribute.com")).toBe(true);
    expect(currentVendorRefusals()).toEqual([]);
  });

  it("every vendor healthy: nothing recorded, both vendors in the map", async () => {
    const logins = await loadMailboxLogins(CALLER);
    expect(logins.has("kevinl@marketingagency.life")).toBe(true);
    expect(logins.has("kevin@boostdistribute.com")).toBe(true);
    expect(currentVendorRefusals()).toEqual([]);
  });

  it("the provenance writer (mailbox sync) still fails loud: it must not rewrite Primeforge rows as credential-less", async () => {
    mockListPrimeforgeRawMailboxes.mockRejectedValue(PRIMEFORGE_402);
    await expect(
      loadMailboxLoginEntries(CALLER, { onVendorRefusal: "throw" }),
    ).rejects.toThrow("402");
    expect(currentVendorRefusals()).toHaveLength(1);
  });

  it("the manual layer failing is NOT a vendor refusal: it still fails the whole read", async () => {
    mockResolvePlatformKey.mockRejectedValue(new KeyServiceError(500, "boom"));
    await expect(loadMailboxLogins(CALLER)).rejects.toThrow("boom");
  });
});
