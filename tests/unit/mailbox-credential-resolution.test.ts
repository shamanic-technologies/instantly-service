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
