import { describe, it, expect, vi, beforeEach } from "vitest";

// SEND-PRIMEFORGE-402-1030 — the send pool end to end through the REAL credential
// loader and the REAL capability filter: with Primeforge answering 402, a send
// for any org still finds its Gandi/Mailforge mailboxes.

const { KeyServiceError } = vi.hoisted(() => {
  class KeyServiceError extends Error {
    constructor(public readonly statusCode: number, message: string) {
      super(message);
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

// No cache between cases.
vi.mock("../../src/lib/stats-cache", () => ({
  getOrSetCachedStats: async (_key: string, loader: () => Promise<unknown>) => loader(),
}));

import { keepSelfSendCapable } from "../../src/lib/self-send/capability";
import {
  MANUAL_CREDENTIALS_PROVIDER,
  resetVendorRefusals,
} from "../../src/lib/self-send/mailbox-credentials";

const CALLER = { method: "POST", path: "/orgs/send" } as const;

const GANDI = {
  address: "kevinl@marketingagency.life",
  appPassword: "gandi-secret",
  smtpHost: "mail.gandi.net",
  imapHost: "mail.gandi.net",
  authUser: "kevin@marketingagency.life",
};
const MAILFORGE = {
  address: "kevin@growthagency.forum",
  appPassword: "mf-secret",
  smtpHost: "smtp.mailforge.ai",
  imapHost: "imap.mailforge.ai",
};

// The send pool in fill order: Gandi (rank 0), Mailforge (1), Primeforge (2).
const POOL = [
  { email: "kevinl@marketingagency.life" },
  { email: "kevin@growthagency.forum" },
  { email: "amy@saviolabsco.com" },
];

beforeEach(() => {
  vi.clearAllMocks();
  resetVendorRefusals();
  vi.spyOn(console, "error").mockImplementation(() => {});
  mockResolvePlatformKey.mockImplementation(async (provider: string) =>
    provider === MANUAL_CREDENTIALS_PROVIDER ? JSON.stringify([GANDI, MAILFORGE]) : "pf-key",
  );
});

describe("send pool when a mailbox vendor refuses us (SEND-PRIMEFORGE-402-1030)", () => {
  it("Primeforge 402: the send goes out from Gandi/Mailforge, only Primeforge's mailboxes leave the pool", async () => {
    mockListPrimeforgeRawMailboxes.mockRejectedValue(
      new Error(
        'Primeforge GET /mailboxes?limit=100&offset=0 failed: 402 - {"message":"active subscription required"}',
      ),
    );

    const pool = await keepSelfSendCapable(POOL, CALLER);

    expect(pool.map((a) => a.email)).toEqual([
      "kevinl@marketingagency.life",
      "kevin@growthagency.forum",
    ]);
  });

  it("every vendor healthy: the pool is unchanged", async () => {
    mockListPrimeforgeRawMailboxes.mockResolvedValue([
      { address: "amy@saviolabsco.com", appPassword: "abcd efgh ijkl mnop" },
    ]);

    const pool = await keepSelfSendCapable(POOL, CALLER);

    expect(pool.map((a) => a.email)).toEqual(POOL.map((a) => a.email));
  });
});
