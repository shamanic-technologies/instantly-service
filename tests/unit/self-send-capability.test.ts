import { describe, it, expect, vi, beforeEach } from "vitest";

const mockLoadCredentialedMailboxes = vi.fn();
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  loadCredentialedMailboxes: (...a: unknown[]) => mockLoadCredentialedMailboxes(...a),
}));

// The real cache would make the second test read the first test's answer.
const mockGetOrSetCachedStats = vi.fn(
  async (_key: string, loader: () => Promise<unknown>) => loader(),
);
vi.mock("../../src/lib/stats-cache", () => ({
  getOrSetCachedStats: (...a: unknown[]) =>
    (mockGetOrSetCachedStats as unknown as (...x: unknown[]) => unknown)(...a),
}));

import {
  isSelfSendCapable,
  resolveTransportForNewSequence,
  CREDENTIALED_MAILBOXES_CACHE_KEY,
} from "../../src/lib/self-send/capability";

const CALLER = { method: "POST", path: "/orgs/send" } as const;

beforeEach(() => {
  vi.clearAllMocks();
  mockGetOrSetCachedStats.mockImplementation(
    async (_key: string, loader: () => Promise<unknown>) => loader(),
  );
});

describe("resolveTransportForNewSequence — one fact decides the pipe", () => {
  it("sends over smtp when we hold a credential for the mailbox", async () => {
    mockLoadCredentialedMailboxes.mockResolvedValue(new Set(["amy@saviolabsco.com"]));

    await expect(
      resolveTransportForNewSequence({ email: "amy@saviolabsco.com" }, CALLER),
    ).resolves.toBe("smtp");
  });

  // The Instantly DFY pool: their Workspace, so no app password can exist. This
  // is the ONLY reason a mailbox stays on Instantly now.
  it("leaves an un-authenticable mailbox on Instantly", async () => {
    mockLoadCredentialedMailboxes.mockResolvedValue(new Set(["amy@saviolabsco.com"]));

    await expect(
      resolveTransportForNewSequence({ email: "persona@axionmilestone.com" }, CALLER),
    ).resolves.toBe("instantly");
  });

  // The account's stored `send_transport` used to take precedence here as a
  // manual pin. Its column default is 'instantly', so "pinned to Instantly" and
  // "never touched" were the same value and the pin could never be read — while
  // the A/B split marked campaigns `smtp` on accounts left at the default, which
  // the dispatcher then refused to serve. 1,486 sequences froze silently.
  it("ignores the account's stored policy entirely", async () => {
    mockLoadCredentialedMailboxes.mockResolvedValue(new Set(["amy@saviolabsco.com"]));

    // Whatever a caller passes alongside the address changes nothing.
    const withStalePolicy = { email: "amy@saviolabsco.com", sendTransport: "instantly" };
    await expect(
      resolveTransportForNewSequence(withStalePolicy as { email: string }, CALLER),
    ).resolves.toBe("smtp");
  });

  it("matches case-insensitively and ignores surrounding space", async () => {
    mockLoadCredentialedMailboxes.mockResolvedValue(new Set(["amy@saviolabsco.com"]));

    await expect(
      resolveTransportForNewSequence({ email: "  AMY@Saviolabsco.com " }, CALLER),
    ).resolves.toBe("smtp");
  });

  // A silent empty set would read as "no mailbox is ours" and route the whole
  // fleet back to a vendor we are cancelling. `loadCredentialedMailboxes` fails
  // loud on anything but a key-service 404, and that must propagate.
  it("propagates a credential-read failure rather than defaulting", async () => {
    mockLoadCredentialedMailboxes.mockRejectedValue(new Error("key-service 503"));

    await expect(
      resolveTransportForNewSequence({ email: "amy@saviolabsco.com" }, CALLER),
    ).rejects.toThrow("key-service 503");
  });
});

describe("isSelfSendCapable", () => {
  it("reads through the shared 60s stats cache, not once per send", async () => {
    mockLoadCredentialedMailboxes.mockResolvedValue(new Set(["amy@saviolabsco.com"]));

    await isSelfSendCapable("amy@saviolabsco.com", CALLER);

    expect(mockGetOrSetCachedStats).toHaveBeenCalledWith(
      CREDENTIALED_MAILBOXES_CACHE_KEY,
      expect.any(Function),
    );
  });
});
