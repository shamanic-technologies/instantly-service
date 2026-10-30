import { describe, it, expect } from "vitest";
import { keepStillValid } from "../../src/lib/self-send/primeforge-last-listing";

// SEND-POOL-VENDOR-402-1030 — the stored listing cannot refresh the one fact the
// vendor told us ahead of time: when a mailbox ends.
describe("keepStillValid", () => {
  const asOf = new Date("2026-10-30T18:00:00Z");

  it("drops a mailbox whose validUntil has passed, keeps the rest", () => {
    const kept = keepStillValid(
      [
        { address: "a@x.com", appPassword: "p" },
        { address: "b@x.com", appPassword: "p", validUntil: "2026-10-29T00:00:00Z" } as never,
        { address: "c@x.com", appPassword: "p", validUntil: "2026-11-13T23:13:55.837Z" } as never,
      ],
      asOf,
    );
    expect(kept.map((m) => m.address)).toEqual(["a@x.com", "c@x.com"]);
  });
});
