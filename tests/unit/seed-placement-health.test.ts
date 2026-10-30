import { describe, it, expect } from "vitest";

// GANDI-OPEN-1030: no seed test graded a mailbox at Google after 2026-10-25 and
// nobody saw it. The check is red when the weekly test missed OR when no Google
// receiver can be read (the next test could not grade the gate anyway).

import {
  SEED_TEST_OVERDUE_AGE_DAYS,
  judgeSeedPlacementHealth,
} from "../../src/lib/seed-placement/health";

const ASOF = new Date("2026-10-30T12:00:00.000Z");
const daysBefore = (n: number) => new Date(ASOF.getTime() - n * 86400000);
const PRIMEFORGE_402 = {
  vendor: "primeforge",
  error: '402 - {"message":"active subscription required"}',
  since: "2026-10-29T15:02:11.000Z",
};

describe("judgeSeedPlacementHealth", () => {
  it("is green inside the weekly cadence with Google receivers readable", () => {
    const h = judgeSeedPlacementHealth({ lastGateTestAt: daysBefore(5), googleReceivers: 10, vendorRefusals: [], asOf: ASOF });
    expect(h.ok).toBe(true);
    expect(h.ageDays).toBeCloseTo(5);
  });

  it("is red past the cadence: the weekly test stopped", () => {
    const h = judgeSeedPlacementHealth({
      lastGateTestAt: daysBefore(SEED_TEST_OVERDUE_AGE_DAYS + 0.5),
      googleReceivers: 10,
      vendorRefusals: [],
      asOf: ASOF,
    });
    expect(h.ok).toBe(false);
    expect(h.reason).toMatch(/weekly test stopped/);
  });

  it("is red at the cadence limit only once it is passed (control)", () => {
    const h = judgeSeedPlacementHealth({
      lastGateTestAt: daysBefore(SEED_TEST_OVERDUE_AGE_DAYS - 0.5),
      googleReceivers: 10,
      vendorRefusals: [],
      asOf: ASOF,
    });
    expect(h.ok).toBe(true);
  });

  it("is red when no Google receiver is readable, even on time, naming the refusing vendor", () => {
    const h = judgeSeedPlacementHealth({ lastGateTestAt: daysBefore(5), googleReceivers: 0, vendorRefusals: [PRIMEFORGE_402], asOf: ASOF });
    expect(h.ok).toBe(false);
    expect(h.reason).toMatch(/No Google mailbox can receive/);
    expect(h.reason).toMatch(/primeforge refuses us since 2026-10-29 15:02 UTC/);
  });

  it("is red when no test ever graded the gate", () => {
    const h = judgeSeedPlacementHealth({ lastGateTestAt: null, googleReceivers: 10, vendorRefusals: [], asOf: ASOF });
    expect(h.ok).toBe(false);
    expect(h.ageDays).toBeNull();
  });
});
