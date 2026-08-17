import { describe, it, expect } from "vitest";
import {
  SENDING_WEEKDAYS,
  isSendingDay,
  nextSendingDay,
} from "../../src/lib/sending-calendar";

describe("sending-calendar", () => {
  describe("SENDING_WEEKDAYS", () => {
    it("mirrors the campaign schedule shipped to Instantly: Mon-Fri, no weekend", () => {
      // instantly-client.ts createAndActivateCampaign sends
      // { "0": false, "1": true ... "5": true, "6": false }
      expect(SENDING_WEEKDAYS).toEqual([1, 2, 3, 4, 5]);
      expect(isSendingDay(new Date("2026-08-15T10:00:00.000Z"))).toBe(false); // Sat
      expect(isSendingDay(new Date("2026-08-16T10:00:00.000Z"))).toBe(false); // Sun
      expect(isSendingDay(new Date("2026-08-17T10:00:00.000Z"))).toBe(true); // Mon
      expect(isSendingDay(new Date("2026-08-21T10:00:00.000Z"))).toBe(true); // Fri
    });
  });

  describe("nextSendingDay", () => {
    it("returns the instant unchanged on a weekday (zero weekday behaviour change)", () => {
      for (const iso of [
        "2026-08-17T08:26:09.123Z", // Mon
        "2026-08-18T23:59:59.999Z", // Tue
        "2026-08-19T00:00:00.000Z", // Wed
        "2026-08-20T12:00:00.000Z", // Thu
        "2026-08-21T17:45:00.000Z", // Fri
      ]) {
        const d = new Date(iso);
        expect(nextSendingDay(d).toISOString()).toBe(iso);
      }
    });

    it("snaps a Saturday to UTC midnight of the following Monday", () => {
      expect(nextSendingDay(new Date("2026-08-15T13:37:00.000Z")).toISOString()).toBe(
        "2026-08-17T00:00:00.000Z",
      );
    });

    it("snaps a Sunday to UTC midnight of the following Monday", () => {
      expect(nextSendingDay(new Date("2026-08-16T23:10:00.000Z")).toISOString()).toBe(
        "2026-08-17T00:00:00.000Z",
      );
    });

    it("crosses a month boundary correctly", () => {
      // Sat 2026-08-29 -> Mon 2026-08-31 ; Sun 2026-08-30 -> Mon 2026-08-31
      expect(nextSendingDay(new Date("2026-08-29T09:00:00.000Z")).toISOString()).toBe(
        "2026-08-31T00:00:00.000Z",
      );
      // Sat 2026-10-31 -> Mon 2026-11-02
      expect(nextSendingDay(new Date("2026-10-31T09:00:00.000Z")).toISOString()).toBe(
        "2026-11-02T00:00:00.000Z",
      );
    });

    it("is idempotent — snapping an already-snapped day is a no-op", () => {
      const once = nextSendingDay(new Date("2026-08-15T13:37:00.000Z"));
      expect(nextSendingDay(once).toISOString()).toBe(once.toISOString());
    });
  });
});
