import { describe, it, expect } from "vitest";
import {
  SENDING_WEEKDAYS,
  isSendingDay,
} from "../../src/lib/sending-calendar";

describe("sending-calendar", () => {
  describe("SENDING_WEEKDAYS", () => {
    it("mirrors the campaign schedule shipped to Instantly: Mon-Sat, Sunday off", () => {
      // instantly-client.ts createAndActivateCampaign sends
      // { "0": false, "1": true ... "6": true }
      expect(SENDING_WEEKDAYS).toEqual([1, 2, 3, 4, 5, 6]);
      expect(isSendingDay(new Date("2026-08-15T10:00:00.000Z"))).toBe(true); // Sat
      expect(isSendingDay(new Date("2026-08-16T10:00:00.000Z"))).toBe(false); // Sun
      expect(isSendingDay(new Date("2026-08-17T10:00:00.000Z"))).toBe(true); // Mon
      expect(isSendingDay(new Date("2026-08-21T10:00:00.000Z"))).toBe(true); // Fri
    });
  });

});
