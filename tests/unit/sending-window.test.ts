import { describe, it, expect } from "vitest";
import {
  DEFAULT_LEAD_TIMEZONE,
  SEND_WINDOW_END_HOUR,
  SEND_WINDOW_START_HOUR,
  bookedDayKey,
  chainBookedDays,
  isLocalSendingDay,
  isWithinLocalSendWindow,
  localParts,
  nextLocalSendInstant,
  resolveLeadTimezone,
  sequenceFootprintDays,
} from "../../src/lib/sending-window";

// 2026-08-31 is a Monday, so every date below is easy to reason about:
//   Mon 08-31, Tue 09-01, Wed 09-02, Thu 09-03, Fri 09-04, Sat 09-05, Sun 09-06.
const CHICAGO = "America/Chicago"; // UTC-5 in summer (CDT)
const AUCKLAND = "Pacific/Auckland"; // UTC+12 in September (NZST)

describe("sending-window", () => {
  describe("resolveLeadTimezone", () => {
    it("degrades an absent zone to the same default the Instantly schedule uses", () => {
      expect(resolveLeadTimezone(null)).toBe(DEFAULT_LEAD_TIMEZONE);
      expect(resolveLeadTimezone(undefined)).toBe(DEFAULT_LEAD_TIMEZONE);
      expect(resolveLeadTimezone("   ")).toBe(DEFAULT_LEAD_TIMEZONE);
    });

    it("canonicalizes a legacy spelling to its primary IANA name", () => {
      expect(resolveLeadTimezone("US/Pacific")).toBe("America/Los_Angeles");
      expect(resolveLeadTimezone("Asia/Calcutta")).toBe("Asia/Kolkata");
    });
  });

  describe("localParts", () => {
    it("reads the prospect's wall clock, not ours", () => {
      // 2026-08-31T14:00Z is 09:00 in Chicago (CDT, UTC-5).
      const p = localParts(new Date("2026-08-31T14:00:00Z"), CHICAGO);
      expect(p).toMatchObject({ year: 2026, month: 8, day: 31, hour: 9 });
    });

    it("crosses the date line — a UTC Sunday evening is a local Monday morning", () => {
      // 2026-08-30T20:00Z (Sunday) is 08:00 Monday in Auckland (UTC+12).
      const p = localParts(new Date("2026-08-30T20:00:00Z"), AUCKLAND);
      expect(p).toMatchObject({ year: 2026, month: 8, day: 31, hour: 8 });
    });
  });

  describe("isLocalSendingDay", () => {
    it("accepts Mon-Fri and refuses the weekend", () => {
      expect(isLocalSendingDay(2026, 8, 31)).toBe(true); // Monday
      expect(isLocalSendingDay(2026, 9, 4)).toBe(true); // Friday
      expect(isLocalSendingDay(2026, 9, 5)).toBe(false); // Saturday
      expect(isLocalSendingDay(2026, 9, 6)).toBe(false); // Sunday
    });
  });

  describe("nextLocalSendInstant", () => {
    it("returns the instant UNCHANGED inside the local window", () => {
      // 09:00 Monday in Chicago — squarely inside 08:00-17:00.
      const asOf = new Date("2026-08-31T14:00:00Z");
      expect(nextLocalSendInstant(asOf, CHICAGO).toISOString()).toBe(asOf.toISOString());
    });

    it("waits for the window to OPEN when the local day has not started", () => {
      // 06:00 Monday in Chicago → 08:00 the same local day (13:00Z).
      const got = nextLocalSendInstant(new Date("2026-08-31T11:00:00Z"), CHICAGO);
      expect(localParts(got, CHICAGO)).toMatchObject({ day: 31, hour: SEND_WINDOW_START_HOUR });
      expect(got.toISOString()).toBe("2026-08-31T13:00:00.000Z");
    });

    it("rolls to the NEXT local weekday once the window has closed", () => {
      // 18:00 Monday in Chicago (23:00Z) → 08:00 Tuesday local.
      const got = nextLocalSendInstant(new Date("2026-08-31T23:00:00Z"), CHICAGO);
      expect(localParts(got, CHICAGO)).toMatchObject({
        month: 9,
        day: 1,
        hour: SEND_WINDOW_START_HOUR,
      });
    });

    it("skips a local weekend to Monday", () => {
      // Saturday 10:00 Chicago → 08:00 Monday local.
      const got = nextLocalSendInstant(new Date("2026-09-05T15:00:00Z"), CHICAGO);
      expect(localParts(got, CHICAGO)).toMatchObject({
        month: 9,
        day: 7,
        hour: SEND_WINDOW_START_HOUR,
      });
    });

    it("is idempotent — snapping an already-open instant returns it", () => {
      const once = nextLocalSendInstant(new Date("2026-09-05T15:00:00Z"), CHICAGO);
      const twice = nextLocalSendInstant(once, CHICAGO);
      expect(twice.toISOString()).toBe(once.toISOString());
    });
  });

  describe("bookedDayKey — the lead's local day is not the mailbox's UTC day", () => {
    it("books a New Zealand lead's local MONDAY on the mailbox's SUNDAY", () => {
      // Sunday 2026-08-30 06:00Z is Sunday 18:00 in Auckland: the local weekend,
      // so the next window is Monday 08:00 local = Sunday 20:00 UTC.
      const key = bookedDayKey(new Date("2026-08-30T06:00:00Z"), AUCKLAND);
      expect(key).toBe("2026-08-30");
      // ...and the instant really is inside the prospect's Monday morning.
      const instant = nextLocalSendInstant(new Date("2026-08-30T06:00:00Z"), AUCKLAND);
      expect(localParts(instant, AUCKLAND)).toMatchObject({ day: 31, hour: 8 });
    });

    it("books a Chicago lead on the same UTC day it is assigned", () => {
      expect(bookedDayKey(new Date("2026-08-31T14:00:00Z"), CHICAGO)).toBe("2026-08-31");
    });
  });

  describe("sequenceFootprintDays", () => {
    it("returns one key per step for the fleet's D0 / D+3 / D+10 cadence", () => {
      // Monday 09:00 Chicago. +3 → Thursday, +7 → the following Thursday.
      const keys = sequenceFootprintDays(new Date("2026-08-31T14:00:00Z"), CHICAGO, [3, 7]);
      expect(keys).toEqual(["2026-08-31", "2026-09-03", "2026-09-10"]);
    });

    it("snaps a hop landing on a local weekend forward, and CHAINS off the snapped day", () => {
      // Thursday 09-03 + 3 = Sunday 09-06 → snapped to Monday 09-07.
      // The next +7 then chains off Monday (→ 09-14), NOT off the nominal Sunday.
      const keys = sequenceFootprintDays(new Date("2026-09-03T14:00:00Z"), CHICAGO, [3, 7]);
      expect(keys).toEqual(["2026-09-03", "2026-09-07", "2026-09-14"]);
    });

    it("uses the fleet default when the lead carries no timezone", () => {
      const withNull = sequenceFootprintDays(new Date("2026-08-31T14:00:00Z"), null, [3, 7]);
      const withDefault = sequenceFootprintDays(
        new Date("2026-08-31T14:00:00Z"),
        DEFAULT_LEAD_TIMEZONE,
        [3, 7],
      );
      expect(withNull).toEqual(withDefault);
    });

    it("a single-step sequence books exactly one day", () => {
      expect(sequenceFootprintDays(new Date("2026-08-31T14:00:00Z"), CHICAGO, [])).toEqual([
        "2026-08-31",
      ]);
    });

    it("treats a zero / negative / non-finite gap as same-day rather than dropping the step", () => {
      const keys = sequenceFootprintDays(new Date("2026-08-31T14:00:00Z"), CHICAGO, [
        0,
        -1,
        Number.NaN,
      ]);
      expect(keys).toEqual(["2026-08-31", "2026-08-31", "2026-08-31", "2026-08-31"]);
    });
  });

  describe("chainBookedDays", () => {
    it("anchors on the first open window, not on the raw instant", () => {
      // Saturday anchor → everything starts from Monday.
      const keys = chainBookedDays(new Date("2026-09-05T15:00:00Z"), CHICAGO, [3]);
      expect(keys).toEqual(["2026-09-07", "2026-09-10"]);
    });
  });

  describe("isWithinLocalSendWindow", () => {
    it("is true only inside the prospect's local business hours on a local weekday", () => {
      expect(isWithinLocalSendWindow(new Date("2026-08-31T14:00:00Z"), CHICAGO)).toBe(true); // 09:00 Mon
      expect(isWithinLocalSendWindow(new Date("2026-08-31T11:00:00Z"), CHICAGO)).toBe(false); // 06:00 Mon
      expect(isWithinLocalSendWindow(new Date("2026-08-31T23:00:00Z"), CHICAGO)).toBe(false); // 18:00 Mon
      expect(isWithinLocalSendWindow(new Date("2026-09-05T15:00:00Z"), CHICAGO)).toBe(false); // Sat
    });

    it("opens for an Auckland lead while it is still Sunday for us", () => {
      // Sunday 20:30Z = Monday 08:30 in Auckland.
      expect(isWithinLocalSendWindow(new Date("2026-08-30T20:30:00Z"), AUCKLAND)).toBe(true);
      expect(isWithinLocalSendWindow(new Date("2026-08-30T20:30:00Z"), CHICAGO)).toBe(false);
    });

    it("closes exactly at the end hour", () => {
      // 17:00 Chicago = 22:00Z — the window is half-open, so this is CLOSED.
      expect(SEND_WINDOW_END_HOUR).toBe(17);
      expect(isWithinLocalSendWindow(new Date("2026-08-31T22:00:00Z"), CHICAGO)).toBe(false);
      expect(isWithinLocalSendWindow(new Date("2026-08-31T21:59:00Z"), CHICAGO)).toBe(true);
    });
  });
});
