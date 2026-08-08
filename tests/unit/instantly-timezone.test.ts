import { describe, it, expect } from "vitest";
import {
  DEFAULT_SCHEDULE_TIMEZONE,
  INSTANTLY_SCHEDULE_TIMEZONES,
  resolveInstantlyTimezone,
} from "../../src/lib/instantly-timezone.js";

/** Fixed clock so every signature-derived expectation below is deterministic. */
const AS_OF = new Date("2026-08-08T00:00:00Z");

const accepted = new Set(INSTANTLY_SCHEDULE_TIMEZONES);

function offsetMinutes(timeZone: string, at: Date): number {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(at);
  const read = (type: string): number => Number(parts.find((part) => part.type === type)?.value);
  const asUtc = Date.UTC(
    read("year"),
    read("month") - 1,
    read("day"),
    read("hour") % 24,
    read("minute"),
    read("second")
  );
  return Math.round((asUtc - at.getTime()) / 60_000);
}

describe("INSTANTLY_SCHEDULE_TIMEZONES", () => {
  // Pins the vendor enum itself. An edit that introduces a value Instantly does
  // not accept re-creates the 2026-08-07 outage (every send 400s before any
  // email exists), and the resolver cannot catch it — it trusts this list.
  it("is the vendor's 102-value enum, verbatim and in spec order", () => {
    expect(INSTANTLY_SCHEDULE_TIMEZONES).toHaveLength(102);
    expect(INSTANTLY_SCHEDULE_TIMEZONES[0]).toBe("Etc/GMT+12");
    expect(INSTANTLY_SCHEDULE_TIMEZONES.at(-1)).toBe("Pacific/Apia");
    expect(new Set(INSTANTLY_SCHEDULE_TIMEZONES).size).toBe(102);
    expect(accepted.has(DEFAULT_SCHEDULE_TIMEZONE)).toBe(true);

    // Spot-check the members the resolver's own expectations depend on, plus the
    // two zones prod proved accepted before the fix.
    for (const zone of [
      "America/Chicago",
      "America/Detroit",
      "America/Dawson",
      "America/Boise",
      "Europe/Isle_of_Man",
      "Europe/Belgrade",
      "Europe/Bucharest",
      "Asia/Hong_Kong",
      "Asia/Jerusalem",
      "Asia/Kolkata",
    ]) {
      expect(accepted.has(zone)).toBe(true);
    }

    // Every entry must be a zone ICU recognises, or the resolver's signature
    // matching silently degrades to the default for the whole fleet.
    for (const zone of INSTANTLY_SCHEDULE_TIMEZONES) {
      expect(() => new Intl.DateTimeFormat("en-US", { timeZone: zone })).not.toThrow();
    }
  });

  it("does NOT contain the zones prod actually 400'd on", () => {
    // Guards the premise of the fix: these are the real lead zones, and they are
    // outside the enum, which is why they must be resolved rather than forwarded.
    for (const zone of [
      "America/New_York",
      "America/Los_Angeles",
      "America/Denver",
      "Europe/London",
      "Europe/Berlin",
      "America/Phoenix",
      "Asia/Shanghai",
    ]) {
      expect(accepted.has(zone)).toBe(false);
    }
  });
});

describe("resolveInstantlyTimezone", () => {
  // The exact zones prod carried on 2026-08-07/08, with their observed volumes.
  const productionZones: Array<[string, string]> = [
    ["America/New_York", "America/Detroit"], // 88 leads
    ["America/Los_Angeles", "America/Dawson"], // 50 leads — no enum member shares
    ["America/Denver", "America/Boise"], // 9 leads      Pacific's DST signature
    ["Europe/London", "Europe/Isle_of_Man"], // 7 leads
    ["Europe/Berlin", "Europe/Belgrade"], // 3 leads
    ["America/Phoenix", "America/Dawson"], // 3 leads
    ["Europe/Brussels", "Europe/Belgrade"],
    ["Europe/Amsterdam", "Europe/Belgrade"],
    ["Europe/Budapest", "Europe/Belgrade"],
    ["Europe/Oslo", "Europe/Belgrade"],
    ["Europe/Rome", "Europe/Belgrade"],
    ["Europe/Sofia", "Europe/Bucharest"],
    ["Europe/Stockholm", "Europe/Belgrade"],
    ["Europe/Zurich", "Europe/Belgrade"],
    ["Asia/Shanghai", "Asia/Hong_Kong"],
  ];

  it.each(productionZones)("maps the production zone %s onto %s", (input, expected) => {
    expect(resolveInstantlyTimezone(input, AS_OF)).toBe(expected);
  });

  it("resolves every production zone to an accepted value at the SAME current UTC offset", () => {
    // The scheduling intent — 08:00-17:00 in the prospect's local time — only
    // survives if the substitute shares the offset. Checked at `AS_OF` itself,
    // which is what today's sends actually use.
    for (const [input] of productionZones) {
      const resolved = resolveInstantlyTimezone(input, AS_OF);
      expect(accepted.has(resolved)).toBe(true);
      expect(offsetMinutes(resolved, AS_OF)).toBe(offsetMinutes(input, AS_OF));
    }
  });

  it("passes through a zone already in the enum", () => {
    // Both were seen creating campaigns successfully in prod before the fix.
    expect(resolveInstantlyTimezone("Asia/Jerusalem", AS_OF)).toBe("Asia/Jerusalem");
    expect(resolveInstantlyTimezone("Asia/Kolkata", AS_OF)).toBe("Asia/Kolkata");
    expect(resolveInstantlyTimezone("America/Chicago", AS_OF)).toBe("America/Chicago");
  });

  it("keeps an enum member that is ITSELF a legacy alias, rather than canonicalizing it off the enum", () => {
    // `America/Godthab` → `America/Nuuk` and `Asia/Rangoon` → `Asia/Yangon` are
    // both in the alias table, and neither primary name is in the enum. Exact
    // membership therefore has to be checked BEFORE canonicalization.
    expect(resolveInstantlyTimezone("America/Godthab", AS_OF)).toBe("America/Godthab");
    expect(resolveInstantlyTimezone("Asia/Rangoon", AS_OF)).toBe("Asia/Rangoon");
  });

  it("absorbs legacy aliases of zones outside the enum", () => {
    expect(resolveInstantlyTimezone("Asia/Calcutta", AS_OF)).toBe("Asia/Kolkata");
    expect(resolveInstantlyTimezone("US/Eastern", AS_OF)).toBe("America/Detroit");
    expect(resolveInstantlyTimezone("US/Pacific", AS_OF)).toBe("America/Dawson");
  });

  it("falls back to US Central for absent, empty and malformed input", () => {
    expect(resolveInstantlyTimezone(undefined, AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
    expect(resolveInstantlyTimezone(null, AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
    expect(resolveInstantlyTimezone("", AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
    expect(resolveInstantlyTimezone("   ", AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
    expect(resolveInstantlyTimezone("Not/AZone", AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
    expect(resolveInstantlyTimezone("UTC+3", AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
    expect(resolveInstantlyTimezone("13", AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
    expect(resolveInstantlyTimezone("<script>", AS_OF)).toBe(DEFAULT_SCHEDULE_TIMEZONE);
  });

  it("trims surrounding whitespace", () => {
    expect(resolveInstantlyTimezone("  America/New_York  ", AS_OF)).toBe("America/Detroit");
  });

  it("returns an accepted value for EVERY zone the runtime knows about", () => {
    // The acceptance criterion "no input can make the call fail on the timezone
    // field", exercised across the whole IANA database rather than a sample.
    const everyZone: string[] =
      (Intl as unknown as { supportedValuesOf?: (key: string) => string[] }).supportedValuesOf?.(
        "timeZone"
      ) ?? [];
    expect(everyZone.length).toBeGreaterThan(100);

    for (const zone of everyZone) {
      const resolved = resolveInstantlyTimezone(zone, AS_OF);
      expect(accepted.has(resolved), `${zone} resolved to un-accepted ${resolved}`).toBe(true);
    }
  });

  it("is stable and idempotent", () => {
    for (const [input] of productionZones) {
      const once = resolveInstantlyTimezone(input, AS_OF);
      expect(resolveInstantlyTimezone(input, AS_OF)).toBe(once);
      expect(resolveInstantlyTimezone(once, AS_OF)).toBe(once);
    }
  });

  it("holds across the year, not only at the reference clock", () => {
    for (const asOf of [
      new Date("2026-01-15T00:00:00Z"),
      new Date("2026-11-30T00:00:00Z"),
      new Date("2027-06-01T00:00:00Z"),
    ]) {
      for (const [input] of productionZones) {
        expect(accepted.has(resolveInstantlyTimezone(input, asOf))).toBe(true);
      }
    }
  });
});
