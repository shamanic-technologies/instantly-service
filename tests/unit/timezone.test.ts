import { describe, it, expect } from "vitest";
import {
  canonicalIanaTimezone,
  LEGACY_IANA_TIMEZONE_ALIASES,
  unrecognizedTimezoneFromError,
} from "../../src/lib/timezone";
import { isValidIanaTimezone } from "../../src/schemas";

/**
 * Every one of these returned 500 from GET /orgs/stats?groupBy=day in prod on
 * 2026-08-08 while the modern spelling of the SAME zone returned 200: the DB
 * host lacks Debian's `tzdata-legacy` package, so `AT TIME ZONE '<alias>'`
 * throws per row. Pairs are (legacy spelling, modern twin).
 */
const INCIDENT_PAIRS: Array<[string, string]> = [
  ["Asia/Saigon", "Asia/Ho_Chi_Minh"],
  ["Asia/Calcutta", "Asia/Kolkata"],
  ["Europe/Kiev", "Europe/Kyiv"],
  ["America/Buenos_Aires", "America/Argentina/Buenos_Aires"],
  ["Asia/Rangoon", "Asia/Yangon"],
  ["Asia/Katmandu", "Asia/Kathmandu"],
  ["US/Pacific", "America/Los_Angeles"],
  ["Japan", "Asia/Tokyo"],
  ["Poland", "Europe/Warsaw"],
  ["Israel", "Asia/Jerusalem"],
  ["Egypt", "Africa/Cairo"],
  ["Turkey", "Europe/Istanbul"],
  ["Cuba", "America/Havana"],
  ["Iran", "Asia/Tehran"],
  ["Singapore", "Asia/Singapore"],
  ["PRC", "Asia/Shanghai"],
  ["NZ", "Pacific/Auckland"],
  ["Eire", "Europe/Dublin"],
  ["GB", "Europe/London"],
  ["Hongkong", "Asia/Hong_Kong"],
  ["Asia/Macao", "Asia/Macau"],
  ["Asia/Dacca", "Asia/Dhaka"],
  ["America/Godthab", "America/Nuuk"],
  ["Asia/Ulan_Bator", "Asia/Ulaanbaatar"],
];

describe("canonicalIanaTimezone", () => {
  it.each(INCIDENT_PAIRS)(
    "resolves the legacy spelling %s to its primary name %s",
    (legacy, primary) => {
      expect(canonicalIanaTimezone(legacy)).toBe(primary);
    },
  );

  it("is idempotent — a primary name resolves to itself", () => {
    for (const [, primary] of INCIDENT_PAIRS) {
      expect(canonicalIanaTimezone(primary)).toBe(primary);
      expect(canonicalIanaTimezone(canonicalIanaTimezone(primary))).toBe(primary);
    }
  });

  it("leaves the zones that already worked untouched or on a primary name", () => {
    // These returned 200 before the fix; canonicalization must not break them.
    for (const tz of ["UTC", "GMT", "Etc/GMT+5", "Pacific/Chatham", "Asia/Chongqing", "Europe/Nicosia"]) {
      const resolved = canonicalIanaTimezone(tz);
      expect(isValidIanaTimezone(resolved)).toBe(true);
    }
    expect(canonicalIanaTimezone("UTC")).toBe("UTC");
  });

  it("resolves aliases ICU hands back unchanged AND aliases ICU maps onto another alias", () => {
    // ECMA-402 deliberately does not canonicalize recently-renamed zones, so
    // the explicit table — not Intl — is what covers this one.
    expect(new Intl.DateTimeFormat("en-US", { timeZone: "Asia/Saigon" }).resolvedOptions().timeZone)
      .toBe("Asia/Saigon");
    expect(canonicalIanaTimezone("Asia/Saigon")).toBe("Asia/Ho_Chi_Minh");
    // Second hop: ICU maps Zulu → UTC, which the table also accepts.
    expect(canonicalIanaTimezone("Zulu")).toBe("UTC");
  });

  it("every alias target is itself a valid, non-aliased zone", () => {
    for (const [alias, target] of Object.entries(LEGACY_IANA_TIMEZONE_ALIASES)) {
      expect(isValidIanaTimezone(target), `${alias} -> ${target}`).toBe(true);
      expect(LEGACY_IANA_TIMEZONE_ALIASES[target], `${target} must not alias again`).toBeUndefined();
    }
  });

  it("returns the input unchanged for a zone ICU rejects (schema owns the 400)", () => {
    expect(canonicalIanaTimezone("Not/AZone")).toBe("Not/AZone");
  });
});

describe("unrecognizedTimezoneFromError", () => {
  it("extracts the zone name from the Postgres error", () => {
    expect(
      unrecognizedTimezoneFromError(new Error('time zone "Asia/Saigon" not recognized')),
    ).toBe("Asia/Saigon");
  });

  it("looks through error.cause (pg errors arrive wrapped)", () => {
    const wrapped = new Error("query failed", {
      cause: new Error('time zone "Europe/Kiev" not recognized'),
    });
    expect(unrecognizedTimezoneFromError(wrapped)).toBe("Europe/Kiev");
  });

  it("returns null for any other failure so it still surfaces as a 500", () => {
    expect(unrecognizedTimezoneFromError(new Error("timeout exceeded"))).toBeNull();
    expect(unrecognizedTimezoneFromError(undefined)).toBeNull();
  });
});
