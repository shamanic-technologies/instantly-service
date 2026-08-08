/**
 * Instantly campaign-schedule timezone resolution.
 *
 * Instantly's `POST /campaigns` rejects any `campaign_schedule.schedules[].timezone`
 * outside a CLOSED enum of 102 values (a Windows-style list carrying roughly one
 * representative zone per UTC offset) with:
 *
 *   400 body/campaign_schedule/schedules/0/timezone must be equal to one of the
 *       allowed values
 *
 * Most real IANA zones are NOT in that list. From 2026-08-07 an upstream
 * enrichment change started threading a real per-prospect IANA zone through on
 * nearly every send, so `America/New_York`, `America/Los_Angeles`, `Europe/London`
 * and friends began 400-ing the campaign creation outright: the run failed before
 * any email existed at Instantly, after the lead had already been served and its
 * body generated and paid for.
 *
 * The timezone is a scheduling PREFERENCE, not a precondition for contacting
 * someone, so it must degrade — never block. This module maps any input onto a
 * member of the enum, in graded order:
 *
 *   1. the input is already an enum member (covers the legacy spellings the enum
 *      itself carries, e.g. `America/Godthab`, `Asia/Rangoon`, `Africa/Blantyre`);
 *   2. its canonical primary IANA name is an enum member (absorbs `Asia/Calcutta`,
 *      `US/Eastern`, `Japan`, … via the existing alias table);
 *   3. the enum member whose UTC-offset SIGNATURE across the year is closest,
 *      preferring the same region prefix, then the earliest enum position. An
 *      identical signature (distance 0) means identical wall-clock scheduling all
 *      year; a non-zero distance means the DST rules differ, which for an
 *      08:00-17:00 window is at worst an hour of drift for part of the year —
 *      acceptable, and far better than not sending;
 *   4. the US-Central default when the input is absent, empty, or not a zone ICU
 *      recognises.
 *
 * Step 3 is what covers US Pacific: no enum member shares Pacific's DST signature
 * (`America/Dawson` stopped observing DST in 2020), so a CLDR windowsZones-style
 * mapping alone leaves our second-largest zone unresolved. Signature distance
 * lands it on `America/Dawson` — exact in summer, one hour late in winter.
 *
 * Nothing here throws: every path ends on an enum member.
 */
import { canonicalIanaTimezone } from "./timezone.js";

/**
 * The vendor's enum, verbatim, in spec order. Pinned by a unit test so an edit
 * cannot silently introduce a value Instantly rejects.
 *
 * Source: https://developer.instantly.ai/api-reference/campaign/create-campaign
 * (`campaign_schedule.schedules.items.properties.timezone.enum`, read from the
 * published OpenAPI document on 2026-08-08).
 */
export const INSTANTLY_SCHEDULE_TIMEZONES: readonly string[] = Object.freeze([
  "Etc/GMT+12",
  "Etc/GMT+11",
  "Etc/GMT+10",
  "America/Anchorage",
  "America/Dawson",
  "America/Creston",
  "America/Chihuahua",
  "America/Boise",
  "America/Belize",
  "America/Chicago",
  "America/Bahia_Banderas",
  "America/Regina",
  "America/Bogota",
  "America/Detroit",
  "America/Indiana/Marengo",
  "America/Caracas",
  "America/Asuncion",
  "America/Glace_Bay",
  "America/Campo_Grande",
  "America/Anguilla",
  "America/Santiago",
  "America/St_Johns",
  "America/Sao_Paulo",
  "America/Argentina/La_Rioja",
  "America/Araguaina",
  "America/Godthab",
  "America/Montevideo",
  "America/Bahia",
  "America/Noronha",
  "America/Scoresbysund",
  "Atlantic/Cape_Verde",
  "Africa/Casablanca",
  "America/Danmarkshavn",
  "Europe/Isle_of_Man",
  "Atlantic/Canary",
  "Africa/Abidjan",
  "Arctic/Longyearbyen",
  "Europe/Belgrade",
  "Africa/Ceuta",
  "Europe/Sarajevo",
  "Africa/Algiers",
  "Africa/Windhoek",
  "Asia/Nicosia",
  "Asia/Beirut",
  "Africa/Cairo",
  "Asia/Damascus",
  "Europe/Bucharest",
  "Africa/Blantyre",
  "Europe/Helsinki",
  "Europe/Istanbul",
  "Asia/Jerusalem",
  "Africa/Tripoli",
  "Asia/Amman",
  "Asia/Baghdad",
  "Europe/Kaliningrad",
  "Asia/Aden",
  "Africa/Addis_Ababa",
  "Europe/Kirov",
  "Europe/Astrakhan",
  "Asia/Tehran",
  "Asia/Dubai",
  "Asia/Baku",
  "Indian/Mahe",
  "Asia/Tbilisi",
  "Asia/Yerevan",
  "Asia/Kabul",
  "Antarctica/Mawson",
  "Asia/Yekaterinburg",
  "Asia/Karachi",
  "Asia/Kolkata",
  "Asia/Colombo",
  "Asia/Kathmandu",
  "Antarctica/Vostok",
  "Asia/Dhaka",
  "Asia/Rangoon",
  "Antarctica/Davis",
  "Asia/Novokuznetsk",
  "Asia/Hong_Kong",
  "Asia/Krasnoyarsk",
  "Asia/Brunei",
  "Australia/Perth",
  "Asia/Taipei",
  "Asia/Choibalsan",
  "Asia/Irkutsk",
  "Asia/Dili",
  "Asia/Pyongyang",
  "Australia/Adelaide",
  "Australia/Darwin",
  "Australia/Brisbane",
  "Australia/Melbourne",
  "Antarctica/DumontDUrville",
  "Australia/Currie",
  "Asia/Chita",
  "Antarctica/Macquarie",
  "Asia/Sakhalin",
  "Pacific/Auckland",
  "Etc/GMT-12",
  "Pacific/Fiji",
  "Asia/Anadyr",
  "Asia/Kamchatka",
  "Etc/GMT-13",
  "Pacific/Apia",
]);

const ENUM_INDEX: ReadonlyMap<string, number> = new Map(
  INSTANTLY_SCHEDULE_TIMEZONES.map((zone, index) => [zone, index] as const)
);

/**
 * US Central — the zone this service scheduled every campaign in before
 * per-prospect timezones existed, and the value proven accepted fleet-wide.
 */
export const DEFAULT_SCHEDULE_TIMEZONE = "America/Chicago";

/**
 * Sample days used to fingerprint a zone's UTC offset across the year. Four
 * points a quarter apart separate northern from southern DST and pin
 * fixed-offset zones, without needing tzdata rule internals.
 */
const SAMPLE_MONTHS = [0, 3, 6, 9] as const;

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
  // `hour12: false` renders midnight as 24 on some ICU versions.
  const hour = read("hour") % 24;
  const asUtc = Date.UTC(
    read("year"),
    read("month") - 1,
    read("day"),
    hour,
    read("minute"),
    read("second")
  );
  return Math.round((asUtc - at.getTime()) / 60_000);
}

function signature(timeZone: string, year: number): number[] {
  return SAMPLE_MONTHS.map((month) =>
    offsetMinutes(timeZone, new Date(Date.UTC(year, month, 15, 12)))
  );
}

/** Enum signatures are stable within a year; recomputing 102 of them per send is waste. */
const signatureCache = new Map<number, ReadonlyArray<{ zone: string; offsets: number[] }>>();

function enumSignatures(year: number): ReadonlyArray<{ zone: string; offsets: number[] }> {
  const cached = signatureCache.get(year);
  if (cached) return cached;
  const computed = INSTANTLY_SCHEDULE_TIMEZONES.map((zone) => ({
    zone,
    offsets: signature(zone, year),
  }));
  signatureCache.set(year, computed);
  return computed;
}

function region(timeZone: string): string {
  const slash = timeZone.indexOf("/");
  return slash === -1 ? timeZone : timeZone.slice(0, slash);
}

function isKnownZone(timeZone: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone });
    return true;
  } catch {
    return false;
  }
}

/**
 * Map any timezone input onto a value Instantly's campaign-schedule enum accepts.
 *
 * Total: absent, empty, malformed and unmappable inputs all resolve to
 * {@link DEFAULT_SCHEDULE_TIMEZONE} rather than throwing or reaching the vendor.
 *
 * @param input caller-supplied zone (the prospect's IANA zone, typically)
 * @param asOf  clock used to pick the sampling year; injectable for tests
 */
export function resolveInstantlyTimezone(input?: string | null, asOf: Date = new Date()): string {
  if (typeof input !== "string") return DEFAULT_SCHEDULE_TIMEZONE;
  const trimmed = input.trim();
  if (trimmed.length === 0) return DEFAULT_SCHEDULE_TIMEZONE;

  // 1. Already accepted — including the legacy spellings the enum itself
  //    carries, which canonicalization would move OFF the enum.
  if (ENUM_INDEX.has(trimmed)) return trimmed;

  if (!isKnownZone(trimmed)) return DEFAULT_SCHEDULE_TIMEZONE;

  // 2. Primary IANA name of a legacy alias (Asia/Calcutta, US/Eastern, Japan…).
  const canonical = canonicalIanaTimezone(trimmed);
  if (ENUM_INDEX.has(canonical)) return canonical;

  // 3. Closest offset signature; same region wins ties, then enum order.
  const year = asOf.getUTCFullYear();
  const target = signature(canonical, year);
  const targetRegion = region(canonical);

  let best = DEFAULT_SCHEDULE_TIMEZONE;
  let bestDistance = Number.POSITIVE_INFINITY;
  let bestSameRegion = false;

  for (const candidate of enumSignatures(year)) {
    let distance = 0;
    for (let index = 0; index < target.length; index += 1) {
      distance += Math.abs(target[index] - candidate.offsets[index]);
    }
    const sameRegion = region(candidate.zone) === targetRegion;

    if (distance < bestDistance || (distance === bestDistance && sameRegion && !bestSameRegion)) {
      best = candidate.zone;
      bestDistance = distance;
      bestSameRegion = sameRegion;
    }
  }

  return best;
}
