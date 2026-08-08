/**
 * IANA timezone canonicalization for the day-grouped stats aggregations.
 *
 * Why this exists: `groupBy=day` buckets rows with
 * `TO_CHAR(... AT TIME ZONE $tz, 'YYYY-MM-DD')`, so the zone NAME is resolved by
 * the Postgres server against the tzdata files installed on that host — not by
 * Node. Debian splits the tz database in two: the base `tzdata` package carries
 * the primary zone names, and **`tzdata-legacy` carries the backward-compat
 * links** (`Asia/Saigon`, `Asia/Calcutta`, `Europe/Kiev`, `US/Pacific`, `Japan`,
 * `PRC`, `GB`, `NZ`, …). A Postgres host built without `tzdata-legacy` therefore
 * throws `time zone "Asia/Saigon" not recognized` (SQLSTATE 22023) while
 * `Asia/Ho_Chi_Minh` — the very same zone — works.
 *
 * The failure is doubly confusing:
 *   - it is DATA-DEPENDENT. `AT TIME ZONE` is evaluated per row, so a filter
 *     matching zero rows never evaluates it and returns 200; the identical
 *     request over real rows 500s.
 *   - the zone is perfectly valid IANA and passes the request schema's
 *     `Intl`-based validator, so it is not a 400.
 *
 * Fix: resolve every accepted spelling to its PRIMARY IANA name before it ever
 * reaches SQL. Two aliases of one zone denote the same instant-to-local-date
 * mapping, so the buckets are identical by construction — this is a pure
 * normalization, never a semantic change.
 *
 * `Intl` alone is NOT sufficient. ECMA-402 deliberately does NOT canonicalize
 * zones IANA renamed recently, so Node returns `Asia/Saigon` → `Asia/Saigon`
 * while returning `Japan` → `Asia/Tokyo`. The explicit table below is therefore
 * the source of truth (and is ICU-version independent); `Intl` is only the
 * fallback for links the table has not enumerated.
 *
 * Real incident 2026-08-08: every user whose browser reported one of these
 * legacy zone names (Vietnam, Ukraine, India, Argentina, Myanmar, Japan, US/*)
 * saw a permanently blank brand Overview chart — instantly-service 500 →
 * email-gateway → features-service → dashboard 502.
 */

/**
 * tzdata `backward` links → their primary zone name. This is the set Debian
 * ships in `tzdata-legacy`, i.e. exactly the names a Postgres host can be
 * missing. Keys are the legacy spelling, values a primary name present in the
 * base `tzdata` package.
 */
export const LEGACY_IANA_TIMEZONE_ALIASES: Readonly<Record<string, string>> = Object.freeze({
  // Africa
  "Africa/Accra": "Africa/Abidjan",
  "Africa/Addis_Ababa": "Africa/Nairobi",
  "Africa/Asmara": "Africa/Nairobi",
  "Africa/Asmera": "Africa/Nairobi",
  "Africa/Bamako": "Africa/Abidjan",
  "Africa/Bangui": "Africa/Lagos",
  "Africa/Banjul": "Africa/Abidjan",
  "Africa/Blantyre": "Africa/Maputo",
  "Africa/Brazzaville": "Africa/Lagos",
  "Africa/Bujumbura": "Africa/Maputo",
  "Africa/Conakry": "Africa/Abidjan",
  "Africa/Dakar": "Africa/Abidjan",
  "Africa/Dar_es_Salaam": "Africa/Nairobi",
  "Africa/Djibouti": "Africa/Nairobi",
  "Africa/Douala": "Africa/Lagos",
  "Africa/Freetown": "Africa/Abidjan",
  "Africa/Gaborone": "Africa/Maputo",
  "Africa/Harare": "Africa/Maputo",
  "Africa/Kampala": "Africa/Nairobi",
  "Africa/Kigali": "Africa/Maputo",
  "Africa/Kinshasa": "Africa/Lagos",
  "Africa/Libreville": "Africa/Lagos",
  "Africa/Lome": "Africa/Abidjan",
  "Africa/Luanda": "Africa/Lagos",
  "Africa/Lubumbashi": "Africa/Maputo",
  "Africa/Lusaka": "Africa/Maputo",
  "Africa/Malabo": "Africa/Lagos",
  "Africa/Maseru": "Africa/Johannesburg",
  "Africa/Mbabane": "Africa/Johannesburg",
  "Africa/Mogadishu": "Africa/Nairobi",
  "Africa/Niamey": "Africa/Lagos",
  "Africa/Nouakchott": "Africa/Abidjan",
  "Africa/Ouagadougou": "Africa/Abidjan",
  "Africa/Porto-Novo": "Africa/Lagos",
  "Africa/Timbuktu": "Africa/Abidjan",
  // Americas
  "America/Anguilla": "America/Puerto_Rico",
  "America/Antigua": "America/Puerto_Rico",
  "America/Argentina/ComodRivadavia": "America/Argentina/Catamarca",
  "America/Aruba": "America/Puerto_Rico",
  "America/Atka": "America/Adak",
  "America/Blanc-Sablon": "America/Puerto_Rico",
  "America/Buenos_Aires": "America/Argentina/Buenos_Aires",
  "America/Catamarca": "America/Argentina/Catamarca",
  "America/Coral_Harbour": "America/Panama",
  "America/Cordoba": "America/Argentina/Cordoba",
  "America/Curacao": "America/Puerto_Rico",
  "America/Dominica": "America/Puerto_Rico",
  "America/Ensenada": "America/Tijuana",
  "America/Fort_Wayne": "America/Indiana/Indianapolis",
  "America/Godthab": "America/Nuuk",
  "America/Grenada": "America/Puerto_Rico",
  "America/Guadeloupe": "America/Puerto_Rico",
  "America/Indianapolis": "America/Indiana/Indianapolis",
  "America/Jujuy": "America/Argentina/Jujuy",
  "America/Knox_IN": "America/Indiana/Knox",
  "America/Kralendijk": "America/Puerto_Rico",
  "America/Louisville": "America/Kentucky/Louisville",
  "America/Lower_Princes": "America/Puerto_Rico",
  "America/Marigot": "America/Puerto_Rico",
  "America/Mendoza": "America/Argentina/Mendoza",
  "America/Montreal": "America/Toronto",
  "America/Montserrat": "America/Puerto_Rico",
  "America/Nipigon": "America/Toronto",
  "America/Pangnirtung": "America/Iqaluit",
  "America/Port_of_Spain": "America/Puerto_Rico",
  "America/Porto_Acre": "America/Rio_Branco",
  "America/Rainy_River": "America/Winnipeg",
  "America/Rosario": "America/Argentina/Cordoba",
  "America/Santa_Isabel": "America/Tijuana",
  "America/Shiprock": "America/Denver",
  "America/St_Barthelemy": "America/Puerto_Rico",
  "America/St_Kitts": "America/Puerto_Rico",
  "America/St_Lucia": "America/Puerto_Rico",
  "America/St_Thomas": "America/Puerto_Rico",
  "America/St_Vincent": "America/Puerto_Rico",
  "America/Thunder_Bay": "America/Toronto",
  "America/Tortola": "America/Puerto_Rico",
  "America/Virgin": "America/Puerto_Rico",
  "America/Yellowknife": "America/Edmonton",
  // Antarctica
  "Antarctica/South_Pole": "Pacific/Auckland",
  // Asia
  "Asia/Ashkhabad": "Asia/Ashgabat",
  "Asia/Calcutta": "Asia/Kolkata",
  "Asia/Chungking": "Asia/Shanghai",
  "Asia/Dacca": "Asia/Dhaka",
  "Asia/Harbin": "Asia/Shanghai",
  "Asia/Kashgar": "Asia/Urumqi",
  "Asia/Katmandu": "Asia/Kathmandu",
  "Asia/Macao": "Asia/Macau",
  "Asia/Rangoon": "Asia/Yangon",
  "Asia/Saigon": "Asia/Ho_Chi_Minh",
  "Asia/Tel_Aviv": "Asia/Jerusalem",
  "Asia/Thimbu": "Asia/Thimphu",
  "Asia/Ujung_Pandang": "Asia/Makassar",
  "Asia/Ulan_Bator": "Asia/Ulaanbaatar",
  // Atlantic
  "Atlantic/Faeroe": "Atlantic/Faroe",
  "Atlantic/Jan_Mayen": "Europe/Oslo",
  // Australia (single-word aliases)
  "Australia/ACT": "Australia/Sydney",
  "Australia/LHI": "Australia/Lord_Howe",
  "Australia/NSW": "Australia/Sydney",
  "Australia/North": "Australia/Darwin",
  "Australia/Queensland": "Australia/Brisbane",
  "Australia/South": "Australia/Adelaide",
  "Australia/Tasmania": "Australia/Hobart",
  "Australia/Victoria": "Australia/Melbourne",
  "Australia/West": "Australia/Perth",
  "Australia/Yancowinna": "Australia/Broken_Hill",
  // Country / region shorthands
  "Brazil/Acre": "America/Rio_Branco",
  "Brazil/DeNoronha": "America/Noronha",
  "Brazil/East": "America/Sao_Paulo",
  "Brazil/West": "America/Manaus",
  "Canada/Atlantic": "America/Halifax",
  "Canada/Central": "America/Winnipeg",
  "Canada/Eastern": "America/Toronto",
  "Canada/Mountain": "America/Edmonton",
  "Canada/Newfoundland": "America/St_Johns",
  "Canada/Pacific": "America/Vancouver",
  "Canada/Saskatchewan": "America/Regina",
  "Canada/Yukon": "America/Whitehorse",
  "Chile/Continental": "America/Santiago",
  "Chile/EasterIsland": "Pacific/Easter",
  Cuba: "America/Havana",
  Egypt: "Africa/Cairo",
  Eire: "Europe/Dublin",
  "Etc/UCT": "UTC",
  "Europe/Belfast": "Europe/London",
  "Europe/Kiev": "Europe/Kyiv",
  "Europe/Tiraspol": "Europe/Chisinau",
  "Europe/Uzhgorod": "Europe/Kyiv",
  "Europe/Zaporozhye": "Europe/Kyiv",
  GB: "Europe/London",
  "GB-Eire": "Europe/London",
  "GMT+0": "UTC",
  "GMT-0": "UTC",
  GMT0: "UTC",
  Greenwich: "UTC",
  Hongkong: "Asia/Hong_Kong",
  Iceland: "Atlantic/Reykjavik",
  Iran: "Asia/Tehran",
  Israel: "Asia/Jerusalem",
  Jamaica: "America/Jamaica",
  Japan: "Asia/Tokyo",
  Kwajalein: "Pacific/Kwajalein",
  Libya: "Africa/Tripoli",
  "Mexico/BajaNorte": "America/Tijuana",
  "Mexico/BajaSur": "America/Mazatlan",
  "Mexico/General": "America/Mexico_City",
  NZ: "Pacific/Auckland",
  "NZ-CHAT": "Pacific/Chatham",
  Navajo: "America/Denver",
  PRC: "Asia/Shanghai",
  "Pacific/Enderbury": "Pacific/Kanton",
  "Pacific/Johnston": "Pacific/Honolulu",
  "Pacific/Ponape": "Pacific/Pohnpei",
  "Pacific/Samoa": "Pacific/Pago_Pago",
  "Pacific/Truk": "Pacific/Chuuk",
  "Pacific/Yap": "Pacific/Chuuk",
  Poland: "Europe/Warsaw",
  Portugal: "Europe/Lisbon",
  ROC: "Asia/Taipei",
  ROK: "Asia/Seoul",
  Singapore: "Asia/Singapore",
  Turkey: "Europe/Istanbul",
  UCT: "UTC",
  "US/Alaska": "America/Anchorage",
  "US/Aleutian": "America/Adak",
  "US/Arizona": "America/Phoenix",
  "US/Central": "America/Chicago",
  "US/East-Indiana": "America/Indiana/Indianapolis",
  "US/Eastern": "America/New_York",
  "US/Hawaii": "Pacific/Honolulu",
  "US/Indiana-Starke": "America/Indiana/Knox",
  "US/Michigan": "America/Detroit",
  "US/Mountain": "America/Denver",
  "US/Pacific": "America/Los_Angeles",
  "US/Samoa": "Pacific/Pago_Pago",
  Universal: "UTC",
  "W-SU": "Europe/Moscow",
  Zulu: "UTC",
});

/**
 * Resolve an accepted IANA timezone spelling to its primary name, so the SQL
 * only ever names zones present in a base `tzdata` install.
 *
 * Order matters: the explicit table first (deterministic, ICU-version
 * independent), then `Intl` for links the table has not enumerated, then the
 * input unchanged. Idempotent — a primary name resolves to itself.
 */
export function canonicalIanaTimezone(timezone: string): string {
  const alias = LEGACY_IANA_TIMEZONE_ALIASES[timezone];
  if (alias) return alias;

  try {
    const resolved = new Intl.DateTimeFormat("en-US", { timeZone: timezone })
      .resolvedOptions().timeZone;
    // A second hop covers ICU resolving one legacy spelling onto another
    // (e.g. `EST5EDT` → `America/New_York`, `NZ` → `Pacific/Auckland`).
    return LEGACY_IANA_TIMEZONE_ALIASES[resolved] ?? resolved ?? timezone;
  } catch {
    // Unknown to ICU — the request schema already rejects those with a 400, so
    // this only fires for a caller reaching the helper directly. Fail loud
    // downstream (Postgres) rather than silently substituting UTC.
    return timezone;
  }
}

/**
 * True when a thrown DB error is Postgres refusing a timezone NAME
 * (`time zone "X" not recognized`, SQLSTATE 22023 invalid_parameter_value).
 * That is a bad-input condition, not a server fault, so callers surface it as a
 * 400 naming the parameter instead of an opaque 500.
 */
export function unrecognizedTimezoneFromError(error: unknown): string | null {
  const candidates: unknown[] = [error, (error as { cause?: unknown })?.cause];
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== "object") continue;
    const message = (candidate as { message?: unknown }).message;
    if (typeof message !== "string") continue;
    const match = /time zone "([^"]+)" not recognized/i.exec(message);
    if (match) return match[1];
  }
  return null;
}
