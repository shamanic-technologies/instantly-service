/**
 * Human or machine? — the one question the `/c/` redirect has to answer before a
 * hit becomes a website visit.
 *
 * Corporate mail security fetches every URL in an inbound message before the
 * human sees it. On the self-send transport that made click tracking mostly a
 * scanner census: 131 of 337 smtp leads on one brand "clicked" (39%, against
 * 95/2049 on the Instantly transport), each of those clicks paused the lead's
 * sequence through `stop-on-click`, and the customer paying for website visits
 * was shown machine prefetches as visits.
 *
 * ⚠️ THERE IS NO TIME THRESHOLD, DELIBERATELY. The measured scanner delay after
 * send clusters at 20-60s — exactly where a human opening the mail on a phone
 * lands. Any cutoff wide enough to catch the scanners discards real clicks, so
 * the rules below key on WHAT the client did, never on WHEN.
 *
 * ⚠️ THE DECISIVE SIGNAL IS THE PAIRED OPT-OUT FETCH, and it is why promotion is
 * deferred rather than done in the route. A scanner fetches EVERY link in the
 * mail, so the body link and the unsubscribe link are fetched seconds apart; a
 * human never touches the opt-out while following a content link. But the
 * opt-out fetch can arrive AFTER the click, so the verdict is not available at
 * request time. Promote-then-retract was rejected: `stop-on-click` would already
 * have paused the sequence, and that pause is the harm being fixed.
 */

import { sql, type SQL } from "drizzle-orm";

/** Every non-human verdict names its reason. A human hit carries none. */
export const SCANNER_REASONS = {
  headRequest: "head_request",
  scannerUserAgent: "scanner_user_agent",
  missingUserAgent: "missing_user_agent",
  pairedUnsubscribeFetch: "paired_unsubscribe_fetch",
  scannerNetwork: "scanner_network",
} as const;

export type ScannerReason = (typeof SCANNER_REASONS)[keyof typeof SCANNER_REASONS];

export type ClickVerdict = "human" | "scanner";

export interface ClickClassification {
  verdict: ClickVerdict;
  /** Null on `human` — absence of every machine signal, not evidence of a person. */
  reason: ScannerReason | null;
}

/**
 * User-agent substrings that no browser following a link ever sends.
 *
 * `Trident/` + `MSIE` is the Microsoft Safe Links fingerprint measured in prod
 * (`Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0 …)`,
 * 19 click hits and 21 opt-out hits). The rest are the ordinary HTTP clients and
 * link-preview bots that reach a tracking redirect.
 *
 * ⚠️ NOT in this list: a plain `Mozilla/5.0 (Windows NT 10.0; Win64; x64) …
 * Chrome/142.0.0.0` string. It is a perfectly ordinary human UA and blocking it
 * would discard every real Windows/Chrome click in the fleet. What separates the
 * machines wearing it is the VERSION SHAPE — see `isUnreducedChromeVersion`.
 */
const SCANNER_USER_AGENT_PATTERNS: RegExp[] = [
  /\bTrident\/\d/i,
  /\bMSIE\s/i,
  /Go-http-client/i,
  /HeadlessChrome/i,
  /python-requests|python-urllib|urllib/i,
  /\bcurl\/|\bWget\//i,
  /Java\/\d|okhttp|Apache-HttpClient|libwww-perl|Guzzle|axios\//i,
  /\bbot\b|crawler|spider|scanner/i,
  /Slackbot|Discordbot|TelegramBot|facebookexternalhit|Twitterbot|LinkedInBot|WhatsApp/i,
  /SkypeUriPreview|MSOffice|ms-office|Microsoft Office|Outlook-iOS|Barracuda|Proofpoint|Mimecast|Symantec|FireEye|Cisco|Forcepoint|Safe ?Links/i,
  /preview|prefetch|fetcher|monitoring|uptime/i,
];

/**
 * Chrome froze its user-agent version at **110**: every real browser since then
 * reports `Chrome/<major>.0.0.0`, minor/build/patch zeroed, whatever build it
 * actually is (the UA-reduction rollout, chromestatus 5704553745874944 — it also
 * covers Edge, Brave, Opera, Samsung Internet and Android WebView, which all
 * carry the same reduced `Chrome/` token). So a user-agent claiming Chrome 110
 * or later WITH a full build number did not come from a browser: it came from
 * something that pinned a plausible-looking string.
 *
 * ⚠️ THIS IS THE SIGNAL THAT SEPARATES THE FLEET, and it is why the classifier
 * catches what a UA blocklist could not. The four largest user-agents in prod
 * are `Chrome/142.0.7444.175` (60 hits / 60 distinct leads), `142.0.7444.163`
 * (40/39), `141.0.7390.0` (32/31) and `142.0.7444.162` (32/32) — one hit per
 * lead, spread over days, i.e. a scanner fleet working through a mail queue.
 * Without this rule 80 of one brand's 131 "clickers" survive; with it, 18 do —
 * and what remains is the shape of real traffic (Mac, Android, Firefox, Linux,
 * mixed versions).
 *
 * The `>= 110` floor is what keeps genuinely old browsers out of it: a real
 * `Chrome/84.0.4147.89` predates the freeze and is left alone.
 */
const CHROME_UA_REDUCTION_MAJOR = 110;

export function isUnreducedChromeVersion(userAgent: string): boolean {
  const match = /Chrome\/(\d+)\.0\.(\d+)\.\d+/.exec(userAgent);
  if (!match) return false;
  const major = Number(match[1]);
  const build = match[2];
  return Number.isFinite(major) && major >= CHROME_UA_REDUCTION_MAJOR && build !== "0";
}

export function isScannerUserAgent(userAgent: string): boolean {
  if (isUnreducedChromeVersion(userAgent)) return true;
  return SCANNER_USER_AGENT_PATTERNS.some((pattern) => pattern.test(userAgent));
}

export interface ClickHitSignals {
  /** The HTTP method the redirect was fetched with. */
  method: string | null;
  userAgent: string | null;
}

/**
 * The signals readable AT REQUEST TIME.
 *
 * Returns a scanner verdict when the request itself gives one away, or null when
 * the hit is still undecided and has to wait for the pairing window. Null is
 * never "human" — the route must not promote on it.
 */
export function classifyImmediateSignals(hit: ClickHitSignals): ClickClassification | null {
  // A browser following a link issues GET. Anything else is something inspecting
  // the URL rather than visiting it.
  if (hit.method && hit.method.toUpperCase() !== "GET") {
    return { verdict: "scanner", reason: SCANNER_REASONS.headRequest };
  }

  const userAgent = hit.userAgent?.trim() ?? "";
  // No browser omits its user-agent. An empty one is a client that did not care
  // to look like one.
  if (userAgent === "") {
    return { verdict: "scanner", reason: SCANNER_REASONS.missingUserAgent };
  }

  if (isScannerUserAgent(userAgent)) {
    return { verdict: "scanner", reason: SCANNER_REASONS.scannerUserAgent };
  }

  return null;
}

export interface ClickHitEvidence extends ClickHitSignals {
  /**
   * Did the SAME (campaign, lead) fetch the opt-out link within the pairing
   * window, either side of this click?
   */
  hasPairedUnsubscribeFetch: boolean;
  /**
   * Did another click from the same /24 network get ruled a scanner within
   * `SCANNER_NETWORK_WINDOW_DAYS` either side of this one? See
   * `scannerNetworkEvidenceSql`.
   */
  sharesScannerNetwork: boolean;
}

/** The full verdict, once the pairing window has closed. Total — never null. */
export function classifyClickHit(hit: ClickHitEvidence): ClickClassification {
  const immediate = classifyImmediateSignals(hit);
  if (immediate) return immediate;

  if (hit.hasPairedUnsubscribeFetch) {
    return { verdict: "scanner", reason: SCANNER_REASONS.pairedUnsubscribeFetch };
  }

  if (hit.sharesScannerNetwork) {
    return { verdict: "scanner", reason: SCANNER_REASONS.scannerNetwork };
  }

  return { verdict: "human", reason: null };
}

/**
 * How far either side of a click an opt-out fetch still counts as the same
 * scanner pass. Measured: 135 of 309 click hits have one inside 30s.
 */
export const PAIRED_UNSUBSCRIBE_WINDOW_SECONDS = 60;

/**
 * How long a click waits before it is decided.
 *
 * Strictly longer than the pairing window, so an opt-out fetch arriving at the
 * far edge of it is already recorded when the verdict is taken. The cost is
 * ~2 minutes of latency on a real click reaching silver — which delays
 * `stop-on-click` by the same, and nothing else.
 */
export const CLICK_DECISION_HOLD_SECONDS = 120;

/**
 * How far either side of a click a scanner verdict on the same /24 still marks
 * that network as a scanner's.
 *
 * ⚠️ WHY A NETWORK RULE AT ALL. Microsoft Defender's link detonation rotates its
 * user-agent between the unreduced `Windows … Chrome/142.0.7444.163` shape that
 * `isUnreducedChromeVersion` catches and a perfectly ordinary reduced
 * `Macintosh … Chrome/143.0.0.0`, from the SAME Azure /24s (`48.209.223.x`,
 * `72.145.83.x`, `57.155.170.x`). Measured 2026-10-02 on Olive: all 7 "human"
 * clicks (6 at Flow Traders, 1 at QCP) came from a /24 that had produced
 * scanner-ruled hits minutes earlier — `bhemelaar` 12 s after `aadit` on the
 * very same IP. Fleet-wide, 268 of 383 "human" clicks sat on such a network.
 *
 * It is a POSITIVE fingerprint, not a time threshold: the evidence is a machine
 * verdict on that network, not how fast the click came. A real person clicks
 * from their ISP or office egress, never from the cloud /24 a mail scanner is
 * detonating links from.
 */
export const SCANNER_NETWORK_WINDOW_DAYS = 7;

/**
 * The /24 of an IPv4 client address (also the IPv4-mapped `::ffff:a.b.c.d` form),
 * as SQL. NULL for anything else, and for private / loopback ranges: every hit
 * recorded before the real client IP was captured carries Caddy's
 * `::ffff:172.18.0.27`, and treating that as one network would brand the whole
 * pre-fix era a scanner.
 */
function networkKeySql(column: SQL): SQL {
  return sql`CASE
    WHEN ${column} ~ '(^|:)(10|127)\\.\\d+\\.\\d+\\.\\d+$'
      OR ${column} ~ '(^|:)172\\.(1[6-9]|2\\d|3[01])\\.\\d+\\.\\d+$'
      OR ${column} ~ '(^|:)192\\.168\\.\\d+\\.\\d+$'
    THEN NULL
    ELSE substring(${column} from '(?:^|:)(\\d+\\.\\d+\\.\\d+)\\.\\d+$')
  END`;
}

/**
 * `EXISTS (…)`: another click from the same /24, within the window, already
 * ruled a scanner. `hitAlias` is the alias of the `tracking_hits_raw` row being
 * decided. Shared by the live promotion and the backfill so both apply ONE rule.
 */
export function scannerNetworkEvidenceSql(hitAlias: string): SQL {
  const h = sql.raw(hitAlias);
  const window = sql.raw(`interval '${SCANNER_NETWORK_WINDOW_DAYS} days'`);
  return sql`EXISTS (
    SELECT 1
    FROM tracking_hits_raw s
    WHERE s.kind = 'click'
      AND s.classification = 'scanner'
      AND s.id <> ${h}.id
      AND s.received_at BETWEEN ${h}.received_at - ${window} AND ${h}.received_at + ${window}
      AND ${networkKeySql(sql`s.client_ip`)} = ${networkKeySql(sql.raw(`${hitAlias}.client_ip`))}
  )`;
}
