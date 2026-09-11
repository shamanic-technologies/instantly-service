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

/** Every non-human verdict names its reason. A human hit carries none. */
export const SCANNER_REASONS = {
  headRequest: "head_request",
  scannerUserAgent: "scanner_user_agent",
  missingUserAgent: "missing_user_agent",
  pairedUnsubscribeFetch: "paired_unsubscribe_fetch",
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
 * ⚠️ NOT in this list: the dominant fixed `Mozilla/5.0 (Windows NT 10.0; Win64;
 * x64) … Chrome/14x` string (213 click hits). It is also a perfectly plausible
 * human UA, so on its own it is a weak signal — those hits are caught, when they
 * are machines, by the paired opt-out fetch instead. Adding it here would
 * discard every real Windows/Chrome click in the fleet.
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

export function isScannerUserAgent(userAgent: string): boolean {
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
}

/** The full verdict, once the pairing window has closed. Total — never null. */
export function classifyClickHit(hit: ClickHitEvidence): ClickClassification {
  const immediate = classifyImmediateSignals(hit);
  if (immediate) return immediate;

  if (hit.hasPairedUnsubscribeFetch) {
    return { verdict: "scanner", reason: SCANNER_REASONS.pairedUnsubscribeFetch };
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
