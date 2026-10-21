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
  ipSharedAcrossCompanies: "ip_shared_across_companies",
  manyNetworks: "many_networks",
  nonBrowserUserAgent: "non_browser_user_agent",
  clickBurst: "click_burst",
  datacenterNetwork: "datacenter_network",
  manyUserAgents: "many_user_agents",
  twoDevices: "two_devices",
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

/**
 * Every browser that follows a link opens its user-agent with a product token:
 * `Mozilla/5.0 (…)` for Chrome, Safari, Firefox, Edge and every in-app webview,
 * `Opera/` for pre-Blink Opera. A string that does not is a program announcing
 * itself as something else (`DNSClient`, measured 2026-10-03 from an AWS address
 * on an oral-surgery lead). Not a blocklist: it names what a browser IS, so a new
 * fetcher with an unseen name is caught the day it appears.
 */
export function isNonBrowserUserAgent(userAgent: string): boolean {
  return !/^(Mozilla|Opera)\//.test(userAgent.trim());
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

  if (isNonBrowserUserAgent(userAgent)) {
    return { verdict: "scanner", reason: SCANNER_REASONS.nonBrowserUserAgent };
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
  /** See `ipSharedAcrossCompaniesEvidenceSql`. */
  ipSharedAcrossCompanies: boolean;
  /** See `manyNetworksEvidenceSql`. */
  clickedFromManyNetworks: boolean;
  /** See `clickBurstEvidenceSql`. */
  inClickBurst: boolean;
  /** See `datacenterNetworkEvidenceSql`. */
  fromDatacenterNetwork: boolean;
  /** See `manyUserAgentsEvidenceSql`. */
  clickedWithManyUserAgents: boolean;
  /** See `twoDevicesEvidenceSql`. */
  clickedFromTwoDevices: boolean;
}

/** The full verdict, once the pairing window has closed. Total — never null. */
export function classifyClickHit(hit: ClickHitEvidence): ClickClassification {
  const immediate = classifyImmediateSignals(hit);
  if (immediate) return immediate;

  if (hit.hasPairedUnsubscribeFetch) {
    return { verdict: "scanner", reason: SCANNER_REASONS.pairedUnsubscribeFetch };
  }

  if (hit.inClickBurst) {
    return { verdict: "scanner", reason: SCANNER_REASONS.clickBurst };
  }

  if (hit.fromDatacenterNetwork) {
    return { verdict: "scanner", reason: SCANNER_REASONS.datacenterNetwork };
  }

  if (hit.sharesScannerNetwork) {
    return { verdict: "scanner", reason: SCANNER_REASONS.scannerNetwork };
  }

  if (hit.ipSharedAcrossCompanies) {
    return { verdict: "scanner", reason: SCANNER_REASONS.ipSharedAcrossCompanies };
  }

  if (hit.clickedFromManyNetworks) {
    return { verdict: "scanner", reason: SCANNER_REASONS.manyNetworks };
  }

  if (hit.clickedWithManyUserAgents) {
    return { verdict: "scanner", reason: SCANNER_REASONS.manyUserAgents };
  }

  if (hit.clickedFromTwoDevices) {
    return { verdict: "scanner", reason: SCANNER_REASONS.twoDevices };
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
 * The first `octets` octets of an IPv4 client address (also the IPv4-mapped
 * `::ffff:a.b.c.d` form) — the /24 for 3, the /16 for 2 — or, for 4, the address
 * itself. NULL for anything else, and for private / loopback ranges: every hit
 * recorded before the real client IP was captured carries Caddy's
 * `::ffff:172.18.0.27`, and treating that as one network would brand the whole
 * pre-fix era a scanner.
 */
function ipv4PrefixSql(column: SQL, octets: 2 | 3 | 4): SQL {
  const capture = sql.raw(
    `'(?:^|:)(${Array.from({ length: octets }, () => "\\d+").join("\\.")})${"\\.\\d+".repeat(4 - octets)}$'`,
  );
  return sql`CASE
    WHEN ${column} ~ '(^|:)(10|127)\\.\\d+\\.\\d+\\.\\d+$'
      OR ${column} ~ '(^|:)172\\.(1[6-9]|2\\d|3[01])\\.\\d+\\.\\d+$'
      OR ${column} ~ '(^|:)192\\.168\\.\\d+\\.\\d+$'
    THEN NULL
    ELSE substring(${column} from ${capture})
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
      AND ${ipv4PrefixSql(sql`s.client_ip`, 3)} = ${ipv4PrefixSql(sql.raw(`${hitAlias}.client_ip`), 3)}
  )`;
}

/**
 * How far either side of a click another company's click from the same address
 * still counts.
 *
 * ⚠️ A person reads their OWN mailbox. One public address following links mailed
 * to two different COMPANIES is a shared fetcher (a security vendor's cloud
 * egress, a link-preview service), never one reader. Measured 2026-10-02: a
 * Microsoft address clicked for 7 leads at 3 companies; AWS addresses for leads
 * at unrelated clinics. Same-company pairs are deliberately NOT counted: two
 * colleagues behind one office NAT are two real people.
 */
export const SHARED_IP_WINDOW_DAYS = 7;

export function ipSharedAcrossCompaniesEvidenceSql(hitAlias: string): SQL {
  const h = sql.raw(hitAlias);
  const window = sql.raw(`interval '${SHARED_IP_WINDOW_DAYS} days'`);
  return sql`EXISTS (
    SELECT 1
    FROM tracking_hits_raw o
    WHERE o.kind = 'click'
      AND o.id <> ${h}.id
      AND o.received_at BETWEEN ${h}.received_at - ${window} AND ${h}.received_at + ${window}
      AND ${ipv4PrefixSql(sql`o.client_ip`, 4)} = ${ipv4PrefixSql(sql.raw(`${hitAlias}.client_ip`), 4)}
      AND split_part(lower(o.lead_email), '@', 2) <> split_part(lower(${h}.lead_email), '@', 2)
  )`;
}

/**
 * A lead "clicked" from this many unrelated /16 networks inside
 * `MANY_NETWORKS_WINDOW_HOURS` is being re-scanned, not read.
 *
 * ⚠️ Measured 2026-10-02: after the network rule, the remaining "human" clicks
 * were dominated by single leads fetched from AWS, OVH and residential-proxy
 * ranges over a few hours (one lead: 8 addresses, 6 providers). A person moving
 * between office, home and phone shows at most two or three — so the floor is 3
 * DISTINCT /16s within a day, counted over the same (campaign, lead) only.
 * Accepted cost (owner, 2026-10-02): a real click buried in such a burst is
 * ruled with the scanner.
 */
export const MANY_NETWORKS_MIN = 3;
export const MANY_NETWORKS_WINDOW_HOURS = 24;

export function manyNetworksEvidenceSql(hitAlias: string): SQL {
  const h = sql.raw(hitAlias);
  const window = sql.raw(`interval '${MANY_NETWORKS_WINDOW_HOURS} hours'`);
  return sql`(
    SELECT COUNT(DISTINCT ${ipv4PrefixSql(sql`o.client_ip`, 2)})
    FROM tracking_hits_raw o
    WHERE o.kind = 'click'
      AND o.instantly_campaign_id = ${h}.instantly_campaign_id
      AND lower(o.lead_email) = lower(${h}.lead_email)
      AND o.received_at BETWEEN ${h}.received_at - ${window} AND ${h}.received_at + ${window}
  ) >= ${sql.raw(String(MANY_NETWORKS_MIN))}`;
}

/**
 * Several clicks of the same (campaign, lead) inside a second or two: a program
 * sweeping every link of the message, not a person. The recognised sources agree
 * on the shape (Suped: an all-links sweep in the first second; Adobe Experience
 * League: many link events for one profile in the same second; vendor burst
 * detectors: 4-5 clicks in one second) and on keeping a single double-click
 * human, hence the floor of THREE clicks within `CLICK_BURST_WINDOW_SECONDS`
 * either side. It is a count of clicks, never a delay after the send.
 *
 * ⚠️ THE ONE RULE THAT ALSO JUDGES A CLICK INSTANTLY RECORDED: Instantly's
 * webhook carries no user-agent, method or address, so this is the only
 * positive fingerprint its clicks can show (`vendor-click-burst.ts`).
 */
export const CLICK_BURST_MIN = 3;
export const CLICK_BURST_WINDOW_SECONDS = 1;

export function clickBurstEvidenceSql(hitAlias: string): SQL {
  const h = sql.raw(hitAlias);
  const window = sql.raw(`interval '${CLICK_BURST_WINDOW_SECONDS} seconds'`);
  return sql`(
    SELECT COUNT(*)
    FROM tracking_hits_raw o
    WHERE o.kind = 'click'
      AND o.instantly_campaign_id = ${h}.instantly_campaign_id
      AND lower(o.lead_email) = lower(${h}.lead_email)
      AND o.received_at BETWEEN ${h}.received_at - ${window} AND ${h}.received_at + ${window}
  ) >= ${sql.raw(String(CLICK_BURST_MIN))}`;
}

/**
 * `EXISTS (…)`: the click came from an address a cloud or hosting operator
 * announces (`datacenter-networks.ts`), and the lead does not work for that
 * operator.
 *
 * ⚠️ A person reads mail on their phone, laptop or office network; the link
 * scanners of Microsoft Defender, Mimecast, Proofpoint and Barracuda detonate
 * links from AWS, Azure, Google Cloud and OVH. Measured 2026-10-18 on every
 * self-send hit with a real address: 3,192 of 3,642 scanner-ruled hits (88%)
 * sat on those networks, and 26 of the 48 hits still called human did too —
 * Linux `Chrome/124` from AWS on four hospitals, Windows `Chrome/122` from OVH
 * on four oral-surgery practices. The lead's own operator is exempt (an Amazon
 * employee clicks from Amazon's network). Accepted cost: a person browsing from
 * a cloud desktop is ruled with the scanners.
 */
export function datacenterNetworkEvidenceSql(hitAlias: string): SQL {
  const h = sql.raw(hitAlias);
  return sql`EXISTS (
    SELECT 1
    FROM datacenter_networks d
    WHERE d.prefix >>= ${clientInetSql(sql`${h}.client_ip`)}
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(d.operator_domains) od
        WHERE split_part(lower(${h}.lead_email), '@', 2) = od
           OR split_part(lower(${h}.lead_email), '@', 2) LIKE '%.' || od
      )
  )`;
}

/**
 * The recorded client address as an `inet`, IPv4-mapped IPv6 unwrapped; NULL
 * when there is none or it is not an address (never a cast error).
 */
function clientInetSql(column: SQL): SQL {
  return sql`(CASE
    WHEN regexp_replace(${column}, '^::ffff:', '') ~ '^\\d{1,3}(\\.\\d{1,3}){3}$'
      OR ${column} ~ '^[0-9a-fA-F:]+$'
    THEN regexp_replace(${column}, '^::ffff:', '')::inet
  END)`;
}

/**
 * A lead "clicked" with this many distinct browsers inside
 * `MANY_USER_AGENTS_WINDOW_HOURS` is being re-scanned, not read: the same reading
 * as `many_networks`, for hits that carry no address (everything before
 * 2026-09-11). A person shows one or two (desktop, phone). Measured: one lead
 * at 9 clicks over two days from SIX browsers, Windows 7 Chrome 83 among them.
 * Only hits not already ruled a scanner are counted, so a person clicking after
 * Defender rotated its user-agents over the same message keeps their click.
 */
export const MANY_USER_AGENTS_MIN = 3;
export const MANY_USER_AGENTS_WINDOW_HOURS = 24;

export function manyUserAgentsEvidenceSql(hitAlias: string): SQL {
  const h = sql.raw(hitAlias);
  const window = sql.raw(`interval '${MANY_USER_AGENTS_WINDOW_HOURS} hours'`);
  return sql`(
    SELECT COUNT(DISTINCT o.user_agent)
    FROM tracking_hits_raw o
    WHERE o.kind = 'click'
      AND o.classification IS DISTINCT FROM 'scanner'
      AND o.instantly_campaign_id = ${h}.instantly_campaign_id
      AND lower(o.lead_email) = lower(${h}.lead_email)
      AND o.received_at BETWEEN ${h}.received_at - ${window} AND ${h}.received_at + ${window}
  ) >= ${sql.raw(String(MANY_USER_AGENTS_MIN))}`;
}

/**
 * The device family a user-agent names, read off its platform token. Ordered:
 * Android before Linux (every Android UA also says `Linux`), iPhone/iPad before
 * Macintosh. NULL when no family is named: an unknown platform never pairs.
 * The patterns are plain substrings so the SQL and the TS readings are ONE list.
 */
export const DEVICE_FAMILY_PATTERNS: ReadonlyArray<readonly [family: string, token: string]> = [
  ["iphone", "iPhone"],
  ["ipad", "iPad"],
  ["android", "Android"],
  ["windows", "Windows"],
  ["mac", "Macintosh"],
  ["chromeos", "CrOS"],
  ["linux", "Linux"],
];

export function deviceFamily(userAgent: string | null): string | null {
  if (!userAgent) return null;
  const match = DEVICE_FAMILY_PATTERNS.find(([, token]) => userAgent.includes(token));
  return match ? match[0] : null;
}

function deviceFamilySql(column: SQL): SQL {
  const branches = DEVICE_FAMILY_PATTERNS.map(
    ([family, token]) => sql`WHEN strpos(${column}, ${sql.raw(`'${token}'`)}) > 0 THEN ${sql.raw(`'${family}'`)}`,
  );
  return sql`(CASE ${sql.join(branches, sql` `)} END)`;
}

/**
 * Two clicks on the SAME email (campaign, lead, step) from two different DEVICE
 * FAMILIES within this many seconds either side: one message cannot be open on a
 * PC and a phone at once, so it is a mail-security gateway rotating user-agents.
 *
 * ⚠️ A GAP BETWEEN TWO HITS OF ONE MESSAGE, NEVER A DELAY AFTER THE SEND (the
 * module's no-time-threshold doctrine stands). Measured 2026-10-21 on Ozon Bank
 * (ibalagurov@ozon.ru): step 1 Windows Chrome/123 then Android Chrome/135 5 s
 * later, step 2 Windows Edge/123 then iPhone GSA 5 s later, all from one
 * corporate /24; `many_user_agents` saw only two per message. Kept human: one
 * click, two clicks from the same family (the Google app's in-app browser then
 * Safari on one iPhone), a desktop click and a phone click minutes apart.
 * Only hits not already ruled a scanner are counted, like `many_user_agents`.
 * Shorter than `CLICK_DECISION_HOLD_SECONDS`, so the second hit is recorded
 * before the first is decided.
 */
export const TWO_DEVICES_WINDOW_SECONDS = 60;

export function twoDevicesEvidenceSql(hitAlias: string): SQL {
  const h = sql.raw(hitAlias);
  const window = sql.raw(`interval '${TWO_DEVICES_WINDOW_SECONDS} seconds'`);
  const hitFamily = deviceFamilySql(sql.raw(`${hitAlias}.user_agent`));
  const otherFamily = deviceFamilySql(sql`o.user_agent`);
  return sql`(${hitFamily} IS NOT NULL AND EXISTS (
    SELECT 1
    FROM tracking_hits_raw o
    WHERE o.kind = 'click'
      AND o.id <> ${h}.id
      AND o.classification IS DISTINCT FROM 'scanner'
      AND o.instantly_campaign_id = ${h}.instantly_campaign_id
      AND lower(o.lead_email) = lower(${h}.lead_email)
      AND o.step IS NOT DISTINCT FROM ${h}.step
      AND o.received_at BETWEEN ${h}.received_at - ${window} AND ${h}.received_at + ${window}
      AND ${otherFamily} IS NOT NULL
      AND ${otherFamily} <> ${hitFamily}
  ))`;
}
