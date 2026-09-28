/**
 * UTM tagging for links that point at distribute.you's OWN site.
 *
 * The landing records first touch in a `distribute_first_touch` cookie and
 * classifies the channel from `utm_source` / `utm_medium` (a source matching
 * /cold.?email|outbound|instantly/ or a medium matching /cold.?email|outbound/
 * reads as `cold_email`). A click from one of our cold emails arrived with no
 * utm at all, so every one was recorded as `direct` — inflating Direct and
 * making cold email's share of signups and payments unmeasurable.
 *
 * ⚠️ SCOPE IS distribute.you AND ITS SUBDOMAINS, NOTHING ELSE. A customer's
 * destination URL is returned as the SAME STRING, untouched: their analytics are
 * theirs, and a parameter we add can break a strict route or poison their own
 * attribution. And a utm_* the link already carries is never overwritten — the
 * person who wrote it meant it.
 *
 * Applied at two points, both idempotent:
 *   - when the body is built (`buildEmailBodyWithSignature`), which is the only
 *     place the Instantly transport can be reached — Instantly's tracker
 *     redirects to whatever href the body carries;
 *   - when our own `/c/` redirect answers, which also reaches self-sent mail
 *     ALREADY in inboxes, since the signed target there predates this tagging.
 */

export const COLD_EMAIL_UTM_SOURCE = "cold_email";
export const COLD_EMAIL_UTM_MEDIUM = "cold_email";

export function isDistributeHost(hostname: string): boolean {
  const h = hostname.toLowerCase().replace(/\.$/, "");
  return h === "distribute.you" || h.endsWith(".distribute.you");
}

export interface ColdEmailUtmExtras {
  /** `utm_content` — e.g. `step-2`. Only set when absent. */
  content?: string;
}

/**
 * Add the cold-email utm params to a distribute.you URL. Any other URL — a
 * customer's domain, a mailto, something unparseable — comes back byte-identical.
 */
export function withColdEmailUtm(url: string, extras: ColdEmailUtmExtras = {}): string {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return url;
  }
  if (parsed.protocol !== "https:" && parsed.protocol !== "http:") return url;
  if (!isDistributeHost(parsed.hostname)) return url;

  const params = parsed.searchParams;
  const additions: Array<[string, string]> = [
    ["utm_source", COLD_EMAIL_UTM_SOURCE],
    ["utm_medium", COLD_EMAIL_UTM_MEDIUM],
  ];
  if (extras.content) additions.push(["utm_content", extras.content]);

  const missing = additions.filter(([k]) => !params.has(k));
  if (missing.length === 0) return url;
  for (const [k, v] of missing) params.append(k, v);
  return parsed.toString();
}

/**
 * Tag every anchor in an html body that points at distribute.you. An href left
 * unchanged is emitted exactly as it was. A tagged one is written with a RAW `&`,
 * the form `autolinkifyHtml` already produces: the self-send tracker signs the
 * href verbatim, so an `&amp;` here would ship inside the redirect target.
 */
export function tagColdEmailLinks(html: string): string {
  return html.replace(
    /(<a\b[^>]*\bhref=)(["'])(.*?)\2/gi,
    (match, prefix: string, quote: string, href: string) => {
      const raw = href.replace(/&amp;/g, "&");
      const tagged = withColdEmailUtm(raw);
      if (tagged === raw) return match;
      return `${prefix}${quote}${tagged}${quote}`;
    },
  );
}
