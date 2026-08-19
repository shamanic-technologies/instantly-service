/**
 * Click tracking for mail we dispatch ourselves.
 *
 * Instantly gets click data by rewriting every link through its custom tracking
 * domain. When we send, we do the same: each anchor in the prospect body is
 * rewritten to a signed URL on our own origin, which records the hit and then
 * redirects to the real target.
 *
 * Opens are deliberately NOT tracked, on either transport — the pixel costs
 * deliverability and `open_tracking: false` is set on every campaign we create.
 * Clicks are, because a click is a real intent signal AND because `stop-on-click`
 * keys on `email_link_clicked`: without this, a self-sent lead on a visit-first
 * funnel would never have its sequence paused.
 *
 * ⚠️ THE TARGET IS INSIDE THE SIGNED PAYLOAD, never a free query parameter.
 * A redirector that forwards to whatever `?url=` says is an OPEN REDIRECT: anyone
 * can borrow our domain to bounce victims at a phishing page, and the domain gets
 * blacklisted — the exact opposite of what a tracking domain is for. Signing the
 * target means a URL we did not mint redirects nowhere.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

/**
 * Links are signed with `SELF_SEND_UNSUBSCRIBE_SECRET`.
 *
 * The variable is named for the opt-out because that shipped first; it is the
 * signing secret for every self-send link. Left as-is rather than renamed — it is
 * already deployed, and a rename buys nothing but a window where it is unset.
 */
import { unsubscribeSecret } from "./unsubscribe";

function base64url(input: Buffer | string): string {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function fromBase64url(input: string): string {
  return Buffer.from(input.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
}

export interface ClickTarget {
  instantlyCampaignId: string;
  leadEmail: string;
  step: number;
  /** The real destination. Signed, so it cannot be swapped. */
  url: string;
}

/**
 * JSON rather than a delimiter-joined string: a target URL can contain any
 * separator we might pick, and a parser that guesses where the URL starts is a
 * parser that can be tricked.
 */
function canonical(target: ClickTarget): string {
  return JSON.stringify({
    c: target.instantlyCampaignId,
    l: target.leadEmail.trim().toLowerCase(),
    s: target.step,
    u: target.url,
  });
}

export function signClick(target: ClickTarget, secret: string): string {
  return base64url(createHmac("sha256", secret).update(canonical(target)).digest());
}

/** `/c/<base64url(json)>/<signature>` */
export function buildClickPath(target: ClickTarget, secret: string): string {
  return `/c/${base64url(canonical(target))}/${signClick(target, secret)}`;
}

/**
 * Decode + verify a click payload.
 *
 * Returns null on ANY failure — malformed base64, bad JSON, missing field, bad
 * MAC. The caller 404s all of them identically, so the route cannot be probed
 * for which campaigns exist.
 */
export function parseSignedClick(
  payload: string,
  signature: string,
  secret: string,
): ClickTarget | null {
  let decoded: string;
  try {
    decoded = fromBase64url(payload);
  } catch {
    return null;
  }

  let parsed: { c?: unknown; l?: unknown; s?: unknown; u?: unknown };
  try {
    parsed = JSON.parse(decoded);
  } catch {
    return null;
  }

  if (
    typeof parsed.c !== "string" ||
    typeof parsed.l !== "string" ||
    typeof parsed.s !== "number" ||
    typeof parsed.u !== "string"
  ) {
    return null;
  }

  const target: ClickTarget = {
    instantlyCampaignId: parsed.c,
    leadEmail: parsed.l,
    step: parsed.s,
    url: parsed.u,
  };

  // Constant-time compare. `timingSafeEqual` throws on a length mismatch, so the
  // lengths are checked first and a wrong-length signature is simply rejected.
  const expected = Buffer.from(signClick(target, secret));
  const given = Buffer.from(signature);
  if (expected.length !== given.length) return null;
  return timingSafeEqual(expected, given) ? target : null;
}

/**
 * Only ever redirect to a real web destination.
 *
 * Even with a valid signature this is checked, because it bounds the damage if
 * the secret ever leaks: `javascript:` and `data:` URLs turn a redirect into
 * script execution in the victim's context.
 */
export function isRedirectableUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" || parsed.protocol === "http:";
  } catch {
    return false;
  }
}

/** Anchors whose href we rewrite: real http(s) links to somewhere else. */
function shouldRewrite(href: string, ownOrigin: string): boolean {
  if (!isRedirectableUrl(href)) return false;
  // Our own links are already ours — the opt-out most of all. Rewriting it would
  // bury the unsubscribe behind a click-tracking hop, which is both pointless and
  // hostile to the one link a prospect must always be able to trust.
  return !href.startsWith(ownOrigin);
}

/**
 * Rewrite every outbound anchor in a built message body.
 *
 * Runs on the FINAL html (after the signature block is appended), and skips
 * anything already pointing at our own origin — which is exactly the opt-out
 * link. Doing it this way rather than before the signature keeps the rule to a
 * single readable predicate instead of splitting the body into regions.
 *
 * A `mailto:`, a `tel:` or an already-relative href is left untouched: there is
 * nothing to track and rewriting would break it.
 */
export function rewriteLinksForTracking(
  html: string,
  target: Omit<ClickTarget, "url">,
  origin: string,
  secret: string,
): string {
  return html.replace(
    /(<a\b[^>]*\bhref=)(["'])(.*?)\2/gi,
    (match, prefix: string, quote: string, href: string) => {
      if (!shouldRewrite(href, origin)) return match;
      const path = buildClickPath({ ...target, url: href }, secret);
      return `${prefix}${quote}${origin}${path}${quote}`;
    },
  );
}

/** Exposed so the route reads the same secret without importing the opt-out module. */
export { unsubscribeSecret as selfSendLinkSecret };
