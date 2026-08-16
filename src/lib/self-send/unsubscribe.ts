/**
 * Opt-out links for mail we dispatch ourselves.
 *
 * While Instantly sends, the footer carries `{unsubscribe_link}` — Instantly's
 * own server-side merge variable, which only Instantly resolves. The moment we
 * dispatch, that placeholder would ship to the prospect verbatim as a dead link,
 * so the sender has to mint the URL itself. That is why unsubscribe lands with
 * the sender rather than later with tracking: an email whose opt-out does not
 * work is not shippable, legally or for deliverability. Open tracking is genuinely
 * separable — an untracked email is still a valid email.
 *
 * The link is stateless and self-authenticating: the recipient identity is in the
 * URL and an HMAC over it proves we minted it. No token table, no lookup, and a
 * link stays valid for the life of the secret — which is what an opt-out link has
 * to do, since prospects act on old mail.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

/**
 * Signing secret. Read lazily at USE rather than captured at module load, so a
 * service booting without it still starts and serves every existing route — only
 * a self-send dispatch fails, and loudly. Capturing at load would turn a missing
 * var into a boot-loop for a feature nobody has switched on yet.
 */
function unsubscribeSecret(): string {
  const secret = process.env.SELF_SEND_UNSUBSCRIBE_SECRET;
  if (!secret) {
    throw new Error(
      "SELF_SEND_UNSUBSCRIBE_SECRET is not set — refusing to send mail with an unsigned opt-out link",
    );
  }
  return secret;
}

/**
 * Public origin the opt-out link points at.
 *
 * Deliberately its own variable rather than the service's own hostname: this URL
 * ships inside every cold email, and pointing it at a brand domain would put that
 * domain's reputation in the cold stream — the exact coupling `instantly_domain_policy`
 * exists to prevent. Give it a neutral domain CNAME'd to this service.
 */
function unsubscribeOrigin(): string {
  const origin = process.env.SELF_SEND_PUBLIC_URL;
  if (!origin) {
    throw new Error(
      "SELF_SEND_PUBLIC_URL is not set — refusing to send mail with no opt-out origin",
    );
  }
  return origin.replace(/\/+$/, "");
}

/** URL-safe base64 with the padding dropped, so the value survives a path segment. */
function base64url(input: Buffer | string): string {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function fromBase64url(input: string): string {
  const padded = input.replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(padded, "base64").toString("utf8");
}

export interface UnsubscribeIdentity {
  instantlyCampaignId: string;
  leadEmail: string;
}

/** Canonical signed string. Lowercased email so casing can never change the MAC. */
function canonical(identity: UnsubscribeIdentity): string {
  return `${identity.instantlyCampaignId}:${identity.leadEmail.trim().toLowerCase()}`;
}

export function signUnsubscribe(identity: UnsubscribeIdentity, secret: string): string {
  return base64url(createHmac("sha256", secret).update(canonical(identity)).digest());
}

/**
 * Verify a signature in constant time.
 *
 * `timingSafeEqual` throws on a length mismatch, so the lengths are compared
 * first — and a wrong-length signature is rejected without leaking which.
 */
export function verifyUnsubscribeSignature(
  identity: UnsubscribeIdentity,
  signature: string,
  secret: string,
): boolean {
  const expected = Buffer.from(signUnsubscribe(identity, secret));
  const given = Buffer.from(signature);
  if (expected.length !== given.length) return false;
  return timingSafeEqual(expected, given);
}

/** `/u/<base64url(campaignId:email)>/<signature>` */
export function buildUnsubscribePath(identity: UnsubscribeIdentity, secret: string): string {
  return `/u/${base64url(canonical(identity))}/${signUnsubscribe(identity, secret)}`;
}

/** Absolute opt-out URL, resolved from the environment. Throws when unconfigured. */
export function buildUnsubscribeUrl(identity: UnsubscribeIdentity): string {
  return `${unsubscribeOrigin()}${buildUnsubscribePath(identity, unsubscribeSecret())}`;
}

/**
 * Decode + verify a path payload back to the identity it was minted for.
 *
 * Returns null on ANY failure — malformed base64, missing separator, bad MAC.
 * A caller cannot distinguish the cases, which is the point: the route must not
 * become an oracle for which campaign ids exist.
 */
export function parseSignedUnsubscribe(
  payload: string,
  signature: string,
  secret: string,
): UnsubscribeIdentity | null {
  let decoded: string;
  try {
    decoded = fromBase64url(payload);
  } catch {
    return null;
  }

  // The email owns the last colon-separated field; campaign ids never contain one,
  // but splitting from the right is correct regardless of what they might carry.
  const separator = decoded.lastIndexOf(":");
  if (separator <= 0 || separator === decoded.length - 1) return null;

  const identity: UnsubscribeIdentity = {
    instantlyCampaignId: decoded.slice(0, separator),
    leadEmail: decoded.slice(separator + 1),
  };

  return verifyUnsubscribeSignature(identity, signature, secret) ? identity : null;
}

/** Exposed for the route, which needs the same lazily-read secret. */
export { unsubscribeSecret, unsubscribeOrigin };
