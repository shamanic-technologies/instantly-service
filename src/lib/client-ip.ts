/**
 * The address the request actually came from.
 *
 * `req.ip` alone reports the immediate peer, which behind the reverse proxy every
 * `*.distribute.you` service sits behind is Caddy's own docker address — all 771
 * tracking hits recorded before this carry `::ffff:172.18.0.27`. With
 * `trust proxy` set (see `src/index.ts`) express resolves `req.ip` through
 * `X-Forwarded-For`, skipping exactly the trusted hop, so a client that forges
 * its own header cannot displace the address Caddy appended.
 *
 * Worth keeping because a scanner's IP ranges are stable in a way its user-agent
 * is not — it is the strongest fingerprint available, and it was being discarded
 * on every hit.
 */
import type { Request } from "express";

export function clientIpOf(req: Request): string | null {
  const ip = req.ip ?? null;
  if (!ip) return null;
  // Express reports an IPv4 peer through the v6 mapping. Store the plain form so
  // a range comparison does not have to know about the prefix.
  return ip.startsWith("::ffff:") ? ip.slice("::ffff:".length) : ip;
}
