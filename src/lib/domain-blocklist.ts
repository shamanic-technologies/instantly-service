/**
 * Sending-domain blocklists (DOMAIN-BLOCKLIST-1016) — the pure half.
 *
 * Prod 2026-10-16: 11 of our sending domains sat on SURBL multi (127.0.0.64 =
 * ABUSE) and carried ~80% of the week's sends; receivers refused mail citing it
 * ("550 A URL in this email (agileconsultco . com) is listed on
 * https://www.surbl.org/lists") since 2026-09-11 and nothing here noticed.
 *
 * WHICH LISTS, and why (market consensus, cited in the PR): the three domain
 * (URI/RHSBL) blocklists every mainstream filter consults — Spamhaus DBL,
 * SURBL multi and URIBL — are the set MXToolbox, Validity/Return Path,
 * Mailgun and Smartlead name as the ones that move inbox placement; SpamAssassin
 * ships rules for exactly these three (URIBL_DBL_*, URIBL_*_SURBL, URIBL_BLACK).
 * Every other domain list is either a re-sale of these or not consulted by the
 * big receivers.
 *
 * ⚠️ HONEST ANSWERS ONLY. A list refuses a query it does not want to serve, and
 * the refusal LOOKS like a listing: URIBL answers 127.0.0.1 and Spamhaus answers
 * 127.255.255.254 to anything coming through a shared/public resolver (the box's
 * resolver is one). So:
 *   - each list names HOW we reach it: the system resolver (SURBL answers it
 *     honestly), or the list's own AUTHORITATIVE nameservers (Spamhaus, URIBL:
 *     our own recursion, the policy-compliant low-volume path);
 *   - each list carries a CANARY its operator publishes as permanently listed
 *     (test.surbl.org, dbltest.com, test.uribl.com). A run where the canary is
 *     not answered as listed is UNANSWERED for every domain on that list — no
 *     state moves, so a domain is never marked clean without a fresh check;
 *   - a refusal code is never a listing, an unknown code is never a listing.
 */

export type BlocklistKey = "surbl" | "spamhaus_dbl" | "uribl";

export interface BlocklistDefinition {
  key: BlocklistKey;
  /** Shown to staff and in the alert. */
  name: string;
  zone: string;
  /** `system` = the container's resolver; `authoritative` = the zone's own nameservers. */
  resolver: "system" | "authoritative";
  /** A name the operator keeps permanently listed: proves the list answered honestly. */
  canary: string;
  /** Decode one A answer: the sub-list labels it means, or `refused`. Empty labels = not a listing. */
  decode(answer: string): { refused: boolean; labels: string[] };
  /** Where a human starts a removal request for `domain`. */
  removalUrl(domain: string): string;
  /** True when we file the removal ourselves (lib/blocklist-removal.ts); false = the alert carries the link. */
  removalAutomated: boolean;
  /** Why removal is not automated, stated to staff. */
  removalNote: string;
}

function lastOctet(answer: string): number | null {
  const m = /^127\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(answer.trim());
  if (!m) return null;
  return Number(m[3]);
}

/** Bitmask decode: every bit of the last octet set in `bits` is a label. */
function bitLabels(octet: number, bits: ReadonlyArray<readonly [number, string]>): string[] {
  return bits.filter(([bit]) => (octet & bit) !== 0).map(([, label]) => label);
}

export const BLOCKLISTS: readonly BlocklistDefinition[] = [
  {
    key: "surbl",
    name: "SURBL",
    zone: "multi.surbl.org",
    resolver: "system",
    canary: "test.surbl.org",
    decode(answer) {
      // 127.0.0.1 = "your access is blocked" (over-use, public resolver).
      if (answer === "127.0.0.1") return { refused: true, labels: [] };
      if (!answer.startsWith("127.0.0.")) return { refused: false, labels: [] };
      const octet = lastOctet(answer);
      if (octet === null) return { refused: false, labels: [] };
      return {
        refused: false,
        labels: bitLabels(octet, [
          [8, "PHISHING"],
          [16, "MALWARE"],
          [64, "ABUSE"],
          [128, "CRACKED"],
        ]),
      };
    },
    removalUrl: () => "https://www.surbl.org/surbl-analysis",
    removalAutomated: true,
    removalNote:
      "We send SURBL's removal request ourselves, by email to whitelist@surbl.org (the channel its removal page offers), from the company mailbox, at most once a week per domain while it stays listed (owner 2026-10-16), with every field its form asks for. SURBL reviews each request by hand and answers that mailbox.",
  },
  {
    key: "spamhaus_dbl",
    name: "Spamhaus DBL",
    zone: "dbl.spamhaus.org",
    resolver: "authoritative",
    canary: "dbltest.com",
    decode(answer) {
      // 127.255.255.252/254/255 = typo, public resolver, over-use: refusals.
      if (answer.startsWith("127.255.255.")) return { refused: true, labels: [] };
      if (!answer.startsWith("127.0.1.")) return { refused: false, labels: [] };
      const octet = lastOctet(answer);
      const label: Record<number, string> = {
        2: "SPAM",
        4: "PHISHING",
        5: "MALWARE",
        6: "BOTNET_CC",
        102: "ABUSED_SPAM",
        103: "ABUSED_REDIRECTOR",
        104: "ABUSED_PHISHING",
        105: "ABUSED_MALWARE",
        106: "ABUSED_BOTNET_CC",
      };
      return { refused: false, labels: octet !== null && label[octet] ? [label[octet]!] : [] };
    },
    removalUrl: () => "https://check.spamhaus.org/",
    removalAutomated: false,
    removalNote:
      "Spamhaus removals are requested by the domain owner through its Checker page after a verification; a person files it.",
  },
  {
    key: "uribl",
    name: "URIBL",
    zone: "multi.uribl.com",
    resolver: "authoritative",
    canary: "test.uribl.com",
    decode(answer) {
      // 127.0.0.1 = query refused (public resolver / over-use), never a listing.
      if (answer === "127.0.0.1") return { refused: true, labels: [] };
      if (!answer.startsWith("127.0.0.")) return { refused: false, labels: [] };
      const octet = lastOctet(answer);
      if (octet === null) return { refused: false, labels: [] };
      // GREY (4) is URIBL's own "not for blocking" tier: recorded nowhere.
      return { refused: false, labels: bitLabels(octet, [[2, "BLACK"], [8, "RED"]]) };
    },
    removalUrl: () => "https://admin.uribl.com/",
    removalAutomated: false,
    removalNote:
      "URIBL removals go through its admin site with an account and a stated reason; a person files it.",
  },
];

export function blocklistByKey(key: string): BlocklistDefinition | undefined {
  return BLOCKLISTS.find((l) => l.key === key);
}

/** What one DNS lookup told us about one name. */
export type LookupResult =
  | { kind: "answer"; addresses: string[] }
  | { kind: "nxdomain" }
  | { kind: "error"; code: string };

/** One (domain, list) verdict for this run. */
export type DomainVerdict =
  | { outcome: "listed"; answer: string; labels: string[] }
  | { outcome: "clean" }
  | { outcome: "unanswered"; error: string };

/**
 * Read one lookup. Pure. NXDOMAIN/NODATA = clean (the list's honest "not
 * listed"); a refusal code or any resolver error = unanswered; an answer that
 * decodes to no label (an unknown code, URIBL grey) = clean.
 */
export function readLookup(list: BlocklistDefinition, lookup: LookupResult): DomainVerdict {
  if (lookup.kind === "nxdomain") return { outcome: "clean" };
  if (lookup.kind === "error") return { outcome: "unanswered", error: lookup.code };
  const decoded = lookup.addresses.map((a) => ({ answer: a, ...list.decode(a) }));
  const refusal = decoded.find((d) => d.refused);
  if (refusal) return { outcome: "unanswered", error: `refused (${refusal.answer})` };
  const listed = decoded.filter((d) => d.labels.length > 0);
  if (listed.length === 0) return { outcome: "clean" };
  const labels = [...new Set(listed.flatMap((d) => d.labels))].sort();
  return { outcome: "listed", answer: listed.map((d) => d.answer).sort().join(","), labels };
}

/** True when the canary answered as listed: the list is answering us honestly. */
export function canaryHonest(list: BlocklistDefinition, lookup: LookupResult): boolean {
  return readLookup(list, lookup).outcome === "listed";
}

/** The stored current state of one (domain, list). */
export interface CurrentState {
  domain: string;
  list: BlocklistKey;
  listed: boolean;
  listedSince: Date | null;
}

export type TransitionType = "listed" | "delisted";

export interface Transition {
  domain: string;
  list: BlocklistKey;
  type: TransitionType;
  answer: string | null;
  labels: string[];
}

/**
 * The events a run writes. Pure. Only a FRESH answer moves state:
 *   - listed now, not listed before (or never checked) -> `listed`
 *   - clean now, listed before -> `delisted`
 *   - unanswered -> nothing, whatever the stored state
 * A listing that persists writes nothing, so it alerts nobody a second time.
 */
export function planTransitions(
  current: readonly CurrentState[],
  verdicts: ReadonlyArray<{ domain: string; list: BlocklistKey; verdict: DomainVerdict }>,
): Transition[] {
  const before = new Map(current.map((c) => [`${c.domain}|${c.list}`, c]));
  const out: Transition[] = [];
  for (const { domain, list, verdict } of verdicts) {
    const prior = before.get(`${domain}|${list}`);
    const wasListed = prior?.listed === true;
    if (verdict.outcome === "listed" && !wasListed) {
      out.push({ domain, list, type: "listed", answer: verdict.answer, labels: verdict.labels });
    } else if (verdict.outcome === "clean" && wasListed) {
      out.push({ domain, list, type: "delisted", answer: null, labels: [] });
    }
  }
  return out;
}

/** Domain part of an address, lowercased; "" when there is none. */
export function domainOf(email: string): string {
  const at = email.lastIndexOf("@");
  return at < 0 ? "" : email.slice(at + 1).trim().toLowerCase();
}
