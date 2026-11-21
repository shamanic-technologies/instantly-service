/**
 * Who a cold send COPIES (OUTREACH-3WAYS-1121): the Cc / Bcc its caller named
 * on `POST /orgs/send`, nobody else (NO-DEFAULT-COPY-1116: this service adds no
 * recipient of its own; who is copied is the script's tactic).
 *
 * Stored on the sequence row (`instantly_campaigns.metadata.copy`) at send and
 * read by the dispatch worker for every step OF THAT SEQUENCE. A later
 * follow-up is its own send (`followUp: true`, its own row): it copies only
 * who ITS caller names, never this list.
 *
 * Pure.
 */

export interface SendCopy {
  cc: string[];
  bcc: string[];
}

function clean(list: readonly string[] | undefined, skip: Set<string>): string[] {
  const out: string[] = [];
  for (const raw of list ?? []) {
    const address = raw.trim();
    if (!address) continue;
    const key = address.toLowerCase();
    if (skip.has(key)) continue;
    skip.add(key);
    out.push(address);
  }
  return out;
}

/**
 * The copy a send carries: trimmed, de-duplicated (case-insensitive), the To
 * address never copied on its own email, an address named in both lists kept
 * in Cc only. Nothing left = null (the row is stored exactly as before).
 */
export function sendCopyFromRequest(
  to: string,
  cc: readonly string[] | undefined,
  bcc: readonly string[] | undefined,
): SendCopy | null {
  const seen = new Set<string>([to.trim().toLowerCase()]);
  const ccList = clean(cc, seen);
  const bccList = clean(bcc, seen);
  if (ccList.length === 0 && bccList.length === 0) return null;
  return { cc: ccList, bcc: bccList };
}

/**
 * The stored copy as the two header strings the message carries (comma lists,
 * the dispatch log's shape). Anything unreadable = nobody copied.
 */
export function copyHeadersFromMetadata(copy: unknown): { cc: string; bcc: string } {
  const c = (copy ?? {}) as Record<string, unknown>;
  const list = (v: unknown): string =>
    Array.isArray(v)
      ? v.filter((a): a is string => typeof a === "string" && a.trim() !== "").map((a) => a.trim()).join(",")
      : "";
  return { cc: list(c.cc), bcc: list(c.bcc) };
}
