/**
 * Seed-placement classification and aggregation (pure — no IO).
 *
 * Turns the two bronze halves — what we dispatched, what we observed — into the
 * SAME `SilverPlacementRow` shape the Instantly path produces, so both land in
 * `instantly_placement_results` and every downstream reader is untouched.
 */

import type { SilverPlacementRow } from "../placement-promote";

/** Where a seed landed. Anything we cannot classify is not a placement at all. */
export type SeedPlacement = "inbox" | "spam";

/**
 * IMAP folder → placement verdict.
 *
 * Only two folders answer the question. Gmail exposes junk as `[Gmail]/Spam`,
 * other servers as `Junk` / `Junk E-mail`; everything else (Sent, Drafts, a user
 * label, All Mail — which MIRRORS both inbox and spam and would therefore
 * double-count) returns null and is skipped. Returning a default here would
 * fabricate a verdict from a folder that carries none.
 */
export function classifySeedFolder(path: string): SeedPlacement | null {
  const normalized = path.trim().toLowerCase();

  if (normalized === "inbox") return "inbox";
  if (
    normalized === "[gmail]/spam" ||
    normalized === "junk" ||
    normalized === "junk e-mail" ||
    normalized === "spam"
  ) {
    return "spam";
  }
  return null;
}

export interface AuthResults {
  spfPass: boolean | null;
  dkimPass: boolean | null;
  dmarcPass: boolean | null;
}

/**
 * Parse an RFC 8601 `Authentication-Results` header.
 *
 * An absent mechanism yields null, NOT false: "the receiver did not report on
 * SPF" and "SPF failed" are different facts, and the silver columns are nullable
 * precisely so the difference survives. Only an explicit `pass` is true.
 */
export function parseAuthResults(header: string | null | undefined): AuthResults {
  if (!header) return { spfPass: null, dkimPass: null, dmarcPass: null };

  const read = (mechanism: string): boolean | null => {
    const match = new RegExp(`\\b${mechanism}=([a-z]+)`, "i").exec(header);
    if (!match) return null;
    return match[1].toLowerCase() === "pass";
  };

  return { spfPass: read("spf"), dkimPass: read("dkim"), dmarcPass: read("dmarc") };
}

/** A seed we successfully put on the wire. Only these count toward the denominator. */
export interface SeedDispatchRecord {
  messageId: string;
  senderEmail: string;
  recipientEsp: number;
}

/** A seed we found in a receiver mailbox. */
export interface SeedObservationRecord {
  messageId: string;
  placement: SeedPlacement;
  spfPass: boolean | null;
  dkimPass: boolean | null;
  dmarcPass: boolean | null;
}

function pct(count: number, total: number): number {
  return total > 0 ? Math.round((count / total) * 100) : 0;
}

/** AND-fold a nullable boolean: true iff every present value is true; null if none. */
function andFold(values: Array<boolean | null>): boolean | null {
  const present = values.filter((v): v is boolean => v !== null && v !== undefined);
  if (present.length === 0) return null;
  return present.every((v) => v === true);
}

/**
 * Aggregate one seed test into per-(sender, ESP) silver rows.
 *
 * ⚠️ THE DENOMINATOR IS THE DISPATCH SIDE, and that is the whole design.
 * `seedTotal` counts seeds we SENT; inbox and spam count seeds we FOUND. A seed
 * that was dispatched and never observed is therefore `missing` — it drags the
 * score down, which is correct, because a message nobody can find did not reach
 * an inbox. Deriving the denominator from the observations instead would make a
 * mailbox whose every seed vanished score 0/0 → and a null score reads as
 * "untested", which would quietly promote a black hole to a passing grade.
 *
 * Mirrors `aggregatePlacementRows` (the Instantly path) in shape and in rounding
 * so the two are comparable during the parallel run.
 */
export function aggregateSeedPlacement(
  dispatches: readonly SeedDispatchRecord[],
  observations: readonly SeedObservationRecord[],
  testId: string,
  testedAt: Date,
): SilverPlacementRow[] {
  const observedByMessageId = new Map<string, SeedObservationRecord>();
  for (const o of observations) observedByMessageId.set(o.messageId, o);

  interface Group {
    senderEmail: string;
    recipientEsp: number;
    seedTotal: number;
    inboxCount: number;
    spamCount: number;
    auth: SeedObservationRecord[];
  }

  const groups = new Map<string, Group>();

  for (const d of dispatches) {
    const key = JSON.stringify([d.senderEmail, d.recipientEsp]);
    let g = groups.get(key);
    if (!g) {
      g = {
        senderEmail: d.senderEmail,
        recipientEsp: d.recipientEsp,
        seedTotal: 0,
        inboxCount: 0,
        spamCount: 0,
        auth: [],
      };
      groups.set(key, g);
    }

    g.seedTotal += 1;

    const observed = observedByMessageId.get(d.messageId);
    if (!observed) continue;

    if (observed.placement === "inbox") g.inboxCount += 1;
    else g.spamCount += 1;
    g.auth.push(observed);
  }

  return [...groups.values()].map((g) => {
    const missingCount = Math.max(0, g.seedTotal - g.inboxCount - g.spamCount);
    return {
      testId,
      accountEmail: g.senderEmail,
      recipientEsp: g.recipientEsp,
      testedAt,
      seedTotal: g.seedTotal,
      inboxCount: g.inboxCount,
      spamCount: g.spamCount,
      missingCount,
      inboxPct: pct(g.inboxCount, g.seedTotal),
      spamPct: pct(g.spamCount, g.seedTotal),
      missingPct: pct(missingCount, g.seedTotal),
      spfPass: andFold(g.auth.map((a) => a.spfPass)),
      dkimPass: andFold(g.auth.map((a) => a.dkimPass)),
      dmarcPass: andFold(g.auth.map((a) => a.dmarcPass)),
    };
  });
}
