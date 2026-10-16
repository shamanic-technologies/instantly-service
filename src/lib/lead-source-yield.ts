/**
 * SOURCE-YIELD-1016: which lead SOURCE feeds our sending domains the dead
 * addresses and the spam traps. Pure: the join of every first email we sent to
 * the source tags lead-service holds for that person, and the per-tag counts.
 *
 * GRAIN = a SEQUENCE (one `instantly_campaigns` row = one person, one brand
 * thread) whose FIRST email really left. Each measure is a yes/no on that
 * sequence, bucketed on the UTC day of its first email (a cohort: the bounce of
 * a first email sent Monday counts on Monday, whenever its DSN arrived).
 *
 * COUNTING RULE = PER TAG, FULL WEIGHT. A person found by two sources carries
 * both tags (lead-service's provenance is multi: being found twice is a higher
 * intent, owner 2026-10-08) and their sequence counts ONCE under EACH tag. So a
 * column summed over tags can exceed the `total` row, which counts each
 * sequence once. No 1/n weighting: a source that hands us a dead address is
 * charged with it in full, whoever else also found it.
 *
 * Two buckets are not sources and are never mixed with one:
 *  - `(no source recorded)`: lead-service answered and named no source (`[]`).
 *  - `(unknown)`: we hold no answer (never fetched yet, lead-service was
 *    unavailable, or the sequence carries no org/brand to ask about). A 502 is
 *    unknown, never "no source".
 */

/** Pseudo keys, written so they cannot collide with a lead-service tag. */
export const NO_SOURCE_KEY = "(no source recorded)";
export const UNKNOWN_KEY = "(unknown)";

export type YieldKeyKind = "source" | "no_source_recorded" | "unknown";

export function keyKind(key: string): YieldKeyKind {
  if (key === NO_SOURCE_KEY) return "no_source_recorded";
  if (key === UNKNOWN_KEY) return "unknown";
  return "source";
}

/** One sequence whose first email left, with what happened to it. */
export interface YieldSequence {
  orgId: string | null;
  brandIds: string[];
  /** Lowercased, trimmed address. */
  email: string;
  /** UTC day (YYYY-MM-DD) of the first email. */
  day: string;
  /** Sending domain of the first email. */
  domain: string;
  /** A permanent bounce on any step (every transport). */
  hardBounced: boolean;
  /** The receiving server sent a delivery-failure notice AFTER accepting the email (our own sender only). */
  refused: boolean;
  /** That notice cites a blocklist (SURBL, Spamhaus, URIBL, any RBL). */
  refusedBlocklist: boolean;
}

/** `${orgId}|${brandId}|${email}` -> the tags lead-service served (possibly `[]`). */
export type ProvenanceIndex = ReadonlyMap<string, readonly string[]>;

export function provenanceKey(orgId: string, brandId: string, email: string): string {
  return `${orgId}|${brandId}|${email}`;
}

/**
 * The keys a sequence counts under. Union over the brands of the sequence of
 * the tags lead-service served; a brand we hold no answer for adds nothing.
 * Nothing known for any brand = `(unknown)`; every answer empty = `(no source recorded)`.
 */
export function keysForSequence(seq: Pick<YieldSequence, "orgId" | "brandIds" | "email">, provenance: ProvenanceIndex): string[] {
  if (!seq.orgId || seq.brandIds.length === 0) return [UNKNOWN_KEY];
  let answered = false;
  const tags = new Set<string>();
  for (const brandId of seq.brandIds) {
    const served = provenance.get(provenanceKey(seq.orgId, brandId, seq.email));
    if (served === undefined) continue;
    answered = true;
    for (const t of served) tags.add(t);
  }
  if (!answered) return [UNKNOWN_KEY];
  if (tags.size === 0) return [NO_SOURCE_KEY];
  return [...tags].sort();
}

export interface YieldCounts {
  firstEmails: number;
  hardBounces: number;
  refusals: number;
  blocklistRefusals: number;
  firstEmailsFromListedDomains: number;
}

export interface YieldMeasures extends YieldCounts {
  /** hardBounces / firstEmails; null when no first email. */
  hardBounceRate: number | null;
  /** refusals / firstEmails; null when no first email. */
  refusalRate: number | null;
  /** firstEmailsFromListedDomains / firstEmails; null when no first email. */
  listedDomainShare: number | null;
}

export const MEASURES = [
  "firstEmails",
  "hardBounces",
  "refusals",
  "blocklistRefusals",
  "firstEmailsFromListedDomains",
] as const satisfies ReadonlyArray<keyof YieldCounts>;
export type Measure = (typeof MEASURES)[number];

function zero(): YieldCounts {
  return { firstEmails: 0, hardBounces: 0, refusals: 0, blocklistRefusals: 0, firstEmailsFromListedDomains: 0 };
}

function add(into: YieldCounts, seq: YieldSequence, listed: boolean): void {
  into.firstEmails += 1;
  if (seq.hardBounced) into.hardBounces += 1;
  if (seq.refused) into.refusals += 1;
  if (seq.refusedBlocklist) into.blocklistRefusals += 1;
  if (listed) into.firstEmailsFromListedDomains += 1;
}

const rate = (n: number, d: number): number | null => (d === 0 ? null : Math.round((n / d) * 10000) / 10000);

export function withRates(c: YieldCounts): YieldMeasures {
  return {
    ...c,
    hardBounceRate: rate(c.hardBounces, c.firstEmails),
    refusalRate: rate(c.refusals, c.firstEmails),
    listedDomainShare: rate(c.firstEmailsFromListedDomains, c.firstEmails),
  };
}

export interface YieldSourceRow extends YieldMeasures {
  key: string;
  kind: YieldKeyKind;
}

export interface YieldSeriesPoint {
  day: string;
  /** Every sequence once (not the sum of `byKey`). */
  total: number;
  /** Per key, zero included for every key of the read. */
  byKey: Record<string, number>;
}

export interface YieldAggregate {
  total: YieldMeasures;
  coverage: { firstEmails: number; withSource: number; noSourceRecorded: number; unknown: number };
  /** Ranked: sources by hard-bounce rate (desc), then first emails (desc); pseudo buckets last. */
  sources: YieldSourceRow[];
  series: { days: string[]; keys: string[] } & Record<Measure, YieldSeriesPoint[]>;
}

/**
 * Counts since inception per key, plus a daily series over `days`.
 * `listedDomains` = sending domains listed on any blocklist TODAY.
 */
export function aggregateSourceYield(
  sequences: readonly YieldSequence[],
  provenance: ProvenanceIndex,
  listedDomains: ReadonlySet<string>,
  days: readonly string[],
): YieldAggregate {
  const total = zero();
  const byKey = new Map<string, YieldCounts>();
  const daySet = new Set(days);
  const dayTotals = new Map<string, YieldCounts>();
  const dayByKey = new Map<string, Map<string, YieldCounts>>();
  const coverage = { firstEmails: 0, withSource: 0, noSourceRecorded: 0, unknown: 0 };

  for (const seq of sequences) {
    const keys = keysForSequence(seq, provenance);
    const listed = listedDomains.has(seq.domain);
    add(total, seq, listed);
    coverage.firstEmails += 1;
    if (keys[0] === UNKNOWN_KEY) coverage.unknown += 1;
    else if (keys[0] === NO_SOURCE_KEY) coverage.noSourceRecorded += 1;
    else coverage.withSource += 1;

    for (const k of keys) {
      const c = byKey.get(k) ?? zero();
      add(c, seq, listed);
      byKey.set(k, c);
    }
    if (daySet.has(seq.day)) {
      const dt = dayTotals.get(seq.day) ?? zero();
      add(dt, seq, listed);
      dayTotals.set(seq.day, dt);
      const m = dayByKey.get(seq.day) ?? new Map<string, YieldCounts>();
      for (const k of keys) {
        const c = m.get(k) ?? zero();
        add(c, seq, listed);
        m.set(k, c);
      }
      dayByKey.set(seq.day, m);
    }
  }

  const pseudoOrder = (k: string) => (k === NO_SOURCE_KEY ? 1 : k === UNKNOWN_KEY ? 2 : 0);
  const sources: YieldSourceRow[] = [...byKey.entries()]
    .map(([key, c]) => ({ key, kind: keyKind(key), ...withRates(c) }))
    .sort(
      (a, b) =>
        pseudoOrder(a.key) - pseudoOrder(b.key) ||
        (b.hardBounceRate ?? -1) - (a.hardBounceRate ?? -1) ||
        b.firstEmails - a.firstEmails ||
        a.key.localeCompare(b.key),
    );
  const keys = sources.map((s) => s.key);

  const seriesFor = (m: Measure): YieldSeriesPoint[] =>
    days.map((day) => {
      const perKey = dayByKey.get(day);
      return {
        day,
        total: dayTotals.get(day)?.[m] ?? 0,
        byKey: Object.fromEntries(keys.map((k) => [k, perKey?.get(k)?.[m] ?? 0])),
      };
    });

  return {
    total: withRates(total),
    coverage,
    sources,
    series: {
      days: [...days],
      keys,
      firstEmails: seriesFor("firstEmails"),
      hardBounces: seriesFor("hardBounces"),
      refusals: seriesFor("refusals"),
      blocklistRefusals: seriesFor("blocklistRefusals"),
      firstEmailsFromListedDomains: seriesFor("firstEmailsFromListedDomains"),
    },
  };
}
