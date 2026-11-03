/**
 * Which opt-out header prospect mail carries, decided by the seeds
 * (UNSUB-3WAY-1103). The service switches by itself, no human step.
 *
 * Every seed carries one of the three variants (`seedOptOutVariant`), stored on
 * its dispatch row and in the daily series. Until EVERY variant has
 * `OPT_OUT_MIN_FOUND_SEEDS` seeds found at Google, prospect mail keeps today's
 * `tracker_link`. Once it has, prospect mail takes the variant with the best
 * inbox rate.
 *
 * Sample size (cited in the PR, UNSUB-3WAY-1103): there is no vendor-published
 * seed count for a placement A/B, so the floor is the standard two-proportion
 * test the A/B guidance points to (Litmus and HubSpot: per-variant samples in
 * the thousands for small differences; MailTester: per-PROVIDER counts and a
 * confidence interval, since one provider hides behind an aggregate): telling
 * 85 % from 90 % inbox at 5 % significance and 80 % power needs ~700 seeds per
 * variant, per provider. One Sunday test sends ~2,000 Google seeds (~660 per
 * variant), so the sample lands after about two tests.
 *
 * Providers: Google is required (most prospects, and the receivers we read
 * reliably). Microsoft 365 joins the score once each variant has the same
 * sample there; Outlook.com (consumer, 4 prospects in 24,118) is shown, never
 * scored. Rate = inbox / found (a seed never found is a reading failure, not a
 * placement: Microsoft 365 read 0 of 227 on 2026-10-25). Score = mean of the
 * scored providers' rates. Tie = the variant with no outside domain first.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import {
  DEFAULT_OPT_OUT_VARIANT,
  OPT_OUT_VARIANTS,
  isOptOutVariant,
  type OptOutVariant,
} from "../self-send/opt-out-variant";
import { ESP_GOOGLE, ESP_OUTLOOK, ESP_OUTLOOK_CONSUMER } from "./seeds";

export const OPT_OUT_MIN_FOUND_SEEDS = 700;

/** Tie order: no outside domain wins a tie, the tracker domain loses it. */
const TIE_ORDER: readonly OptOutVariant[] = ["mailbox_only", "none", "tracker_link"];

export interface ProviderCounts {
  seeds: number;
  inbox: number;
  spam: number;
  /** inbox + spam. */
  found: number;
  /** inbox / found, 0..100 rounded to 1 decimal; null when nothing was found. */
  inboxPct: number | null;
}

export interface VariantCounts {
  variant: OptOutVariant;
  google: ProviderCounts;
  microsoft: ProviderCounts;
  microsoftConsumer: ProviderCounts;
}

export type OptOutDecisionReason = "sample_not_reached" | "measured";

export interface OptOutDecision {
  variant: OptOutVariant;
  reason: OptOutDecisionReason;
  /** Providers in the score (`google`, `microsoft`); empty before the sample. */
  scoredProviders: ("google" | "microsoft")[];
  /** Per variant, the score the decision compared (0..100); null when not scored. */
  scores: Record<OptOutVariant, number | null>;
}

export function emptyCounts(): ProviderCounts {
  return { seeds: 0, inbox: 0, spam: 0, found: 0, inboxPct: null };
}

export function finishCounts(c: Omit<ProviderCounts, "found" | "inboxPct">): ProviderCounts {
  const found = c.inbox + c.spam;
  return { ...c, found, inboxPct: found > 0 ? Math.round((c.inbox * 1000) / found) / 10 : null };
}

const rate = (c: ProviderCounts): number => (c.found > 0 ? c.inbox / c.found : 0);

/** Pure. Picks the variant prospect mail carries. */
export function chooseOptOutVariant(
  variants: readonly VariantCounts[],
  minFound: number = OPT_OUT_MIN_FOUND_SEEDS,
): OptOutDecision {
  const byVariant = new Map(variants.map((v) => [v.variant, v]));
  const all = OPT_OUT_VARIANTS.map((v) => byVariant.get(v));
  const noScores = Object.fromEntries(OPT_OUT_VARIANTS.map((v) => [v, null])) as Record<OptOutVariant, number | null>;

  const reached = (provider: "google" | "microsoft") =>
    all.every((v) => v !== undefined && v[provider].found >= minFound);

  if (!reached("google")) {
    return { variant: DEFAULT_OPT_OUT_VARIANT, reason: "sample_not_reached", scoredProviders: [], scores: noScores };
  }
  const scoredProviders: ("google" | "microsoft")[] = reached("microsoft") ? ["google", "microsoft"] : ["google"];

  const scores = { ...noScores };
  for (const v of all as VariantCounts[]) {
    const mean = scoredProviders.reduce((sum, p) => sum + rate(v[p]), 0) / scoredProviders.length;
    scores[v.variant] = Math.round(mean * 1000) / 10;
  }
  const exact = (v: OptOutVariant) =>
    scoredProviders.reduce((sum, p) => sum + rate((byVariant.get(v) as VariantCounts)[p]), 0) / scoredProviders.length;
  const best = [...TIE_ORDER].sort((a, b) => exact(b) - exact(a))[0];
  return { variant: best, reason: "measured", scoredProviders, scores };
}

interface VariantDailyRow {
  day: string;
  variant: string;
  recipientEsp: number;
  seeds: number;
  inbox: number;
  spam: number;
}

/** Pure. Folds stored rows (one per day x variant x ESP) into per-variant counts. */
export function foldVariantCounts(rows: readonly Omit<VariantDailyRow, "day">[]): VariantCounts[] {
  return OPT_OUT_VARIANTS.map((variant) => {
    const acc = {
      google: { seeds: 0, inbox: 0, spam: 0 },
      microsoft: { seeds: 0, inbox: 0, spam: 0 },
      microsoftConsumer: { seeds: 0, inbox: 0, spam: 0 },
    };
    for (const r of rows) {
      if (r.variant !== variant) continue;
      const key =
        r.recipientEsp === ESP_GOOGLE
          ? "google"
          : r.recipientEsp === ESP_OUTLOOK
            ? "microsoft"
            : r.recipientEsp === ESP_OUTLOOK_CONSUMER
              ? "microsoftConsumer"
              : null;
      if (!key) continue;
      acc[key].seeds += r.seeds;
      acc[key].inbox += r.inbox;
      acc[key].spam += r.spam;
    }
    return {
      variant,
      google: finishCounts(acc.google),
      microsoft: finishCounts(acc.microsoft),
      microsoftConsumer: finishCounts(acc.microsoftConsumer),
    };
  });
}

function rowsOf<T>(result: unknown): T[] {
  return ((result as { rows?: T[] }).rows ?? []) as T[];
}

/** Every stored (day, variant, ESP) row of the experiment (legacy seeds carry no variant). */
async function loadVariantRows(): Promise<VariantDailyRow[]> {
  const result = await db.execute(sql`
    SELECT to_char(day, 'YYYY-MM-DD') AS day, opt_out_variant AS variant, recipient_esp AS "recipientEsp",
           sum(seeds)::int AS seeds, sum(inbox)::int AS inbox, sum(spam)::int AS spam
    FROM seed_placement_daily
    WHERE opt_out_variant <> ''
    GROUP BY 1, 2, 3
    ORDER BY 1
  `);
  return rowsOf<VariantDailyRow>(result)
    .filter((r) => isOptOutVariant(r.variant))
    .map((r) => ({
      ...r,
      recipientEsp: Number(r.recipientEsp),
      seeds: Number(r.seeds),
      inbox: Number(r.inbox),
      spam: Number(r.spam),
    }));
}

/** The staff read behind the Monitoring chart: totals, the decision, and the daily series. */
export async function loadOptOutComparison(asOf: Date = new Date()) {
  const rows = await loadVariantRows();
  const variants = foldVariantCounts(rows);
  const days = [...new Set(rows.map((r) => r.day))].sort().map((day) => ({
    day,
    variants: foldVariantCounts(rows.filter((r) => r.day === day)),
  }));
  return {
    asOf: asOf.toISOString(),
    minFoundSeedsPerVariant: OPT_OUT_MIN_FOUND_SEEDS,
    defaultVariant: DEFAULT_OPT_OUT_VARIANT,
    decision: chooseOptOutVariant(variants),
    variants,
    days,
  };
}

const CACHE_MS = 10 * 60_000;
let cached: { at: number; decision: OptOutDecision } | null = null;

/**
 * The variant a prospect email carries now. Read once per dispatch run, cached
 * 10 minutes (the series moves once a day). Fails LOUD: a run that cannot read
 * it sends nothing rather than guessing.
 */
export async function currentOptOutVariant(now: number = Date.now()): Promise<OptOutVariant> {
  if (cached && now - cached.at < CACHE_MS) return cached.decision.variant;
  const decision = chooseOptOutVariant(foldVariantCounts(await loadVariantRows()));
  if (cached && cached.decision.variant !== decision.variant) {
    console.log(
      `[instantly-service] opt-out variant switched ${cached.decision.variant} -> ${decision.variant} (${decision.reason}, scores ${JSON.stringify(decision.scores)})`,
    );
  }
  cached = { at: now, decision };
  return decision.variant;
}

/** Tests only. */
export function clearOptOutVariantCache(): void {
  cached = null;
}
