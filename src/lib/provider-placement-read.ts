/**
 * Read side of `provider-placement.ts`: each sending address's placement at
 * Google, Microsoft 365 and Outlook.com, pooled from our seed tests.
 * SENDER-HEALTH-1019 (was `microsoft-placement-read.ts`, MS-SEED-PLACEMENT-1018).
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { DELIVERY_EVIDENCE_MAX_AGE_DAYS } from "./account-lifecycle";
import {
  poolPlacementTests,
  providerPlacement,
  type PlacementTest,
  type PlacementVerdict,
  type RecipientProvider,
} from "./provider-placement";
import { ESP_GOOGLE, ESP_OUTLOOK, ESP_OUTLOOK_CONSUMER } from "./seed-placement/seeds";

const PROVIDER_OF_ESP = new Map<number, RecipientProvider>([
  [ESP_GOOGLE, "google"],
  [ESP_OUTLOOK, "microsoft"],
  [ESP_OUTLOOK_CONSUMER, "microsoft_consumer"],
]);

/** One mailbox's placement at one provider, as served and as routed on. */
export interface ProviderPlacementDetail {
  verdict: PlacementVerdict;
  mostlySpam: boolean;
  /** Pooled evidence (newest tests until 10 seeds, `poolPlacementTests`). */
  seedTotal: number;
  inboxCount: number;
  spamCount: number;
  /** inbox / (inbox + spam), 0..100; null when no seed was found. */
  inboxPctOfFound: number | null;
  /** When the newest pooled test ran. */
  testedAt: string;
  testsPooled: number;
}

/** Kept for the served `microsoftPlacement` field (Microsoft 365 business tenant). */
export type MicrosoftPlacement = ProviderPlacementDetail;

export type PlacementByProvider = Partial<Record<RecipientProvider, ProviderPlacementDetail>>;

/** Pure: shape silver rows into a per-address, per-provider placement. */
export function buildPlacementByProvider(
  rows: readonly { accountEmail: string; recipientEsp: number; test: PlacementTest }[],
  asOf: Date,
): Map<string, PlacementByProvider> {
  const tests = new Map<string, Map<RecipientProvider, PlacementTest[]>>();
  for (const r of rows) {
    const provider = PROVIDER_OF_ESP.get(r.recipientEsp);
    if (!provider) continue;
    const email = r.accountEmail.trim().toLowerCase();
    const byProvider = tests.get(email) ?? new Map<RecipientProvider, PlacementTest[]>();
    byProvider.set(provider, [...(byProvider.get(provider) ?? []), r.test]);
    tests.set(email, byProvider);
  }
  const out = new Map<string, PlacementByProvider>();
  for (const [email, byProvider] of tests) {
    const entry: PlacementByProvider = {};
    for (const [provider, list] of byProvider) {
      const pooled = poolPlacementTests(list, asOf);
      if (!pooled) continue;
      const found = pooled.inboxCount + pooled.spamCount;
      entry[provider] = {
        ...providerPlacement(pooled, asOf),
        seedTotal: pooled.seedTotal,
        inboxCount: pooled.inboxCount,
        spamCount: pooled.spamCount,
        inboxPctOfFound: found > 0 ? Math.round((pooled.inboxCount * 100) / found) : null,
        testedAt: pooled.testedAt.toISOString(),
        testsPooled: pooled.testIds.length,
      };
    }
    out.set(email, entry);
  }
  return out;
}

/**
 * Every address's placement per provider. Only tests younger than the
 * evidence limit are read: older evidence is `unmeasured` anyway.
 */
export async function fetchPlacementByProvider(asOf: Date = new Date()): Promise<Map<string, PlacementByProvider>> {
  const result = await db.execute(sql`
    SELECT account_email AS "accountEmail", recipient_esp::int AS "recipientEsp", test_id AS "testId",
           tested_at AS "testedAt", seed_total::int AS "seedTotal",
           inbox_count::int AS "inboxCount", spam_count::int AS "spamCount"
    FROM instantly_placement_results
    WHERE recipient_esp IN (${ESP_GOOGLE}, ${ESP_OUTLOOK}, ${ESP_OUTLOOK_CONSUMER})
      AND tested_at > ${asOf.toISOString()}::timestamptz AT TIME ZONE 'UTC' - make_interval(days => ${DELIVERY_EVIDENCE_MAX_AGE_DAYS + 1})
  `);
  const rows = ((result as { rows?: Record<string, unknown>[] }).rows ?? []).map((r) => ({
    accountEmail: String(r.accountEmail),
    recipientEsp: Number(r.recipientEsp),
    test: {
      testId: String(r.testId),
      testedAt: toUtcDate(r.testedAt),
      seedTotal: Number(r.seedTotal),
      inboxCount: Number(r.inboxCount),
      spamCount: Number(r.spamCount),
    },
  }));
  return buildPlacementByProvider(rows, asOf);
}

/** `tested_at` is a naive UTC timestamp; never read it as local time. */
function toUtcDate(value: unknown): Date {
  if (value instanceof Date) return value;
  const s = String(value);
  const iso = s.includes("T") ? s : s.replace(" ", "T");
  return new Date(/[Zz]|[+-]\d{2}:?\d{2}$/.test(iso) ? iso : `${iso}Z`);
}
