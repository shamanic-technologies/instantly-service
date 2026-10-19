/**
 * Read side of `microsoft-placement.ts`: each sending address's latest
 * Microsoft seed test, with its verdict. MS-SEED-PLACEMENT-1018.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { ESP_OUTLOOK } from "./seed-placement/seeds";
import { microsoftPlacementVerdict, type MicrosoftPlacementVerdict } from "./microsoft-placement";


/** One mailbox's Microsoft placement, served beside its Google score. */
export interface MicrosoftPlacement {
  verdict: MicrosoftPlacementVerdict;
  /** Seeds of the latest Microsoft test: sent, in Inbox, in Junk. */
  seedTotal: number;
  inboxCount: number;
  spamCount: number;
  /** inbox / (inbox + junk), 0..100; null when no seed was found. */
  inboxPctOfFound: number | null;
  testedAt: string;
}

/** Latest Microsoft test per sending address. Absent = never measured at Microsoft. */
export async function fetchMicrosoftPlacementByAccount(
  asOf: Date = new Date(),
): Promise<Map<string, MicrosoftPlacement>> {
  const result = await db.execute(sql`
    SELECT DISTINCT ON (account_email)
      account_email AS "accountEmail", seed_total::int AS "seedTotal",
      inbox_count::int AS "inboxCount", spam_count::int AS "spamCount", tested_at AS "testedAt"
    FROM instantly_placement_results
    WHERE recipient_esp = ${ESP_OUTLOOK}
    ORDER BY account_email, tested_at DESC, test_id DESC
  `);
  const out = new Map<string, MicrosoftPlacement>();
  for (const r of ((result as { rows?: Record<string, unknown>[] }).rows ?? [])) {
    const inboxCount = Number(r.inboxCount);
    const spamCount = Number(r.spamCount);
    const testedAt = new Date(r.testedAt as string);
    const found = inboxCount + spamCount;
    out.set(String(r.accountEmail).toLowerCase(), {
      verdict: microsoftPlacementVerdict({ inboxCount, spamCount, testedAt }, asOf),
      seedTotal: Number(r.seedTotal),
      inboxCount,
      spamCount,
      inboxPctOfFound: found > 0 ? Math.round((inboxCount * 100) / found) : null,
      testedAt: testedAt.toISOString(),
    });
  }
  return out;
}
