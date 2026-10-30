/**
 * Is the seed placement test still MEASURING? (GANDI-OPEN-1030)
 *
 * No seed test graded a mailbox at Google after 2026-10-25 and nobody saw it:
 * the cron answered 202 every day, the run either declined ("not due") or would
 * have died on a refusing vendor, and the evidence just aged. The lifecycle gate
 * expires evidence after 16 days, so a silent harness ends with the whole fleet
 * out of production.
 *
 * This check is reported every hour to the Monitoring watchdog registry
 * (features-service `POST /internal/box-checks/runs`, drawn red on dashboard v2
 * Monitoring > Box > Watchdogs). Red = BROKEN only:
 *   - the newest test that graded a gate provider (anything but Microsoft) is
 *     older than {@link SEED_TEST_OVERDUE_AGE_DAYS}: the weekly cadence missed;
 *   - or no Google receiver can be read today (their vendor refusing us): the
 *     next test cannot grade the gate, whatever the calendar says;
 *   - or a Microsoft receiver can no longer be read (its token refused for
 *     more than {@link MICROSOFT_RECEIVER_STALE_DAYS} days): prod 2026-10-25,
 *     the Microsoft 365 one read 0 of 227 seeds, refused since 10-19.
 * The registry also turns the check red on its own when this service stops
 * reporting (3 x the hourly cadence). Never an email.
 */

import type { CallerInfo } from "../key-client";
import { fetchTestablePoolEmails } from "../account-lifecycle-sync";
import { currentVendorRefusals } from "../self-send/mailbox-credentials";
import { loadSeedCredentialResolver } from "./credentials";
import { MAX_SEED_RECEIVERS, selectSeedReceivers } from "./seeds";
import { fetchLastSeedTestAt } from "./run";
import { listMicrosoftSeedMailboxes } from "./microsoft-oauth";
import { SEED_TEST_INTERVAL_DAYS } from "./due";

/**
 * Past this age the weekly test MISSED: a test lands every Sunday (interval 6
 * days, then the next non-sending day), so a healthy gap is at most 7 days plus
 * the day the sync takes to read it.
 */
export const SEED_TEST_OVERDUE_AGE_DAYS = SEED_TEST_INTERVAL_DAYS + 2;

/** A Microsoft receiver whose token has not refreshed for this long is broken (refreshed daily after each sync). */
export const MICROSOFT_RECEIVER_STALE_DAYS = 2;

export const SEED_PLACEMENT_CHECK = "seed-placement-test";
export const SEED_PLACEMENT_CHECK_LABEL = "Inbox placement test";
export const SEED_PLACEMENT_CHECK_CADENCE_MINUTES = 60;

export interface SeedPlacementHealthInput {
  /** Newest seed SENT to a gate-grading receiver (any provider but Microsoft). */
  lastGateTestAt: Date | null;
  /** Google receivers the next test could read right now. */
  googleReceivers: number;
  /** Vendors refusing us right now, with their verbatim answer. */
  vendorRefusals: ReadonlyArray<{ vendor: string; error: string; since: string }>;
  /** Every Microsoft receiver on file (a pending or never-authorized one is not a receiver yet). */
  microsoftReceivers: ReadonlyArray<{ email: string; status: string; lastRefreshedAt: string | null; lastError: string | null }>;
  asOf: Date;
}

export interface SeedPlacementHealth {
  ok: boolean;
  reason: string;
  ageDays: number | null;
}

const MS_PER_DAY = 24 * 60 * 60 * 1000;

/** Pure verdict. Every reason is plain words: it is what the hub prints. */
export function judgeSeedPlacementHealth(input: SeedPlacementHealthInput): SeedPlacementHealth {
  const ageDays = input.lastGateTestAt
    ? (input.asOf.getTime() - input.lastGateTestAt.getTime()) / MS_PER_DAY
    : null;
  const lastWords = input.lastGateTestAt
    ? `last test graded at Google on ${input.lastGateTestAt.toISOString().slice(0, 10)} (${ageDays!.toFixed(1)} days ago)`
    : "no test has ever graded a mailbox at Google";
  const problems: string[] = [];

  if (input.googleReceivers === 0) {
    const refusal = input.vendorRefusals
      .map((r) => `${r.vendor} refuses us since ${r.since.slice(0, 16).replace("T", " ")} UTC (${r.error.slice(0, 160)})`)
      .join("; ");
    problems.push(
      `No Google mailbox can receive test emails${refusal ? `: ${refusal}` : ""}. The next test cannot grade any mailbox at Google`,
    );
  }
  for (const r of input.microsoftReceivers) {
    const refreshedAgeDays = r.lastRefreshedAt
      ? (input.asOf.getTime() - new Date(r.lastRefreshedAt).getTime()) / MS_PER_DAY
      : null;
    const dead = r.status === "expired" || (r.status === "authorized" && r.lastError !== null && (refreshedAgeDays === null || refreshedAgeDays > MICROSOFT_RECEIVER_STALE_DAYS));
    if (dead) {
      problems.push(
        `Microsoft test mailbox ${r.email} cannot be read${r.lastRefreshedAt ? ` since ${r.lastRefreshedAt.slice(0, 10)}` : ""}${r.lastError ? ` (${r.lastError.slice(0, 160)})` : ""}`,
      );
    }
  }
  if (ageDays === null || ageDays > SEED_TEST_OVERDUE_AGE_DAYS) {
    problems.push(`The weekly test stopped: ${lastWords}, limit ${SEED_TEST_OVERDUE_AGE_DAYS} days`);
  }

  if (problems.length) return { ok: false, reason: problems.join(". "), ageDays };
  return { ok: true, reason: `Weekly test on time: ${lastWords}`, ageDays };
}

const CALLER: CallerInfo = { method: "GET", path: "/internal/seed-placement/health" };

/** The Google receivers the run would pick right now, by the run's own rule. */
async function countGoogleReceivers(): Promise<number> {
  const [pool, resolve] = await Promise.all([fetchTestablePoolEmails(), loadSeedCredentialResolver(CALLER)]);
  const candidates = [];
  for (const email of pool) {
    const credential = resolve(email);
    if (credential) candidates.push({ email, imapHost: credential.imapHost });
  }
  return selectSeedReceivers(candidates, MAX_SEED_RECEIVERS).length;
}

export async function readSeedPlacementHealth(asOf: Date = new Date()): Promise<SeedPlacementHealth & { googleReceivers: number }> {
  // The receivers first: reading them refreshes the vendor refusal state.
  const googleReceivers = await countGoogleReceivers();
  // The same "last test" the cadence decides on: one that graded a gate provider.
  const [lastGateTestAt, microsoftReceivers] = await Promise.all([fetchLastSeedTestAt(), listMicrosoftSeedMailboxes()]);
  return {
    ...judgeSeedPlacementHealth({ lastGateTestAt, googleReceivers, vendorRefusals: currentVendorRefusals(), microsoftReceivers, asOf }),
    googleReceivers,
  };
}

/** Deliver one run of the check to the Monitoring watchdog registry. Throws on failure. */
export async function reportSeedPlacementHealth(asOf: Date = new Date()): Promise<SeedPlacementHealth> {
  const url = process.env.FEATURES_SERVICE_URL;
  const key = process.env.FEATURES_SERVICE_API_KEY;
  if (!url || !key) throw new Error("FEATURES_SERVICE_URL / FEATURES_SERVICE_API_KEY not set");

  const health = await readSeedPlacementHealth(asOf);
  const run = {
    check: SEED_PLACEMENT_CHECK,
    label: SEED_PLACEMENT_CHECK_LABEL,
    ranAt: asOf.toISOString(),
    ok: health.ok,
    reason: health.reason,
    cadenceMinutes: SEED_PLACEMENT_CHECK_CADENCE_MINUTES,
    ...(health.ageDays === null ? {} : { value: Math.round(health.ageDays * 10) / 10, unit: "days" }),
    details: { googleReceivers: health.googleReceivers, overdueAfterDays: SEED_TEST_OVERDUE_AGE_DAYS },
  };
  const response = await fetch(`${url}/internal/box-checks/runs`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": key },
    body: JSON.stringify({ runs: [run] }),
  });
  if (!response.ok) {
    throw new Error(`features-service POST /internal/box-checks/runs failed: ${response.status} - ${(await response.text()).slice(0, 200)}`);
  }
  return health;
}
