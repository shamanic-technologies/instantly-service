/**
 * The lifecycle rules, served as data.
 *
 * "What does in_production mean, and what moves an account between states?" is
 * answered by `deriveLifecycle` and the constants beside it. A dashboard that
 * hand-writes the same sentences drifts the day a bar moves; this serves the
 * constants THEMSELVES, so the rendered explanation is the decision's own
 * inputs. A test pins every number here to its source constant.
 */

import {
  DELIVERY_EVIDENCE_MAX_AGE_DAYS,
  IN_PRODUCTION_DAILY_LIMIT,
  IN_PRODUCTION_WARMUP_DAILY,
  MATURE_AGE_DAYS,
  PRODUCTION_DELIVERY_PCT_BAR,
  PRODUCTION_HEALTH_BAR,
  RAMP_FLOOR_PER_DAY,
  RAMP_GROWTH_FACTOR,
  RAMP_VOLUME_WINDOW_DAYS,
  RECOVERY_DAILY_LIMIT,
  RECOVERY_WARMUP_DAILY,
} from "../account-lifecycle";
import { SEED_EVIDENCE_URGENT_AGE_DAYS, SEED_TEST_INTERVAL_DAYS } from "../seed-placement/due";
import {
  WARMUP_MAX_PER_DAY,
  WARMUP_MAX_SHARE_OF_CAP,
  WARMUP_PARTNERS_PER_DAY,
} from "../warmup/plan";
import { TESTABLE_MIN_AGE_DAYS } from "../account-lifecycle-sync";

export interface LifecycleRuleState {
  status: string;
  meaning: string;
  newSends: boolean;
  campaignDailyLimit: number | null;
  warmupDaily: number | null;
}

export interface LifecycleRules {
  /** First match wins, in this order. */
  order: Array<{ rule: string; leadsTo: string; appliesTo: "all" | "instantly-transport-only" }>;
  states: LifecycleRuleState[];
  bars: {
    healthEntryBar: number;
    deliveryPctBar: number;
    deliveryEvidenceMaxAgeDays: number;
  };
  ramp: {
    floorPerDay: number;
    growthFactor: number;
    volumeWindowDays: number;
    ceiling: number;
    matureAgeDays: number;
    statistic: string;
  };
  placement: {
    testableMinAgeDays: number;
    seedTestIntervalDays: number;
    seedEvidenceUrgentAgeDays: number;
  };
  warmup: {
    partnersPerDay: number;
    maxPerDay: number;
    maxShareOfCap: number;
  };
}

export function lifecycleRules(): LifecycleRules {
  return {
    order: [
      { rule: "domain listed in instantly_domain_policy (a brand/product domain)", leadsTo: "deactivated_by_user", appliesTo: "all" },
      { rule: "Instantly reports the account disabled (status <= 0)", leadsTo: "deactivated_by_instantly", appliesTo: "instantly-transport-only" },
      { rule: `warmup health score below ${PRODUCTION_HEALTH_BAR} AND not already in_production (entry bar only)`, leadsTo: "in_recovery", appliesTo: "instantly-transport-only" },
      { rule: `latest placement test pools below ${PRODUCTION_DELIVERY_PCT_BAR}% inbox, or never tested, or evidence older than ${DELIVERY_EVIDENCE_MAX_AGE_DAYS} days (unless nothing can refresh it)`, leadsTo: "in_recovery", appliesTo: "all" },
      { rule: "otherwise", leadsTo: "in_production", appliesTo: "all" },
    ],
    states: [
      { status: "in_production", meaning: "Eligible for NEW sequences. Only a failed or stale placement test demotes it.", newSends: true, campaignDailyLimit: IN_PRODUCTION_DAILY_LIMIT, warmupDaily: IN_PRODUCTION_WARMUP_DAILY },
      { status: "in_recovery", meaning: "Not offered new sequences; keeps draining what it already holds at a reduced cap while it re-earns a passing placement test.", newSends: false, campaignDailyLimit: RECOVERY_DAILY_LIMIT, warmupDaily: RECOVERY_WARMUP_DAILY },
      { status: "deactivated_by_instantly", meaning: "Instantly turned it off; limits untouched, re-tried by the hourly reactivation when the refusal was transient.", newSends: false, campaignDailyLimit: null, warmupDaily: null },
      { status: "deactivated_by_user", meaning: "A brand/product domain pulled out of cold outreach by policy. Never auto-promoted; still measured.", newSends: false, campaignDailyLimit: null, warmupDaily: RECOVERY_WARMUP_DAILY },
    ],
    bars: {
      healthEntryBar: PRODUCTION_HEALTH_BAR,
      deliveryPctBar: PRODUCTION_DELIVERY_PCT_BAR,
      deliveryEvidenceMaxAgeDays: DELIVERY_EVIDENCE_MAX_AGE_DAYS,
    },
    ramp: {
      floorPerDay: RAMP_FLOOR_PER_DAY,
      growthFactor: RAMP_GROWTH_FACTOR,
      volumeWindowDays: RAMP_VOLUME_WINDOW_DAYS,
      ceiling: IN_PRODUCTION_DAILY_LIMIT,
      matureAgeDays: MATURE_AGE_DAYS,
      statistic: "second-highest daily volume in the window (outreach + warmup + seeds), per real mailbox",
    },
    placement: {
      testableMinAgeDays: TESTABLE_MIN_AGE_DAYS,
      seedTestIntervalDays: SEED_TEST_INTERVAL_DAYS,
      seedEvidenceUrgentAgeDays: SEED_EVIDENCE_URGENT_AGE_DAYS,
    },
    warmup: {
      partnersPerDay: WARMUP_PARTNERS_PER_DAY,
      maxPerDay: WARMUP_MAX_PER_DAY,
      maxShareOfCap: WARMUP_MAX_SHARE_OF_CAP,
    },
  };
}
