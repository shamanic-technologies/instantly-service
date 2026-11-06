/**
 * The capacity verdict, served by the producer (SENDING-INFRA-AUDIT-1106 item 11).
 *
 * The page used to compute "Capacity covers every planned send" itself, from the
 * daily capacity and the forecast, while 1,783 follow-ups were overdue and 1,611
 * steps sat on mailboxes outside production. A displayed verdict belongs to the
 * service that owns the queue, so it is computed here, from the queue itself.
 *
 * Red ONLY when something is broken (global rule): a follow-up more than a day
 * late, or a step on a mailbox that cannot send it. A follow-up due yesterday,
 * or a day planned above capacity, is `behind` (amber): it drains by itself.
 */

const DAY_MS = 24 * 60 * 60 * 1000;

export interface VerdictStep {
  account: string;
  isFirstEmail: boolean;
  theoreticalAt: string | null;
  overdue: boolean;
}

export interface CapacityVerdict {
  /** `ok` | `behind` (late by a day at most, or today planned above capacity) | `broken` (red). */
  status: "ok" | "behind" | "broken";
  /** One plain sentence per problem, most serious first; empty when `ok`. */
  reasons: string[];
  /** Follow-ups due before today and not sent. */
  overdueFollowUps: number;
  /** Of those, the ones more than one day late. */
  followUpsLateOverOneDay: number;
  /** When the oldest overdue follow-up was due (ISO), null when none. */
  oldestOverdueFollowUpAt: string | null;
  /** First emails waiting since before today (no thread yet: they start late, nobody is left hanging). */
  overdueFirstEmails: number;
  /** Queued steps on a mailbox that cannot send at all (gone, no credential, refused by the relay, replies unreadable). */
  strandedSteps: number;
  /** Queued steps on mailboxes outside production (they still send, at a reduced cap). */
  queuedOnNonProduction: number;
  /** Steps owed by the end of today (late ones included), at their theoretical dates, against the fleet's capacity. */
  scheduledToday: number;
  dailyCapacity: number;
}

export function capacityVerdict(input: {
  steps: readonly VerdictStep[];
  asOf: Date;
  /** Addresses that cannot send at all (lower-cased). */
  cannotSend: ReadonlySet<string>;
  /** Address -> current lifecycle status (lower-cased keys). */
  lifecycleOf: ReadonlyMap<string, string | null>;
  dailyCapacity: number;
}): CapacityVerdict {
  const startOfToday = Date.UTC(input.asOf.getUTCFullYear(), input.asOf.getUTCMonth(), input.asOf.getUTCDate());
  let overdueFollowUps = 0;
  let lateOverOneDay = 0;
  let oldest: number | null = null;
  let overdueFirstEmails = 0;
  let stranded = 0;
  let nonProduction = 0;
  let scheduledToday = 0;
  for (const s of input.steps) {
    const account = s.account.trim().toLowerCase();
    if (input.cannotSend.has(account)) stranded += 1;
    else if ((input.lifecycleOf.get(account) ?? null) !== "in_production") nonProduction += 1;
    const at = s.theoreticalAt ? Date.parse(s.theoreticalAt) : NaN;
    if (!Number.isNaN(at) && at < startOfToday + DAY_MS) scheduledToday += 1;
    if (Number.isNaN(at) || at >= startOfToday) continue;
    if (s.isFirstEmail) {
      overdueFirstEmails += 1;
      continue;
    }
    overdueFollowUps += 1;
    if (at < startOfToday - DAY_MS) lateOverOneDay += 1;
    if (oldest === null || at < oldest) oldest = at;
  }

  const reasons: string[] = [];
  if (lateOverOneDay > 0) reasons.push(`${lateOverOneDay} follow-ups are more than a day late.`);
  if (stranded > 0) reasons.push(`${stranded} queued emails sit on mailboxes that cannot send.`);
  const broken = reasons.length > 0;
  if (overdueFollowUps > lateOverOneDay) reasons.push(`${overdueFollowUps - lateOverOneDay} follow-ups were due yesterday.`);
  if (scheduledToday > input.dailyCapacity) {
    reasons.push(`${scheduledToday} emails are owed today (late ones included) for a capacity of ${input.dailyCapacity}.`);
  }
  return {
    status: broken ? "broken" : reasons.length > 0 ? "behind" : "ok",
    reasons,
    overdueFollowUps,
    followUpsLateOverOneDay: lateOverOneDay,
    oldestOverdueFollowUpAt: oldest === null ? null : new Date(oldest).toISOString(),
    overdueFirstEmails,
    strandedSteps: stranded,
    queuedOnNonProduction: nonProduction,
    scheduledToday,
    dailyCapacity: input.dailyCapacity,
  };
}
