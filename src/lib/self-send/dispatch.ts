/**
 * Deciding WHAT to send now — pure, so the scheduling rules are testable without
 * a database or a mail server.
 *
 * The queue is not a new table: it is the set of `sequence_costs` rows still
 * `provisioned`, which is already what the fleet forecast and the per-account
 * queue breakdown read. Cadence is the shared `delayForGap` over the delays
 * persisted in `sequence_steps`, and the caps are the same `rampCapForVolume` /
 * `dailyLimitForStatus` the Instantly path already enforces. This module only
 * picks; it performs no IO and sends nothing.
 */

import { rampCapForVolume } from "../account-lifecycle";
import { isSendingDay } from "../sending-calendar";
import { delayForGap } from "../sending-forecast";
import { isWithinLocalSendWindow } from "../sending-window";

const MS_PER_DAY = 24 * 60 * 60 * 1000;

/** One lead's outstanding sequence, as read from the cost ledger. */
export interface PendingSequence {
  instantlyCampaignId: string;
  leadEmail: string;
  accountEmail: string;
  /** Steps still `provisioned`, i.e. not yet sent. */
  provisionedSteps: readonly number[];
  /** Highest step with a real send, null when nothing has gone out yet. */
  lastSentStep: number | null;
  /** When that step went out. Null when nothing has gone out yet. */
  lastSentAt: Date | null;
  /** 0-based delays from `sequence_steps`, ordered by step. */
  stepDelays: readonly (number | null)[];
  /**
   * The lead's IANA timezone, as persisted on the campaign row. Null when we
   * hold none; the fleet default then applies — the same one the Instantly
   * schedule degrades to, so both transports treat such a lead identically.
   */
  timezone?: string | null;
  /**
   * When the lead was handed to us — the campaign row's `created_at`. The FIRST
   * email of a sequence came due at this instant, not "now".
   *
   * ⚠️ Load-bearing for the ordering. When this was absent a never-sent first
   * email was stamped `dueAt = asOf`, i.e. the LEAST overdue thing in every run,
   * so on a mailbox short of room every overdue followup sorted ahead of it,
   * forever. Prod 2026-09-24: 678 first emails never sent, the oldest handed to
   * us on 2026-08-30, parked on mailboxes that spent their whole cap on
   * followups each day. Optional so a caller that has not loaded it keeps the
   * old behaviour.
   */
  queuedAt?: Date | null;
}

export interface DueStep {
  instantlyCampaignId: string;
  leadEmail: string;
  accountEmail: string;
  step: number;
  /** When this step became due. Earlier = more overdue = sent first. */
  dueAt: Date;
  /**
   * Set when this is a FIRST email moved off the mailbox it was assigned to,
   * because that mailbox could not send it this run — see {@link selectDueSteps}.
   * `accountEmail` is then the NEW sender; this is the one it was parked on.
   * The worker persists the move (and its reason) before sending.
   */
  rehomedFrom?: string;
}

/**
 * The next step to send for one lead, and when it came due.
 *
 * At most ONE step per lead per run. Two emails of the same sequence must never
 * go out together — that is not a throughput limit, it is the sequence itself:
 * a followup only makes sense after the previous one has had its gap.
 *
 * A lead that has never been sent to is due IMMEDIATELY: the lead was pushed to
 * us to be contacted, and there is no prior step to measure a gap from. Anchoring
 * it on "now" for a future date would invent a delay nobody configured.
 */
export function nextDueStep(sequence: PendingSequence, asOf: Date): DueStep | null {
  const pending = [...sequence.provisionedSteps].sort((a, b) => a - b);
  if (pending.length === 0) return null;

  const step = pending[0]!;

  // Never sent: the first email came due the moment the lead was handed to us.
  // Clamped to `asOf` so a clock skew can never date it in the future.
  if (sequence.lastSentAt === null || sequence.lastSentStep === null) {
    const queuedAt = sequence.queuedAt ?? null;
    return {
      instantlyCampaignId: sequence.instantlyCampaignId,
      leadEmail: sequence.leadEmail,
      accountEmail: sequence.accountEmail,
      step,
      dueAt:
        queuedAt !== null && queuedAt.getTime() < asOf.getTime() ? queuedAt : asOf,
    };
  }

  // A step at or below the last sent one is already done; nothing is due from a
  // ledger that disagrees with itself, and guessing would re-send a real email.
  if (step <= sequence.lastSentStep) return null;

  // Chain every hop from the last sent step up to this one, so a step two hops
  // out waits for both gaps rather than just the immediate one.
  let days = 0;
  for (let hop = sequence.lastSentStep; hop < step; hop += 1) {
    days += delayForGap(hop, sequence.stepDelays);
  }

  const dueAt = new Date(sequence.lastSentAt.getTime() + days * MS_PER_DAY);
  return dueAt.getTime() > asOf.getTime()
    ? null
    : {
        instantlyCampaignId: sequence.instantlyCampaignId,
        leadEmail: sequence.leadEmail,
        accountEmail: sequence.accountEmail,
        step,
        dueAt,
      };
}

/** Room left on one mailbox today. */
export interface AccountCapacity {
  accountEmail: string;
  /**
   * The REAL mailbox this sending address authenticates as — its SMTP/IMAP
   * login, per `loginFor`.
   *
   * ⚠️ Load-bearing, and the reason capacity is not keyed on `accountEmail`.
   * A Gandi domain is typically ONE mailbox carrying several aliases, and we
   * hold a sending account per alias: 154 accounts sit on 44 real mailboxes.
   * Keyed per address, five aliases each get their own cap and the single
   * mailbox behind them is offered five times its quota — which is what the
   * relay answers with `450 4.7.1 Too many mail per day for sasl <user>`,
   * per SASL USER, not per alias. The provider's own limit is at this grain,
   * so ours has to be too.
   *
   * For a Primeforge mailbox the address IS the login, so this equals
   * `accountEmail` and the grouping is a no-op.
   */
  mailbox: string;
  /**
   * The operator-set limit for this address (`daily_limit`), NOT the ramped cap.
   *
   * The ramp is applied at MAILBOX grain below, because it reads volume and the
   * volume of a mailbox is the sum of its aliases'. Ramping each address first
   * and taking the minimum would hold a five-alias mailbox sending 20/day to the
   * cap earned by 4/day — it could never grow, which is the failure the volume
   * ramp exists to remove, re-expressed one level down.
   */
  cap: number;
  /**
   * The highest single-day volume THIS ADDRESS reached over the ramp window
   * (outreach + warmup + seed). Summed across a mailbox's aliases below.
   */
  recentSustainedDaily: number;
  /** Real dispatches already made today (UTC). */
  sentToday: number;
  /**
   * Whether this address may take over a FIRST email another mailbox cannot
   * send — true only for an address in production (the same gate that decides
   * where a NEW sequence is assigned). Absent ⇒ false, so a caller that has not
   * loaded the lifecycle never moves anything.
   */
  adoptsFirstEmails?: boolean;
}

/**
 * Everything due now, ordered and clipped to what each mailbox can still send.
 *
 * Oldest-due first, so the most overdue step goes out before a step that only
 * just came due — the alternative starves a backlog behind fresher work. Ties
 * break on campaign id purely for determinism, so a run is reproducible.
 *
 * An account with no capacity row is treated as having NO room rather than full
 * room. A missing row means we could not establish the account's limits, and
 * inventing capacity there is how a fresh mailbox gets pushed past what Gmail
 * will accept — the exact failure the age ramp exists to prevent.
 */
export interface DueSelection {
  /** What will actually be sent this run. */
  selected: DueStep[];
  /**
   * How many steps were due BEFORE capacity clipped them.
   *
   * ⚠️ Reported because its absence made a throttled worker read as an idle one.
   * The run summary carried only the post-clip count, so `due: 0` meant either
   * "nothing to send" or "nothing had room" and no operator could tell which —
   * which is how a fleet pinned at a cap of 5 while 1,208 first emails waited
   * looked healthy for eight days (prod 2026-09-07).
   */
  dueBeforeCapacity: number;
  /**
   * Steps whose assigned mailbox has NO capacity row at all — we hold no
   * credential for it, so nothing can ever dispatch them. Distinct from being
   * throttled: no cap will grow into these, they need a credential. Silent until
   * now, which is why ~600 sequences sat abandoned without a single log line.
   */
  blockedNoCapacityRow: number;
  /**
   * Steps whose mailbox the relay has been refusing — see `sender-health.ts`.
   *
   * ⚠️ Deliberately NOT folded into `blockedNoCapacityRow`. That one means "we
   * hold no credential"; this one means "we hold one and it is being refused",
   * and the two need opposite responses (get a credential vs fix or retire the
   * mailbox). Reporting a silenced mailbox as uncredentialed sends whoever
   * reads the summary looking for something that is already there.
   */
  skippedSilenced: number;
  /**
   * First emails moved this run from a mailbox that could not send them (no
   * room left today, silenced by the relay, or no credential) onto a production
   * mailbox with room. Each one is in `selected` with `rehomedFrom` set.
   */
  rehomed: number;
}

export function selectDueSteps(
  sequences: readonly PendingSequence[],
  capacities: readonly AccountCapacity[],
  asOf: Date,
  /**
   * Real mailboxes (SASL logins) to skip this run. Empty by default, so the
   * caller that has not loaded the health read behaves exactly as before.
   */
  silencedMailboxes: ReadonlySet<string> = new Set(),
): DueSelection {
  // Nothing goes out on a weekend, matching the Mon-Fri window every campaign in
  // the fleet is created with. Two reasons this is not optional:
  //
  //   - The transport this replaces does not send on weekends, and both run on
  //     the SAME mailboxes. Diverging would change a mailbox's behaviour purely
  //     because of which pipe a lead happened to be assigned to.
  //   - The weekly placement test runs on a Saturday PRECISELY because mailboxes
  //     are otherwise empty that day and can absorb a ~30-50 seed spike. Real
  //     volume on top of that spike is exactly what the Saturday slot avoids.
  //
  // An overdue step simply stays overdue and goes out on the next sending day —
  // the ordering below is most-overdue-first, so Monday drains the backlog in
  // the right order. Deliberately a gate on the RUN rather than a snap on each
  // step's due date: `sending-calendar` is scoped to send selection, and snapping
  // due dates here would drift this module away from the ops projections, which
  // bucket on the raw nominal day on purpose.
  if (!isSendingDay(asOf))
    return {
      selected: [],
      dueBeforeCapacity: 0,
      blockedNoCapacityRow: 0,
      skippedSilenced: 0,
      rehomed: 0,
    };

  // Capacity is spent per REAL MAILBOX, not per sending address — several
  // aliases share one mailbox, one relay login and one reputation, so they share
  // one day's quota. `cap` takes the MINIMUM across the aliases (an operator who
  // lowers one alias means it for the mailbox) while `sentToday` SUMS them (every
  // alias's send came out of the same quota).
  const mailboxOf = new Map<string, string>();
  // The address a mailbox would send an adopted first email FROM — its first
  // production alias, alphabetically, so a run is reproducible.
  const adopterAddressByMailbox = new Map<string, string>();
  const limitByMailbox = new Map<string, number>();
  const peakByMailbox = new Map<string, number>();
  const sentByMailbox = new Map<string, number>();

  for (const capacity of capacities) {
    mailboxOf.set(capacity.accountEmail, capacity.mailbox);
    if (capacity.adoptsFirstEmails === true) {
      const known = adopterAddressByMailbox.get(capacity.mailbox);
      if (known === undefined || capacity.accountEmail < known) {
        adopterAddressByMailbox.set(capacity.mailbox, capacity.accountEmail);
      }
    }
    const knownLimit = limitByMailbox.get(capacity.mailbox);
    limitByMailbox.set(
      capacity.mailbox,
      knownLimit === undefined ? capacity.cap : Math.min(knownLimit, capacity.cap),
    );
    // Volume SUMS where the operator limit takes the MINIMUM, and the asymmetry
    // is the point: every alias's send came out of the one quota, so the mailbox
    // has demonstrably carried their total — while an operator who lowers one
    // alias means it for the mailbox behind it.
    peakByMailbox.set(
      capacity.mailbox,
      (peakByMailbox.get(capacity.mailbox) ?? 0) + capacity.recentSustainedDaily,
    );
    sentByMailbox.set(
      capacity.mailbox,
      (sentByMailbox.get(capacity.mailbox) ?? 0) + capacity.sentToday,
    );
  }

  const remaining = new Map<string, number>();
  for (const [mailbox, limit] of limitByMailbox) {
    const cap = Math.min(limit, rampCapForVolume(peakByMailbox.get(mailbox) ?? 0, limit));
    remaining.set(mailbox, Math.max(0, cap - (sentByMailbox.get(mailbox) ?? 0)));
  }

  const sequenceByCampaign = new Map(
    sequences.map((sequence) => [sequence.instantlyCampaignId, sequence]),
  );

  const due = sequences
    // A step due by cadence still waits for its prospect's business hours. The
    // Instantly transport gets this from the campaign schedule it dispatches
    // against; here we are the scheduler, so the gate has to be ours. Without
    // it a lead's first email fires at whatever hour the hourly cron happens to
    // run — 03:00 local for anyone far enough east — purely because their
    // mailbox was flipped to this pipe.
    //
    // Note this is STRICTER than the UTC gate above, never looser: a lead whose
    // local window opens while it is still the weekend here is held to the next
    // UTC sending day. Capacity books the earlier of the two, so such a send can
    // arrive on its booked day or after it, never before — the same one-sided
    // slip the nominal-cadence projection already carries.
    .filter((sequence) => isWithinLocalSendWindow(asOf, sequence.timezone))
    .map((sequence) => nextDueStep(sequence, asOf))
    .filter((step): step is DueStep => step !== null)
    .sort(
      (a, b) =>
        a.dueAt.getTime() - b.dueAt.getTime() ||
        a.instantlyCampaignId.localeCompare(b.instantlyCampaignId),
    );

  const selected: DueStep[] = [];
  // First emails whose own mailbox cannot send them this run, in due order.
  const stranded: DueStep[] = [];
  let blockedNoCapacityRow = 0;
  let skippedSilenced = 0;

  const isFirstEmail = (step: DueStep): boolean => {
    const sequence = sequenceByCampaign.get(step.instantlyCampaignId);
    return sequence !== undefined && sequence.lastSentStep === null;
  };

  for (const step of due) {
    // No capacity row ⇒ no mailbox ⇒ no room, per the invariant above. An
    // account whose limits we could not establish must not be sent from.
    const mailbox = mailboxOf.get(step.accountEmail);
    if (mailbox === undefined) {
      blockedNoCapacityRow += 1;
      if (isFirstEmail(step)) stranded.push(step);
      continue;
    }

    // The relay is refusing this mailbox and has accepted nothing from it. Its
    // steps stay provisioned and overdue — which means they sort FIRST on every
    // run, so without this they consume the run ahead of every mailbox that
    // works. Counted, never silently dropped.
    if (silencedMailboxes.has(mailbox)) {
      skippedSilenced += 1;
      if (isFirstEmail(step)) stranded.push(step);
      continue;
    }

    const room = remaining.get(mailbox) ?? 0;
    if (room <= 0) {
      if (isFirstEmail(step)) stranded.push(step);
      continue;
    }
    selected.push(step);
    remaining.set(mailbox, room - 1);
  }

  // ── Second pass: move stranded FIRST emails to a production mailbox with room.
  //
  // A sequence is pinned to its mailbox for its whole life because its followups
  // must thread from the same sender — but until the first email goes out there
  // is no thread, so the pin protects nothing and only strands the lead. Prod
  // 2026-09-24: 678 first emails waiting, some since 2026-08-30, on recovering
  // Gandi mailboxes capped at 20/day and carrying 100+ sequences each, while the
  // production fleet had ~3,700 unused sends a day. Waiting for room that
  // arrives at 20/day is not a queue, it is a loss the customer cannot see.
  //
  // A FOLLOWUP is never moved: its prospect has already heard from one mailbox.
  // Only a production mailbox that is not silenced adopts — the same gate the
  // assignment of a NEW sequence applies — and only with room left AFTER its own
  // due steps were served, so moving a lead never delays a sequence already
  // riding the target. Most room first, then mailbox name, for determinism.
  let rehomed = 0;
  for (const step of stranded) {
    let target: string | null = null;
    let targetRoom = 0;
    for (const mailbox of adopterAddressByMailbox.keys()) {
      if (silencedMailboxes.has(mailbox)) continue;
      if (mailbox === mailboxOf.get(step.accountEmail)) continue;
      const room = remaining.get(mailbox) ?? 0;
      if (room <= 0) continue;
      if (target === null || room > targetRoom || (room === targetRoom && mailbox < target)) {
        target = mailbox;
        targetRoom = room;
      }
    }
    // Nobody has room left: the rest stay where they are, counted as due.
    if (target === null) break;
    selected.push({
      ...step,
      accountEmail: adopterAddressByMailbox.get(target)!,
      rehomedFrom: step.accountEmail,
    });
    remaining.set(target, targetRoom - 1);
    rehomed += 1;
  }

  return {
    selected,
    dueBeforeCapacity: due.length,
    blockedNoCapacityRow,
    skippedSilenced,
    rehomed,
  };
}

// ─── Failure semantics ────────────────────────────────────────────────────────

/**
 * Who a permanent rejection is actually about.
 *
 * This distinction is load-bearing and easy to get wrong. A 5xx is permanent
 * either way, but:
 *
 *   - `recipient` — the address is bad (no such user, dead domain, mailbox full).
 *     That IS a bounce: mark the lead bounced and stop its sequence.
 *
 *   - `sender` — WE were refused (daily sending limit, policy block, reputation).
 *     The prospect's address is fine and untested. Recording this as a bounce
 *     would poison the lead's record with a fact about our own mailbox, and mark
 *     a perfectly reachable prospect as undeliverable forever.
 *
 * This repo has already paid for the confusion in the other direction: a prod
 * audit found 9 of 12 deactivated accounts were refused over `Recipient address
 * rejected: Domain not found` — a list-hygiene problem misfiled as a sender
 * problem. Same axis, opposite mistake.
 *
 * RFC 3463 gives the answer directly: subject `1` is addressing and subject `2`
 * is the mailbox, both about the recipient; `4` (network), `5` (protocol), `6`
 * (content) and `7` (policy/security) are not. With no enhanced code, the basic
 * reply is read the same way — 550/551/553 are classically "no such recipient",
 * while everything else defaults to `sender`, the side that never poisons lead
 * data.
 */
export type PermanentFailureSubject = "recipient" | "sender";

export function classifyPermanentFailure(
  response: string,
  responseCode: number | null,
): PermanentFailureSubject {
  // The separator may be a HYPHEN, not just whitespace: SMTP's multiline
  // continuation form puts the enhanced code straight after it, and Gmail's
  // daily-limit refusal arrives exactly that way — `550-5.4.5 Daily user
  // sending limit exceeded`. Requiring whitespace here silently classified the
  // single most common sender-side throttle as a recipient bounce, which would
  // have marked reachable prospects undeliverable forever. A leading digit still
  // cannot match, since a digit is neither whitespace nor a hyphen.
  const enhanced = /(?:^|[\s-])5\.(\d+)\.\d+/.exec(response);
  if (enhanced?.[1]) {
    const subject = Number(enhanced[1]);
    return subject === 1 || subject === 2 ? "recipient" : "sender";
  }

  if (responseCode === 550 || responseCode === 551 || responseCode === 553) {
    // A bare 550 with no enhanced code is classically "no such user". Gmail's
    // sender-side refusals (5.4.5 daily limit, 5.7.x policy) always carry one,
    // so they are caught above and never reach here.
    return "recipient";
  }

  return "sender";
}
