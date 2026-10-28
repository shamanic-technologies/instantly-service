/**
 * Why a send left WHEN it did, as a self-contained document (FACTORY-RECORD-SEND-1028).
 *
 * Pure. The dispatch worker calls it for every email it sends, with the very
 * objects the selection decided on: the queued sequence (cadence, zone, verdict,
 * answer owed), the capacity rows of every alias of the sending mailbox (limits,
 * ramp volume, sends already made today incl. warmup), the selection's own room
 * figure (`mailboxRoom`, THE computation `selectDueSteps` spends) and the rules
 * in force. Stored beside the exact message (`send-record.ts`) so anyone can
 * re-run the decision for that send and compare.
 *
 * It RECORDS, it never decides: nothing here is read back by the send path.
 */

import { RAMP_FLOOR_PER_DAY, RAMP_GROWTH_FACTOR, RAMP_VOLUME_WINDOW_DAYS } from "../account-lifecycle";
import { isSendingDay, SENDING_WEEKDAYS } from "../sending-calendar";
import { STEP_GAP_CALENDAR_DAYS, delayForGap } from "../sending-forecast";
import {
  DEFAULT_LEAD_TIMEZONE,
  SEND_WINDOW_END_HOUR,
  SEND_WINDOW_START_HOUR,
  isWithinLocalSendWindow,
} from "../sending-window";
import {
  isAnswerOwed,
  mailboxRoom,
  type AccountCapacity,
  type DueStep,
  type MailboxRoom,
  type PendingSequence,
} from "./dispatch";

export const SEND_DECISION_SCHEMA = "send-decision/v1";

export type SendTrigger = "sweep" | "answer_now";

export interface SendDecision {
  schema: typeof SEND_DECISION_SCHEMA;
  trigger: SendTrigger;
  /** The `asOf` the selection ran at (every gate below is read at it). */
  selectedAt: string;
  /** When the SMTP server accepted the message. */
  sentAt: string;
  step: {
    step: number;
    dueAt: string;
    isFirstEmail: boolean;
    /** First email answering a person's own action: no window, no Sunday gate. */
    answerOwed: boolean;
    /** The mailbox a first email was moved off, null when it was not moved. */
    rehomedFrom: string | null;
    cadence:
      | { rule: "first_email_due_when_queued"; queuedAt: string | null }
      | {
          rule: "gap_after_last_sent_step";
          lastSentStep: number;
          lastSentAt: string;
          /** Sum of `delayForGap` over every hop from the last sent step. */
          gapDays: number;
        };
  };
  window: {
    timezone: string;
    timezoneIsDefault: boolean;
    localStartHour: number;
    localEndHour: number;
    sendingWeekdays: readonly number[];
    isSendingDay: boolean;
    withinLocalWindow: boolean;
  };
  mailbox: {
    address: string;
    /** The real mailbox (SASL login) whose daily quota the send came out of. */
    login: string | null;
    /** Room at the START of the selection, as `selectDueSteps` counted it. */
    room: MailboxRoom | null;
    /** Steps this same selection placed on that mailbox before this one. */
    selectedBeforeInRun: number;
    /** Every alias capacity row of that mailbox, verbatim. */
    aliases: AccountCapacity[];
  };
  /** The queued sequence, verbatim, as the selection read it. */
  sequence: PendingSequence | null;
  rules: {
    rampFloorPerDay: number;
    rampGrowthFactor: number;
    rampVolumeWindowDays: number;
    stepGapCalendarDays: number;
  };
}

export interface SendDecisionInput {
  trigger: SendTrigger;
  step: DueStep;
  sequence: PendingSequence | undefined;
  capacities: readonly AccountCapacity[];
  /** The selection the step came from, in order (for `selectedBeforeInRun`). */
  selected: readonly DueStep[];
  selectedAt: Date;
  sentAt: Date;
}

export function buildSendDecision(input: SendDecisionInput): SendDecision {
  const { step, sequence, capacities, selectedAt } = input;

  const loginOf = new Map(capacities.map((c) => [c.accountEmail, c.mailbox]));
  const login = loginOf.get(step.accountEmail) ?? null;
  const aliases = login === null ? [] : capacities.filter((c) => c.mailbox === login);

  const position = input.selected.indexOf(step);
  const before = position < 0 ? input.selected : input.selected.slice(0, position);
  const selectedBeforeInRun =
    login === null ? 0 : before.filter((s) => loginOf.get(s.accountEmail) === login).length;

  const lastSentStep = sequence?.lastSentStep ?? null;
  const lastSentAt = sequence?.lastSentAt ?? null;
  const cadence: SendDecision["step"]["cadence"] =
    lastSentStep === null || lastSentAt === null
      ? { rule: "first_email_due_when_queued", queuedAt: sequence?.queuedAt?.toISOString() ?? null }
      : {
          rule: "gap_after_last_sent_step",
          lastSentStep,
          lastSentAt: lastSentAt.toISOString(),
          gapDays: gapDays(lastSentStep, step.step, sequence?.stepDelays ?? []),
        };

  const zone = sequence?.timezone ?? null;

  return {
    schema: SEND_DECISION_SCHEMA,
    trigger: input.trigger,
    selectedAt: selectedAt.toISOString(),
    sentAt: input.sentAt.toISOString(),
    step: {
      step: step.step,
      dueAt: step.dueAt.toISOString(),
      isFirstEmail: lastSentStep === null,
      answerOwed: sequence !== undefined && isAnswerOwed(sequence),
      rehomedFrom: step.rehomedFrom ?? null,
      cadence,
    },
    window: {
      timezone: zone ?? DEFAULT_LEAD_TIMEZONE,
      timezoneIsDefault: zone === null,
      localStartHour: SEND_WINDOW_START_HOUR,
      localEndHour: SEND_WINDOW_END_HOUR,
      sendingWeekdays: SENDING_WEEKDAYS,
      isSendingDay: isSendingDay(selectedAt),
      withinLocalWindow: isWithinLocalSendWindow(selectedAt, zone),
    },
    mailbox: {
      address: step.accountEmail,
      login,
      room: login === null ? null : (mailboxRoom(capacities).get(login) ?? null),
      selectedBeforeInRun,
      aliases,
    },
    sequence: sequence ?? null,
    rules: {
      rampFloorPerDay: RAMP_FLOOR_PER_DAY,
      rampGrowthFactor: RAMP_GROWTH_FACTOR,
      rampVolumeWindowDays: RAMP_VOLUME_WINDOW_DAYS,
      stepGapCalendarDays: STEP_GAP_CALENDAR_DAYS,
    },
  };
}

function gapDays(fromStep: number, toStep: number, stepDelays: readonly (number | null)[]): number {
  let days = 0;
  for (let hop = fromStep; hop < toStep; hop += 1) days += delayForGap(hop, stepDelays);
  return days;
}
