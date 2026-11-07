/**
 * MIGRATE-1105 (message_queued): the decision turning a written sequence into the step rows the sender dispatches
 * (`POST /orgs/send` 6b), asked of a stored factory Script through the shared service switch (`service-script.ts`).
 *
 * The DECISION (pure, `decideQueuedSteps`): the subject the rows carry (the follow-up thread's `Re: <subject>` when
 * the email continues that thread from its own mailbox on our sender, else the caller's) and the rows themselves
 * (`stepRowsFromSendPayload`: renumbered, delay shifted by one, subject on step 1 only). Everything around it stays
 * in the route: the account, the transport, the reservation, the DB writes, the costs.
 *
 * ONE decision per enrolled sequence, under our platform run `queue-decision`.
 */
import { threadSubjectFor, type FollowUpThread } from "./follow-up-send";
import { SEND_TRANSPORT_SMTP } from "./self-send/transport";
import { stepRowsFromSendPayload, type InboundSequenceStep, type SequenceStep } from "./self-send/sequence-steps";
import { decideThroughScript, type DecideDeps, type ServiceScriptEngine } from "./service-script";

/** The Engine the queue decision's script makes (MIGRATE-1105 S5: the finest WHAT, the email queued; was message_queued). */
export const MESSAGE_QUEUED_OUTCOME = "email_queued";
export const QUEUE_DECISION_TASK = "queue-decision";

export interface QueuedStepsInput {
  /** The subject the caller sent (`body.subject`). */
  callerSubject: string;
  /** The steps as the caller sent them (sorted by step). */
  sequence: readonly InboundSequenceStep[];
  /** The thread a follow-up continues (null = not a follow-up, or no earlier email of the brand). */
  followUpThread: FollowUpThread | null;
  /** The mailbox chosen for this sequence. */
  sendingAccountEmail: string;
  /** The pipe frozen for this sequence (`smtp` = our own sender). */
  transport: string;
}

export interface QueuedStepsDecision {
  /** The sequence this one continues (set only when it continues that thread). */
  followUpOf: string | null;
  /** `Re: <thread subject>` when it continues the thread, else null (the caller's subject is used). */
  threadSubject: string | null;
  steps: SequenceStep[];
}

/** Our own decision (pure). */
export function decideQueuedSteps(input: QueuedStepsInput): QueuedStepsDecision {
  const threadSubject = input.transport === SEND_TRANSPORT_SMTP ? threadSubjectFor(input.followUpThread, input.sendingAccountEmail) : null;
  return {
    followUpOf: threadSubject ? input.followUpThread!.instantlyCampaignId : null,
    threadSubject,
    steps: stepRowsFromSendPayload(threadSubject ?? input.callerSubject, input.sequence),
  };
}

/** The decision's inputs as the script reads them. */
export function queuedStepsMaterial(input: QueuedStepsInput): Record<string, unknown> {
  const t = input.followUpThread;
  return {
    callerSubject: input.callerSubject,
    sequence: input.sequence.map((s) => ({ step: s.step, bodyHtml: s.bodyHtml, daysSinceLastStep: s.daysSinceLastStep })),
    followUpThread: t ? { instantlyCampaignId: t.instantlyCampaignId, accountEmail: t.accountEmail, subject: t.subject, reason: t.reason } : null,
    sendingAccountEmail: input.sendingAccountEmail,
    transport: input.transport,
  };
}

/** A decision in the words both sides are compared in. */
export function queuedStepsWords(d: QueuedStepsDecision): Record<string, unknown> {
  return {
    followUpOf: d.followUpOf,
    threadSubject: d.threadSubject,
    steps: d.steps.map((s) => ({ step: s.step, subject: s.subject, bodyHtml: s.bodyHtml, delayDays: s.delayDays })),
  };
}

const nullableString = (v: unknown, what: string): string | null => {
  if (v === null) return null;
  if (typeof v === "string") return v;
  throw new Error(`the message_queued script answered no ${what}`);
};

/** The script's answer as our own decision. */
export function queuedStepsFromWords(words: Record<string, unknown>): QueuedStepsDecision {
  if (!Array.isArray(words.steps)) throw new Error("the message_queued script answered no steps");
  return {
    followUpOf: nullableString(words.followUpOf, "followUpOf"),
    threadSubject: nullableString(words.threadSubject, "threadSubject"),
    steps: (words.steps as Array<Record<string, unknown>>).map((s) => {
      if (!Number.isInteger(s.step) || typeof s.bodyHtml !== "string") throw new Error("the message_queued script answered a step without step/bodyHtml");
      const delay = s.delayDays;
      if (delay !== null && typeof delay !== "number") throw new Error("the message_queued script answered a non-numeric delayDays");
      return { step: Number(s.step), subject: nullableString(s.subject, "step subject"), bodyHtml: s.bodyHtml, delayDays: delay };
    }),
  };
}

export const MESSAGE_QUEUED_ENGINE: ServiceScriptEngine<QueuedStepsDecision> = {
  outcome: MESSAGE_QUEUED_OUTCOME,
  task: QUEUE_DECISION_TASK,
  consumes: "message_written",
  fromWords: queuedStepsFromWords,
};

/** The decision to write: ours, or the stored script's while switched. Recorded either way. */
export async function queuedStepsToStore(input: QueuedStepsInput, deps?: DecideDeps): Promise<QueuedStepsDecision> {
  const ours = decideQueuedSteps(input);
  return decideThroughScript(MESSAGE_QUEUED_ENGINE, ours, () => ({ material: queuedStepsMaterial(input), oursWords: queuedStepsWords(ours) }), deps);
}
