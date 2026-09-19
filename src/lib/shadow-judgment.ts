/**
 * Running a calibrated judgment engine BESIDE the reply classifier, and acting
 * on neither.
 *
 * Why this exists. The reply classification is FROZEN at write time and a frozen
 * wrong one stays wrong forever, invisibly — a customer once read "1 sales
 * interest" for weeks while their own board said the opposite, and the
 * correction needed a backfill. The LLM returns a label and nothing else, so a
 * reply it hesitated between two labels on looks exactly like one it was certain
 * about. That is the gap: we cannot decline to freeze a shaky classification,
 * because nothing ever tells us there was anything to decline.
 *
 * chat-service's judgment route answers the SAME typed question with a full
 * probability distribution and a confidence. This module asks it, records what
 * both engines said, and stops there.
 *
 * ⚠️ IT DECIDES NOTHING, and that is the entire contract of this file. The
 * stored classification, the forward to the agency inbox, the opt-out recording
 * and every statistic are exactly what `qualifyReply` returned, unchanged, for
 * every input — including when this fails. Do not add a reader that prefers the
 * judgment, or that gates on the confidence: that is a separate decision the
 * measurement in `reply_classification_shadow` exists to inform, not a follow-up
 * chore this file half-implements.
 *
 * ⚠️ IT ADDS NO LATENCY. `recordShadowJudgment` is launched DETACHED by
 * `qualifyReply` — never awaited — because one of its callers runs inside
 * Instantly's webhook, where a slow or failed delivery counts toward disabling
 * the whole subscription (already worth a six-day outage once). Known cost,
 * stated: a deploy that recreates the container mid-flight loses that one
 * measurement row. A lost measurement is the right thing to lose.
 *
 * The spend is chat-service's, on a platform run, against the vendor's reported
 * input-token count. This service declares none — same as for completions.
 */

import { db } from "../db";
import { replyClassificationShadow } from "../db/schema";
import { platformJudgment, type JudgmentChoiceQuestion } from "./chat-client";
import {
  QUALIFICATION_EVENT_TYPES,
  REPLY_KIND_CRITERIA,
  REPLY_KIND_JUDGMENT_INSTRUCTIONS,
  type QualificationEventType,
} from "./self-send/qualify-reply";

/** The caller-chosen key the answer comes back under. */
export const REPLY_KIND_QUESTION_KEY = "reply_kind";

/** Which caller classified the reply. Recorded so a disagreement can be traced to a path. */
export type ShadowJudgmentSource =
  | "imap_poller"
  | "reply_opt_out"
  | "inbound_replies_backfill"
  | "reply_optout_backfill"
  | "unattributed";

export interface ShadowJudgmentContext {
  instantlyCampaignId?: string | null;
  leadEmail?: string | null;
  source?: ShadowJudgmentSource;
}

/**
 * Build the choice question from the EXISTING vocabulary — never a re-worded
 * one. `REPLY_KIND_CRITERIA` is keyed on `QualificationEventType` and its
 * descriptions are verbatim from `SYSTEM_PROMPT`, so both engines are provably
 * answering the same question and a disagreement means something.
 */
export function buildReplyKindQuestion(): JudgmentChoiceQuestion {
  const criteria: JudgmentChoiceQuestion["criteria"] = {};
  for (const label of QUALIFICATION_EVENT_TYPES) {
    criteria[label] = REPLY_KIND_CRITERIA[label];
  }
  return {
    type: "choice",
    instructions: REPLY_KIND_JUDGMENT_INSTRUCTIONS,
    criteria,
  };
}

/** True when the engine picked a label this service actually knows. */
export function isKnownReplyKind(value: unknown): value is QualificationEventType {
  return (
    typeof value === "string" &&
    (QUALIFICATION_EVENT_TYPES as readonly string[]).includes(value)
  );
}

/**
 * Did the two engines agree?
 *
 * NULL whenever either side is absent. An absence is not a disagreement, and
 * counting it as one would inflate the very number this measurement exists to
 * report — the same reason `deliveryAtBar: null` is never read as a pass.
 */
export function enginesAgreed(
  llm: string | null,
  judgment: string | null,
): boolean | null {
  if (llm === null || judgment === null) return null;
  return llm === judgment;
}

export interface ShadowJudgmentInput extends ShadowJudgmentContext {
  /** The reply text, already stripped of quoted history by the caller. */
  replyText: string;
  /** What the existing engine said. Null when it returned nothing usable. */
  llmClassification: QualificationEventType | null;
}

/**
 * Ask the judgment engine and record the pair. Resolves to the row id.
 *
 * Fails LOUD on its own terms — a vendor error is recorded in the row's `error`
 * column AND re-thrown, so the detached caller logs it with enough detail to
 * tell a vendor failure (429, 502, a distribution we could not read) from a bug
 * of ours (a 400 naming the field we got wrong). The row is written either way:
 * a judgment that could not be obtained is a fact worth counting, and a silently
 * missing row would read as "we never classified that reply".
 */
export async function recordShadowJudgment(input: ShadowJudgmentInput): Promise<void> {
  const source: ShadowJudgmentSource = input.source ?? "unattributed";
  const base = {
    instantlyCampaignId: input.instantlyCampaignId ?? null,
    leadEmail: input.leadEmail ?? null,
    source,
    llmClassification: input.llmClassification,
  };

  let result;
  try {
    result = await platformJudgment({
      state: input.replyText,
      questions: { [REPLY_KIND_QUESTION_KEY]: buildReplyKindQuestion() },
    });
  } catch (error) {
    await db.insert(replyClassificationShadow).values({
      ...base,
      error: error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000),
    });
    throw error;
  }

  const answer = result.answers?.[REPLY_KIND_QUESTION_KEY];
  // A shape we cannot read is recorded as a failure rather than coerced. Writing
  // a label we did not obtain would put a fabricated judgement into the very
  // table meant to tell us whether the engines agree.
  if (!answer || answer.type !== "choice" || !isKnownReplyKind(answer.choice)) {
    await db.insert(replyClassificationShadow).values({
      ...base,
      judgmentModel: result.model ?? null,
      judgmentInputTokens: result.usage?.inputTokens ?? null,
      error: `unreadable judgment answer: ${JSON.stringify(answer ?? null).slice(0, 500)}`,
    });
    throw new Error(
      `[instantly-service] shadow-judgment: unreadable answer under "${REPLY_KIND_QUESTION_KEY}"`,
    );
  }

  await db.insert(replyClassificationShadow).values({
    ...base,
    judgmentClassification: answer.choice,
    judgmentConfidence: answer.confidence,
    judgmentProbabilities: answer.probabilities,
    judgmentModel: result.model,
    judgmentInputTokens: result.usage?.inputTokens ?? null,
    agreed: enginesAgreed(input.llmClassification, answer.choice),
  });
}

/**
 * Launch the measurement DETACHED, so it can never delay or fail the caller.
 *
 * This is the one place in this repo where swallowing is correct, and it is
 * swallowed LOUDLY: the log line carries the lead, the campaign and the vendor's
 * own message, which is what separates "TypeSafe rate-limited us" from "we sent
 * a field it rejects".
 */
export function shadowJudgeReply(input: ShadowJudgmentInput): void {
  void recordShadowJudgment(input).catch((error: unknown) => {
    console.error(
      `[instantly-service] shadow-judgment: failed for lead=${input.leadEmail ?? "unknown"} campaign=${input.instantlyCampaignId ?? "unknown"} source=${input.source ?? "unattributed"} — the stored classification is unaffected:`,
      error,
    );
  });
}
