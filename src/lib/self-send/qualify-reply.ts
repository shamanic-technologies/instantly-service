/**
 * Qualifying a reply we read out of our own mailbox.
 *
 * While Instantly sends, IT classifies inbound replies and we trust that
 * qualification — `POSITIVE_QUALIFICATION_EVENT_TYPES` mirrors the events that
 * flip `reply_classification` to positive. On the self-send transport there is
 * no Instantly to ask, so we classify the reply ourselves through chat-service.
 *
 * What this does NOT decide: whether to stop the sequence. `reply_received` is
 * in `SEQUENCE_STOP_EVENTS`, so ANY human reply already stopped it and cancelled
 * the remaining holds, whatever the sentiment, before this runs. Qualification
 * only decides what the reply MEANS — which drives the forward to the agency
 * inbox and the gold sentiment stats. Do not make the stop conditional on it.
 */

import { platformComplete } from "../chat-client";
import type { ShadowJudgmentContext } from "../shadow-judgment";

/**
 * The only outputs we accept, and they are exactly the reply-kind vocabulary
 * (`REPLY_KINDS` in lib/reply-kind, projected by `REPLY_CLASSIFICATION_MAP`).
 * Emitting a name outside this set would write an event no reader maps.
 *
 * `lead_opt_out_requested` is the one label with a consequence beyond a stat:
 * the caller turns it into a recorded opt-out, which stops every campaign this
 * org holds for the address. That is why the prompt spends a paragraph telling
 * the model to ignore our own unsubscribe footer quoted back at it.
 *
 * Deal progress is absent by construction: a closed deal, or a meeting sitting
 * on a calendar, is an outcome someone records in the lead-outcomes service —
 * never something a reply's text can honestly support. `lead_meeting_requested`
 * is the reply fact that CAN be read off the text ("they asked for a call").
 */
export const QUALIFICATION_EVENT_TYPES = [
  "lead_interested",
  "lead_referral",
  "lead_info_requested",
  "lead_meeting_requested",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_changed_job",
  "lead_opt_out_requested",
  "lead_out_of_office",
  "lead_neutral",
] as const;

export type QualificationEventType = (typeof QUALIFICATION_EVENT_TYPES)[number];

export const SYSTEM_PROMPT = `You classify a single reply to a cold outreach email.

Answer with JSON only: {"classification": "<one of the labels>"}

Labels, and what each one means:
- lead_interested — they are personally interested and say so, without asking a question or proposing a time
- lead_referral — they are not the buyer themselves, but it is relevant to their company and they point you at the right person
- lead_info_requested — they want to know more: they ask a question about the offer without committing
- lead_meeting_requested — they propose or accept a specific time, or share a booking link
- lead_not_interested — they decline or say it is not relevant, WITHOUT asking to be removed
- lead_opt_out_requested — they ask to be taken off the list: "unsubscribe", "remove me", "take me off your list", "stop emailing me", "do not contact me again". Pick this over lead_not_interested whenever the reply contains a removal request, even if it also declines the offer
- lead_wrong_person — they are not the right contact and hand you nothing: no name, no relevance
- lead_changed_job — they say they have left the role or the company, so the role we wrote to is no longer theirs
- lead_out_of_office — they are away and will return; the message says nothing about the offer
- lead_neutral — anything else, including a bare acknowledgement or an unclear reply

Judge only what the reply says. Do not infer enthusiasm from politeness, and do
not treat a question about how you got their address as interest.

The reply may quote our own email beneath it, and every email we send ends with
the words "Not relevant? Reply "stop" and I won't email you again." (older emails
ended with "Don't want to hear from me again? unsubscribe"). That is OUR footer,
not their request — only a removal request THEY wrote is lead_opt_out_requested.

Worked examples, from real replies:
- "Stop" -> lead_opt_out_requested
- "Unsubscribe" -> lead_opt_out_requested
- "unsusbsribe" -> lead_opt_out_requested (a misspelling is still the request)
- "No interest, please stop sending emails." -> lead_opt_out_requested (the second clause asks to stop; the first alone would not)
- "Not for us, thanks." -> lead_not_interested (a decline, with no request to be removed)
- "No interest" -> lead_not_interested (declining is not asking to be taken off the list)`;

/**
 * The SAME question, expressed as a typed CHOICE question for the judgment
 * engine (`buildReplyKindQuestion` in lib/shadow-judgment).
 *
 * ⚠️ DERIVED FROM THE VOCABULARY ABOVE, NOT A SECOND ONE. Every key is a member
 * of `QUALIFICATION_EVENT_TYPES` and every description is lifted VERBATIM from
 * the sentence `SYSTEM_PROMPT` already gives that label. Two engines answering
 * two subtly different questions cannot disagree meaningfully — a divergence
 * would measure the wording, not the judgement — so the criteria and the prompt
 * are pinned to each other by test, and the prompt itself is NOT touched (a
 * reworded prompt would move the stored classification, which must stay
 * byte-identical).
 *
 * Deal progress is absent here for the same reason it is absent from the
 * vocabulary: a closed deal or a booked meeting is an outcome someone records,
 * never something a reply's text can support.
 */
export const REPLY_KIND_CRITERIA: Record<
  QualificationEventType,
  string | { what: string; examples?: string[] }
> = {
  lead_interested:
    "they are personally interested and say so, without asking a question or proposing a time",
  lead_referral:
    "they are not the buyer themselves, but it is relevant to their company and they point you at the right person",
  lead_info_requested:
    "they want to know more: they ask a question about the offer without committing",
  lead_meeting_requested:
    "they propose or accept a specific time, or share a booking link",
  lead_not_interested: {
    what: "they decline or say it is not relevant, WITHOUT asking to be removed",
    examples: ["Not for us, thanks.", "No interest"],
  },
  lead_opt_out_requested: {
    what: `they ask to be taken off the list: "unsubscribe", "remove me", "take me off your list", "stop emailing me", "do not contact me again". Pick this over lead_not_interested whenever the reply contains a removal request, even if it also declines the offer`,
    examples: ["Stop", "Unsubscribe", "unsusbsribe", "No interest, please stop sending emails."],
  },
  lead_wrong_person:
    "they are not the right contact and hand you nothing: no name, no relevance",
  lead_changed_job:
    "they say they have left the role or the company, so the role we wrote to is no longer theirs",
  lead_out_of_office:
    "they are away and will return; the message says nothing about the offer",
  lead_neutral: "anything else, including a bare acknowledgement or an unclear reply",
};

/**
 * The judging rules, verbatim from `SYSTEM_PROMPT`'s own two paragraphs. The
 * footer paragraph is load-bearing and not boilerplate: every email we send ends
 * with our own unsubscribe line, which the reply quotes back, so an engine that
 * has not been told reads OUR words as THEIR removal request.
 */
export const REPLY_KIND_JUDGMENT_INSTRUCTIONS = `Classify a single reply to a cold outreach email.

Judge only what the reply says. Do not infer enthusiasm from politeness, and do not treat a question about how you got their address as interest.

The reply may quote our own email beneath it, and every email we send ends with the words "Not relevant? Reply "stop" and I won't email you again." (older emails ended with "Don't want to hear from me again? unsubscribe"). That is OUR footer, not their request — only a removal request THEY wrote is lead_opt_out_requested.`;

/** Strip quoted history so the model judges what THEY wrote, not our own email. */
export function stripQuotedHistory(text: string): string {
  const lines = text.split(/\r?\n/);
  const kept: string[] = [];

  for (const line of lines) {
    // A quote marker, or the "On <date>, <someone> wrote:" attribution line that
    // every client puts above the quoted block.
    if (/^\s*>/.test(line)) break;
    if (/^\s*On .{0,120}\bwrote:\s*$/i.test(line)) break;
    if (/^\s*-{2,}\s*Original Message\s*-{2,}/i.test(line)) break;
    kept.push(line);
  }

  return kept.join("\n").trim();
}

/** True when the value is one of the labels we accept. */
export function isQualificationEventType(
  value: unknown,
): value is QualificationEventType {
  return (
    typeof value === "string" &&
    (QUALIFICATION_EVENT_TYPES as readonly string[]).includes(value)
  );
}

/**
 * Read a classification out of a chat-service response.
 *
 * Returns null for anything unexpected — a missing field, a label we do not
 * know, prose instead of JSON. The caller then promotes NOTHING rather than
 * defaulting to neutral: asserting a sentiment we did not obtain would put a
 * fabricated fact into the gold stats, and a wrong "neutral" on a hot reply is
 * worse than an absent one, which at least reads as absent.
 */
export function parseQualification(result: {
  json?: Record<string, unknown>;
  content?: string;
}): QualificationEventType | null {
  const fromJson = result.json?.classification;
  if (isQualificationEventType(fromJson)) return fromJson;

  if (typeof result.content === "string") {
    try {
      const parsed = JSON.parse(result.content) as { classification?: unknown };
      if (isQualificationEventType(parsed.classification)) return parsed.classification;
    } catch {
      // Not JSON. Fall through to null — see the docstring.
    }
  }

  return null;
}

/**
 * Classify one reply. Returns null when no trustworthy label could be obtained.
 *
 * `deepseek-flash` on purpose: this is a short, closed-set classification on a
 * few hundred words, run once per reply, and DeepSeek V4 Flash is the cheapest
 * model in the catalogue that does it reliably. Reasoning is disabled for the
 * same reason — there is nothing to reason about, only a label to pick.
 *
 * The spend is chat-service's, on a platform run: this runs inside a sweep with
 * no inbound org request, so there is no customer to bill for classifying a
 * reply to our own outreach.
 */
export async function qualifyReply(
  replyText: string,
  context?: ShadowJudgmentContext,
): Promise<QualificationEventType | null> {
  const message = stripQuotedHistory(replyText).slice(0, 4000);
  if (!message) return null;

  const result = await platformComplete({
    message,
    systemPrompt: SYSTEM_PROMPT,
    provider: "deepseek",
    model: "deepseek-flash",
    responseFormat: "json",
    temperature: 0,
    disableThinking: true,
  });

  const classification = parseQualification(result);

  // Ask the calibrated judgment engine the SAME question and record what both
  // said, so the disagreement rate and the confidence distribution can be
  // measured on real replies. It decides NOTHING — `classification` below is
  // returned unchanged whatever the judgment says, and unchanged when it fails.
  //
  // DETACHED, never awaited: one caller runs inside Instantly's webhook, where a
  // slow delivery counts toward disabling the whole subscription. Imported
  // dynamically so the measurement module can import this vocabulary without a
  // load-time cycle.
  void import("../shadow-judgment")
    .then((m) => m.shadowJudgeReply({ ...context, replyText: message, llmClassification: classification }))
    .catch((error: unknown) => {
      console.error(
        "[instantly-service] shadow-judgment: could not be launched — the stored classification is unaffected:",
        error,
      );
    });

  return classification;
}
