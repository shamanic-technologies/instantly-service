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

/**
 * The only outputs we accept, and they are exactly the reply-kind vocabulary
 * (`REPLY_KINDS` in lib/reply-kind, projected by `REPLY_CLASSIFICATION_MAP`).
 * Emitting a name outside this set would write an event no reader maps.
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
  "lead_out_of_office",
  "lead_neutral",
] as const;

export type QualificationEventType = (typeof QUALIFICATION_EVENT_TYPES)[number];

const SYSTEM_PROMPT = `You classify a single reply to a cold outreach email.

Answer with JSON only: {"classification": "<one of the labels>"}

Labels, and what each one means:
- lead_interested — they are personally interested and say so, without asking a question or proposing a time
- lead_referral — they are not the buyer themselves, but it is relevant to their company and they point you at the right person
- lead_info_requested — they want to know more: they ask a question about the offer without committing
- lead_meeting_requested — they propose or accept a specific time, or share a booking link
- lead_not_interested — they decline, say it is not relevant, or ask to stop
- lead_wrong_person — they are not the right contact and hand you nothing: no name, no relevance
- lead_changed_job — they say they have left the role or the company, so the role we wrote to is no longer theirs
- lead_out_of_office — they are away and will return; the message says nothing about the offer
- lead_neutral — anything else, including a bare acknowledgement or an unclear reply

Judge only what the reply says. Do not infer enthusiasm from politeness, and do
not treat a question about how you got their address as interest.`;

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
 * `deepseek-flash` on purpose: a short, closed-set classification over a few
 * hundred words, run once per reply, where the answer is one token from a list
 * of nine. Reasoning is disabled for the same reason — there is nothing here to
 * reason about, and on this vendor `disableThinking` is a genuine full-off.
 *
 * ⚠️ THE JSON GUARANTEE IS WEAKER HERE THAN ON GEMINI, and that is why the
 * parse is allowed to fail. DeepSeek supports `json_object` only — it refuses a
 * `json_schema` outright — so the model is asked for JSON but nothing enforces
 * the SHAPE of it server-side. `parseQualification` therefore does real work
 * rather than being a formality: an answer that is prose, or JSON carrying a
 * label outside the vocabulary, returns null and the caller promotes NOTHING.
 * Do not "fix" a parse failure by defaulting to `lead_neutral`; a fabricated
 * sentiment on a hot reply reads as a real judgement, where an absent one
 * reads as absent. The vendor also documents that the word "JSON" must appear
 * in the prompt for `json_object` to engage, which `SYSTEM_PROMPT` satisfies.
 */
export async function qualifyReply(
  replyText: string,
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

  return parseQualification(result);
}
