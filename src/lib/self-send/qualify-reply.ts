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
 * The only outputs we accept, and they are exactly the existing silver
 * vocabulary (`REPLY_CLASSIFICATION_MAP` in silver-promote.ts). Emitting a name
 * outside this set would write an event no reader maps to a sentiment.
 *
 * `lead_closed` is deliberately absent even though it is a positive event: a
 * closed deal is an outcome someone records, not something a reply's text can
 * honestly support.
 */
export const QUALIFICATION_EVENT_TYPES = [
  "lead_interested",
  "lead_meeting_booked",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_out_of_office",
  "lead_neutral",
] as const;

export type QualificationEventType = (typeof QUALIFICATION_EVENT_TYPES)[number];

const SYSTEM_PROMPT = `You classify a single reply to a cold outreach email.

Answer with JSON only: {"classification": "<one of the labels>"}

Labels, and what each one means:
- lead_interested — they want to know more, ask a question about the offer, or say yes
- lead_meeting_booked — they propose or accept a specific time, or share a booking link
- lead_not_interested — they decline, say it is not relevant, or ask to stop
- lead_wrong_person — they are not the right contact, and may or may not name someone else
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
 * `flash-lite` on purpose: this is a short, closed-set classification on a few
 * hundred words, run once per reply. Reasoning is disabled for the same reason.
 */
export async function qualifyReply(
  replyText: string,
): Promise<QualificationEventType | null> {
  const message = stripQuotedHistory(replyText).slice(0, 4000);
  if (!message) return null;

  const result = await platformComplete({
    message,
    systemPrompt: SYSTEM_PROMPT,
    provider: "google",
    model: "flash-lite",
    responseFormat: "json",
    temperature: 0,
    disableThinking: true,
  });

  return parseQualification(result);
}
