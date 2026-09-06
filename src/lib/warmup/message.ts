/**
 * The warmup message — a different one every time.
 *
 * ⚠️ VARIED PER EMAIL, NOT PER DAY, AND THIS IS THE WHOLE POINT OF THE MODULE.
 * The first design here sent one generated body to every pair in a day's run, on
 * the reasoning that a shared body keeps the arms comparable. That is backwards:
 * ~800 byte-identical messages crossing a filter within an hour IS the bulk
 * signal warmup exists to avoid, and it is far easier to detect than any content
 * we could write. Variance across senders is not noise to be controlled — the
 * measurement lives in the seed harness, which is a separate population of
 * receivers (see `partnerCandidates`); this traffic is judged by nobody.
 *
 * It also matches what the production data says warms a mailbox. Measured
 * 2026-09-06 on the Gandi fleet: mailboxes doing ONLY uniform warmup at 30/day
 * inboxed at 15.9%, while those sending real, varied mail at a moderate rate
 * inboxed at 48.6%. Uniformity is the thing that did not work.
 *
 * Spend goes through chat-service, which owns the cost declaration — this
 * service declares none, exactly as it does for the reply qualification. The
 * cheapest model in the catalogue, thinking disabled: this is a two-sentence
 * note, not a reasoning task.
 */

import { platformComplete } from "../chat-client";

/** Plain-text bodies; a warmup note between colleagues is not an HTML campaign. */
export interface WarmupMessage {
  subject: string;
  text: string;
}

const SYSTEM_PROMPT = [
  "You write very short, ordinary internal work emails between two colleagues at a small marketing agency.",
  "Two or three sentences. No greeting line beyond a first name, no signature, no marketing language,",
  "no links, no attachments, no bullet lists, no emoji. Mundane on purpose: a scheduling note, a question",
  "about a document, a short status update. Vary the topic, the length and the phrasing every time.",
  "Return STRICT JSON: {\"subject\": string, \"text\": string}. Nothing else.",
].join(" ");

/**
 * Topic seeds, rotated so the model is not asked the same question 800 times.
 *
 * The model varies wording on its own, but an identical prompt pulls it toward
 * the same handful of openings — which reintroduces exactly the uniformity this
 * module exists to avoid, one layer up.
 */
const TOPICS = [
  "a question about when a shared document will be ready",
  "a short note moving a call by half an hour",
  "a one-line status update on a piece of work",
  "asking whether someone got a file that was sent",
  "confirming a detail agreed in a previous conversation",
  "flagging that a task will slip by a day",
  "asking who is covering something next week",
  "a quick thank-you for something handed over",
] as const;

/**
 * ⚠️ THE FALLBACK IS A REAL PATH, NOT A DEGRADED ONE — do NOT make an LLM
 * failure abort the send.
 *
 * Elsewhere in this service a missing model answer means we refuse to assert
 * something (a reply sentiment we did not obtain is left unset rather than
 * guessed). Here nothing is being asserted: the message carries no information
 * anyone consumes, its only job is to be ordinary mail crossing a filter. So a
 * chat-service outage must not stop the fleet warming — it would take the mesh
 * down for the duration, which is the one thing warmup cannot afford to skip.
 *
 * The fallback still varies, seeded off the message's own topic and recipient,
 * so an outage does not silently collapse the run into one repeated body.
 */
function fallbackMessage(topic: string, receiverEmail: string): WarmupMessage {
  const name = receiverEmail.split("@")[0]?.split(/[._-]/)[0] ?? "there";
  const pretty = name.charAt(0).toUpperCase() + name.slice(1);
  return {
    subject: topic.replace(/^(a|an|asking|confirming|flagging) /i, "").slice(0, 60),
    text: `Hi ${pretty},\n\nQuick one on ${topic}. Let me know when you get a moment.\n\nThanks`,
  };
}

/** Deterministic topic pick, so a re-run of the same day's edge reuses it. */
export function topicFor(senderEmail: string, receiverEmail: string, dayKey: string): string {
  const s = `${dayKey}|${senderEmail}|${receiverEmail}`;
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return TOPICS[h % TOPICS.length]!;
}

/** Parse the model's JSON, tolerating a fenced block. Null when unusable. */
export function parseWarmupMessage(raw: string): WarmupMessage | null {
  const cleaned = raw.trim().replace(/^```(?:json)?/i, "").replace(/```$/, "").trim();
  try {
    const parsed = JSON.parse(cleaned) as Record<string, unknown>;
    const subject = typeof parsed.subject === "string" ? parsed.subject.trim() : "";
    const text = typeof parsed.text === "string" ? parsed.text.trim() : "";
    if (!subject || !text) return null;
    return { subject, text };
  } catch {
    return null;
  }
}

export async function buildWarmupMessage(
  senderEmail: string,
  receiverEmail: string,
  dayKey: string,
): Promise<WarmupMessage> {
  const topic = topicFor(senderEmail, receiverEmail, dayKey);

  try {
    const result = await platformComplete({
      message: `Write the email. Topic: ${topic}. The recipient's first name is "${
        receiverEmail.split("@")[0]?.split(/[._-]/)[0] ?? "there"
      }".`,
      systemPrompt: SYSTEM_PROMPT,
      provider: "deepseek",
      model: "deepseek-flash",
      temperature: 1,
      disableThinking: true,
    });
    return parseWarmupMessage(result.content ?? "") ?? fallbackMessage(topic, receiverEmail);
  } catch (error) {
    console.warn(
      `[warmup] message generation failed, using fallback: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
    return fallbackMessage(topic, receiverEmail);
  }
}

/** The answer to a warmup message we received. Same shape, same fallback rules. */
export async function buildWarmupReply(
  originalSubject: string,
  originalText: string,
): Promise<WarmupMessage> {
  const subject = originalSubject.toLowerCase().startsWith("re:")
    ? originalSubject
    : `Re: ${originalSubject}`;

  try {
    const result = await platformComplete({
      message: `Reply in one or two sentences to this email:\n\n${originalText.slice(0, 800)}`,
      systemPrompt: SYSTEM_PROMPT,
      provider: "deepseek",
      model: "deepseek-flash",
      temperature: 1,
      disableThinking: true,
    });
    const parsed = parseWarmupMessage(result.content ?? "");
    return { subject, text: parsed?.text ?? "Got it, thanks. Will take a look today." };
  } catch {
    return { subject, text: "Got it, thanks. Will take a look today." };
  }
}
