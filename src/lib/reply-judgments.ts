/**
 * Jev judgments about a REPLY that its kind does not carry, judged ONCE per
 * (reply, question) and kept in bronze `reply_judgments` (drizzle/0067).
 *
 * Two questions, both asked in ONE chat-service judgment call per reply:
 *
 *  - `proposal_type` — only for a reply about something other than buying
 *    (`lead_off_topic`): a partnership, a job, money in the company, or other.
 *  - `question` — for every reply a PERSON wrote (not an autoresponder): does it
 *    ask us nothing, a question the outreach team can answer from the offer
 *    itself, or one only the sender company's own people can answer?
 *
 * The reply's KIND stays the classifier's (one verdict per reply fleet-wide,
 * which drives sequence stops and escalation); these answer finer questions the
 * kind leaves open, never a second opinion on it.
 *
 * Why Jev: owner, 2026-10-08 — "use Jev as much as possible everywhere, it is
 * super cheap". Platform-billed (a background projection has no inbound org
 * request); chat-service bills input tokens only and declares the cost.
 *
 * Judged in the outreach-facts projection BEFORE the reply's fact is emitted,
 * so the feed rarely has to correct itself. A failed call writes nothing (the
 * next tick retries) and is logged loud by the caller.
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { platformJudgment, type JudgmentChoiceQuestion } from "./chat-client";
import { AUTOMATED_REPLY_KINDS, OFF_TOPIC_REPLY_KINDS } from "./reply-kind";
import { htmlToText } from "./forward-positive-reply";
import { stripQuotedHistory, withSubject } from "./self-send/qualify-reply";

export const PROPOSAL_TYPES = ["partnership", "hiring", "investment", "other"] as const;
export type ProposalType = (typeof PROPOSAL_TYPES)[number];

export const QUESTION_TYPES = ["none", "answerable", "needs_sender_company"] as const;
export type QuestionType = (typeof QUESTION_TYPES)[number];

export const JUDGMENT_QUESTIONS = ["proposal_type", "question"] as const;
export type JudgmentQuestionKey = (typeof JUDGMENT_QUESTIONS)[number];

const INSTRUCTIONS = `You read a single reply a person sent to a cold outreach email. The email was written on behalf of a company (the sender) to sell its offer.

Judge only what the reply says, not our own email quoted beneath it.`;

export const PROPOSAL_TYPE_QUESTION: JudgmentChoiceQuestion = {
  type: "choice",
  instructions: `${INSTRUCTIONS}

This reply is about something OTHER than buying the sender's offer. What is it about?`,
  criteria: {
    partnership: {
      what: "working together as partners: a partnership, reseller, distributor, affiliate, referral, integration or co-marketing proposal",
      examples: ["Would you be open to a reseller agreement?", "We could co-host a webinar for both our audiences"],
    },
    hiring: {
      what: "a job: they apply or offer themselves as a candidate, ask whether the company is hiring, or pitch recruiting services",
      examples: ["Are you hiring? I'd love to join your team"],
    },
    investment: {
      what: "money in the company: they want to invest, ask about fundraising, or propose an acquisition or financing",
      examples: ["We invest in companies like yours, are you raising?"],
    },
    other: "anything else that is not a purchase: pitching their own product or service to the sender, press or media, sponsorship, events",
  },
};

export const QUESTION_QUESTION: JudgmentChoiceQuestion = {
  type: "choice",
  instructions: `${INSTRUCTIONS}

The sender's outreach team answers routine questions about the offer right away. Anything only the sender company's own people can decide or know must be handed to them. Does the reply ask a question, and who can answer it?`,
  criteria: {
    none: "the reply asks no question (a statement, a yes, a no, an acknowledgement)",
    answerable: {
      what: "it asks something the outreach team can answer from the offer itself: what it is, how it works, typical pricing, who it is for, next steps, how to book",
      examples: ["How does it work?", "What does it cost roughly?", "Can you send more info?"],
    },
    needs_sender_company: {
      what: "it asks something only the sender company itself can answer: a custom quote, contract or legal terms, a commitment or decision, a detail about the company's own operations, clients or people",
      examples: ["Can you guarantee delivery by March 3 for 400 units?", "Will your CEO sign our NDA?"],
    },
  },
};

/** Which questions apply to a reply of this kind. */
export function questionsForKind(kind: string | null): JudgmentQuestionKey[] {
  if (kind === null) return [];
  if ((AUTOMATED_REPLY_KINDS as readonly string[]).includes(kind)) return [];
  const keys: JudgmentQuestionKey[] = [];
  if ((OFF_TOPIC_REPLY_KINDS as readonly string[]).includes(kind)) keys.push("proposal_type");
  keys.push("question");
  return keys;
}

const QUESTION_BY_KEY: Record<JudgmentQuestionKey, JudgmentChoiceQuestion> = {
  proposal_type: PROPOSAL_TYPE_QUESTION,
  question: QUESTION_QUESTION,
};

const CHOICES_BY_KEY: Record<JudgmentQuestionKey, readonly string[]> = {
  proposal_type: PROPOSAL_TYPES,
  question: QUESTION_TYPES,
};

/** Stored when a reply has no words to judge; never served as an answer. */
export const NO_TEXT = "no_text";

export interface StoredJudgment {
  value: string;
  confidence: number | null;
}

export interface ReplyToJudge {
  replyId: string;
  subject: string | null;
  /** Raw body (text or html) of the stored message. */
  body: string;
  missing: JudgmentQuestionKey[];
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * The replies still owed a judgment, oldest first: a current kind that calls
 * for a question this reply has no row for, and a stored message to read (a
 * reply known only through its verdict has no words to judge).
 */
export async function selectRepliesToJudge(limit: number): Promise<ReplyToJudge[]> {
  const rows = rowsOf(
    await db.execute(sql`
      SELECT r.id, r.subject, r.current_kind,
             COALESCE(NULLIF(ie.payload->'body'->>'text', ''), ie.payload->'body'->>'html',
                      m.payload->>'textSnippet', '') AS body,
             COALESCE((SELECT jsonb_agg(j.question) FROM reply_judgments j WHERE j.reply_id = r.id), '[]'::jsonb) AS judged
      FROM replies r
      LEFT JOIN instantly_emails_raw ie ON r.source_table = 'instantly_emails_raw' AND ie.id = r.source_row_id
      LEFT JOIN imap_messages_raw m ON r.source_table = 'imap_messages_raw' AND m.id = r.source_row_id
      WHERE r.current_kind IS NOT NULL
        AND r.current_kind NOT IN (${sql.join(
          AUTOMATED_REPLY_KINDS.map((k) => sql`${k}`),
          sql`, `,
        )})
        AND r.source_table IN ('instantly_emails_raw', 'imap_messages_raw')
        AND (
          NOT EXISTS (SELECT 1 FROM reply_judgments j WHERE j.reply_id = r.id AND j.question = 'question')
          OR (r.current_kind IN (${sql.join(
            OFF_TOPIC_REPLY_KINDS.map((k) => sql`${k}`),
            sql`, `,
          )})
              AND NOT EXISTS (SELECT 1 FROM reply_judgments j WHERE j.reply_id = r.id AND j.question = 'proposal_type'))
        )
      ORDER BY r.received_at
      LIMIT ${limit}
    `),
  );
  return rows.map((row) => {
    const judged = Array.isArray(row.judged) ? (row.judged as string[]) : [];
    return {
      replyId: String(row.id),
      subject: typeof row.subject === "string" ? row.subject : null,
      body: String(row.body ?? ""),
      missing: questionsForKind(String(row.current_kind)).filter((q) => !judged.includes(q)),
    };
  });
}

/** The text the engine reads: their own words, under the thread's subject. */
export function judgmentState(reply: Pick<ReplyToJudge, "subject" | "body">): string {
  return withSubject(stripQuotedHistory(htmlToText(reply.body)), reply.subject);
}

/**
 * Ask the missing questions about one reply in ONE call and keep the answers.
 * Returns how many answers were stored. Throws on a vendor error or an answer
 * outside the vocabulary (nothing is coerced, nothing is stored for it).
 * A reply with no words left after stripping is skipped (returns 0).
 */
export async function judgeReply(reply: ReplyToJudge): Promise<number> {
  if (reply.missing.length === 0) return 0;
  if (!stripQuotedHistory(htmlToText(reply.body)).trim()) {
    // Nothing they wrote is left to read: recorded once, so the reply is not
    // re-selected forever; readers serve it as "not judged" (null).
    for (const key of reply.missing) {
      await db.execute(sql`
        INSERT INTO reply_judgments (reply_id, question, choice)
        VALUES (${reply.replyId}, ${key}, ${NO_TEXT})
        ON CONFLICT (reply_id, question) DO NOTHING
      `);
    }
    return 0;
  }
  const state = judgmentState(reply);

  const questions: Record<string, JudgmentChoiceQuestion> = {};
  for (const key of reply.missing) questions[key] = QUESTION_BY_KEY[key];
  const result = await platformJudgment({ state, questions });

  let stored = 0;
  for (const key of reply.missing) {
    const answer = result.answers?.[key];
    if (!answer || answer.type !== "choice" || !CHOICES_BY_KEY[key].includes(answer.choice)) {
      throw new Error(
        `[instantly-service] reply-judgments: unreadable answer for reply=${reply.replyId} question=${key}: ${JSON.stringify(answer ?? null).slice(0, 300)}`,
      );
    }
    await db.execute(sql`
      INSERT INTO reply_judgments (reply_id, question, choice, confidence, probabilities, model, input_tokens)
      VALUES (${reply.replyId}, ${key}, ${answer.choice}, ${answer.confidence},
              ${JSON.stringify(answer.probabilities ?? null)}::jsonb, ${result.model ?? null},
              ${result.usage?.inputTokens ?? null})
      ON CONFLICT (reply_id, question) DO NOTHING
    `);
    stored += 1;
  }
  return stored;
}

export interface JudgeSummary {
  candidates: number;
  judged: number;
  skippedNoText: number;
  /** Reply ids whose call failed this pass (retried next pass). */
  failed: string[];
}

/** Judge up to `limit` replies still owed a judgment. Fail-loud per reply, never per batch. */
export async function judgePendingReplies(limit: number): Promise<JudgeSummary> {
  const candidates = await selectRepliesToJudge(limit);
  const summary: JudgeSummary = { candidates: candidates.length, judged: 0, skippedNoText: 0, failed: [] };
  for (const reply of candidates) {
    try {
      const stored = await judgeReply(reply);
      if (stored === 0) summary.skippedNoText += 1;
      else summary.judged += 1;
    } catch (error: unknown) {
      summary.failed.push(reply.replyId);
      console.error(
        `[instantly-service] reply-judgments: FAILED reply=${reply.replyId}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  return summary;
}

/** The stored judgments of these replies, keyed by reply id then question. */
export async function readReplyJudgments(
  replyIds: string[],
): Promise<Map<string, Partial<Record<JudgmentQuestionKey, StoredJudgment>>>> {
  const out = new Map<string, Partial<Record<JudgmentQuestionKey, StoredJudgment>>>();
  if (replyIds.length === 0) return out;
  const rows = rowsOf(
    await db.execute(sql`
      SELECT reply_id, question, choice, confidence FROM reply_judgments
      WHERE reply_id = ANY(${sql.param(replyIds)}::text[])
        AND choice <> ${NO_TEXT}
    `),
  );
  for (const row of rows) {
    const id = String(row.reply_id);
    const entry = out.get(id) ?? {};
    entry[String(row.question) as JudgmentQuestionKey] = {
      value: String(row.choice),
      confidence: row.confidence == null ? null : Number(row.confidence),
    };
    out.set(id, entry);
  }
  return out;
}
