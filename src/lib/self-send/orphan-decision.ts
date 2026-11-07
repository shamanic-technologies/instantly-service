/**
 * MIGRATE-1105 S2b (reply_received, task `orphan-judgment`): which lead a message from ANOTHER address answers, for one
 * message the poller kept as a possible prospect answer, asked of a stored factory Script through the shared service
 * switch (`service-script.ts`), shadow first. The sweep around it is unchanged (`orphan-reply-sweep.ts`): which rows,
 * the body re-read on a backfill, the mailbox's leads, what a verdict does to the row.
 *
 * The DECISION, in the sweep's order (`orphan-reply.ts`, the rules unchanged):
 *   1. `orphanReplyExclusion` with the body (the warmup tag may live only there) -> `excluded`;
 *   2. `rankOrphanReplyLeads` over the leads written to BEFORE the message -> nobody = `no_candidate_lead`;
 *   3. ONE Jev choice (`buildOrphanReplyState` + `buildOrphanReplyQuestion`) -> `readOrphanReplyVerdict`.
 * Our own path judges on the org's reply-judging run as before; the script asks `jev.choice` on its own run. Our call is
 * recorded in the script's words (`jev.choice`, the exact request and chat's answer) so a recorded decision replays.
 */
import { platformJudgment, type JudgmentsResult } from "../chat-client";
import { replyJudgingRunForSequence } from "../reply-judging-run";
import { decideThroughScript, type DecideDeps, type ServiceScriptEngine } from "../service-script";
import { STAFF_ADDRESSES, staffDomains } from "../staff-senders";
import type { InboundHeaders } from "./inbound";
import {
  buildOrphanReplyQuestion,
  buildOrphanReplyState,
  ORPHAN_REPLY_QUESTION_KEY,
  orphanJudgmentOrg,
  orphanReplyExclusion,
  rankOrphanReplyLeads,
  readOrphanReplyVerdict,
  type OrphanReplyExclusion,
  type OrphanReplyLead,
} from "./orphan-reply";

export const ORPHAN_JUDGMENT_TASK = "orphan-judgment";
/** MIGRATE-1105 S5: the orphan judgment's script makes the same Engine as the inbound decision (an email that came back). */
export const ORPHAN_JUDGMENT_OUTCOME = "email_reply_received";
export const JEV_CHOICE = "jev.choice";

/** One message as the decision reads it. */
export interface OrphanMessage {
  fromAddress: string | null;
  /** The mailbox it landed in. */
  toAddress: string;
  subject: string | null;
  headers: InboundHeaders;
  text: string | null;
  receivedAt: Date;
  /** To + Cc, lowercased (`headerRecipients`). */
  recipients: string[];
}

export interface OrphanDecisionInput {
  message: OrphanMessage;
  ownMailDomains: ReadonlySet<string>;
  /** The leads this mailbox wrote to, as they stood when the message arrived (`leadsAsOf`). */
  leads: readonly OrphanReplyLead[];
}

/** The decision in words (what the script answers, what the sweep persists and acts on). */
export type OrphanWords =
  | { outcome: "excluded"; reason: OrphanReplyExclusion }
  | { outcome: "no_candidate_lead" }
  | {
      outcome: "matched" | "none" | "low_confidence";
      model: string | null;
      choice: string;
      probability: number;
      confidence: number;
      probabilities: Record<string, number>;
      inputTokens: number;
      candidates: Array<{ instantlyCampaignId: string; leadEmail: string; score: number }>;
      instantlyCampaignId?: string;
      leadEmail?: string;
    };

export interface OrphanDecision {
  words: OrphanWords;
  /** Our Jev call in the script's words (null when nothing was judged, or the script decided). */
  call: Record<string, unknown> | null;
}

const iso = (d: Date) => d.toISOString();

/** The decision's inputs as the script reads them (dates as ISO text, staff lists stated). */
export function orphanMaterial(input: OrphanDecisionInput): Record<string, unknown> {
  const m = input.message;
  return {
    message: { fromAddress: m.fromAddress, toAddress: m.toAddress, subject: m.subject, headers: m.headers, text: m.text, receivedAt: iso(m.receivedAt), recipients: m.recipients },
    ownMailDomains: [...input.ownMailDomains].sort(),
    staffAddresses: [...STAFF_ADDRESSES].sort(),
    staffDomains: [...staffDomains()].sort(),
    leads: input.leads.map((l) => ({ ...l, firstSentAt: iso(l.firstSentAt), lastSentAt: iso(l.lastSentAt) })),
  };
}

export type Judge = (body: { state: string; questions: Record<string, ReturnType<typeof buildOrphanReplyQuestion>> }, judgingSequence: string, judgingOrg: string) => Promise<JudgmentsResult>;

/** The production judge: platform-billed, on the reply-judging run of the likeliest lead's sequence (as before). */
export const platformJudge: Judge = async (body, judgingSequence, judgingOrg) =>
  platformJudgment(body, { runId: await replyJudgingRunForSequence(judgingSequence, { fallbackOrgId: judgingOrg }) });

/** Our own decision: the sweep's code, unchanged, made into words + the call it made. */
export async function decideOrphanReply(input: OrphanDecisionInput, judge: Judge = platformJudge): Promise<OrphanDecision> {
  const m = input.message;
  const reason = orphanReplyExclusion({ fromAddress: m.fromAddress, subject: m.subject, headers: m.headers, text: m.text }, input.ownMailDomains);
  if (reason) return { words: { outcome: "excluded", reason }, call: null };
  const ranked = rankOrphanReplyLeads({ fromAddress: m.fromAddress, subject: m.subject, text: m.text, receivedAt: m.receivedAt, recipients: m.recipients }, input.leads);
  if (ranked.length === 0) return { words: { outcome: "no_candidate_lead" }, call: null };
  const judgingOrg = orphanJudgmentOrg(ranked);
  if (!judgingOrg) throw new Error("no candidate lead carries an org: no reply-judging run to judge it for");
  // The judgment is made for the likeliest lead of that org: its sequence's pipe.
  const judgingSequence = ranked.find(({ lead }) => lead.orgId === judgingOrg)!.lead.instantlyCampaignId;
  const body = {
    state: buildOrphanReplyState({ fromAddress: m.fromAddress, toAddress: m.toAddress, subject: m.subject, receivedAt: m.receivedAt, text: m.text }),
    questions: { [ORPHAN_REPLY_QUESTION_KEY]: buildOrphanReplyQuestion(ranked) },
  };
  const result = await judge(body, judgingSequence, judgingOrg);
  const call = { name: JEV_CHOICE, args: body, answer: { ok: true, data: { answers: result.answers, model: result.model ?? null, usage: result.usage ?? null } } };
  const answer = result.answers[ORPHAN_REPLY_QUESTION_KEY];
  if (!answer) throw new Error("judgment returned no answer for the orphan-reply question");
  const verdict = readOrphanReplyVerdict(answer, ranked);
  return {
    call,
    words: {
      outcome: verdict.outcome,
      model: result.model ?? null,
      choice: answer.choice,
      probability: verdict.probability,
      confidence: answer.confidence,
      probabilities: answer.probabilities,
      inputTokens: result.usage?.inputTokens ?? 0,
      candidates: ranked.map(({ lead, score }) => ({ instantlyCampaignId: lead.instantlyCampaignId, leadEmail: lead.leadEmail, score })),
      ...(verdict.outcome === "matched" ? { instantlyCampaignId: verdict.lead.instantlyCampaignId, leadEmail: verdict.lead.leadEmail } : {}),
    },
  };
}

const OUTCOMES: ReadonlySet<string> = new Set(["excluded", "no_candidate_lead", "matched", "none", "low_confidence"]);

/** The script's answer as our decision (checked: the sweep acts on it). */
export function orphanFromWords(words: Record<string, unknown>): OrphanDecision {
  if (typeof words.outcome !== "string" || !OUTCOMES.has(words.outcome)) throw new Error(`the orphan-reply script answered outcome ${String(words.outcome)}`);
  if (words.outcome === "matched" && (typeof words.instantlyCampaignId !== "string" || typeof words.leadEmail !== "string")) {
    throw new Error("the orphan-reply script matched no lead");
  }
  return { words: words as unknown as OrphanWords, call: null };
}

/** Two answers are the same decision when they agree on everything but the judgment's figures (a second Jev call). */
export function orphanComparedOn(words: Record<string, unknown>): Record<string, unknown> {
  const { outcome, reason, choice, candidates, instantlyCampaignId, leadEmail } = words;
  return { outcome, reason, choice, candidates, instantlyCampaignId, leadEmail };
}

export const ORPHAN_JUDGMENT_ENGINE: ServiceScriptEngine<OrphanDecision> = {
  outcome: ORPHAN_JUDGMENT_OUTCOME,
  task: ORPHAN_JUDGMENT_TASK,
  consumes: "message_sent",
  fromWords: orphanFromWords,
  paidVendor: true,
  compareOn: orphanComparedOn,
};

/** The message's decision to act on: ours, or the stored script's while switched. Recorded either way. */
export async function orphanDecisionToAct(input: OrphanDecisionInput, deps?: DecideDeps & { judge?: Judge }): Promise<OrphanDecision> {
  return decideThroughScript(
    ORPHAN_JUDGMENT_ENGINE,
    () => decideOrphanReply(input, deps?.judge),
    (ours) => ({ material: orphanMaterial(input), oursWords: ours ? (ours.words as unknown as Record<string, unknown>) : null, calls: ours?.call ? [ours.call] : [] }),
    deps,
  );
}
