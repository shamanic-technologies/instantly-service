/**
 * Is a permanent delivery-status notification about the ADDRESS or about US?
 * Pure: the question, the state and the reading of the answer. IO lives in
 * `dsn-judgment-sweep.ts`.
 *
 * A permanent DSN has two very different meanings (BOUNCE-SPAM-BLOCK-1018):
 *  - the address does not exist / the mailbox is gone: the person cannot be
 *    reached at this address, ever. That is `email_bounced`, and every reader
 *    (lead-service suppression, human-service's serve, gold) treats it as
 *    permanent and fleet-wide.
 *  - the receiving server refused OUR MESSAGE or OUR SENDER (spam/content
 *    filter, policy, blocklist, reputation, authentication): the address is
 *    fine. It is a fact about our deliverability, never about the person.
 *
 * The machine-readable code cannot tell them apart. Prod 2026-10-15
 * (DefiniteJobs, team@definitejobsgroup.co.uk, the client's own valid
 * address): GoDaddy answered `552 5.2.0` (RFC 3463 reads "mailbox") with the
 * words "rejected due to content judged to be spam". The words decide, so the
 * DSN is JUDGED by chat-service (Jev, `choice`), once, and the judgment is
 * persisted on the bronze row. No regex is the classifier.
 *
 * Burden of proof is on "dead": only an answer that the address is gone makes
 * the person undeliverable forever. A refused message leaves them contactable.
 */
import type { JudgmentChoiceAnswer, JudgmentChoiceQuestion } from "../chat-client";

export const DSN_QUESTION_KEY = "dsn_meaning";

/**
 * Bumped whenever the question's wording changes: a row judged under another
 * version is judged again (one more paid call), so every stored verdict reads
 * the current rule.
 */
export const DSN_QUESTION_VERSION = 2;

export const DSN_ADDRESS_DEAD = "address_dead" as const;
export const DSN_MESSAGE_REFUSED = "message_refused" as const;
export type DsnVerdict = typeof DSN_ADDRESS_DEAD | typeof DSN_MESSAGE_REFUSED;

/** What the bronze row carries under `payload.dsnJudgment`. */
export interface DsnJudgment {
  verdict: DsnVerdict;
  questionVersion: number;
  judgedAt: string;
  model: string;
  probability: number;
  confidence: number;
  probabilities: Record<string, number>;
}

/** The words the judgment reads. Bounded: a DSN quotes our whole email below. */
const STATE_TEXT_CHARS = 3000;

export function buildDsnState(input: {
  from: string | null;
  subject: string | null;
  text: string;
}): string {
  return [
    `From: ${input.from ?? "(unknown)"}`,
    `Subject: ${input.subject ?? "(none)"}`,
    "",
    input.text.slice(0, STATE_TEXT_CHARS),
  ].join("\n");
}

export function buildDsnQuestion(): JudgmentChoiceQuestion {
  return {
    type: "choice",
    instructions:
      "This is a delivery failure notice (a bounce message) that a mail server sent back about an email we sent to one recipient. " +
      "Decide what the failure says about the RECIPIENT'S ADDRESS. " +
      "Judge on the human-readable words of the remote server's answer, not on the numeric status code: servers often send codes like 550 5.1.1 or 552 5.2.0 when they actually blocked the message as spam. " +
      "Answer address_dead ONLY when the words say the address itself cannot receive mail (it does not exist, the account was deleted or disabled, the domain does not exist or has no mail server). " +
      "Whenever the words say the server refused this message or the sender (spam, content, policy, blocklist, reputation, authentication, rate limits, any other reason), or do not say the address is gone, answer message_refused.",
    criteria: {
      [DSN_ADDRESS_DEAD]: {
        what: "The recipient address does not exist or its mailbox is gone: nobody can ever receive mail there.",
        examples: [
          "550 5.1.1 The email account that you tried to reach does not exist",
          "Address not found: your message wasn't delivered because the address couldn't be found",
          "User unknown / No such user / Recipient address rejected: user does not exist",
          "The account has been disabled or deleted",
          "The domain couldn't be found / has no mail server (DNS error, no MX)",
          "550 5.4.1 Recipient address rejected: Access denied (Microsoft 365: the recipient is not in the organization's directory)",
          "The recipient is no longer working here / this mailbox is no longer monitored and was closed",
        ],
      },
      [DSN_MESSAGE_REFUSED]: {
        what: "The server refused this message or our sender; the recipient address itself may well be fine.",
        examples: [
          "Message blocked: rejected due to content judged to be spam",
          "Our system has detected that this message is likely unsolicited mail",
          "Sender IP or domain is listed on a blocklist (Spamhaus, SURBL, Barracuda...)",
          "Message rejected due to policy / sender not authorized / DMARC or SPF failure",
          "Mailbox full or over quota, message too large, too many messages",
          "Recipient only accepts mail from known senders or the organization",
        ],
      },
    },
  };
}

/** Read the judgment. An option outside the two is a bug, never a default. */
export function readDsnJudgment(
  answer: JudgmentChoiceAnswer,
  model: string,
  judgedAt: Date,
): DsnJudgment {
  if (answer.choice !== DSN_ADDRESS_DEAD && answer.choice !== DSN_MESSAGE_REFUSED) {
    throw new Error(`dsn judgment answered an unknown option "${answer.choice}"`);
  }
  return {
    verdict: answer.choice,
    questionVersion: DSN_QUESTION_VERSION,
    judgedAt: judgedAt.toISOString(),
    model,
    probability: answer.probabilities[answer.choice] ?? 0,
    confidence: answer.confidence,
    probabilities: answer.probabilities,
  };
}

/** One stored permanent DSN tied to one of our sends. */
export interface JudgedDsnRow {
  id: string;
  instantlyCampaignId: string;
  step: number | null;
  verdict: DsnVerdict;
}

/** One `email_bounced` silver event on a campaign that has DSN rows. */
export interface BounceEventRef {
  id: string;
  campaignId: string;
  leadEmail: string | null;
  step: number | null;
  sourceRowId: string | null;
  source: string;
}

export interface DsnSettlementPlan {
  /** Bounce events a refusal produced: deleted (archived first). */
  retract: BounceEventRef[];
  /** Dead-address DSN rows no bounce event stands on yet: promoted. */
  promote: JudgedDsnRow[];
}

const sendKey = (campaignId: string, step: number | null) => `${campaignId}|${step ?? ""}`;

/**
 * Pure: what each judged DSN changes.
 *
 * Grain = one SEND (campaign + step): every DSN about that email and every
 * `email_bounced` event on it describe the same failure. A send is a refusal
 * when every DSN about it was judged `message_refused`; then EVERY bounce event
 * on it goes, whoever wrote it (the DSN poll, or Instantly's webhook which
 * reported the same refusal as a bounce: 113 such events in prod, 2026-10-18).
 * One DSN saying the address is dead keeps the send a bounce.
 *
 * Only sends for which every DSN is judged are decided; an unjudged DSN on a
 * send leaves it untouched until it is.
 */
export function planDsnSettlement(
  rows: readonly JudgedDsnRow[],
  unjudgedSendKeys: ReadonlySet<string>,
  events: readonly BounceEventRef[],
): DsnSettlementPlan {
  const verdictsBySend = new Map<string, DsnVerdict[]>();
  for (const row of rows) {
    const key = sendKey(row.instantlyCampaignId, row.step);
    const list = verdictsBySend.get(key) ?? [];
    list.push(row.verdict);
    verdictsBySend.set(key, list);
  }

  const eventsBySend = new Map<string, BounceEventRef[]>();
  for (const event of events) {
    const key = sendKey(event.campaignId, event.step);
    const list = eventsBySend.get(key) ?? [];
    list.push(event);
    eventsBySend.set(key, list);
  }

  const plan: DsnSettlementPlan = { retract: [], promote: [] };
  for (const [key, verdicts] of verdictsBySend) {
    if (unjudgedSendKeys.has(key)) continue;
    const sendEvents = eventsBySend.get(key) ?? [];
    if (verdicts.every((v) => v === DSN_MESSAGE_REFUSED)) {
      plan.retract.push(...sendEvents);
    }
  }

  for (const row of rows) {
    if (row.verdict !== DSN_ADDRESS_DEAD) continue;
    const key = sendKey(row.instantlyCampaignId, row.step);
    if ((eventsBySend.get(key) ?? []).length > 0) continue;
    // One promotion per send: silver dedups anyway, the plan should not ask twice.
    if (plan.promote.some((p) => sendKey(p.instantlyCampaignId, p.step) === key)) continue;
    plan.promote.push(row);
  }

  return plan;
}

export { sendKey as dsnSendKey };
