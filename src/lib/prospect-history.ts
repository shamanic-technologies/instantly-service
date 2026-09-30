/**
 * The WHOLE story of one prospect, as the agency inbox reads it.
 *
 * Every email that hands a prospect's thread to a person — the positive-reply
 * forward, the "Needs you" escalation, the off-topic / referral escalation —
 * renders this. The reader must be able to answer, or forward to the client,
 * without opening any other tool, so the email carries, oldest first:
 *
 *   - every email we sent them (not only their reply — a reply does NOT always
 *     quote what it answers: jakub@marktize.com's "can you explain?" arrived
 *     with one quoted message out of three sent),
 *   - every reply they wrote,
 *   - every action they took: a website visit (with the page when we know it),
 *     a bounce, an unsubscribe — each dated, placed among the messages.
 *
 * ⚠️ NO OPENS. Open rates are deprecated product-wide; an open is never listed.
 *
 * ⚠️ A CLICK'S PAGE IS NEVER A PROVIDER REDIRECT. On our own transport the page
 * is OBSERVED (the signed destination our `/c/` redirect recorded). On
 * Instantly's the webhook carries no URL at all, so the page is DEDUCED from
 * the body we sent at that step, and only when that body holds exactly one
 * link — several links means there is nowhere honest to point, and the line
 * says a link was clicked without naming one.
 *
 * ⚠️ A PART WE COULD NOT READ IS SAID, NEVER DROPPED — and never fatal. The
 * notification is the deliverable, so every read here degrades to a note at
 * the top of the history rather than throwing.
 *
 * Messages come from `fetchLeadConversation`, the same whole-campaign read the
 * lead panel uses (every stored row of the campaign, mirror first, our own
 * dispatched answers included). Actions come from our own silver event log.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import {
  formatThreadDate,
  selectThreadMessages,
  type ForwardPositiveReplyCampaign,
  type ThreadMessage,
} from "./forward-positive-reply";
import { fetchLeadConversation } from "./lead-conversation";
import { resolveInstantlyApiKey } from "./key-client";
import { listEmails } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { fetchSelfSendThread } from "./self-send/thread";
import { subjectForStep } from "./self-send/message";

const NIL_USER_ID = "00000000-0000-0000-0000-000000000000";

/** What the prospect did, as opposed to what they wrote. Opens are deliberately absent. */
export type ProspectActionKind = "click" | "bounce" | "unsubscribe";

export interface ProspectAction {
  kind: ProspectActionKind;
  /** ISO 8601 UTC. */
  at: string;
  /** The sequence step the action is about, when known. */
  step: number | null;
  /** The page a click led to. Null when it cannot be stated without guessing. */
  page: string | null;
}

export type HistoryItem =
  | { type: "message"; message: ThreadMessage }
  | { type: "action"; action: ProspectAction };

export interface ProspectHistory {
  items: HistoryItem[];
  /** The messages alone, oldest first (the subject is read off them). */
  messages: ThreadMessage[];
  /** What could not be read, stated in the email. Empty when everything was. */
  notes: string[];
}

/** The silver events that are actions a prospect took. `email_opened` is NOT one. */
const ACTION_EVENT_TYPES: Record<string, ProspectActionKind> = {
  email_link_clicked: "click",
  email_bounced: "bounce",
  lead_unsubscribed: "unsubscribe",
};

/** Two clicks on the same page of the same email this close together are one visit. */
const DUPLICATE_CLICK_WINDOW_MS = 5 * 60 * 1000;

/** Hosts that only ever serve a provider's click-tracking redirect. */
const TRACKING_HOST = /(^|\.)(itrackly\.com|instantly\.ai)$/i;

/**
 * A URL the reader can follow: http(s), not a tracking redirect, not our own
 * opt-out or click redirect, with our own UTM tagging removed (it says nothing
 * about the page). Null when the URL is not one of those.
 */
export function readablePage(raw: string): string | null {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return null;
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") return null;
  if (TRACKING_HOST.test(url.hostname)) return null;
  const own = process.env.SELF_SEND_PUBLIC_URL;
  if (own) {
    try {
      if (new URL(own).host === url.host) return null;
    } catch {
      // An unparseable origin simply excludes nothing.
    }
  }
  for (const key of [...url.searchParams.keys()]) {
    if (key.toLowerCase().startsWith("utm_")) url.searchParams.delete(key);
  }
  return url.toString();
}

/**
 * The single page a sent email pointed at, or null when it pointed at none or
 * at several (a click on it then cannot be placed without guessing).
 */
export function soleLinkIn(bodyHtml: string | null | undefined): string | null {
  if (!bodyHtml) return null;
  const found = bodyHtml.match(/https?:\/\/[^\s"'<>]+/gi) ?? [];
  const pages = new Set<string>();
  for (const raw of found) {
    const page = readablePage(raw.replace(/[.,;:!?)\]]+$/, "").replace(/&amp;/g, "&"));
    if (page) pages.add(page);
  }
  return pages.size === 1 ? [...pages][0] : null;
}

/** One silver action row, with whatever it takes to name the page of a click. */
export interface ActionRow {
  eventType: string;
  at: string;
  step: number | null;
  /** The URL our `/c/` redirect recorded — present only for a self-send click. */
  observedUrl: string | null;
  /** The body we sent at that step — the fallback for a click Instantly reported. */
  sentBodyHtml: string | null;
}

/** Pure: rows → dated actions, opens excluded, duplicate clicks collapsed, oldest first. */
export function buildProspectActions(rows: ActionRow[]): ProspectAction[] {
  const actions: ProspectAction[] = [];
  const sorted = rows
    .filter((r) => ACTION_EVENT_TYPES[r.eventType] !== undefined)
    .slice()
    .sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
  for (const row of sorted) {
    const kind = ACTION_EVENT_TYPES[row.eventType];
    const page =
      kind === "click"
        ? (row.observedUrl ? readablePage(row.observedUrl) : null) ?? soleLinkIn(row.sentBodyHtml)
        : null;
    const previous = actions[actions.length - 1];
    if (
      kind === "click" &&
      previous?.kind === "click" &&
      previous.step === row.step &&
      previous.page === page &&
      Date.parse(row.at) - Date.parse(previous.at) < DUPLICATE_CLICK_WINDOW_MS
    ) {
      continue;
    }
    actions.push({ kind, at: row.at, step: row.step, page });
  }
  return actions;
}

/**
 * Pure: a follow-up we sent is stored with no subject of its own (the sender
 * derives it at dispatch as `Re: <first subject>`), so it would render as a
 * blank `Subject:` line. Fill it the way the sender did, from the thread's
 * latest known subject. A message that carries its own subject is untouched.
 */
export function fillThreadSubjects(messages: ThreadMessage[]): ThreadMessage[] {
  let known: string | null = null;
  return messages.map((m) => {
    const own = m.subject?.trim();
    if (own && own !== "(no subject)") {
      known = own;
      return m;
    }
    return known ? { ...m, subject: subjectForStep(known, 2) } : m;
  });
}

/** Pure: interleave messages and actions by date. An undated entry sorts last, never dropped. */
export function mergeHistory(
  messages: ThreadMessage[],
  actions: ProspectAction[],
): HistoryItem[] {
  const entries = [
    ...messages.map((message, i) => ({
      i,
      time: Date.parse(message.date),
      item: { type: "message", message } as HistoryItem,
    })),
    ...actions.map((action, i) => ({
      i: messages.length + i,
      time: Date.parse(action.at),
      item: { type: "action", action } as HistoryItem,
    })),
  ];
  entries.sort((a, b) => {
    const aDated = Number.isFinite(a.time);
    const bDated = Number.isFinite(b.time);
    if (aDated && bDated && a.time !== b.time) return a.time - b.time;
    if (aDated !== bDated) return aDated ? -1 : 1;
    return a.i - b.i;
  });
  return entries.map((e) => e.item);
}

function actionLine(action: ProspectAction, leadEmail: string): string {
  const who = leadEmail || "The prospect";
  const email = action.step ? `email ${action.step}` : "an email";
  switch (action.kind) {
    case "click":
      return action.page
        ? `${who} visited ${action.page} (clicked the link in ${email})`
        : `${who} clicked a link in ${email} (page not recorded)`;
    case "bounce":
      return `${capitalize(email)} to ${who} bounced (not delivered)`;
    case "unsubscribe":
      return `${who} unsubscribed`;
  }
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

const SEPARATOR = "\n\n─────────────────────────────────────────\n\n";

/**
 * Render the history as plain text for the template's `<pre>`: clean and
 * client-forwardable — each email a standard From/To/Date/Subject block with
 * its body, each action one dated line, notes about unreadable parts first.
 */
export function renderProspectHistory(
  history: Pick<ProspectHistory, "items" | "notes">,
  leadEmail: string,
): string {
  const blocks = history.items.map((item) =>
    item.type === "message"
      ? [
          `From: ${item.message.from}`,
          `To: ${item.message.to}`,
          `Date: ${formatThreadDate(item.message.date)}`,
          `Subject: ${item.message.subject}`,
          ``,
          item.message.bodyText,
        ].join("\n")
      : [`Date: ${formatThreadDate(item.action.at)}`, actionLine(item.action, leadEmail)].join("\n"),
  );
  const notes = history.notes.map((n) => `Note: ${n}`).join("\n");
  const body = blocks.length > 0 ? blocks.join(SEPARATOR) : "(conversation unavailable)";
  return notes ? `${notes}${SEPARATOR}${body}` : body;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

function toIso(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  const s = String(value ?? "");
  // A naive `timestamp` column comes back without a zone; it is UTC.
  return /[zZ]|[+-]\d\d:?\d\d$/.test(s) ? s : `${s.replace(" ", "T")}Z`;
}

/** The prospect's actions across the given sequences, from our own silver log. */
async function loadActionRows(
  instantlyCampaignIds: string[],
  leadEmail: string,
): Promise<ActionRow[]> {
  const result = await db.execute(sql`
    SELECT e.event_type, e.timestamp, e.step,
           t.payload->>'url' AS observed_url,
           s.body_html AS sent_body_html
    FROM instantly_events e
    LEFT JOIN tracking_hits_raw t
      ON e.source = 'self_send' AND t.id = e.source_row_id AND t.kind = 'click'
    LEFT JOIN sequence_steps s
      ON s.instantly_campaign_id = e.campaign_id AND s.step = e.step
    WHERE e.campaign_id = ANY(${sql.param(instantlyCampaignIds)}::text[])
      AND lower(e.lead_email) = lower(${leadEmail})
      AND e.inferred = false
      AND e.withdrawn_at IS NULL
      AND e.event_type IN ('email_link_clicked', 'email_bounced', 'lead_unsubscribed')
    ORDER BY e.timestamp
  `);
  return rowsOf(result).map((r) => ({
    eventType: String(r.event_type),
    at: toIso(r.timestamp),
    step: r.step === null || r.step === undefined ? null : Number(r.step),
    observedUrl: (r.observed_url as string | null) ?? null,
    sentBodyHtml: (r.sent_body_html as string | null) ?? null,
  }));
}

/** One sequence's thread — the pre-family read, kept as the fallback. */
async function loadSingleSequenceThread(
  campaign: ForwardPositiveReplyCampaign,
): Promise<ThreadMessage[]> {
  if (isSelfSendCampaignId(campaign.instantlyCampaignId)) {
    return fetchSelfSendThread(campaign.instantlyCampaignId);
  }
  const { key } = await resolveInstantlyApiKey(campaign.orgId!, "system", {
    method: "POST",
    path: "/internal/forward-positive-reply",
  });
  return selectThreadMessages(await listEmails(key, { campaignId: campaign.instantlyCampaignId }));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Read everything that happened with this prospect. Never throws: a part that
 * cannot be read becomes a note, because the email is the deliverable.
 */
export async function loadProspectHistory(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
): Promise<ProspectHistory> {
  const notes: string[] = [];
  let messages: ThreadMessage[] | null = null;
  const sequenceIds = new Set<string>([campaign.instantlyCampaignId]);

  const conversationCampaignId = campaign.conversationCampaignId ?? campaign.campaignId;
  if (conversationCampaignId && campaign.orgId) {
    try {
      const conversation = await fetchLeadConversation({
        orgId: campaign.orgId,
        userId: campaign.userId || NIL_USER_ID,
        campaignId: conversationCampaignId,
        leadEmail,
      });
      messages = conversation.messages.map((m) => ({
        direction: m.direction,
        from: m.from,
        to: m.to,
        date: m.at,
        subject: m.subject,
        bodyText: m.text,
      }));
      for (const s of conversation.sequences) sequenceIds.add(s.instantlyCampaignId);
    } catch (error) {
      console.warn(
        `[instantly-service] prospect-history: whole-campaign read failed for campaign=${conversationCampaignId} lead=${leadEmail}, falling back to this sequence — ${errorMessage(error)}`,
      );
    }
  }

  if (messages === null) {
    try {
      messages = await loadSingleSequenceThread(campaign);
      if (conversationCampaignId) {
        notes.push(
          "emails sent to this prospect under earlier versions of this campaign could not be read; only this sequence is shown.",
        );
      }
    } catch (error) {
      console.warn(
        `[instantly-service] prospect-history: thread read failed for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${errorMessage(error)}`,
      );
      messages = [];
      notes.push("the emails exchanged with this prospect could not be read.");
    }
  }

  let actions: ProspectAction[] = [];
  try {
    actions = buildProspectActions(await loadActionRows([...sequenceIds], leadEmail));
  } catch (error) {
    console.warn(
      `[instantly-service] prospect-history: action read failed for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${errorMessage(error)}`,
    );
    notes.push("this prospect's website visits, bounces and unsubscribes could not be read.");
  }

  messages = fillThreadSubjects(messages);
  return { items: mergeHistory(messages, actions), messages, notes };
}

/**
 * The history, WAITED ON until it holds the prospect's reply.
 *
 * ⚠️ A REPLY IS ANNOUNCED BEFORE IT IS READABLE. Instantly qualifies a reply
 * (the `lead_interested` webhook) the moment it lands, and our copy of its words
 * is written by the mirror side effect that runs AFTER; the whole-campaign read
 * serves the mirror first. So a history read at the moment of the announcement
 * held our three outbound emails and not the reply that triggered it, and two
 * emails went out one minute after michael@thekarlfeldtcenter.com wrote, both
 * without his words (2026-09-29).
 *
 * So: read; when no inbound message is there, re-mirror the Instantly thread
 * and read again after each wait in `waitsMs`. Bounded — a reply that is still
 * unreadable after the last wait is STATED as a note at the top of the history,
 * never silently dropped, and the caller decides whether to send without it.
 */
export interface HistoryWithReply {
  history: ProspectHistory;
  /** The prospect's latest inbound message, verbatim. Null when unreadable. */
  latestReply: ThreadMessage | null;
}

/** Background paths (the celebration) can afford to wait a few minutes. */
export const REPLY_WAIT_BACKGROUND_MS = [2_000, 15_000, 30_000, 60_000, 120_000];
/** Request paths (an escalation, a webhook side effect) wait seconds, not minutes. */
export const REPLY_WAIT_SHORT_MS = [2_000, 5_000, 10_000];

export const REPLY_UNREADABLE_NOTE =
  "the prospect's latest reply could not be read when this email was sent, so it is missing below. Nothing here is a summary of it.";

/** Pure: the prospect's latest inbound message in the history, or null. */
export function latestInboundMessage(messages: ThreadMessage[]): ThreadMessage | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].direction === "inbound") return messages[i];
  }
  return null;
}

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export async function loadHistoryWithLatestReply(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  options: { waitsMs?: number[]; sleep?: (ms: number) => Promise<void> } = {},
): Promise<HistoryWithReply> {
  const waits = options.waitsMs ?? REPLY_WAIT_SHORT_MS;
  const sleep = options.sleep ?? defaultSleep;

  let history = await loadProspectHistory(campaign, leadEmail);
  let latestReply = latestInboundMessage(history.messages);

  for (const wait of waits) {
    if (latestReply) break;
    // Copy the thread into bronze again: the mirror is what the read serves,
    // and it is the step that had not run yet when the reply was announced.
    if (campaign.orgId && !isSelfSendCampaignId(campaign.instantlyCampaignId)) {
      try {
        const { mirrorCampaignEmails, isInstantlyHeldCampaignId } = await import("./mirror-emails");
        if (isInstantlyHeldCampaignId(campaign.instantlyCampaignId)) {
          await mirrorCampaignEmails({
            instantlyCampaignId: campaign.instantlyCampaignId,
            orgId: campaign.orgId,
            userId: campaign.userId,
          });
        }
      } catch (error) {
        console.warn(
          `[instantly-service] prospect-history: re-mirror failed for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${errorMessage(error)}`,
        );
      }
    }
    await sleep(wait);
    history = await loadProspectHistory(campaign, leadEmail);
    latestReply = latestInboundMessage(history.messages);
  }

  if (!latestReply) {
    console.error(
      `[instantly-service] prospect-history: no reply from ${leadEmail} readable on campaign=${campaign.instantlyCampaignId} after ${waits.length} wait(s); the email says so`,
    );
    history = { ...history, notes: [REPLY_UNREADABLE_NOTE, ...history.notes] };
  }
  return { history, latestReply };
}
