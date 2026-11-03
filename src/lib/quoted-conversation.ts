/**
 * QUOTE-THREAD-1103: every reply we send to a lead quotes the conversation
 * below the signature, the way any mail client does.
 *
 * Doc Dinners, 2026-11-03: our AI answered drfastiggi@wiltonchiro.com with only
 * the new words and the signature. The copy mirrored from the agency inbox in
 * Cc then held a thread signed "Distribute.you | Marketing Agency" with no
 * trace of Doc Dinners, and the people classifier filed him as OUR lead. The
 * hand-over and the not-a-prospect answer already quoted the conversation; the
 * ordinary reply did not. Now `replyToLead` quotes it by default, for every
 * caller that does not bring its own quote (those two keep theirs, unchanged).
 *
 * The conversation is the same read the hand-over quotes (lib/prospect-history:
 * the lead's whole campaign, the cold email included), rendered by the same
 * `renderQuotedHistory`. Each message is quoted with its OWN words only: a
 * reply's body already carries the mail client's quote of what came before,
 * and that message is quoted on its own just below, so keeping both would
 * repeat the thread once per message.
 */

import { renderQuotedHistory } from "./escalate-reply";
import type { ThreadMessage } from "./forward-positive-reply";
import { loadProspectHistory } from "./prospect-history";
import { stripQuotedHistory } from "./self-send/qualify-reply";

/** The thread a reply answers: the lead's sequence row and the campaign it belongs to. */
export interface QuotedThread {
  orgId: string;
  userId: string;
  campaignId: string;
  instantlyCampaignId: string;
  brandId: string | null;
  leadEmail: string;
}

/** Pure: each message reduced to its own words (its quoted tail dropped). */
export function ownWords(messages: ThreadMessage[]): ThreadMessage[] {
  return messages.map((m) => {
    const own = stripQuotedHistory(m.bodyText).trim();
    return own ? { ...m, bodyText: own } : m;
  });
}

/**
 * The quote of the conversation to put under a reply, or "" when it holds no
 * message. `loadProspectHistory` never throws (a part it cannot read becomes a
 * note): the answer is the deliverable, so an unreadable thread is logged and
 * the reply leaves without its quote rather than keeping the prospect waiting.
 */
export async function loadQuotedConversation(thread: QuotedThread): Promise<string> {
  const history = await loadProspectHistory(
    {
      instantlyCampaignId: thread.instantlyCampaignId,
      campaignId: null,
      orgId: thread.orgId,
      userId: thread.userId,
      runId: null,
      brandIds: thread.brandId ? [thread.brandId] : null,
      conversationCampaignId: thread.campaignId,
    },
    thread.leadEmail,
  );
  if (history.notes.length > 0) {
    console.warn(
      `[instantly-service] quoted-conversation: partial read for campaign=${thread.instantlyCampaignId} lead=${thread.leadEmail} — ${history.notes.join(" ")}`,
    );
  }
  return renderQuotedHistory(ownWords(history.messages));
}
